package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// The control-plane record is what makes a leader's writes safe: both stores
// must hand out strictly increasing epochs and refuse a check made under any
// other one.
func testControlPlane(t *testing.T, s Store) {
	ctx := context.Background()

	fresh, err := s.GetControlPlane(ctx)
	noErr(t, err)
	if fresh.Epoch != 0 || fresh.Leader != "" || !fresh.ElectedAt.Equal(time.Unix(0, 0)) {
		t.Fatalf("a new database starts at epoch 0 with nobody elected: %+v", fresh)
	}
	noErr(t, s.CheckEpoch(ctx, 0))
	wantErr(t, s.CheckEpoch(ctx, 1), ErrStaleLeader)

	first, prev, err := s.BeginTerm(ctx, "manager-a", testTime)
	noErr(t, err)
	if first.Epoch != 1 || first.Leader != "manager-a" || !first.ElectedAt.Equal(testTime) || prev != fresh {
		t.Fatalf("first term = %+v (previous %+v)", first, prev)
	}
	second, prev, err := s.BeginTerm(ctx, "manager-b", testTime.Add(time.Minute))
	noErr(t, err)
	if second.Epoch != 2 || second.Leader != "manager-b" || prev.Epoch != 1 || prev.Leader != "manager-a" || !prev.ElectedAt.Equal(testTime) {
		t.Fatalf("second term = %+v (previous %+v)", second, prev)
	}
	// The same manager beginning again is a new term too: its old epoch is no longer current.
	third, _, err := s.BeginTerm(ctx, "manager-b", testTime.Add(2*time.Minute))
	noErr(t, err)
	if third.Epoch != 3 {
		t.Fatalf("third term = %+v", third)
	}

	// Only the current epoch passes.
	noErr(t, s.CheckEpoch(ctx, 3))
	for _, stale := range []int64{0, 1, 2, 4, -1} {
		err := s.CheckEpoch(ctx, stale)
		wantErr(t, err, ErrStaleLeader)
	}
	if err := s.CheckEpoch(ctx, 1); err == nil || !errors.Is(err, ErrStaleLeader) {
		t.Fatalf("error = %v", err)
	}

	got, err := s.GetControlPlane(ctx)
	noErr(t, err)
	if got != third {
		t.Fatalf("GetControlPlane = %+v, want %+v", got, third)
	}

	// A leader must be a name the store can keep.
	for _, bad := range []string{"", "has space", "-leading", "a/b"} {
		if _, _, err := s.BeginTerm(ctx, bad, testTime); !errors.Is(err, ErrInvalid) {
			t.Errorf("BeginTerm(%q) = %v, want ErrInvalid", bad, err)
		}
	}
	if got, _ := s.GetControlPlane(ctx); got.Epoch != 3 {
		t.Errorf("a refused term changed the epoch to %d", got.Epoch)
	}
}

// A term begun inside a transaction that fails is not begun, and one checked
// inside a transaction is checked against that transaction's view.
func testControlPlaneInTransactions(t *testing.T, s Store) {
	ctx := context.Background()
	boom := errors.New("boom")

	err := s.InTx(ctx, func(tx Tx) error {
		if _, _, err := tx.BeginTerm(ctx, "manager-a", testTime); err != nil {
			return err
		}
		if _, err := tx.AppendEvent(ctx, Event{At: testTime, Actor: "manager-a", Action: "leader.elected", ResourceType: "control-plane", ResourceID: "1"}); err != nil {
			return err
		}
		return boom
	})
	wantErr(t, err, boom)
	if got, _ := s.GetControlPlane(ctx); got.Epoch != 0 {
		t.Fatalf("a term begun in a transaction that rolled back survived: %+v", got)
	}
	if events, _ := s.ListEvents(ctx, EventFilter{}); len(events) != 0 {
		t.Fatalf("the event of a rolled-back term survived: %+v", events)
	}

	// Begun and recorded together, then used.
	err = s.InTx(ctx, func(tx Tx) error {
		cur, _, err := tx.BeginTerm(ctx, "manager-a", testTime)
		if err != nil {
			return err
		}
		if err := tx.CheckEpoch(ctx, cur.Epoch); err != nil {
			return err
		}
		return nil
	})
	noErr(t, err)

	// A transaction that acts under an epoch that has been replaced is refused,
	// and the writes it made before the check are rolled back with it.
	_, _, err = s.BeginTerm(ctx, "manager-b", testTime)
	noErr(t, err)
	err = s.InTx(ctx, func(tx Tx) error {
		if err := tx.CheckEpoch(ctx, 1); err != nil {
			return err
		}
		_, err := tx.UpsertNode(ctx, Node{ID: "n-stale", Name: "n-stale", CPUMillis: 1000, MemoryBytes: 1 << 30, RegisteredAt: testTime, LastHeartbeat: testTime})
		return err
	})
	wantErr(t, err, ErrStaleLeader)
	if _, err := s.GetNode(ctx, "n-stale"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a stale leader's write got through: %v", err)
	}
}

// Of two managers that begin a term at the same time, each gets an epoch of its
// own, the epochs are consecutive, and only the higher one is current. This is
// the property the whole fencing rests on, so it is tried many times at once.
func testBeginTermsConcurrently(t *testing.T, s Store) {
	ctx := context.Background()
	const managers, rounds = 6, 5
	var mu sync.Mutex
	seen := map[int64]string{}
	var wg sync.WaitGroup
	for m := 0; m < managers; m++ {
		id := string(rune('a'+m)) + "-manager"
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				cur, prev, err := s.BeginTerm(ctx, id, testTime)
				if err != nil {
					t.Errorf("BeginTerm: %v", err)
					return
				}
				mu.Lock()
				if other, dup := seen[cur.Epoch]; dup {
					t.Errorf("epoch %d was given to both %s and %s", cur.Epoch, other, id)
				}
				seen[cur.Epoch] = id
				mu.Unlock()
				if prev.Epoch != cur.Epoch-1 {
					t.Errorf("term %d replaced epoch %d, not %d", cur.Epoch, prev.Epoch, cur.Epoch-1)
				}
			}
		}()
	}
	wg.Wait()
	const total = managers * rounds
	if len(seen) != total {
		t.Fatalf("%d distinct epochs for %d terms", len(seen), total)
	}
	for e := int64(1); e <= total; e++ {
		if _, ok := seen[e]; !ok {
			t.Fatalf("epoch %d was never handed out: the epochs must have no gaps", e)
		}
	}
	noErr(t, s.CheckEpoch(ctx, total))
	wantErr(t, s.CheckEpoch(ctx, total-1), ErrStaleLeader)
}

// A transaction that has checked the epoch holds it in place: the next term
// cannot begin until the transaction is over. So what a leader writes under its
// epoch lands before its successor's term, never after it; and a transaction
// that starts after the new term is refused.
func testNextTermWaitsForTransactionsThatCheckedTheEpoch(t *testing.T, s Store) {
	ctx := context.Background()
	_, _, err := s.BeginTerm(ctx, "manager-a", testTime)
	noErr(t, err)

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	letGo := func() { releaseOnce.Do(func() { close(release) }) }
	defer letGo()
	finished := make(chan error, 1)
	go func() {
		finished <- s.InTx(ctx, func(tx Tx) error {
			if err := tx.CheckEpoch(ctx, 1); err != nil {
				return err
			}
			close(entered)
			<-release
			_, err := tx.UpsertNode(ctx, Node{ID: "written-under-1", Name: "n", CPUMillis: 1000, MemoryBytes: 1 << 30, RegisteredAt: testTime, LastHeartbeat: testTime})
			return err
		})
	}()
	select {
	case <-entered:
	case err := <-finished:
		t.Fatalf("the first transaction ended before it checked the epoch: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the first transaction did not start")
	}

	type begun struct {
		term ControlPlane
		err  error
	}
	second := make(chan begun, 1)
	go func() {
		cur, _, err := s.BeginTerm(ctx, "manager-b", testTime.Add(time.Minute))
		second <- begun{cur, err}
	}()
	select {
	case b := <-second:
		t.Fatalf("a term began (%+v, %v) while a transaction that checked the previous epoch was open", b.term, b.err)
	case <-time.After(300 * time.Millisecond): // it is waiting, as it must
	}

	letGo()
	noErr(t, <-finished)
	select {
	case b := <-second:
		noErr(t, b.err)
		if b.term.Epoch != 2 || b.term.Leader != "manager-b" {
			t.Fatalf("the second term = %+v", b.term)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the second term never began after the first transaction ended")
	}
	if _, err := s.GetNode(ctx, "written-under-1"); err != nil {
		t.Fatalf("what the first leader wrote under its epoch is lost: %v", err)
	}

	// A transaction that starts now is refused under the old epoch.
	err = s.InTx(ctx, func(tx Tx) error { return tx.CheckEpoch(ctx, 1) })
	wantErr(t, err, ErrStaleLeader)
}
