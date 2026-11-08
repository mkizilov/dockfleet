package election

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// These tests run electors against a backend whose leases expire on a manual
// clock, so that every transition happens when the test says and not when a
// timer does.

var start = time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

const ttl = 10 * time.Second

type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// fakeBackend is a lease store with one key.
type fakeBackend struct {
	clock *manualClock

	mu       sync.Mutex
	next     int
	leases   map[string]time.Time
	owner    string
	ownerRev int64
	keyLease string
	revision int64
	fail     map[string]error
	calls    map[string]int
}

func newBackend(c *manualClock) *fakeBackend {
	return &fakeBackend{clock: c, leases: map[string]time.Time{}, fail: map[string]error{}, calls: map[string]int{}}
}

// expire drops the leases that have run out, and the key with them.
func (b *fakeBackend) expire() {
	for id, until := range b.leases {
		if !b.clock.Now().Before(until) {
			delete(b.leases, id)
			if b.keyLease == id {
				b.owner, b.keyLease = "", ""
				b.revision++
			}
		}
	}
}

func (b *fakeBackend) enter(op string) error {
	b.calls[op]++
	if err := b.fail[op]; err != nil {
		return err
	}
	b.expire()
	return nil
}

func (b *fakeBackend) Grant(_ context.Context, d time.Duration) (Lease, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.enter("Grant"); err != nil {
		return Lease{}, err
	}
	b.next++
	id := fmt.Sprintf("lease-%d", b.next)
	b.leases[id] = b.clock.Now().Add(d)
	return Lease{ID: id, TTL: d}, nil
}

func (b *fakeBackend) KeepAlive(_ context.Context, l Lease) (time.Duration, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.enter("KeepAlive"); err != nil {
		return 0, err
	}
	if _, ok := b.leases[l.ID]; !ok {
		return 0, ErrLeaseExpired
	}
	b.leases[l.ID] = b.clock.Now().Add(l.TTL)
	return l.TTL, nil
}

func (b *fakeBackend) Revoke(_ context.Context, l Lease) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.enter("Revoke"); err != nil {
		return err
	}
	delete(b.leases, l.ID)
	if b.keyLease == l.ID {
		b.owner, b.keyLease = "", ""
		b.revision++
	}
	return nil
}

func (b *fakeBackend) Claim(_ context.Context, _ string, id string, l Lease) (Claim, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.enter("Claim"); err != nil {
		return Claim{}, err
	}
	if _, ok := b.leases[l.ID]; !ok {
		return Claim{}, ErrLeaseExpired
	}
	if b.owner != "" {
		return Claim{Holder: Holder{ID: b.owner, Revision: b.ownerRev}}, nil
	}
	b.revision++
	b.owner, b.ownerRev, b.keyLease = id, b.revision, l.ID
	return Claim{Won: true, Holder: Holder{ID: id, Revision: b.revision}}, nil
}

func (b *fakeBackend) Observe(_ context.Context, _ string) (Holder, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.enter("Observe"); err != nil {
		return Holder{}, false, err
	}
	return Holder{ID: b.owner, Revision: b.ownerRev}, b.owner != "", nil
}

func (b *fakeBackend) failing(op string, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err == nil {
		delete(b.fail, op)
		return
	}
	b.fail[op] = err
}

func (b *fakeBackend) holder() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.expire()
	return b.owner
}

// database stands for the shared database that hands out epochs.
type database struct {
	mu      sync.Mutex
	epoch   int64
	leader  string
	begins  int
	failing error
}

func (d *database) begin(id string) func(context.Context, int64) (Term, error) {
	return func(_ context.Context, _ int64) (Term, error) {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.begins++
		if d.failing != nil {
			return Term{}, d.failing
		}
		prev, prevEpoch := d.leader, d.epoch
		d.epoch++
		d.leader = id
		return Term{Epoch: d.epoch, Previous: prev, PreviousEpoch: prevEpoch}, nil
	}
}

// dutyLog records what a manager's duties did.
type dutyLog struct {
	mu      sync.Mutex
	running int
	peak    int
	started []Term
	stopped []int64
}

func (d *dutyLog) duty(ctx context.Context, term Term) {
	d.mu.Lock()
	d.running++
	d.peak = max(d.peak, d.running)
	d.started = append(d.started, term)
	d.mu.Unlock()
	<-ctx.Done()
	d.mu.Lock()
	d.running--
	d.stopped = append(d.stopped, term.Epoch)
	d.mu.Unlock()
}

func (d *dutyLog) snapshot() (running, peak int, started []Term, stopped []int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.running, d.peak, append([]Term(nil), d.started...), append([]int64(nil), d.stopped...)
}

type manager struct {
	*Elector
	duties  *dutyLog
	changes *changeLog
}

type changeLog struct {
	mu   sync.Mutex
	list []Change
}

func (c *changeLog) add(ch Change) {
	c.mu.Lock()
	c.list = append(c.list, ch)
	c.mu.Unlock()
}

func (c *changeLog) all() []Change {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Change(nil), c.list...)
}

type world struct {
	t     *testing.T
	clock *manualClock
	back  *fakeBackend
	db    *database
	ctx   context.Context
}

func newWorld(t *testing.T) *world {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := &manualClock{t: start}
	return &world{t: t, clock: c, back: newBackend(c), db: &database{}, ctx: ctx}
}

func (w *world) manager(id string, tweak ...func(*Config)) *manager {
	w.t.Helper()
	m := &manager{duties: &dutyLog{}, changes: &changeLog{}}
	cfg := Config{
		Backend: w.back, ID: id, Key: "/dockfleet/leader", TTL: ttl, Begin: w.db.begin(id),
		Duties: []Duty{m.duties.duty}, OnChange: m.changes.add, Now: w.clock.Now,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	for _, f := range tweak {
		f(&cfg)
	}
	m.Elector = New(cfg)
	return m
}

func (w *world) step(ms ...*manager) {
	w.t.Helper()
	for _, m := range ms {
		m.Step(w.ctx)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func (m *manager) wantRole(t *testing.T, role Role, epoch int64) {
	t.Helper()
	s := m.Status()
	got, err := m.Epoch()
	if s.Role != role || s.Epoch != epoch || (role == Leader) != (err == nil) || got != epoch {
		t.Fatalf("%s: status %+v, Epoch() = %d, %v; want %s at epoch %d", m.cfg.ID, s, got, err, role, epoch)
	}
}

func TestFirstElectorWinsAndRunsItsDuties(t *testing.T) {
	w := newWorld(t)
	a, b := w.manager("a"), w.manager("b")

	w.step(a, b)
	a.wantRole(t, Leader, 1)
	b.wantRole(t, Follower, 0)
	if got := b.Status().Leader; got != "a" {
		t.Errorf("b sees leader %q, want a", got)
	}
	if w.back.holder() != "a" {
		t.Errorf("the key is held by %q", w.back.holder())
	}

	waitFor(t, "a's duty", func() bool { _, _, started, _ := a.duties.snapshot(); return len(started) == 1 })
	_, _, started, _ := a.duties.snapshot()
	if started[0].Epoch != 1 || started[0].Leader != "a" || started[0].Previous != "" || !started[0].Since.Equal(start) {
		t.Errorf("term = %+v", started[0])
	}
	if _, _, started, _ := b.duties.snapshot(); len(started) != 0 {
		t.Errorf("a follower ran duties: %+v", started)
	}
	if changes := a.changes.all(); len(changes) != 1 || changes[0].From != Follower || changes[0].To != Leader || changes[0].Reason != "elected" || changes[0].Leader != "a" || changes[0].Term.Epoch != 1 {
		t.Errorf("changes = %+v", changes)
	}
	if changes := b.changes.all(); len(changes) != 0 {
		t.Errorf("a manager that stayed a follower reported %+v", changes)
	}

	// Renewing changes nothing, however often it happens.
	for i := 0; i < 5; i++ {
		w.clock.Advance(ttl / 3)
		w.step(a, b)
	}
	a.wantRole(t, Leader, 1)
	if w.db.begins != 1 {
		t.Errorf("%d terms began, want one: a renewal must not start another", w.db.begins)
	}
}

func TestLeadershipTransfersWhenTheLeaderResigns(t *testing.T) {
	w := newWorld(t)
	a, b := w.manager("a"), w.manager("b")
	w.step(a, b)
	waitFor(t, "a's duty", func() bool { r, _, _, _ := a.duties.snapshot(); return r == 1 })

	a.Resign(w.ctx)
	// Resign returns after the duties have: nothing of a's term is left running.
	if running, _, _, stopped := a.duties.snapshot(); running != 0 || len(stopped) != 1 || stopped[0] != 1 {
		t.Fatalf("after Resign: %d duties running, stopped %v", running, stopped)
	}
	a.wantRole(t, Follower, 0)
	if w.back.holder() != "" {
		t.Fatalf("a resigned but the key is still held by %q", w.back.holder())
	}

	// The key is free, and a does not take it back before b had its turn.
	w.step(a)
	a.wantRole(t, Follower, 0)
	w.step(b)
	b.wantRole(t, Leader, 2)
	waitFor(t, "b's duty", func() bool { _, _, started, _ := b.duties.snapshot(); return len(started) == 1 })
	_, _, started, _ := b.duties.snapshot()
	if started[0].Previous != "a" || started[0].PreviousEpoch != 1 {
		t.Errorf("b's term = %+v, want it to name a's epoch 1 as the previous", started[0])
	}
	w.step(a)
	if got := a.Status().Leader; got != "b" {
		t.Errorf("a sees leader %q, want b", got)
	}
	changes := a.changes.all()
	if len(changes) != 2 || changes[1].From != Leader || changes[1].To != Follower || changes[1].Reason != "resigned" || changes[1].Term.Epoch != 1 {
		t.Errorf("a's changes = %+v", changes)
	}
}

func TestALeaderWhoseLeaseRanOutStepsDownAndAnotherTakesOver(t *testing.T) {
	w := newWorld(t)
	a, b := w.manager("a"), w.manager("b")
	w.step(a, b)
	waitFor(t, "a's duty", func() bool { r, _, _, _ := a.duties.snapshot(); return r == 1 })

	// a stops renewing (it is stalled, or cut off); its lease runs out.
	w.clock.Advance(ttl + time.Second)
	w.step(b)
	b.wantRole(t, Leader, 2)

	// a has not noticed yet, but its own clock already says it must not act.
	if _, err := a.Epoch(); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Epoch() = %v on a lease that ran out, want ErrNotLeader", err)
	}
	if got := a.Status().Role; got != Leader {
		t.Fatalf("role = %s before a looked; only a step changes the role", got)
	}
	// When a wakes up it finds out: its duties stop before it does anything else.
	w.step(a)
	a.wantRole(t, Follower, 0)
	if running, _, _, stopped := a.duties.snapshot(); running != 0 || len(stopped) != 1 {
		t.Errorf("a's duties: %d running, stopped %v", running, stopped)
	}
	if changes := a.changes.all(); len(changes) != 2 || changes[1].Reason != "lease expired" {
		t.Errorf("a's changes = %+v", changes)
	}

	// b resigns later; a leads again, and its two terms never overlapped.
	b.Resign(w.ctx)
	w.clock.Advance(ttl)
	w.step(a)
	a.wantRole(t, Leader, 3)
	waitFor(t, "a's second term", func() bool { _, _, started, _ := a.duties.snapshot(); return len(started) == 2 })
	if _, peak, started, _ := a.duties.snapshot(); peak != 1 || started[1].Epoch != 3 || started[1].Previous != "b" {
		t.Errorf("peak %d duties at once, terms %+v", peak, started)
	}
}

func TestALeaderStopsActingByItselfWhenItCannotRenewInTime(t *testing.T) {
	w := newWorld(t)
	a := w.manager("a")
	w.step(a)
	a.wantRole(t, Leader, 1)

	w.back.failing("KeepAlive", errors.New("connection refused"))
	w.clock.Advance(ttl / 2)
	w.step(a)
	a.wantRole(t, Leader, 1) // the lease may well still be alive, and was trusted for 80% of its time
	if s := a.Status(); s.Errors != 1 || s.LastError == "" {
		t.Errorf("status = %+v; the failed renewal must be visible", s)
	}

	// Past what it trusts, with its goroutine stuck (no Step): Epoch alone says no.
	w.clock.Advance(ttl * 35 / 100)
	if _, err := a.Epoch(); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Epoch() = %v after the trusted time ran out, want ErrNotLeader", err)
	}
	if got := a.Status().Role; got != Leader {
		t.Fatalf("role = %s; only Step changes the role", got)
	}
	// And then Step makes it official.
	w.step(a)
	a.wantRole(t, Follower, 0)
	if running, _, _, stopped := a.duties.snapshot(); running != 0 || len(stopped) != 1 {
		t.Errorf("duties: %d running, stopped %v", running, stopped)
	}
	if changes := a.changes.all(); len(changes) != 2 || changes[1].Reason != "lease could not be renewed in time" {
		t.Errorf("changes = %+v", changes)
	}
}

func TestAnEpochThatCannotBeAllocatedMeansNoLeadership(t *testing.T) {
	w := newWorld(t)
	a, b := w.manager("a"), w.manager("b")
	w.db.failing = errors.New("database is down")
	w.step(a)
	a.wantRole(t, Follower, 0)
	if _, _, started, _ := a.duties.snapshot(); len(started) != 0 {
		t.Fatalf("duties started without an epoch: %+v", started)
	}
	if w.back.holder() != "" {
		t.Fatalf("a keeps the key (%q) although it cannot lead; nobody else could", w.back.holder())
	}
	if s := a.Status(); s.Errors != 1 || s.LastError == "" {
		t.Errorf("status = %+v", s)
	}

	// The database comes back; the next election is won by whoever steps first.
	w.db.failing = nil
	w.step(b)
	b.wantRole(t, Leader, 1)
}

func TestAnElectionThatLosesItsLeaseWhileBeginningDoesNotLead(t *testing.T) {
	w := newWorld(t)
	var a *manager
	// The lease runs out during Begin: the term cannot be trusted to be the latest.
	a = w.manager("a", func(c *Config) {
		inner := c.Begin
		c.Begin = func(ctx context.Context, rev int64) (Term, error) {
			term, err := inner(ctx, rev)
			w.clock.Advance(2 * ttl)
			return term, err
		}
	})
	w.step(a)
	a.wantRole(t, Follower, 0)
	if _, _, started, _ := a.duties.snapshot(); len(started) != 0 {
		t.Errorf("duties started on a term whose lease was gone: %+v", started)
	}
}

func TestAFencedLeaderStepsDownAndStaysOutForAWhile(t *testing.T) {
	w := newWorld(t)
	a, b := w.manager("a"), w.manager("b")
	w.step(a)
	waitFor(t, "a's duty", func() bool { r, _, _, _ := a.duties.snapshot(); return r == 1 })

	a.Fenced(99) // not a's epoch: nothing happens
	w.step(a)
	a.wantRole(t, Leader, 1)

	a.Fenced(1)
	w.step(a)
	a.wantRole(t, Follower, 0)
	if changes := a.changes.all(); len(changes) != 2 || changes[1].Reason != "fenced" {
		t.Errorf("changes = %+v", changes)
	}
	if w.back.holder() != "" {
		t.Errorf("the key is still held by %q", w.back.holder())
	}
	w.step(a) // within the hold-off: a does not even try
	a.wantRole(t, Follower, 0)
	w.step(b)
	b.wantRole(t, Leader, 2)
	w.step(a)
	if got := a.Status().Leader; got != "b" {
		t.Errorf("a sees leader %q while it sits out, want b", got)
	}
	// The hold-off ends (b renews as a leader does) and a campaigns again, and loses.
	for i := 0; i < 3; i++ {
		w.clock.Advance(ttl/3 + time.Second)
		w.step(b)
	}
	w.step(a)
	a.wantRole(t, Follower, 0)
	if got := a.Status().Leader; got != "b" {
		t.Errorf("a sees leader %q after campaigning, want b", got)
	}
}

func TestBackendErrorsAreSurvivedAndCounted(t *testing.T) {
	w := newWorld(t)
	a := w.manager("a")
	for _, op := range []string{"Grant", "Claim"} {
		w.back.failing(op, errors.New(op+" failed"))
		w.step(a)
		a.wantRole(t, Follower, 0)
		w.back.failing(op, nil)
	}
	if s := a.Status(); s.Errors != 2 {
		t.Errorf("errors = %d, want 2", s.Errors)
	}
	w.step(a)
	a.wantRole(t, Leader, 1)
	if s := a.Status(); s.LastError != "" {
		t.Errorf("a success must clear the last error: %q", s.LastError)
	}
}

func TestAFollowerKeepsItsLeaseAndSurvivesItsExpiry(t *testing.T) {
	w := newWorld(t)
	a, b := w.manager("a"), w.manager("b")
	w.step(a, b)
	for i := 0; i < 4; i++ {
		w.clock.Advance(ttl / 3)
		w.step(a, b)
	}
	if got := w.back.calls["Grant"]; got != 2 {
		t.Errorf("%d leases granted for two managers over a long time, want 2: a follower must renew, not re-grant", got)
	}
	// b is away for longer than its lease while a keeps leading; when it is back it
	// notices its lease is gone, grants itself a new one, and goes on following.
	for i := 0; i < 10; i++ {
		w.clock.Advance(ttl / 3)
		w.step(a)
	}
	w.step(b)
	b.wantRole(t, Follower, 0)
	if got := b.Status().Leader; got != "a" {
		t.Errorf("b sees leader %q, want a", got)
	}
	if got := w.back.calls["Grant"]; got != 3 {
		t.Errorf("%d leases granted, want 3 (a's, b's, and b's second)", got)
	}
	a.wantRole(t, Leader, 1)
}

func TestRunElectsAndReleasesTheKeyOnShutdown(t *testing.T) {
	w := newWorld(t)
	a, b := w.manager("a", func(c *Config) { c.Interval = time.Millisecond }), w.manager("b")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		a.Run(ctx)
		close(done)
	}()
	waitFor(t, "a to lead", func() bool { return a.Status().Role == Leader })
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after its context ended")
	}
	if running, _, _, stopped := a.duties.snapshot(); running != 0 || len(stopped) != 1 {
		t.Errorf("duties: %d running, stopped %v", running, stopped)
	}
	if changes := a.changes.all(); len(changes) != 2 || changes[1].Reason != "shutdown" {
		t.Errorf("changes = %+v", changes)
	}
	// The key was given up, so b does not wait for the lease to expire.
	w.step(b)
	b.wantRole(t, Leader, 2)
}

func TestRunWakesEarlyWhenFenced(t *testing.T) {
	w := newWorld(t)
	a := w.manager("a", func(c *Config) { c.Interval = time.Hour })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		a.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()
	waitFor(t, "a to lead", func() bool { return a.Status().Role == Leader })
	a.Fenced(1)
	waitFor(t, "a to step down without waiting for the next interval", func() bool { return a.Status().Role == Follower })
}

// Epoch is read by many goroutines while terms begin and end.
func TestEpochIsSafeToReadWhileTermsChange(t *testing.T) {
	w := newWorld(t)
	a := w.manager("a")
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = a.Epoch()
					_ = a.Status()
				}
			}
		}()
	}
	for i := 0; i < 20; i++ {
		w.step(a)
		a.Resign(w.ctx)
		w.clock.Advance(ttl + time.Second)
	}
	close(stop)
	wg.Wait()
	if s := a.Status(); s.Transitions != 40 {
		t.Errorf("transitions = %d, want 40 (20 terms, each begun and ended)", s.Transitions)
	}
}
