package store

import (
	"context"
	"strings"
	"testing"
)

func TestMemoryConformance(t *testing.T) {
	runStoreConformance(t, func(t *testing.T) Store { return NewMemory() })
}

func TestMemoryDoesNotAliasCallerData(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	labels := map[string]string{"a": "1"}
	if _, err := m.UpsertNode(ctx, Node{ID: "n", Labels: labels}); err != nil {
		t.Fatal(err)
	}
	labels["a"] = "mutated"
	got, err := m.GetNode(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}
	if got.Labels["a"] != "1" {
		t.Fatalf("store aliased caller's map: %v", got.Labels)
	}

	spec := webSpec()
	svc, err := m.CreateService(ctx, NewService{ID: "s", Name: "web", Replicas: 1, Spec: spec, At: testTime})
	if err != nil {
		t.Fatal(err)
	}
	spec.Env["MODE"] = "mutated"
	spec.Command[0] = "mutated"
	rev, _ := m.GetRevision(ctx, svc.ID, 1)
	if rev.Spec.Env["MODE"] != "prod" || rev.Spec.Command[0] != "nginx" {
		t.Fatalf("store aliased the caller's spec: %+v", rev.Spec)
	}
}

func TestValidName(t *testing.T) {
	valid := []string{"a", "node-1", "web.api_2", "A1"}
	invalid := []string{"", "-a", ".a", "a b", "a/b", "a;drop", "ünï", strings.Repeat("a", 64)}
	for _, s := range valid {
		if !ValidName(s) {
			t.Errorf("ValidName(%q) = false, want true", s)
		}
	}
	for _, s := range invalid {
		if ValidName(s) {
			t.Errorf("ValidName(%q) = true, want false", s)
		}
	}
}

func TestNewIDIsUniqueAndValid(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := NewID()
		if len(id) != 32 || !ValidName(id) || seen[id] {
			t.Fatalf("NewID() = %q (duplicate or malformed)", id)
		}
		seen[id] = true
	}
}

func TestServiceSpecHash(t *testing.T) {
	a, b := webSpec(), webSpec()
	if a.Hash() != b.Hash() || len(a.Hash()) != 64 {
		t.Fatalf("equal specs must hash equally: %q vs %q", a.Hash(), b.Hash())
	}
	// Map insertion order must not matter.
	a.Env = map[string]string{"A": "1", "B": "2", "C": "3"}
	b.Env = map[string]string{"C": "3", "B": "2", "A": "1"}
	if a.Hash() != b.Hash() {
		t.Fatal("hash depends on map order")
	}
	for name, mutate := range map[string]func(*ServiceSpec){
		"image":    func(s *ServiceSpec) { s.Image = "other" },
		"command":  func(s *ServiceSpec) { s.Command = []string{"x"} },
		"env":      func(s *ServiceSpec) { s.Env["A"] = "changed" },
		"cpu":      func(s *ServiceSpec) { s.CPUMillis++ },
		"memory":   func(s *ServiceSpec) { s.MemoryBytes++ },
		"selector": func(s *ServiceSpec) { s.NodeSelector = map[string]string{"zone": "b"} },
	} {
		c := b
		c.Env = cloneLabels(b.Env)
		mutate(&c)
		if c.Hash() == a.Hash() {
			t.Errorf("changing the %s must change the hash", name)
		}
	}
}

func TestObservationSameReport(t *testing.T) {
	base := Observation{WorkloadID: "w", NodeID: "n", State: WorkloadRunning, Message: "up", ExitCode: intPtr(0), StartedAt: timePtr(testTime), ObservedAt: testTime}
	same := base
	same.NodeID, same.ObservedAt = "other", testTime.Add(1)
	same.ExitCode = intPtr(0)
	same.StartedAt = timePtr(testTime)
	if !base.SameReport(same) {
		t.Error("reporter and time must not matter")
	}
	for name, mutate := range map[string]func(*Observation){
		"state":     func(o *Observation) { o.State = WorkloadFailed },
		"message":   func(o *Observation) { o.Message = "down" },
		"container": func(o *Observation) { o.ContainerID = "c" },
		"exit code": func(o *Observation) { o.ExitCode = intPtr(1) },
		"no exit":   func(o *Observation) { o.ExitCode = nil },
		"oom":       func(o *Observation) { o.OOMKilled = true },
		"restarts":  func(o *Observation) { o.RestartCount = 3 },
		"started":   func(o *Observation) { o.StartedAt = timePtr(testTime.Add(1)) },
		"finished":  func(o *Observation) { o.FinishedAt = timePtr(testTime) },
	} {
		c := base
		c.ExitCode, c.StartedAt = intPtr(0), timePtr(testTime)
		mutate(&c)
		if base.SameReport(c) {
			t.Errorf("a different %s must not count as the same report", name)
		}
	}
}

func TestWorkloadPhase(t *testing.T) {
	w := Workload{}
	if w.Phase() != WorkloadScheduled {
		t.Errorf("no observation: phase %q", w.Phase())
	}
	w.Observed = &Observation{State: WorkloadRunning}
	if w.Phase() != WorkloadRunning {
		t.Errorf("phase = %q", w.Phase())
	}
}

func TestMemoryTransactionsAreAtomicUnderConcurrency(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	done := make(chan error, 20)
	for i := 0; i < 20; i++ {
		go func() {
			done <- m.InTx(ctx, func(tx Tx) error {
				if err := tx.LockPlacement(ctx); err != nil {
					return err
				}
				n, _ := tx.GetNode(ctx, "counter")
				n.ID = "counter"
				n.CPUMillis++
				_, err := tx.UpsertNode(ctx, n)
				return err
			})
		}()
	}
	for i := 0; i < 20; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	n, err := m.GetNode(ctx, "counter")
	if err != nil || n.CPUMillis != 20 {
		t.Fatalf("lost updates: counter = %d (%v), want 20", n.CPUMillis, err)
	}
}
