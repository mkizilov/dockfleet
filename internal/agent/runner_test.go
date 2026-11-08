package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/dockfleet/dockfleet/internal/docker"
	"github.com/dockfleet/dockfleet/internal/dockerfake"
	dockfleetv1 "github.com/dockfleet/dockfleet/internal/protocol/dockfleet/v1"
)

const (
	phasePending   = dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_PENDING
	phaseRunning   = dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_RUNNING
	phaseSucceeded = dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_SUCCEEDED
	phaseFailed    = dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_FAILED
	phaseStopped   = dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_STOPPED

	wantRunning = dockfleetv1.WorkloadDesiredState_WORKLOAD_DESIRED_STATE_RUNNING
	wantStopped = dockfleetv1.WorkloadDesiredState_WORKLOAD_DESIRED_STATE_STOPPED
)

// clock is a clock the test moves by hand.
type clock struct{ now time.Time }

func (c *clock) Now() time.Time          { return c.now }
func (c *clock) advance(d time.Duration) { c.now = c.now.Add(d) }

// newTestRunner returns a runner on a fresh engine that has the image "img".
func newTestRunner(t *testing.T) (*runner, *dockerfake.Fake, *clock) {
	t.Helper()
	engine := dockerfake.New("node-1")
	engine.AddImage("img")
	clk := &clock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	r := newRunner(engine, slog.New(slog.NewTextHandler(io.Discard, nil)), runnerConfig{
		Now: clk.Now, OrphanGrace: time.Minute, BackoffBase: time.Second, BackoffMax: 4 * time.Second,
	})
	return r, engine, clk
}

func assigned(id string, generation int64, state dockfleetv1.WorkloadDesiredState) *dockfleetv1.WorkloadAssignment {
	return &dockfleetv1.WorkloadAssignment{
		WorkloadId: id, Name: id, Image: "img", CpuMillis: 100, MemoryBytes: 64 << 20,
		Generation: generation, DesiredState: state,
	}
}

func (r *runner) pass() { r.converge(context.Background()) }

func statusOf(r *runner, id string) *dockfleetv1.WorkloadStatus {
	for _, st := range r.Statuses() {
		if st.GetWorkloadId() == id {
			return st
		}
	}
	return nil
}

func wantStatus(t *testing.T, r *runner, id string, phase dockfleetv1.WorkloadPhase, messagePart string) *dockfleetv1.WorkloadStatus {
	t.Helper()
	st := statusOf(r, id)
	if st == nil || st.GetPhase() != phase || !strings.Contains(st.GetMessage(), messagePart) {
		t.Fatalf("status of %s = %v, want phase %s with %q in the message", id, st, phase, messagePart)
	}
	return st
}

func TestNothingHappensUntilTheManagerHasAnswered(t *testing.T) {
	r, engine, clk := newTestRunner(t)
	if _, err := engine.Create(context.Background(), docker.ContainerSpec{WorkloadID: "left-behind", Image: "img", CPUMillis: 100, MemoryBytes: 64 << 20}); err != nil {
		t.Fatal(err)
	}

	// An agent that has not heard from the manager knows nothing: it must not
	// treat the containers it finds as unwanted, however long it waits.
	for i := 0; i < 5; i++ {
		r.pass()
		clk.advance(time.Hour)
	}
	if got := engine.Managed(); len(got) != 1 || engine.Count("remove") != 0 {
		t.Fatalf("containers = %+v, calls = %v", got, engine.Calls())
	}
	if len(r.Statuses()) != 0 {
		t.Fatalf("statuses before the first answer: %v", r.Statuses())
	}
}

func TestAnAssignmentBecomesARunningContainerAndIsAcknowledged(t *testing.T) {
	r, engine, _ := newTestRunner(t)
	web := assigned("w1", 2, wantRunning)
	web.Command, web.Env = []string{"sleep", "300"}, map[string]string{"MODE": "prod"}
	web.Ports = []*dockfleetv1.PortMapping{{ContainerPort: 80, HostPort: 18080, Protocol: "tcp"}}

	r.Assign([]*dockfleetv1.WorkloadAssignment{web})
	st := wantStatus(t, r, "w1", phasePending, "assignment received")
	if st.GetAcknowledgedGeneration() != 2 {
		t.Fatalf("an assignment is acknowledged as soon as it is received: %v", st)
	}

	r.pass()
	wantStatus(t, r, "w1", phasePending, "starting")
	r.pass()
	st = wantStatus(t, r, "w1", phaseRunning, "running")
	if st.GetContainerId() == "" || st.GetAcknowledgedGeneration() != 2 {
		t.Fatalf("running status = %v", st)
	}

	spec, ok := engine.Spec("w1")
	if !ok || spec.Image != "img" || spec.WorkloadName != "w1" || spec.CPUMillis != 100 || spec.MemoryBytes != 64<<20 ||
		len(spec.Command) != 2 || spec.Env["MODE"] != "prod" || len(spec.Ports) != 1 || spec.Ports[0].ContainerPort != 80 || spec.Ports[0].HostPort != 18080 {
		t.Fatalf("container spec = %+v", spec)
	}
	if got := engine.Calls(); len(got) != 2 || got[0] != "create w1" || got[1] != "start w1" {
		t.Fatalf("calls = %v", got)
	}
}

// Passes are idempotent: any number of them, with the same assignments handed
// over again each time, leaves exactly one container per workload.
func TestRepeatedPassesNeverCreateDuplicates(t *testing.T) {
	r, engine, _ := newTestRunner(t)
	set := []*dockfleetv1.WorkloadAssignment{assigned("w3", 1, wantRunning), assigned("w1", 1, wantRunning), assigned("w2", 1, wantRunning)}
	for i := 0; i < 10; i++ {
		r.Assign(set)
		r.pass()
	}
	if engine.Count("create") != 3 || engine.Count("start") != 3 || len(engine.Managed()) != 3 {
		t.Fatalf("calls = %v, containers = %+v", engine.Calls(), engine.Managed())
	}
	for _, id := range []string{"w1", "w2", "w3"} {
		wantStatus(t, r, id, phaseRunning, "running")
	}
}

// A new agent process on the same node adopts what is there.
func TestARestartedAgentAdoptsRunningContainers(t *testing.T) {
	first, engine, clk := newTestRunner(t)
	set := []*dockfleetv1.WorkloadAssignment{assigned("w1", 1, wantRunning), assigned("w2", 1, wantRunning)}
	first.Assign(set)
	first.pass()
	first.pass()
	before := append([]string(nil), engine.Calls()...)

	second := newRunner(engine, slog.New(slog.NewTextHandler(io.Discard, nil)), runnerConfig{Now: clk.Now})
	second.Assign(set)
	second.pass()
	wantStatus(t, second, "w1", phaseRunning, "running")
	wantStatus(t, second, "w2", phaseRunning, "running")
	if got := engine.Calls(); len(got) != len(before) {
		t.Fatalf("the new agent changed things: %v, was %v", got, before)
	}
}

func TestStoppingRemovesTheContainerOnceAndReportsIt(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, engine *dockerfake.Fake)
	}{
		{"a running container", nil},
		{"a container that exited", func(t *testing.T, engine *dockerfake.Fake) { engine.Crash("w1", 1) }},
		{"no container at all", func(t *testing.T, engine *dockerfake.Fake) {
			if err := engine.Remove(context.Background(), "w1", true); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, engine, _ := newTestRunner(t)
			r.Assign([]*dockfleetv1.WorkloadAssignment{assigned("w1", 1, wantRunning)})
			r.pass()
			r.pass()
			if tt.setup != nil {
				tt.setup(t, engine)
			}
			removalsBefore := engine.Count("remove")

			r.Assign([]*dockfleetv1.WorkloadAssignment{assigned("w1", 2, wantStopped)})
			for i := 0; i < 3; i++ {
				r.pass()
				st := wantStatus(t, r, "w1", phaseStopped, "removed")
				if st.GetContainerId() != "" || st.GetAcknowledgedGeneration() != 2 {
					t.Fatalf("pass %d: status = %v; the report says stopped, with no container, acknowledging generation 2", i+1, st)
				}
			}
			if len(engine.Managed()) != 0 {
				t.Fatalf("containers left: %+v", engine.Managed())
			}
			if got := engine.Count("remove") - removalsBefore; got > 1 {
				t.Fatalf("the container was removed %d times; repeating the instruction must change nothing", got)
			}
		})
	}
}

func TestAStopThatFailsIsRetriedAndNotReportedAsDone(t *testing.T) {
	r, engine, _ := newTestRunner(t)
	r.Assign([]*dockfleetv1.WorkloadAssignment{assigned("w1", 1, wantRunning)})
	r.pass()
	r.pass()

	engine.FailNext("stop", errors.New("daemon busy"), 1)
	r.Assign([]*dockfleetv1.WorkloadAssignment{assigned("w1", 2, wantStopped)})
	r.pass()
	st := wantStatus(t, r, "w1", phaseRunning, "stopping failed")
	if st.GetContainerId() == "" {
		t.Fatalf("the container is still there and the report must say so: %v", st)
	}
	r.pass()
	if st := wantStatus(t, r, "w1", phaseStopped, "removed"); st.GetContainerId() != "" {
		t.Fatalf("after the retry: %v", st)
	}
}

func TestContainersThatAreNotDockFleetsAreNeverTouched(t *testing.T) {
	r, engine, clk := newTestRunner(t)
	engine.AddUnmanaged("redis", "redis:7", nil)
	engine.AddUnmanaged("dockfleet-w1", "squatter:1", nil)
	engine.AddUnmanaged("dockfleet-w9", "img", map[string]string{
		docker.LabelManaged: "true", docker.LabelNodeID: "node-2", docker.LabelWorkloadID: "w9",
	})

	// w1 wants the name a stranger holds; w9 is stopped, but the container with that ID is another node's.
	r.Assign([]*dockfleetv1.WorkloadAssignment{assigned("w1", 1, wantRunning), assigned("w9", 1, wantStopped)})
	for i := 0; i < 3; i++ {
		r.pass()
	}
	wantStatus(t, r, "w1", phaseFailed, "creating the container failed")
	wantStatus(t, r, "w9", phaseStopped, "removed")

	// And an agent that is told nothing is assigned removes nothing of theirs.
	r.Assign(nil)
	for i := 0; i < 3; i++ {
		r.pass()
		clk.advance(time.Hour)
	}
	if got := engine.Containers(); len(got) != 3 || len(engine.Managed()) != 0 {
		t.Fatalf("containers = %+v", got)
	}
	if engine.Count("remove") != 0 || engine.Count("stop") != 0 || engine.Count("create") != 0 {
		t.Fatalf("calls = %v; none of them may touch a bystander", engine.Calls())
	}
}

func TestOrphansAreRemovedOnlyAfterTheGracePeriod(t *testing.T) {
	r, engine, clk := newTestRunner(t)
	r.Assign([]*dockfleetv1.WorkloadAssignment{assigned("w1", 1, wantRunning), assigned("w2", 1, wantRunning)})
	r.pass()
	r.pass()

	// The manager stops mentioning w2.
	r.Assign([]*dockfleetv1.WorkloadAssignment{assigned("w1", 1, wantRunning)})
	r.pass()
	clk.advance(30 * time.Second)
	r.pass()
	if len(engine.Managed()) != 2 {
		t.Fatalf("w2 was removed after 30s: %+v", engine.Managed())
	}

	// Mentioned again within the grace period: it is not an orphan after all.
	r.Assign([]*dockfleetv1.WorkloadAssignment{assigned("w1", 1, wantRunning), assigned("w2", 1, wantRunning)})
	r.pass()
	clk.advance(2 * time.Minute)
	r.Assign([]*dockfleetv1.WorkloadAssignment{assigned("w1", 1, wantRunning)})
	r.pass() // the clock for w2 starts again
	clk.advance(30 * time.Second)
	r.pass()
	if len(engine.Managed()) != 2 {
		t.Fatalf("the grace period did not start over: %+v", engine.Managed())
	}

	clk.advance(time.Minute)
	r.pass()
	if got := engine.Managed(); len(got) != 1 || got[0].WorkloadID != "w1" {
		t.Fatalf("containers = %+v, want only w1", got)
	}
	if statusOf(r, "w2") != nil {
		t.Fatalf("an unassigned workload must not be reported: %v", r.Statuses())
	}
}

func TestMissingImagesArePulledOnce(t *testing.T) {
	r, engine, _ := newTestRunner(t)
	fresh := assigned("w1", 1, wantRunning)
	fresh.Image = "other:2"
	r.Assign([]*dockfleetv1.WorkloadAssignment{fresh})
	r.pass()
	r.pass()
	r.pass()
	wantStatus(t, r, "w1", phaseRunning, "running")
	if got := engine.Calls(); len(got) != 3 || got[0] != "pull other:2" || got[1] != "create w1" || got[2] != "start w1" {
		t.Fatalf("calls = %v", got)
	}
}

// Failures are retried, but not in a tight loop: each one doubles the wait.
func TestFailedAttemptsBackOff(t *testing.T) {
	r, engine, clk := newTestRunner(t)
	r.Assign([]*dockfleetv1.WorkloadAssignment{assigned("w1", 1, wantRunning)})
	engine.FailNext("create", errors.New("daemon busy"), 2)

	r.pass()
	wantStatus(t, r, "w1", phaseFailed, "daemon busy")
	r.pass() // no time has passed: no new attempt
	if engine.Pending("create") != 1 {
		t.Fatalf("an attempt was made without waiting; %d failures are left", engine.Pending("create"))
	}
	clk.advance(time.Second)
	r.pass()
	if engine.Pending("create") != 0 {
		t.Fatalf("no new attempt after the first wait; %d failures are left", engine.Pending("create"))
	}
	clk.advance(time.Second) // the second failure doubled the wait to 2s
	r.pass()
	if engine.Count("create") != 0 {
		t.Fatalf("the wait after a second failure is longer; calls = %v", engine.Calls())
	}
	clk.advance(time.Second)
	r.pass()
	wantStatus(t, r, "w1", phasePending, "starting")
	r.pass()
	wantStatus(t, r, "w1", phaseRunning, "running")
	if engine.Count("create") != 1 || len(engine.Managed()) != 1 {
		t.Fatalf("calls = %v", engine.Calls())
	}
}

func TestAssignmentsThePolicyOrTheRuntimeCannotAcceptAreReportedFailed(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*dockfleetv1.WorkloadAssignment)
		message string
	}{
		{"no CPU limit", func(a *dockfleetv1.WorkloadAssignment) { a.CpuMillis = 0 }, "rejected by the container policy"},
		{"no memory limit", func(a *dockfleetv1.WorkloadAssignment) { a.MemoryBytes = 0 }, "rejected by the container policy"},
		{"a port out of range", func(a *dockfleetv1.WorkloadAssignment) {
			a.Ports = []*dockfleetv1.PortMapping{{ContainerPort: 70000}}
		}, "invalid assignment"},
		{"an image that looks like a flag", func(a *dockfleetv1.WorkloadAssignment) { a.Image = "--privileged" }, "rejected by the container policy"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, engine, _ := newTestRunner(t)
			a := assigned("w1", 1, wantRunning)
			tt.mutate(a)
			r.Assign([]*dockfleetv1.WorkloadAssignment{a})
			r.pass()
			st := wantStatus(t, r, "w1", phaseFailed, tt.message)
			if len(st.GetMessage()) > maxStatusText || len(engine.Containers()) != 0 {
				t.Fatalf("status %v, containers %+v", st, engine.Containers())
			}
		})
	}
}

func TestRestartPolicies(t *testing.T) {
	tests := []struct {
		name       string
		policy     *dockfleetv1.RestartPolicy
		exit       int
		wantStarts int // -1: more than three, the container keeps being restarted
		wantPhase  dockfleetv1.WorkloadPhase
	}{
		{"no restart policy means on-failure: it gives up after five", nil, 1, 6, phaseFailed},
		{"on-failure restarts a failure up to the limit", &dockfleetv1.RestartPolicy{Condition: "on-failure", MaxRetries: 2}, 1, 3, phaseFailed},
		{"on-failure leaves a success alone", &dockfleetv1.RestartPolicy{Condition: "on-failure", MaxRetries: 2}, 0, 1, phaseSucceeded},
		{"no never restarts a failure", &dockfleetv1.RestartPolicy{Condition: "no"}, 1, 1, phaseFailed},
		{"no leaves a success alone", &dockfleetv1.RestartPolicy{Condition: "no"}, 0, 1, phaseSucceeded},
		{"always restarts a success", &dockfleetv1.RestartPolicy{Condition: "always"}, 0, -1, phasePending},
		{"always restarts a failure", &dockfleetv1.RestartPolicy{Condition: "always"}, 7, -1, phasePending},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, engine, clk := newTestRunner(t)
			a := assigned("w1", 1, wantRunning)
			a.RestartPolicy = tt.policy
			r.Assign([]*dockfleetv1.WorkloadAssignment{a})
			for i := 0; i < 12; i++ {
				r.pass()
				engine.Crash("w1", tt.exit)
				clk.advance(time.Minute)
			}
			r.pass()

			starts := engine.Count("start")
			if (tt.wantStarts >= 0 && starts != tt.wantStarts) || (tt.wantStarts < 0 && starts <= 3) {
				t.Fatalf("started %d times, want %d", starts, tt.wantStarts)
			}
			if engine.Count("create") != 1 {
				t.Fatalf("restarting must reuse the container: calls = %v", engine.Calls())
			}
			st := statusOf(r, "w1")
			if st.GetPhase() != tt.wantPhase || st.GetContainerId() == "" {
				t.Fatalf("status = %v, want phase %s", st, tt.wantPhase)
			}
			if tt.wantPhase != phasePending && (st.ExitCode == nil || int(*st.ExitCode) != tt.exit) {
				t.Fatalf("the report must carry the exit code %d: %v", tt.exit, st)
			}
			if got := int(st.GetRestartCount()); got != starts-1 {
				t.Fatalf("restart count %d after %d starts", got, starts)
			}
		})
	}
}

func TestACrashedContainerIsNotRestartedBeforeTheBackoffEnds(t *testing.T) {
	r, engine, clk := newTestRunner(t)
	a := assigned("w1", 1, wantRunning)
	a.RestartPolicy = &dockfleetv1.RestartPolicy{Condition: "always"}
	r.Assign([]*dockfleetv1.WorkloadAssignment{a})
	r.pass() // create and start
	// The first restart is immediate. A second crash at once waits one second,
	// the next two, then four, which is the maximum and stays.
	steps := []struct {
		wait       time.Duration
		wantStarts int
	}{
		{0, 2},
		{0, 2},
		{time.Second, 3},
		{time.Second, 3},
		{time.Second, 4},
		{3 * time.Second, 4},
		{time.Second, 5},
		{4 * time.Second, 6},
	}
	for i, step := range steps {
		engine.Crash("w1", 1)
		clk.advance(step.wait)
		r.pass()
		if got := engine.Count("start"); got != step.wantStarts {
			t.Fatalf("step %d: started %d times, want %d", i, got, step.wantStarts)
		}
	}
	engine.Crash("w1", 1)
	r.pass()
	wantStatus(t, r, "w1", phaseFailed, "restart pending")
}

func TestAnOutOfMemoryKillIsReported(t *testing.T) {
	r, engine, _ := newTestRunner(t)
	a := assigned("w1", 1, wantRunning)
	a.RestartPolicy = &dockfleetv1.RestartPolicy{Condition: "no"}
	r.Assign([]*dockfleetv1.WorkloadAssignment{a})
	r.pass()
	engine.OOMKill("w1")
	r.pass()
	if st := wantStatus(t, r, "w1", phaseFailed, "out of memory"); !st.GetOomKilled() {
		t.Fatalf("status = %v", st)
	}
}

func TestContainersThatDisappearOrWereNeverStartedAreRepaired(t *testing.T) {
	t.Run("a container removed behind the agent's back is created again", func(t *testing.T) {
		r, engine, _ := newTestRunner(t)
		r.Assign([]*dockfleetv1.WorkloadAssignment{assigned("w1", 1, wantRunning)})
		r.pass()
		r.pass()
		if err := engine.Remove(context.Background(), "w1", true); err != nil {
			t.Fatal(err)
		}
		r.pass()
		r.pass()
		wantStatus(t, r, "w1", phaseRunning, "running")
		if engine.Count("create") != 2 || len(engine.Managed()) != 1 {
			t.Fatalf("calls = %v", engine.Calls())
		}
	})
	t.Run("a container created but never started is started, not created again", func(t *testing.T) {
		r, engine, _ := newTestRunner(t)
		spec := docker.ContainerSpec{WorkloadID: "w1", WorkloadName: "w1", Image: "img", CPUMillis: 100, MemoryBytes: 64 << 20}
		if _, err := engine.Create(context.Background(), spec); err != nil {
			t.Fatal(err)
		}
		r.Assign([]*dockfleetv1.WorkloadAssignment{assigned("w1", 1, wantRunning)})
		r.pass()
		r.pass()
		wantStatus(t, r, "w1", phaseRunning, "running")
		if engine.Count("create") != 1 || engine.Count("start") != 1 {
			t.Fatalf("calls = %v", engine.Calls())
		}
	})
}

func TestAnUnreachableEngineChangesNothing(t *testing.T) {
	r, engine, _ := newTestRunner(t)
	r.Assign([]*dockfleetv1.WorkloadAssignment{assigned("w1", 1, wantRunning)})
	r.pass()
	r.pass()

	engine.FailNext("list", errors.New("daemon down"), 2)
	r.Assign([]*dockfleetv1.WorkloadAssignment{assigned("w1", 2, wantStopped)})
	r.pass()
	st := wantStatus(t, r, "w1", phaseRunning, "running")
	if st.GetAcknowledgedGeneration() != 2 {
		t.Fatalf("the instruction was received, whether or not it can be carried out yet: %v", st)
	}
	if len(engine.Managed()) != 1 {
		t.Fatalf("containers = %+v", engine.Managed())
	}
	r.pass()
	r.pass()
	wantStatus(t, r, "w1", phaseStopped, "removed")
}

func TestStatusesAreOrderedAndAcknowledgmentsNeverGoBack(t *testing.T) {
	r, _, _ := newTestRunner(t)
	r.Assign([]*dockfleetv1.WorkloadAssignment{assigned("w2", 5, wantRunning), assigned("w1", 1, wantRunning)})
	got := r.Statuses()
	if len(got) != 2 || got[0].GetWorkloadId() != "w1" || got[1].GetWorkloadId() != "w2" {
		t.Fatalf("statuses = %v", got)
	}
	r.Assign([]*dockfleetv1.WorkloadAssignment{assigned("w2", 3, wantRunning)})
	if st := statusOf(r, "w2"); st.GetAcknowledgedGeneration() != 5 {
		t.Fatalf("the acknowledgment went back: %v", st)
	}
	if statusOf(r, "w1") != nil {
		t.Fatalf("a workload that is no longer assigned must not be reported: %v", r.Statuses())
	}
}

func TestLongMessagesAreClippedToValidText(t *testing.T) {
	for name, msg := range map[string]string{
		"a rune cut in half": "a" + strings.Repeat("é", 400),
		"invalid bytes":      strings.Repeat("a", 10) + "\xff" + strings.Repeat("b", 600),
		"short":              "fine",
	} {
		got := clip(msg)
		if len(got) > maxStatusText || !utf8.ValidString(got) || got == "" {
			t.Errorf("%s: clip returned %d bytes, valid UTF-8 %v", name, len(got), utf8.ValidString(got))
		}
	}
}

func TestRunConvergesInTheBackgroundAndStops(t *testing.T) {
	engine := dockerfake.New("node-1")
	engine.AddImage("img")
	r := newRunner(engine, slog.New(slog.NewTextHandler(io.Discard, nil)), runnerConfig{Resync: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Run did not stop after its context was canceled")
		}
	}()

	r.Assign([]*dockfleetv1.WorkloadAssignment{assigned("w1", 1, wantRunning)})
	waitFor(t, "w1 to run", func() bool {
		st := statusOf(r, "w1")
		return st != nil && st.GetPhase() == phaseRunning
	})
	r.Assign([]*dockfleetv1.WorkloadAssignment{assigned("w1", 2, wantStopped)})
	waitFor(t, "w1 to be stopped and removed", func() bool {
		st := statusOf(r, "w1")
		return st != nil && st.GetPhase() == phaseStopped && st.GetContainerId() == "" && len(engine.Managed()) == 0
	})
}
