package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dockfleet/dockfleet/internal/docker"
	dockfleetv1 "github.com/dockfleet/dockfleet/internal/protocol/dockfleet/v1"
)

const (
	defaultResync      = 2 * time.Second
	defaultOrphanGrace = 2 * time.Minute
	defaultStopTimeout = 10 * time.Second
	defaultOpTimeout   = 30 * time.Second
	defaultPullTimeout = 10 * time.Minute
	defaultBackoffBase = time.Second
	defaultBackoffMax  = time.Minute

	// defaultRestartRetries is how often "on-failure" restarts a container
	// when the assignment does not say.
	defaultRestartRetries = 5

	// maxStatusText keeps messages inside the protocol's limit.
	maxStatusText = 500
)

// runnerConfig holds the timings of a runner; zero values mean the defaults.
type runnerConfig struct {
	// Resync is how often containers are looked at when nothing else wakes the runner.
	Resync time.Duration
	// OrphanGrace is how long a managed container may go unassigned before it is removed.
	OrphanGrace time.Duration
	StopTimeout time.Duration
	OpTimeout   time.Duration
	PullTimeout time.Duration
	BackoffBase time.Duration
	BackoffMax  time.Duration
	Now         func() time.Time
}

func (c runnerConfig) withDefaults() runnerConfig {
	for _, d := range []struct {
		field *time.Duration
		value time.Duration
	}{
		{&c.Resync, defaultResync}, {&c.OrphanGrace, defaultOrphanGrace}, {&c.StopTimeout, defaultStopTimeout},
		{&c.OpTimeout, defaultOpTimeout}, {&c.PullTimeout, defaultPullTimeout},
		{&c.BackoffBase, defaultBackoffBase}, {&c.BackoffMax, defaultBackoffMax},
	} {
		if *d.field <= 0 {
			*d.field = d.value
		}
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

// workloadStatus is what the agent reports about one workload.
type workloadStatus struct {
	phase        dockfleetv1.WorkloadPhase
	message      string
	containerID  string // empty when the node has no container
	exitCode     *int32
	oomKilled    bool
	restartCount int32
	acked        int64 // the highest generation of the assignment seen
}

func (s workloadStatus) proto(id string) *dockfleetv1.WorkloadStatus {
	phase := s.phase
	if phase == dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_UNSPECIFIED {
		phase = dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_PENDING // the manager refuses a report without a phase
	}
	st := &dockfleetv1.WorkloadStatus{
		WorkloadId: id, Phase: phase, Message: s.message, AcknowledgedGeneration: s.acked,
		ContainerId: s.containerID, OomKilled: s.oomKilled, RestartCount: s.restartCount,
	}
	if s.exitCode != nil {
		v := *s.exitCode
		st.ExitCode = &v
	}
	return st
}

// sameObservation compares everything but the acknowledgment.
func (s workloadStatus) sameObservation(o workloadStatus) bool {
	if s.phase != o.phase || s.message != o.message || s.containerID != o.containerID || s.oomKilled != o.oomKilled || s.restartCount != o.restartCount {
		return false
	}
	if s.exitCode == nil || o.exitCode == nil {
		return s.exitCode == o.exitCode
	}
	return *s.exitCode == *o.exitCode
}

// tracker is what the runner remembers about one workload between passes. It
// is only a pacing aid: losing it (an agent restart) costs a retry budget, never
// correctness, because the containers on the node are the truth.
type tracker struct {
	restarts  int       // restarts by this agent since it started
	restartAt time.Time // no restart before this
	attempts  int       // consecutive failed attempts to create or start
	retryAt   time.Time // no new attempt before this
	failure   workloadStatus
}

// runner brings the node's containers to the manager's assignments. A pass
// looks at the containers that exist and takes the steps that are missing,
// so running it again, after a crash, a lost message or a restart, changes
// nothing that is already right: it never creates a second container for a
// workload and never touches a container that is not DockFleet's.
type runner struct {
	rt   docker.Runtime
	log  *slog.Logger
	cfg  runnerConfig
	wake chan struct{}

	mu       sync.Mutex
	assigned map[string]*dockfleetv1.WorkloadAssignment
	heard    bool // the manager has answered at least once
	statuses map[string]workloadStatus

	// Only converge touches these.
	trackers map[string]*tracker
	orphans  map[string]time.Time
}

func newRunner(rt docker.Runtime, log *slog.Logger, cfg runnerConfig) *runner {
	return &runner{
		rt: rt, log: log, cfg: cfg.withDefaults(), wake: make(chan struct{}, 1),
		statuses: map[string]workloadStatus{}, trackers: map[string]*tracker{}, orphans: map[string]time.Time{},
	}
}

func (r *runner) now() time.Time { return r.cfg.Now() }

// Assign records the manager's complete set of assignments and wakes the
// runner. Receiving it is the acknowledgment: from now on the workload's
// status carries the assignment's generation. Until the first call the runner
// does nothing at all, so an agent that has not yet heard from the manager
// never removes a container.
func (r *runner) Assign(assigned []*dockfleetv1.WorkloadAssignment) {
	desired := make(map[string]*dockfleetv1.WorkloadAssignment, len(assigned))
	for _, a := range assigned {
		desired[a.GetWorkloadId()] = a
	}
	r.mu.Lock()
	r.assigned, r.heard = desired, true
	for id, a := range desired {
		st, known := r.statuses[id]
		if !known {
			st = workloadStatus{phase: dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_PENDING, message: "assignment received"}
			r.log.Info("workload assigned", "workload_id", id, "name", a.GetName(), "image", a.GetImage(), "generation", a.GetGeneration())
		}
		st.acked = max(st.acked, a.GetGeneration())
		r.statuses[id] = st
	}
	for id := range r.statuses {
		if _, ok := desired[id]; !ok {
			delete(r.statuses, id)
		}
	}
	r.mu.Unlock()
	r.poke()
}

// Statuses returns the report for every assigned workload, ordered by ID.
func (r *runner) Statuses() []*dockfleetv1.WorkloadStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]string, 0, len(r.statuses))
	for id := range r.statuses {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]*dockfleetv1.WorkloadStatus, 0, len(ids))
	for _, id := range ids {
		out = append(out, r.statuses[id].proto(id))
	}
	return out
}

func (r *runner) poke() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Run converges until ctx ends: after every new assignment set and at least
// every Resync, which is how a container that died is noticed. It leaves the
// containers as they are when it stops, so they survive an agent restart.
func (r *runner) Run(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.Resync)
	defer ticker.Stop()
	for {
		r.converge(ctx)
		select {
		case <-ctx.Done():
			return
		case <-r.wake:
		case <-ticker.C:
		}
	}
}

// converge makes one pass over the assignments.
func (r *runner) converge(ctx context.Context) {
	r.mu.Lock()
	if !r.heard {
		r.mu.Unlock()
		return
	}
	desired := make([]*dockfleetv1.WorkloadAssignment, 0, len(r.assigned))
	for _, a := range r.assigned {
		desired = append(desired, a)
	}
	r.mu.Unlock()
	sort.Slice(desired, func(i, j int) bool { return desired[i].GetWorkloadId() < desired[j].GetWorkloadId() })

	containers, err := r.rt.List(ctx)
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("listing containers failed; trying again", "error", err)
		}
		return
	}
	existing := make(map[string]docker.Container, len(containers))
	for _, c := range containers {
		existing[c.WorkloadID] = c
	}

	assignedIDs := make(map[string]bool, len(desired))
	for _, a := range desired {
		if ctx.Err() != nil {
			return
		}
		id := a.GetWorkloadId()
		assignedIDs[id] = true
		c, has := existing[id]
		r.setStatus(id, r.apply(ctx, a, c, has))
	}
	for id := range r.trackers {
		if !assignedIDs[id] {
			delete(r.trackers, id)
		}
	}
	r.sweepOrphans(ctx, existing, assignedIDs)
}

// setStatus publishes the outcome of a pass, unless the workload was
// unassigned meanwhile.
func (r *runner) setStatus(id string, st workloadStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cur, ok := r.assigned[id]
	if !ok {
		return
	}
	prev := r.statuses[id]
	st.acked = max(prev.acked, cur.GetGeneration())
	r.statuses[id] = st
	if !st.sameObservation(prev) {
		r.log.Info("workload status", "workload_id", id, "phase", st.phase.String(), "message", st.message, "container", st.containerID)
	}
}

func (r *runner) tracker(id string) *tracker {
	tr, ok := r.trackers[id]
	if !ok {
		tr = &tracker{}
		r.trackers[id] = tr
	}
	return tr
}

func (r *runner) do(ctx context.Context, timeout time.Duration, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return fn(ctx)
}

func (r *runner) apply(ctx context.Context, a *dockfleetv1.WorkloadAssignment, c docker.Container, has bool) workloadStatus {
	if a.GetDesiredState() == dockfleetv1.WorkloadDesiredState_WORKLOAD_DESIRED_STATE_STOPPED {
		return r.retire(ctx, a.GetWorkloadId(), c, has)
	}
	return r.run(ctx, a, c, has)
}

// retire stops and removes the container of a workload that is meant to be
// stopped. Both steps ignore a container that is already gone, so repeating
// them is harmless; the report says "stopped, no container" only once that is true.
func (r *runner) retire(ctx context.Context, id string, c docker.Container, has bool) workloadStatus {
	stopped := dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_STOPPED
	if !has {
		return workloadStatus{phase: stopped, message: "stopped; the container is removed"}
	}
	if c.State == docker.StateRunning || c.State == docker.StateRestarting || c.State == docker.StatePaused {
		err := r.do(ctx, r.cfg.StopTimeout+r.cfg.OpTimeout, func(ctx context.Context) error {
			return r.rt.Stop(ctx, id, r.cfg.StopTimeout)
		})
		if err != nil && !errors.Is(err, docker.ErrNotFound) {
			return workloadStatus{phase: dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_RUNNING, message: clip("stopping failed: " + err.Error()), containerID: c.ID}
		}
	}
	err := r.do(ctx, r.cfg.OpTimeout, func(ctx context.Context) error { return r.rt.Remove(ctx, id, true) })
	if err != nil && !errors.Is(err, docker.ErrNotFound) {
		return workloadStatus{phase: stopped, message: clip("removing failed: " + err.Error()), containerID: c.ID}
	}
	return workloadStatus{phase: stopped, message: "stopped; the container is removed"}
}

// run brings the workload's container to "exists and runs" and applies the restart policy.
func (r *runner) run(ctx context.Context, a *dockfleetv1.WorkloadAssignment, c docker.Container, has bool) workloadStatus {
	id := a.GetWorkloadId()
	tr := r.tracker(id)
	if !has {
		return r.create(ctx, a, tr)
	}
	var cs docker.ContainerState
	err := r.do(ctx, r.cfg.OpTimeout, func(ctx context.Context) error {
		var err error
		cs, err = r.rt.Inspect(ctx, id)
		return err
	})
	pending := dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_PENDING
	switch {
	case errors.Is(err, docker.ErrNotFound):
		return workloadStatus{phase: pending, message: "the container disappeared; creating it again"}
	case err != nil:
		return workloadStatus{phase: pending, message: clip("inspecting failed: " + err.Error()), containerID: c.ID}
	}

	switch cs.State {
	case docker.StateRunning:
		tr.attempts, tr.failure = 0, workloadStatus{}
		return workloadStatus{phase: dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_RUNNING, message: "running", containerID: cs.ID, restartCount: int32(tr.restarts)}
	case docker.StateCreated:
		// Created but never started: the agent stopped between the two steps.
		return r.start(ctx, id, cs.ID, tr)
	case docker.StateExited:
		return r.exited(ctx, a, cs, tr)
	case docker.StateDead:
		// A dead container cannot be started again; remove it so the next pass creates a new one.
		_ = r.do(ctx, r.cfg.OpTimeout, func(ctx context.Context) error { return r.rt.Remove(ctx, id, true) })
		return workloadStatus{phase: pending, message: "the container died; creating it again", containerID: cs.ID}
	}
	return workloadStatus{phase: pending, message: fmt.Sprintf("the container is %s", cs.State), containerID: cs.ID}
}

// create makes the container, pulling the image when the node lacks it, and starts it.
func (r *runner) create(ctx context.Context, a *dockfleetv1.WorkloadAssignment, tr *tracker) workloadStatus {
	if r.now().Before(tr.retryAt) && tr.failure.phase != dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_UNSPECIFIED {
		return tr.failure
	}
	spec, err := specFor(a)
	if err != nil {
		return r.fail(tr, "invalid assignment: "+err.Error())
	}
	create := func() error {
		return r.do(ctx, r.cfg.OpTimeout, func(ctx context.Context) error {
			_, err := r.rt.Create(ctx, spec)
			return err
		})
	}
	err = create()
	if errors.Is(err, docker.ErrImageNotFound) {
		r.log.Info("pulling image", "workload_id", spec.WorkloadID, "image", spec.Image)
		if perr := r.do(ctx, r.cfg.PullTimeout, func(ctx context.Context) error { return r.rt.PullImage(ctx, spec.Image) }); perr != nil {
			return r.fail(tr, "pulling the image failed: "+perr.Error())
		}
		err = create()
	}
	switch {
	case err == nil:
	case errors.Is(err, docker.ErrRejected):
		return r.fail(tr, "rejected by the container policy: "+err.Error())
	default:
		return r.fail(tr, "creating the container failed: "+err.Error())
	}
	return r.start(ctx, spec.WorkloadID, "", tr)
}

// start starts a container that exists. containerID is for the report only.
func (r *runner) start(ctx context.Context, id, containerID string, tr *tracker) workloadStatus {
	if r.now().Before(tr.retryAt) && tr.failure.phase != dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_UNSPECIFIED {
		st := tr.failure
		st.containerID = containerID
		return st
	}
	err := r.do(ctx, r.cfg.OpTimeout, func(ctx context.Context) error { return r.rt.Start(ctx, id) })
	if err != nil {
		st := r.fail(tr, "starting the container failed: "+err.Error())
		st.containerID = containerID
		return st
	}
	tr.attempts, tr.failure = 0, workloadStatus{}
	return workloadStatus{phase: dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_PENDING, message: "starting", containerID: containerID, restartCount: int32(tr.restarts)}
}

// exited decides what happens to a container that stopped on its own.
func (r *runner) exited(ctx context.Context, a *dockfleetv1.WorkloadAssignment, cs docker.ContainerState, tr *tracker) workloadStatus {
	failed := cs.ExitCode != 0 || cs.OOMKilled
	exit := int32(cs.ExitCode)
	what := fmt.Sprintf("exited with code %d", cs.ExitCode)
	if cs.OOMKilled {
		what = "killed: out of memory"
	}
	st := workloadStatus{containerID: cs.ID, exitCode: &exit, oomKilled: cs.OOMKilled, restartCount: int32(tr.restarts)}
	st.phase = dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_SUCCEEDED
	if failed {
		st.phase = dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_FAILED
	}

	condition, retries := restartPolicy(a)
	restart := condition == "always" || (condition == "on-failure" && failed && tr.restarts < retries)
	if !restart {
		st.message = what
		return st
	}
	if r.now().Before(tr.restartAt) {
		st.message = what + "; restart pending"
		return st
	}
	id := a.GetWorkloadId()
	err := r.do(ctx, r.cfg.OpTimeout, func(ctx context.Context) error { return r.rt.Start(ctx, id) })
	if err != nil {
		failure := r.fail(tr, "restarting the container failed: "+err.Error())
		failure.containerID = cs.ID
		return failure
	}
	tr.restarts++
	tr.restartAt = r.now().Add(r.backoff(tr.restarts))
	st.phase = dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_PENDING
	st.message = "restarting"
	st.restartCount = int32(tr.restarts)
	return st
}

// fail records a failed attempt and when the next one may happen.
func (r *runner) fail(tr *tracker, msg string) workloadStatus {
	tr.attempts++
	tr.retryAt = r.now().Add(r.backoff(tr.attempts))
	tr.failure = workloadStatus{phase: dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_FAILED, message: clip(msg)}
	return tr.failure
}

// backoff is the wait after the n-th failure in a row: BackoffBase, doubling, up to BackoffMax.
func (r *runner) backoff(n int) time.Duration {
	d := r.cfg.BackoffBase
	for i := 1; i < n && d < r.cfg.BackoffMax; i++ {
		d *= 2
	}
	return min(d, r.cfg.BackoffMax)
}

// sweepOrphans removes managed containers of this node that the manager no
// longer assigns. A container must be unassigned for OrphanGrace before it goes,
// which keeps one odd response from costing a running service.
func (r *runner) sweepOrphans(ctx context.Context, existing map[string]docker.Container, assignedIDs map[string]bool) {
	now := r.now()
	for id := range r.orphans {
		if _, ok := existing[id]; !ok || assignedIDs[id] {
			delete(r.orphans, id)
		}
	}
	var ids []string
	for id := range existing {
		if !assignedIDs[id] {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		first, seen := r.orphans[id]
		if !seen {
			r.orphans[id] = now
			r.log.Warn("a managed container has no assignment; it is removed if that lasts", "workload_id", id, "grace", r.cfg.OrphanGrace)
			continue
		}
		if now.Sub(first) < r.cfg.OrphanGrace || ctx.Err() != nil {
			continue
		}
		r.log.Warn("removing a managed container the manager does not assign", "workload_id", id)
		if c := existing[id]; c.State == docker.StateRunning {
			_ = r.do(ctx, r.cfg.StopTimeout+r.cfg.OpTimeout, func(ctx context.Context) error { return r.rt.Stop(ctx, id, r.cfg.StopTimeout) })
		}
		err := r.do(ctx, r.cfg.OpTimeout, func(ctx context.Context) error { return r.rt.Remove(ctx, id, true) })
		if err != nil && !errors.Is(err, docker.ErrNotFound) {
			r.log.Warn("removing the container failed; trying again", "workload_id", id, "error", err)
			continue
		}
		delete(r.orphans, id)
	}
}

// restartPolicy is the assignment's restart condition and retry limit; the
// default is "on-failure" with 5 retries.
func restartPolicy(a *dockfleetv1.WorkloadAssignment) (condition string, retries int) {
	p := a.GetRestartPolicy()
	switch p.GetCondition() {
	case "no":
		return "no", 0
	case "always":
		return "always", 0
	}
	retries = int(p.GetMaxRetries())
	if p.GetCondition() != "on-failure" || retries == 0 {
		retries = defaultRestartRetries
	}
	return "on-failure", retries
}

// specFor translates an assignment into a container spec. The container
// policy decides whether the spec is acceptable.
func specFor(a *dockfleetv1.WorkloadAssignment) (docker.ContainerSpec, error) {
	spec := docker.ContainerSpec{
		WorkloadID: a.GetWorkloadId(), WorkloadName: a.GetName(), Image: a.GetImage(),
		Command: a.GetCommand(), Env: a.GetEnv(),
		CPUMillis: a.GetCpuMillis(), MemoryBytes: a.GetMemoryBytes(),
	}
	for _, p := range a.GetPorts() {
		if p.GetContainerPort() == 0 || p.GetContainerPort() > 65535 || p.GetHostPort() > 65535 {
			return docker.ContainerSpec{}, fmt.Errorf("port %d:%d is out of range", p.GetHostPort(), p.GetContainerPort())
		}
		spec.Ports = append(spec.Ports, docker.PortMapping{
			ContainerPort: uint16(p.GetContainerPort()), HostPort: uint16(p.GetHostPort()), Protocol: p.GetProtocol(),
		})
	}
	return spec, nil
}

// clip shortens a message to what the protocol accepts, keeping it valid UTF-8.
func clip(msg string) string {
	if len(msg) > maxStatusText {
		msg = msg[:maxStatusText]
	}
	return strings.ToValidUTF8(msg, "")
}
