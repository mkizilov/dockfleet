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

	// defaultStableAfter is how long a restarted container must run before the
	// restarts in a row are forgotten and the next failure starts over with the
	// shortest wait.
	defaultStableAfter = 30 * time.Second

	// Defaults for a health check that arrives without them.
	defaultHealthInterval = 30 * time.Second
	defaultHealthTimeout  = 5 * time.Second
	defaultHealthRetries  = 3

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
	// StableAfter is how long a restarted container must keep running before
	// its restarts in a row are forgotten.
	StableAfter time.Duration
	// Fencing makes the runner stop every container of the node when the
	// manager's lease runs out (see RenewLease). Off, containers outlive any
	// outage of the manager.
	Fencing bool
	Now     func() time.Time
}

func (c runnerConfig) withDefaults() runnerConfig {
	for _, d := range []struct {
		field *time.Duration
		value time.Duration
	}{
		{&c.Resync, defaultResync}, {&c.OrphanGrace, defaultOrphanGrace}, {&c.StopTimeout, defaultStopTimeout},
		{&c.OpTimeout, defaultOpTimeout}, {&c.PullTimeout, defaultPullTimeout},
		{&c.BackoffBase, defaultBackoffBase}, {&c.BackoffMax, defaultBackoffMax}, {&c.StableAfter, defaultStableAfter},
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
	health       string // the health check result: empty without a check
	acked        int64  // the highest generation of the assignment seen
}

func (s workloadStatus) proto(id string) *dockfleetv1.WorkloadStatus {
	phase := s.phase
	if phase == dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_UNSPECIFIED {
		phase = dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_PENDING // the manager refuses a report without a phase
	}
	st := &dockfleetv1.WorkloadStatus{
		WorkloadId: id, Phase: phase, Message: s.message, AcknowledgedGeneration: s.acked,
		ContainerId: s.containerID, OomKilled: s.oomKilled, RestartCount: s.restartCount, Health: s.health,
	}
	if s.exitCode != nil {
		v := *s.exitCode
		st.ExitCode = &v
	}
	return st
}

// sameObservation compares everything but the acknowledgment.
func (s workloadStatus) sameObservation(o workloadStatus) bool {
	if s.phase != o.phase || s.message != o.message || s.containerID != o.containerID || s.oomKilled != o.oomKilled || s.restartCount != o.restartCount || s.health != o.health {
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
	restarts     int       // restarts by this agent since it started
	streak       int       // restarts in a row, forgotten once the container has run steadily
	restartAt    time.Time // no restart before this
	runningSince time.Time // when the container was first seen running since its last start
	unhealthy    bool      // the agent stopped the container because its health check fails
	attempts     int       // consecutive failed attempts to create or start
	retryAt      time.Time // no new attempt before this
	failure      workloadStatus
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
	// leaseUntil is when the manager's lease on this node runs out unless a
	// heartbeat renews it; zero until the manager has granted one.
	leaseUntil time.Time

	// Only converge touches these.
	trackers map[string]*tracker
	orphans  map[string]time.Time
	// fenced are the containers stopped because the lease ran out. They are
	// started again, without counting as a failure, once it is renewed.
	fenced map[string]bool
}

func newRunner(rt docker.Runtime, log *slog.Logger, cfg runnerConfig) *runner {
	return &runner{
		rt: rt, log: log, cfg: cfg.withDefaults(), wake: make(chan struct{}, 1),
		statuses: map[string]workloadStatus{}, trackers: map[string]*tracker{}, orphans: map[string]time.Time{}, fenced: map[string]bool{},
	}
}

// RenewLease extends the manager's lease on this node until the given time. The
// agent computes it from when it sent the heartbeat that the manager answered,
// never from when the answer arrived, so the lease ends no later than the
// manager thinks it does.
func (r *runner) RenewLease(until time.Time) {
	r.mu.Lock()
	r.leaseUntil = until
	r.mu.Unlock()
	r.poke()
}

// leaseLapsed is whether fencing is on and the lease has run out.
func (r *runner) leaseLapsed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cfg.Fencing && !r.leaseUntil.IsZero() && r.now().After(r.leaseUntil)
}

// nextWake is when the runner should look at the containers again: after the
// resync period, or sooner if the lease runs out before then.
func (r *runner) nextWake() time.Duration {
	wait := r.cfg.Resync
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cfg.Fencing && !r.leaseUntil.IsZero() {
		if d := r.leaseUntil.Sub(r.now()); d > 0 && d+time.Millisecond < wait {
			wait = d + time.Millisecond
		}
	}
	return wait
}

func (r *runner) now() time.Time { return r.cfg.Now() }

// Assign records the manager's complete set of assignments and wakes the
// runner. Receiving it is the acknowledgment: from now on the workload's
// status carries the assignment's generation. Until the first call the runner
// does nothing at all, so an agent that has not yet heard from the manager
// never removes a container.
//
// An assignment older than the one in force for the same workload is ignored: a
// workload's generation only grows, so a lower one is an instruction that was
// overtaken (from a manager that restarted from an older backup of the database,
// say) and obeying it would undo what was decided since.
func (r *runner) Assign(assigned []*dockfleetv1.WorkloadAssignment) {
	r.mu.Lock()
	desired := make(map[string]*dockfleetv1.WorkloadAssignment, len(assigned))
	for _, a := range assigned {
		id := a.GetWorkloadId()
		if cur, ok := r.assigned[id]; ok && cur.GetGeneration() > a.GetGeneration() {
			r.log.Warn("ignoring an assignment older than the one in force", "workload_id", id, "generation", a.GetGeneration(), "in_force", cur.GetGeneration())
			desired[id] = cur
			continue
		}
		desired[id] = a
	}
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
	timer := time.NewTimer(r.cfg.Resync)
	defer timer.Stop()
	for {
		r.converge(ctx)
		timer.Reset(r.nextWake())
		select {
		case <-ctx.Done():
			return
		case <-r.wake:
		case <-timer.C:
		}
	}
}

// fence stops every running container of the node, with no grace period. It
// is what an agent does when the manager's lease on it has run out: from then
// on the manager may start the node's replicas on other nodes, and a replica must
// not run in two places. The containers are stopped, not removed, so a node that
// reaches the manager again in time loses nothing but the downtime.
func (r *runner) fence(ctx context.Context) {
	containers, err := r.rt.List(ctx)
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("listing containers to fence them failed; trying again", "error", err)
		}
		return
	}
	stopped := 0
	for _, c := range containers {
		if ctx.Err() != nil {
			return
		}
		if c.State != docker.StateRunning && c.State != docker.StateRestarting && c.State != docker.StatePaused {
			continue
		}
		err := r.do(ctx, r.cfg.OpTimeout, func(ctx context.Context) error { return r.rt.Stop(ctx, c.WorkloadID, 0) })
		if err != nil && !errors.Is(err, docker.ErrNotFound) {
			r.log.Warn("stopping a container to fence it failed; trying again", "workload_id", c.WorkloadID, "error", err)
			continue
		}
		r.fenced[c.WorkloadID] = true
		stopped++
		r.setStatus(c.WorkloadID, workloadStatus{
			phase: dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_PENDING, containerID: c.ID,
			message: "stopped: the manager's lease on this node ran out",
		})
	}
	if stopped > 0 {
		r.log.Warn("the manager's lease on this node ran out; containers stopped so that no replica runs twice", "containers", stopped)
	}
}

// converge makes one pass over the assignments.
func (r *runner) converge(ctx context.Context) {
	if r.leaseLapsed() {
		r.fence(ctx)
		return
	}
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
	for id := range r.fenced {
		if !assignedIDs[id] {
			delete(r.fenced, id)
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
		// A container fenced earlier is not started again: the manager moved the workload.
		delete(r.fenced, a.GetWorkloadId())
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
		now := r.now()
		if tr.runningSince.IsZero() {
			tr.runningSince = now
		}
		// A container that has run steadily is not in a crash loop any more: the next
		// failure starts over with the shortest wait and the full number of restarts.
		if tr.streak > 0 && now.Sub(tr.runningSince) >= r.cfg.StableAfter {
			tr.streak, tr.restartAt = 0, time.Time{}
		}
		st := workloadStatus{
			phase: dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_RUNNING, message: "running", containerID: cs.ID,
			restartCount: int32(tr.restarts), health: cs.Health,
		}
		if cs.Health == docker.HealthUnhealthy {
			return r.unhealthy(ctx, a, tr, st)
		}
		tr.unhealthy = false
		return st
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

// unhealthy deals with a container that runs but fails its health check. Docker
// only reports the result, so the agent acts on it: it stops the container and
// lets the restart policy decide what happens next, exactly as for a container
// that crashed (with a restart policy of "no" it only reports it).
func (r *runner) unhealthy(ctx context.Context, a *dockfleetv1.WorkloadAssignment, tr *tracker, st workloadStatus) workloadStatus {
	st.message = "running, but its health check fails"
	if restartPolicy(a).condition == "no" {
		return st
	}
	id := a.GetWorkloadId()
	err := r.do(ctx, r.cfg.StopTimeout+r.cfg.OpTimeout, func(ctx context.Context) error { return r.rt.Stop(ctx, id, r.cfg.StopTimeout) })
	if err != nil && !errors.Is(err, docker.ErrNotFound) {
		st.message = clip("stopping the unhealthy container failed: " + err.Error())
		return st
	}
	tr.unhealthy = true
	st.phase = dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_PENDING
	st.message = "stopped because its health check fails"
	return st
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

// exited decides what happens to a container that stopped: it is started again
// when the restart policy says so, after a wait that doubles with every restart
// in a row, and not at all once the policy's limit is reached.
func (r *runner) exited(ctx context.Context, a *dockfleetv1.WorkloadAssignment, cs docker.ContainerState, tr *tracker) workloadStatus {
	id := a.GetWorkloadId()
	if r.fenced[id] {
		// Stopped by the agent itself while the lease was gone; the manager still
		// wants it running, so it is a pause, not a failure, and costs no restart.
		err := r.do(ctx, r.cfg.OpTimeout, func(ctx context.Context) error { return r.rt.Start(ctx, id) })
		if err != nil {
			failure := r.fail(tr, "starting the container again after the lease was lost failed: "+err.Error())
			failure.containerID = cs.ID
			return failure
		}
		delete(r.fenced, id)
		tr.runningSince = time.Time{}
		return workloadStatus{
			phase: dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_PENDING, message: "started again after the lease was renewed",
			containerID: cs.ID, restartCount: int32(tr.restarts),
		}
	}

	failed := cs.ExitCode != 0 || cs.OOMKilled || tr.unhealthy
	exit := int32(cs.ExitCode)
	what := fmt.Sprintf("exited with code %d", cs.ExitCode)
	switch {
	case cs.OOMKilled:
		what = "killed: out of memory"
	case tr.unhealthy:
		what = fmt.Sprintf("stopped because its health check failed (exit code %d)", cs.ExitCode)
	}
	st := workloadStatus{containerID: cs.ID, exitCode: &exit, oomKilled: cs.OOMKilled, restartCount: int32(tr.restarts)}
	st.phase = dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_SUCCEEDED
	if failed {
		st.phase = dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_FAILED
	}

	policy := restartPolicy(a)
	restart := policy.condition == "always" || (policy.condition == "on-failure" && failed)
	switch {
	case !restart:
		st.message = what
		return st
	case policy.retries > 0 && tr.streak >= policy.retries:
		st.phase = dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_FAILED
		st.message = fmt.Sprintf("%s; gave up after %d restarts in a row", what, tr.streak)
		return st
	case r.now().Before(tr.restartAt):
		st.message = what + "; restart pending"
		return st
	}
	err := r.do(ctx, r.cfg.OpTimeout, func(ctx context.Context) error { return r.rt.Start(ctx, id) })
	if err != nil {
		failure := r.fail(tr, "restarting the container failed: "+err.Error())
		failure.containerID = cs.ID
		return failure
	}
	tr.restarts++
	tr.streak++
	tr.restartAt = r.now().Add(r.restartDelay(policy, tr.streak))
	tr.runningSince = time.Time{}
	tr.unhealthy = false
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

// restartSettings is an assignment's restart policy with the defaults filled in.
type restartSettings struct {
	condition string // "no", "on-failure" or "always"
	// retries is how many restarts in a row are allowed; 0 means no limit.
	retries int
	// base and max bound the wait between restarts; 0 means the runner's own.
	base, max time.Duration
}

// restartPolicy reads the assignment's restart policy. The default is
// "on-failure" with 5 retries; "always" has no limit unless the policy sets one.
func restartPolicy(a *dockfleetv1.WorkloadAssignment) restartSettings {
	p := a.GetRestartPolicy()
	out := restartSettings{
		base: time.Duration(p.GetBackoffSeconds()) * time.Second,
		max:  time.Duration(p.GetMaxBackoffSeconds()) * time.Second,
	}
	switch p.GetCondition() {
	case "no":
		out.condition = "no"
	case "always":
		out.condition, out.retries = "always", int(p.GetMaxRetries())
	default:
		out.condition, out.retries = "on-failure", int(p.GetMaxRetries())
		if out.retries == 0 {
			out.retries = defaultRestartRetries
		}
	}
	return out
}

// restartDelay is the wait after the n-th restart in a row: the policy's base
// (or the runner's), doubling with each one, up to the policy's maximum.
func (r *runner) restartDelay(p restartSettings, n int) time.Duration {
	base, limit := p.base, p.max
	if base <= 0 {
		base = r.cfg.BackoffBase
	}
	if limit <= 0 {
		limit = r.cfg.BackoffMax
	}
	limit = max(limit, base)
	d := base
	for i := 1; i < n && d < limit; i++ {
		d *= 2
	}
	return min(d, limit)
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
	if h := a.GetHealthCheck(); h != nil {
		spec.HealthCheck = &docker.HealthCheck{
			Command:     h.GetCommand(),
			Interval:    secondsOr(h.GetIntervalSeconds(), defaultHealthInterval),
			Timeout:     secondsOr(h.GetTimeoutSeconds(), defaultHealthTimeout),
			StartPeriod: time.Duration(h.GetStartPeriodSeconds()) * time.Second,
			Retries:     int(h.GetRetries()),
		}
		if spec.HealthCheck.Retries == 0 {
			spec.HealthCheck.Retries = defaultHealthRetries
		}
	}
	return spec, nil
}

// secondsOr converts a number of seconds, or returns def when it is 0.
func secondsOr(seconds uint32, def time.Duration) time.Duration {
	if seconds == 0 {
		return def
	}
	return time.Duration(seconds) * time.Second
}

// clip shortens a message to what the protocol accepts, keeping it valid UTF-8.
func clip(msg string) string {
	if len(msg) > maxStatusText {
		msg = msg[:maxStatusText]
	}
	return strings.ToValidUTF8(msg, "")
}
