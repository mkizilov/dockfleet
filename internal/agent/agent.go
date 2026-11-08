package agent

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/dockfleet/dockfleet/internal/docker"
	"github.com/dockfleet/dockfleet/internal/protocol"
	dockfleetv1 "github.com/dockfleet/dockfleet/internal/protocol/dockfleet/v1"
)

const (
	defaultRetryMin   = time.Second
	defaultRetryMax   = 30 * time.Second
	defaultRPCTimeout = 5 * time.Second
	defaultHeartbeat  = 10 * time.Second
	dockerPingWait    = 5 * time.Second

	// No container runtime exists yet, so assigned workloads stay pending.
	runtimeMissing = "accepted; container runtime is not implemented yet"
)

// Manager is the agent's view of the control plane; *protocol.Client satisfies it.
type Manager interface {
	Register(ctx context.Context, n protocol.NodeInfo) (protocol.Session, error)
	Heartbeat(ctx context.Context, nodeID, sessionID string, statuses []*dockfleetv1.WorkloadStatus) ([]*dockfleetv1.WorkloadAssignment, error)
	Deregister(ctx context.Context, nodeID, sessionID string) error
}

// Config assembles an Agent.
type Config struct {
	Node    protocol.NodeInfo
	Manager Manager
	Docker  docker.Client
	Logger  *slog.Logger
	// RPCTimeout is the deadline for each manager call; zero means 5 seconds.
	RPCTimeout time.Duration
	// RetryMin and RetryMax bound the registration backoff; zero means 1s and 30s.
	RetryMin time.Duration
	RetryMax time.Duration
}

// Agent keeps one node connected to the manager until its context is
// canceled: it registers, heartbeats, mirrors assignments into status reports,
// and registers again whenever the manager loses the session. It does not run
// containers yet.
type Agent struct {
	cfg      Config
	statuses map[string]*dockfleetv1.WorkloadStatus
}

func New(cfg Config) *Agent {
	if cfg.RPCTimeout <= 0 {
		cfg.RPCTimeout = defaultRPCTimeout
	}
	if cfg.RetryMin <= 0 {
		cfg.RetryMin = defaultRetryMin
	}
	if cfg.RetryMax <= 0 {
		cfg.RetryMax = defaultRetryMax
	}
	return &Agent{cfg: cfg, statuses: map[string]*dockfleetv1.WorkloadStatus{}}
}

// Run blocks until ctx is canceled and then returns nil after deregistering
// from the manager. Transient manager outages are retried with exponential
// backoff and never end the agent. It returns an error only for failures that
// retrying cannot fix: an incompatible protocol version, an invalid
// registration, a node ID that differs from the client certificate, or
// rejected credentials. A node that is not enrolled, or whose certificate is
// expired, is retried: an administrator enrolling it or renewing the
// certificate fixes it without restarting the agent.
func (a *Agent) Run(ctx context.Context) error {
	a.checkDocker(ctx)
	for {
		session, err := a.register(ctx)
		if err != nil {
			if ctx.Err() != nil {
				a.cfg.Logger.Info("agent stopped before registering")
				return nil
			}
			return err
		}
		lost, err := a.serve(ctx, session)
		if err != nil {
			return err
		}
		if !lost {
			a.deregister(ctx, session)
			a.cfg.Logger.Info("agent stopped")
			return nil
		}
	}
}

func (a *Agent) checkDocker(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, dockerPingWait)
	defer cancel()
	if err := a.cfg.Docker.Ping(ctx); err != nil {
		a.cfg.Logger.Warn("docker engine is not reachable", "error", err)
		return
	}
	a.cfg.Logger.Info("docker engine reachable")
}

// register retries until it succeeds, ctx ends, or the manager rejects the
// registration permanently.
func (a *Agent) register(ctx context.Context) (protocol.Session, error) {
	delay := a.cfg.RetryMin
	for {
		callCtx, cancel := context.WithTimeout(ctx, a.cfg.RPCTimeout)
		session, err := a.cfg.Manager.Register(callCtx, a.cfg.Node)
		cancel()
		if err == nil {
			a.cfg.Logger.Info("registered with manager", "node_id", a.cfg.Node.ID, "session", session.ID,
				"heartbeat_interval", session.HeartbeatInterval, "assignments", len(session.Assignments))
			return session, nil
		}
		if ctx.Err() != nil {
			return protocol.Session{}, ctx.Err()
		}
		if protocol.IsPermanent(err) {
			a.cfg.Logger.Error("manager rejected registration", "error", err)
			return protocol.Session{}, fmt.Errorf("registration rejected: %w", err)
		}
		if protocol.IsNotEnrolled(err) {
			a.cfg.Logger.Error("this node is not enrolled; an administrator must enroll it", "node_id", a.cfg.Node.ID, "retry_in", delay)
		} else {
			a.cfg.Logger.Warn("registration failed", "error", err, "retry_in", delay)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return protocol.Session{}, ctx.Err()
		case <-timer.C:
		}
		delay = min(delay*2, a.cfg.RetryMax)
	}
}

// serve heartbeats until ctx ends (false, nil), the manager no longer
// recognizes the session (true, nil: the caller registers again), or the
// manager reports a failure that retrying cannot fix (an error).
func (a *Agent) serve(ctx context.Context, session protocol.Session) (sessionLost bool, err error) {
	a.reconcile(session.Assignments)
	interval := session.HeartbeatInterval
	if interval <= 0 {
		interval = defaultHeartbeat
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false, nil
		case <-ticker.C:
		}

		callCtx, cancel := context.WithTimeout(ctx, a.cfg.RPCTimeout)
		assigned, hbErr := a.cfg.Manager.Heartbeat(callCtx, a.cfg.Node.ID, session.ID, a.snapshot())
		cancel()
		switch {
		case hbErr == nil:
			a.reconcile(assigned)
		case ctx.Err() != nil:
			return false, nil
		case protocol.NeedsRegistration(hbErr):
			a.cfg.Logger.Warn("manager no longer recognizes this session; registering again", "error", hbErr)
			return true, nil
		case protocol.IsPermanent(hbErr):
			a.cfg.Logger.Error("manager rejected the agent; giving up", "error", hbErr)
			return false, fmt.Errorf("heartbeat rejected: %w", hbErr)
		default:
			a.cfg.Logger.Warn("heartbeat failed", "error", hbErr)
		}
	}
}

// deregister tells the manager about a clean shutdown. The run context is
// already canceled, so it gets its own deadline.
func (a *Agent) deregister(ctx context.Context, session protocol.Session) {
	callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.cfg.RPCTimeout)
	defer cancel()
	if err := a.cfg.Manager.Deregister(callCtx, a.cfg.Node.ID, session.ID); err != nil {
		a.cfg.Logger.Warn("deregistration failed", "error", err)
		return
	}
	a.cfg.Logger.Info("deregistered from manager")
}

// reconcile aligns the local status table with the assignments the manager
// returned: new workloads start pending and unassigned ones are forgotten.
func (a *Agent) reconcile(assigned []*dockfleetv1.WorkloadAssignment) {
	desired := make(map[string]struct{}, len(assigned))
	for _, w := range assigned {
		id := w.GetWorkloadId()
		desired[id] = struct{}{}
		if _, known := a.statuses[id]; known {
			continue
		}
		a.statuses[id] = &dockfleetv1.WorkloadStatus{
			WorkloadId: id,
			Phase:      dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_PENDING,
			Message:    runtimeMissing,
		}
		a.cfg.Logger.Info("workload assigned", "workload_id", id, "name", w.GetName(), "image", w.GetImage())
	}
	for id := range a.statuses {
		if _, ok := desired[id]; !ok {
			delete(a.statuses, id)
			a.cfg.Logger.Info("workload unassigned", "workload_id", id)
		}
	}
}

// snapshot returns the current status table ordered by workload ID.
func (a *Agent) snapshot() []*dockfleetv1.WorkloadStatus {
	out := make([]*dockfleetv1.WorkloadStatus, 0, len(a.statuses))
	for _, st := range a.statuses {
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetWorkloadId() < out[j].GetWorkloadId() })
	return out
}
