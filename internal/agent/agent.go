package agent

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
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
	// Docker is the node's container runtime; the agent creates, starts,
	// stops and removes containers through it, and only ever sees the ones
	// DockFleet manages.
	Docker docker.Runtime
	Logger *slog.Logger
	// RPCTimeout is the deadline for each manager call; zero means 5 seconds.
	RPCTimeout time.Duration
	// RetryMin and RetryMax bound the registration backoff; zero means 1s and 30s.
	RetryMin time.Duration
	RetryMax time.Duration
	// Resync is how often containers are checked when nothing else prompts it
	// (zero means 2 seconds). OrphanGrace is how long a managed container may
	// go without an assignment before it is removed (zero means 2 minutes).
	Resync      time.Duration
	OrphanGrace time.Duration
}

// Agent keeps one node connected to the manager until its context is
// canceled: it registers, heartbeats, registers again whenever the manager
// loses the session, and keeps the node's containers in line with the
// assignments it receives, reporting what it finds.
type Agent struct {
	cfg    Config
	runner *runner
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
	return &Agent{cfg: cfg, runner: newRunner(cfg.Docker, cfg.Logger, runnerConfig{Resync: cfg.Resync, OrphanGrace: cfg.OrphanGrace})}
}

// Run blocks until ctx is canceled and then returns nil after deregistering
// from the manager. Transient manager outages are retried with exponential
// backoff and never end the agent. It returns an error only for failures that
// retrying cannot fix: an incompatible protocol version, an invalid
// registration, a node ID that differs from the client certificate, or
// rejected credentials. A node that is not enrolled, or whose certificate is
// expired, is retried: an administrator enrolling it or renewing the
// certificate fixes it without restarting the agent.
//
// Containers are not stopped when the agent stops: they keep running, and the
// next agent that starts on the node finds them and carries on.
func (a *Agent) Run(ctx context.Context) error {
	a.checkDocker(ctx)

	// The runner works on its own so that a slow image pull never delays a heartbeat.
	runCtx, stopRunner := context.WithCancel(ctx)
	var runnerDone sync.WaitGroup
	runnerDone.Add(1)
	go func() {
		defer runnerDone.Done()
		a.runner.Run(runCtx)
	}()
	defer func() {
		stopRunner()
		runnerDone.Wait()
	}()

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
	a.runner.Assign(session.Assignments)
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
		assigned, hbErr := a.cfg.Manager.Heartbeat(callCtx, a.cfg.Node.ID, session.ID, a.runner.Statuses())
		cancel()
		switch {
		case hbErr == nil:
			a.runner.Assign(assigned)
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
