package agent

import (
	"context"
	"log/slog"
	"time"

	"github.com/dockfleet/dockfleet/internal/docker"
	"github.com/dockfleet/dockfleet/internal/protocol"
)

const (
	defaultRetryMin  = time.Second
	defaultRetryMax  = 30 * time.Second
	defaultHeartbeat = 10 * time.Second
	dockerPingWait   = 5 * time.Second
)

// Manager is the agent's view of the control plane; *protocol.Client satisfies it.
type Manager interface {
	RegisterNode(ctx context.Context, n protocol.NodeInfo) (time.Duration, error)
	Heartbeat(ctx context.Context, nodeID string) error
}

// Agent registers a node with the manager and keeps it alive until its
// context is canceled. It does not run containers yet.
type Agent struct {
	node     protocol.NodeInfo
	manager  Manager
	docker   docker.Client
	logger   *slog.Logger
	retryMin time.Duration
	retryMax time.Duration
}

func New(node protocol.NodeInfo, manager Manager, dock docker.Client, logger *slog.Logger) *Agent {
	return &Agent{
		node:     node,
		manager:  manager,
		docker:   dock,
		logger:   logger,
		retryMin: defaultRetryMin,
		retryMax: defaultRetryMax,
	}
}

// Run blocks until ctx is canceled, then returns nil. Manager outages are
// retried with exponential backoff and never terminate the agent.
func (a *Agent) Run(ctx context.Context) error {
	a.checkDocker(ctx)

	interval, ok := a.register(ctx)
	if !ok {
		return nil
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			a.logger.Info("agent stopping")
			return nil
		case <-ticker.C:
		}

		err := a.manager.Heartbeat(ctx, a.node.ID)
		switch {
		case err == nil:
		case ctx.Err() != nil:
			return nil
		case protocol.IsNotRegistered(err):
			a.logger.Warn("manager no longer knows this node; re-registering")
			next, ok := a.register(ctx)
			if !ok {
				return nil
			}
			if next != interval {
				interval = next
				ticker.Reset(interval)
			}
		default:
			a.logger.Warn("heartbeat failed", "error", err)
		}
	}
}

func (a *Agent) checkDocker(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, dockerPingWait)
	defer cancel()
	if err := a.docker.Ping(ctx); err != nil {
		a.logger.Warn("docker engine is not reachable", "error", err)
		return
	}
	a.logger.Info("docker engine reachable")
}

// register retries until it succeeds. ok is false when ctx ended first.
func (a *Agent) register(ctx context.Context) (time.Duration, bool) {
	delay := a.retryMin
	for {
		interval, err := a.manager.RegisterNode(ctx, a.node)
		if err == nil {
			if interval <= 0 {
				interval = defaultHeartbeat
			}
			a.logger.Info("registered with manager", "node_id", a.node.ID, "heartbeat_interval", interval)
			return interval, true
		}
		if ctx.Err() != nil {
			return 0, false
		}
		a.logger.Warn("registration failed", "error", err, "retry_in", delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, false
		case <-timer.C:
		}
		delay = min(delay*2, a.retryMax)
	}
}
