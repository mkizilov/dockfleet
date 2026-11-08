//go:build !e2efake

package main

import (
	"fmt"
	"log/slog"

	"github.com/dockfleet/dockfleet/internal/config"
	"github.com/dockfleet/dockfleet/internal/docker"
)

// newEngine connects to the node's Docker Engine. Only the agent talks to it;
// the manager never does.
func newEngine(cfg config.Agent, _ *slog.Logger) (engine, error) {
	policy, err := docker.NewPolicy(docker.PolicyConfig{
		AllowedBindRoots:    cfg.AllowedBindRoots,
		AllowedCapabilities: cfg.AllowedCapabilities,
	})
	if err != nil {
		return nil, fmt.Errorf("container policy: %w", err)
	}
	return docker.NewAdapter(docker.AdapterConfig{Host: cfg.DockerHost, NodeID: cfg.NodeID, Policy: policy})
}
