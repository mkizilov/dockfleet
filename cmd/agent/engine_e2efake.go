//go:build e2efake

package main

import (
	"log/slog"

	"github.com/dockfleet/dockfleet/internal/config"
	"github.com/dockfleet/dockfleet/internal/dockerfake"
)

// This file is compiled only into the agent the end-to-end tests build
// (`go build -tags e2efake ./cmd/agent`): the real agent, with its
// configuration, mutual TLS, health endpoint and shutdown, but with its
// containers in memory, because those tests run where there is no Docker daemon
// to use. Nothing a release builds contains it, and a test pins that.
func newEngine(cfg config.Agent, logger *slog.Logger) (engine, error) {
	logger.Warn("THIS AGENT KEEPS ITS CONTAINERS IN MEMORY: it was built with -tags e2efake for tests and must never be deployed")
	return dockerfake.New(cfg.NodeID), nil
}
