package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/dockfleet/dockfleet/internal/agent"
	"github.com/dockfleet/dockfleet/internal/config"
	"github.com/dockfleet/dockfleet/internal/docker"
	"github.com/dockfleet/dockfleet/internal/logging"
	"github.com/dockfleet/dockfleet/internal/protocol"
	"github.com/dockfleet/dockfleet/internal/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "dockfleet-agent:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadAgent(os.Getenv)
	if err != nil {
		return fmt.Errorf("invalid configuration:\n%w", err)
	}
	logger, err := logging.New(os.Stderr, cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	engine, err := docker.NewEngine(cfg.DockerHost)
	if err != nil {
		return err
	}
	defer engine.Close()

	var tlsConfig *tls.Config
	if cfg.TLSCAFile != "" {
		tlsConfig, err = protocol.ClientTLSConfig(cfg.TLSCAFile)
		if err != nil {
			return err
		}
	}
	client, err := protocol.Dial(protocol.ClientConfig{Addr: cfg.ManagerAddr, Token: cfg.Token, TLS: tlsConfig})
	if err != nil {
		return err
	}
	defer client.Close()

	node := protocol.NodeInfo{
		ID:           cfg.NodeID,
		Name:         cfg.NodeName,
		CPUMillis:    cfg.CPUMillis,
		MemoryBytes:  cfg.MemoryBytes,
		Labels:       cfg.Labels,
		AgentVersion: version.Version,
	}
	logger.Info("starting dockfleet agent", "version", version.Version, "node_id", cfg.NodeID, "manager_addr", cfg.ManagerAddr)
	return agent.New(agent.Config{
		Node:       node,
		Manager:    client,
		Docker:     engine,
		Logger:     logger,
		RPCTimeout: cfg.RPCTimeout,
	}).Run(ctx)
}
