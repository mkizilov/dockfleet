package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/dockfleet/dockfleet/internal/agent"
	"github.com/dockfleet/dockfleet/internal/config"
	"github.com/dockfleet/dockfleet/internal/docker"
	"github.com/dockfleet/dockfleet/internal/healthcheck"
	"github.com/dockfleet/dockfleet/internal/logging"
	"github.com/dockfleet/dockfleet/internal/pki"
	"github.com/dockfleet/dockfleet/internal/protocol"
	"github.com/dockfleet/dockfleet/internal/version"
)

const certCheckInterval = time.Hour

// engine is the node's container runtime and the means to let go of it.
type engine interface {
	docker.Runtime
	Close() error
}

func main() {
	if len(os.Args) > 1 {
		if os.Args[1] == "healthcheck" && len(os.Args) == 2 {
			if err := probeHealth(); err != nil {
				fmt.Fprintln(os.Stderr, "dockfleet-agent: unhealthy:", err)
				os.Exit(1)
			}
			return
		}
		fmt.Fprintln(os.Stderr, "usage: dockfleet-agent [healthcheck]")
		os.Exit(2)
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "dockfleet-agent:", err)
		os.Exit(1)
	}
}

// probeHealth is `dockfleet-agent healthcheck`, the HEALTHCHECK of the image: it
// asks the agent's own health endpoint whether the agent is ready.
func probeHealth() error {
	addr := config.AgentHealthAddr(os.Getenv)
	if addr == "" {
		return errors.New("the health endpoint is turned off (DOCKFLEET_HEALTH_ADDR=off)")
	}
	url, err := healthcheck.URLFor(addr, false, "/readyz")
	if err != nil {
		return err
	}
	return healthcheck.Probe{URL: url}.Run(context.Background())
}

// checkIdentity refuses to start with a certificate that is unusable right
// now or that belongs to a different node than the configured one.
func checkIdentity(cfg config.Agent) error {
	cert, err := pki.ReadCertificate(cfg.TLSCertFile)
	if err != nil {
		return err
	}
	if err := pki.CheckValidAt(cert, time.Now()); err != nil {
		return fmt.Errorf("agent certificate %s: %w; renew it with your CA", cfg.TLSCertFile, err)
	}
	id, err := pki.NodeIDFromCert(cert)
	if err != nil {
		return fmt.Errorf("agent certificate %s: %w", cfg.TLSCertFile, err)
	}
	if id != cfg.NodeID {
		return fmt.Errorf("agent certificate %s is for node %q but DOCKFLEET_NODE_ID is %q; they must match", cfg.TLSCertFile, id, cfg.NodeID)
	}
	return nil
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

	engine, err := newEngine(cfg, logger)
	if err != nil {
		return err
	}
	defer engine.Close()

	clientCfg := protocol.ClientConfig{Addrs: cfg.ManagerAddrs, Token: cfg.Token}
	if cfg.TLSEnabled() {
		if err := checkIdentity(cfg); err != nil {
			return err
		}
		var tlsConfig *tls.Config
		tlsConfig, err = pki.ClientTLS(pki.ClientConfig{
			CAFile:     cfg.TLSCAFile,
			CertFile:   cfg.TLSCertFile,
			KeyFile:    cfg.TLSKeyFile,
			ServerName: cfg.TLSServerName,
			Logger:     logger,
		})
		if err != nil {
			return err
		}
		clientCfg.TLS = tlsConfig
		clientCfg.Token = ""

		watchCtx, stopWatching := context.WithCancel(ctx)
		var watchers sync.WaitGroup
		watchers.Add(1)
		go func() {
			defer watchers.Done()
			pki.WatchExpiry(watchCtx, logger, "agent", cfg.TLSCertFile, pki.DefaultWarnWithin, certCheckInterval)
		}()
		defer func() {
			stopWatching()
			watchers.Wait()
		}()
	}
	client, err := protocol.Dial(clientCfg)
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
	logger.Info("starting dockfleet agent", "version", version.Version, "node_id", cfg.NodeID, "manager_addrs", cfg.ManagerAddrs, "mutual_tls", cfg.TLSEnabled())
	// The agent treats zero as "the default" and a negative number as "none";
	// in the environment zero means none.
	logRequests := cfg.LogRequests
	if logRequests == 0 {
		logRequests = -1
	}
	a := agent.New(agent.Config{
		Node:             node,
		Manager:          client,
		Docker:           engine,
		Logger:           logger,
		RPCTimeout:       cfg.RPCTimeout,
		FenceOnLeaseLoss: cfg.FenceOnLeaseLoss,
		LogRequests:      logRequests,
	})
	if cfg.HealthAddr != "" {
		stopHealth, err := serveHealth(cfg.HealthAddr, a, logger)
		if err != nil {
			return err
		}
		defer stopHealth()
	}
	return a.Run(ctx)
}

// serveHealth serves the agent's health endpoint on a loopback address and
// returns the function that stops it.
func serveHealth(addr string, a *agent.Agent, logger *slog.Logger) (stop func(), err error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on the health address: %w", err)
	}
	srv := &http.Server{Handler: a.HealthHandler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("the health endpoint stopped", "error", err)
		}
	}()
	logger.Info("health endpoint", "addr", ln.Addr().String())
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-done
	}, nil
}
