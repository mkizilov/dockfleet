package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/dockfleet/dockfleet/internal/config"
	"github.com/dockfleet/dockfleet/internal/etcd"
	"github.com/dockfleet/dockfleet/internal/healthcheck"
	"github.com/dockfleet/dockfleet/internal/logging"
	"github.com/dockfleet/dockfleet/internal/manager"
	"github.com/dockfleet/dockfleet/internal/pki"
	"github.com/dockfleet/dockfleet/internal/store"
	"github.com/dockfleet/dockfleet/internal/version"
)

func main() {
	if len(os.Args) > 1 {
		if os.Args[1] == "healthcheck" && len(os.Args) == 2 {
			if err := probeHealth(); err != nil {
				fmt.Fprintln(os.Stderr, "dockfleet-manager: unhealthy:", err)
				os.Exit(1)
			}
			return
		}
		fmt.Fprintln(os.Stderr, "usage: dockfleet-manager [healthcheck]")
		os.Exit(2)
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "dockfleet-manager:", err)
		os.Exit(1)
	}
}

// probeHealth is `dockfleet-manager healthcheck`, the HEALTHCHECK of the image:
// it asks the manager's own /readyz, which is ready while the manager can reach
// its store. Over TLS it trusts the certificate the manager itself serves
// (DOCKFLEET_TLS_CERT_FILE) and nothing else, and sends no credential.
func probeHealth() error {
	addr := os.Getenv("DOCKFLEET_API_ADDR")
	if addr == "" {
		addr = config.DefaultAPIAddr
	}
	certFile := os.Getenv("DOCKFLEET_TLS_CERT_FILE")
	url, err := healthcheck.URLFor(addr, certFile != "", "/readyz")
	if err != nil {
		return err
	}
	return healthcheck.Probe{URL: url, CertFile: certFile, ServerName: os.Getenv("DOCKFLEET_HEALTHCHECK_SERVER_NAME")}.Run(context.Background())
}

func run() error {
	cfg, err := config.LoadManager(os.Getenv)
	if err != nil {
		return fmt.Errorf("invalid configuration:\n%w", err)
	}
	logger, err := logging.New(os.Stderr, cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.TLSEnabled() && cfg.AgentToken != "" {
		logger.Warn("DOCKFLEET_AGENT_TOKEN is ignored: agents authenticate with their client certificate")
	}

	var st store.Store
	switch cfg.Store {
	case "postgres":
		pg, err := store.OpenPostgres(ctx, cfg.DatabaseURL)
		if err != nil {
			return err
		}
		if err := pg.Migrate(ctx); err != nil {
			pg.Close()
			return err
		}
		st = pg
	default:
		logger.Warn("using the in-memory store; state is lost on exit")
		st = store.NewMemory()
	}
	defer st.Close()

	var opts []manager.Option
	if cfg.HAEnabled() {
		backend, err := newElectionBackend(cfg, logger)
		if err != nil {
			return err
		}
		defer backend.Close()
		opts = append(opts, manager.WithElection(backend))
	}
	m, err := manager.New(cfg, st, logger, opts...)
	if err != nil {
		return err
	}

	var lc net.ListenConfig
	apiLn, err := lc.Listen(ctx, "tcp", cfg.APIAddr)
	if err != nil {
		return fmt.Errorf("listen on API address: %w", err)
	}
	grpcLn, err := lc.Listen(ctx, "tcp", cfg.GRPCAddr)
	if err != nil {
		_ = apiLn.Close()
		return fmt.Errorf("listen on gRPC address: %w", err)
	}

	logger.Info("starting dockfleet manager", "version", version.Version, "store", cfg.Store, "instance_id", cfg.InstanceID,
		"high_availability", cfg.HAEnabled(), "etcd_endpoints", len(cfg.EtcdEndpoints))
	return m.Serve(ctx, apiLn, grpcLn)
}

// newElectionBackend connects to etcd, with mutual TLS unless it is on loopback.
func newElectionBackend(cfg config.Manager, logger *slog.Logger) (*etcd.Client, error) {
	var tlsConfig *tls.Config
	if cfg.EtcdTLSEnabled() {
		var err error
		tlsConfig, err = pki.ClientTLS(pki.ClientConfig{CAFile: cfg.EtcdCAFile, CertFile: cfg.EtcdCertFile, KeyFile: cfg.EtcdKeyFile, Logger: logger})
		if err != nil {
			return nil, fmt.Errorf("etcd TLS: %w", err)
		}
	}
	client, err := etcd.New(etcd.Config{Endpoints: cfg.EtcdEndpoints, TLS: tlsConfig})
	if err != nil {
		return nil, err
	}
	return client, nil
}
