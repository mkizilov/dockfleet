package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/dockfleet/dockfleet/internal/config"
	"github.com/dockfleet/dockfleet/internal/logging"
	"github.com/dockfleet/dockfleet/internal/manager"
	"github.com/dockfleet/dockfleet/internal/store"
	"github.com/dockfleet/dockfleet/internal/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "dockfleet-manager:", err)
		os.Exit(1)
	}
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

	m, err := manager.New(cfg, st, logger)
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

	logger.Info("starting dockfleet manager", "version", version.Version, "store", cfg.Store)
	return m.Serve(ctx, apiLn, grpcLn)
}
