package manager

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/dockfleet/dockfleet/internal/api"
	"github.com/dockfleet/dockfleet/internal/config"
	"github.com/dockfleet/dockfleet/internal/protocol"
	"github.com/dockfleet/dockfleet/internal/store"
)

// Manager is the control plane: the admin HTTP API plus the agent-facing gRPC service.
type Manager struct {
	cfg    config.Manager
	store  store.Store
	logger *slog.Logger
	tls    *tls.Config
}

// New prepares the manager. TLS material is loaded eagerly so a bad
// certificate fails startup instead of the first connection.
func New(cfg config.Manager, st store.Store, logger *slog.Logger) (*Manager, error) {
	m := &Manager{cfg: cfg, store: st, logger: logger}
	if cfg.TLSCertFile != "" {
		tlsConfig, err := protocol.ServerTLSConfig(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, err
		}
		m.tls = tlsConfig
	}
	return m, nil
}

// Serve runs both servers on the given listeners until ctx is canceled or one
// of them fails, then shuts both down within cfg.ShutdownTimeout. It returns
// nil after a clean, requested shutdown.
func (m *Manager) Serve(ctx context.Context, apiLn, grpcLn net.Listener) error {
	httpSrv := &http.Server{
		Handler: api.New(api.Config{
			Store:   m.store,
			Token:   m.cfg.APIToken,
			NodeTTL: m.cfg.NodeTTL,
			Logger:  m.logger,
		}),
		TLSConfig:         m.tls,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		ErrorLog:          slog.NewLogLogger(m.logger.Handler(), slog.LevelWarn),
	}
	nodes := protocol.NewNodeServer(m.store, m.cfg.HeartbeatInterval, m.logger)
	grpcSrv := protocol.NewGRPCServer(nodes, m.cfg.AgentToken, m.tls)

	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		var err error
		if m.tls != nil {
			err = httpSrv.ServeTLS(apiLn, "", "")
		} else {
			err = httpSrv.Serve(apiLn)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("api server: %w", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := grpcSrv.Serve(grpcLn); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			errCh <- fmt.Errorf("grpc server: %w", err)
		}
	}()
	m.logger.Info("manager serving", "api_addr", apiLn.Addr().String(), "grpc_addr", grpcLn.Addr().String(), "tls", m.tls != nil)

	var errs []error
	select {
	case <-ctx.Done():
		m.logger.Info("shutdown requested")
	case err := <-errCh:
		errs = append(errs, err)
		m.logger.Error("server failed, shutting down", "error", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), m.cfg.ShutdownTimeout)
	defer cancel()

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		m.logger.Warn("http shutdown incomplete; closing connections", "error", err)
		_ = httpSrv.Close()
	}
	stopped := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-shutdownCtx.Done():
		m.logger.Warn("grpc graceful stop timed out; forcing stop")
		grpcSrv.Stop()
		<-stopped
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		errs = append(errs, err)
	}
	m.logger.Info("manager stopped")
	return errors.Join(errs...)
}
