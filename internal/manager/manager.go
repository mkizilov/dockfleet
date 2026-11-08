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
	"github.com/dockfleet/dockfleet/internal/logstream"
	"github.com/dockfleet/dockfleet/internal/netutil"
	"github.com/dockfleet/dockfleet/internal/pki"
	"github.com/dockfleet/dockfleet/internal/placement"
	"github.com/dockfleet/dockfleet/internal/protocol"
	"github.com/dockfleet/dockfleet/internal/reconcile"
	"github.com/dockfleet/dockfleet/internal/registry"
	"github.com/dockfleet/dockfleet/internal/scheduler"
	"github.com/dockfleet/dockfleet/internal/store"
)

const (
	// maxConnectionAge bounds how long an agent connection lives. Agents
	// reconnect transparently, so a renewed agent certificate (and a changed
	// client CA bundle) applies within this time even to a quiet connection.
	maxConnectionAge = 10 * time.Minute

	certCheckInterval = time.Hour
)

// Manager is the control plane: the admin HTTP API plus the agent-facing gRPC service.
type Manager struct {
	cfg        config.Manager
	store      store.Store
	registry   *registry.Registry
	scheduler  *placement.Scheduler
	reconciler *reconcile.Reconciler
	logs       *logstream.Hub
	timing     scheduler.Timing
	logger     *slog.Logger
	apiTLS     *tls.Config
	grpcTLS    *tls.Config
}

// overcommit is the configured factors as the scheduler takes them.
func overcommit(cfg config.Manager) scheduler.Overcommit {
	return scheduler.Overcommit{CPU: cfg.CPUOvercommit, Memory: cfg.MemoryOvercommit}
}

// New prepares the manager. TLS material is loaded eagerly so a bad
// certificate or CA bundle fails startup instead of the first connection.
//
// With TLS configured, the admin API serves HTTPS (authenticated by its bearer
// token) and the agent gRPC listener requires a client certificate signed by
// the client CA; the certificate files are re-read when they change.
func New(cfg config.Manager, st store.Store, logger *slog.Logger) (*Manager, error) {
	// Silence from before this manager started is not counted against a node:
	// a manager that was down cannot tell a dead node from one it could not hear.
	timing := scheduler.Timing{SuspectAfter: cfg.NodeTTL, UnavailableAfter: cfg.NodeUnavailableAfter, Since: time.Now()}
	m := &Manager{
		cfg: cfg, store: st, logger: logger, timing: timing,
		registry: registry.New(st, logger).WithInstanceID(cfg.InstanceID),
		scheduler: placement.New(placement.Config{
			Store: st, Logger: logger, NodeTTL: cfg.NodeTTL, Timing: timing, Overcommit: overcommit(cfg),
			Interval: cfg.SchedulerInterval, InstanceID: cfg.InstanceID,
		}),
		reconciler: reconcile.New(reconcile.Config{
			Store: st, Logger: logger, Interval: cfg.ReconcileInterval, InstanceID: cfg.InstanceID, Timing: timing,
		}),
	}
	m.logs = logstream.NewHub(logstream.HubConfig{
		MaxStreams: cfg.LogMaxStreams, Window: cfg.LogWindowBytes, ReconnectWait: cfg.LogReconnectWait, Logger: logger,
	})
	if cfg.TLSCertFile != "" {
		var err error
		m.apiTLS, err = pki.ServerTLS(pki.ServerConfig{CertFile: cfg.TLSCertFile, KeyFile: cfg.TLSKeyFile, Logger: logger})
		if err != nil {
			return nil, fmt.Errorf("API TLS: %w", err)
		}
		m.grpcTLS, err = pki.ServerTLS(pki.ServerConfig{CertFile: cfg.TLSCertFile, KeyFile: cfg.TLSKeyFile, ClientCAFile: cfg.TLSClientCAFile, Logger: logger})
		if err != nil {
			return nil, fmt.Errorf("agent TLS: %w", err)
		}
	}
	return m, nil
}

// Sessions returns the node sessions currently held by the registry.
func (m *Manager) Sessions() []registry.Session {
	return m.registry.Sessions()
}

// Serve runs both servers on the given listeners until ctx is canceled or one
// of them fails, then shuts both down within cfg.ShutdownTimeout. It takes
// ownership of the listeners (closing them on every path) and returns nil
// after a clean, requested shutdown. Without TLS both listeners must be
// loopback: remote access requires TLS, and the agent listener then requires
// mutual TLS.
func (m *Manager) Serve(ctx context.Context, apiLn, grpcLn net.Listener) error {
	closeListeners := func() {
		_ = apiLn.Close()
		_ = grpcLn.Close()
	}
	if m.grpcTLS == nil {
		err := errors.Join(netutil.RequireLoopback("api", apiLn), netutil.RequireLoopback("grpc", grpcLn))
		if err != nil {
			closeListeners()
			return err
		}
	}
	nodes := protocol.NewNodeServer(m.registry, m.cfg.HeartbeatInterval, m.logger).WithLease(m.cfg.NodeLease).WithLogs(m.logs, 0)
	grpcSrv, err := protocol.NewGRPCServer(nodes, protocol.ServerOptions{
		TLS:              m.grpcTLS,
		Token:            m.cfg.AgentToken,
		HandlerTimeout:   m.cfg.RequestTimeout,
		MaxConnectionAge: maxConnectionAge,
	})
	if err != nil {
		closeListeners()
		return err
	}
	httpSrv := &http.Server{
		Handler: api.New(api.Config{
			Store:      m.store,
			Token:      m.cfg.APIToken,
			NodeTTL:    m.cfg.NodeTTL,
			Timing:     m.timing,
			NodeLease:  m.cfg.NodeLease,
			Logger:     m.logger,
			InstanceID: m.cfg.InstanceID,
			Overcommit: overcommit(m.cfg),
			Logs:       m.logs,

			LogWriteTimeout: m.cfg.LogWriteTimeout,
		}),
		TLSConfig:         m.apiTLS,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		ErrorLog:          slog.NewLogLogger(m.logger.Handler(), slog.LevelWarn),
	}

	watchCtx, stopWatching := context.WithCancel(ctx)
	defer stopWatching()
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		var err error
		if m.apiTLS != nil {
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
	if m.cfg.SchedulerInterval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.scheduler.Run(watchCtx)
		}()
	}
	if m.cfg.ReconcileInterval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.reconciler.Run(watchCtx)
		}()
	}
	if m.cfg.TLSCertFile != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pki.WatchExpiry(watchCtx, m.logger, "manager", m.cfg.TLSCertFile, pki.DefaultWarnWithin, certCheckInterval)
		}()
	}
	m.logger.Info("manager serving", "api_addr", apiLn.Addr().String(), "grpc_addr", grpcLn.Addr().String(), "mutual_tls", m.grpcTLS != nil)

	var errs []error
	select {
	case <-ctx.Done():
		m.logger.Info("shutdown requested")
	case err := <-errCh:
		errs = append(errs, err)
		m.logger.Error("server failed, shutting down", "error", err)
	}

	// Open log streams would hold both servers up until the shutdown timeout:
	// end them first, so that clients and agents see the manager go away.
	m.logs.Shutdown()

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

	stopWatching()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		errs = append(errs, err)
	}
	m.logger.Info("manager stopped")
	return errors.Join(errs...)
}
