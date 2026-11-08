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
	"google.golang.org/grpc/codes"

	"github.com/dockfleet/dockfleet/internal/api"
	"github.com/dockfleet/dockfleet/internal/config"
	"github.com/dockfleet/dockfleet/internal/dashboard"
	"github.com/dockfleet/dockfleet/internal/election"
	"github.com/dockfleet/dockfleet/internal/logstream"
	"github.com/dockfleet/dockfleet/internal/metrics"
	"github.com/dockfleet/dockfleet/internal/netutil"
	"github.com/dockfleet/dockfleet/internal/pki"
	"github.com/dockfleet/dockfleet/internal/placement"
	"github.com/dockfleet/dockfleet/internal/protocol"
	"github.com/dockfleet/dockfleet/internal/reconcile"
	"github.com/dockfleet/dockfleet/internal/registry"
	"github.com/dockfleet/dockfleet/internal/scheduler"
	"github.com/dockfleet/dockfleet/internal/store"
	"github.com/dockfleet/dockfleet/internal/version"
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
	metrics    *metrics.Metrics // nil when metrics are off
	timing     scheduler.Timing
	logger     *slog.Logger
	apiTLS     *tls.Config
	grpcTLS    *tls.Config

	// elector is nil for a manager that runs alone. With one, only the leader
	// schedules and reconciles, and only the leader answers agents; the loops
	// below are built afresh for each term (see leaderDuty). schedulingObserver
	// and reconcileObserver are what each term's loops report to.
	elector            *election.Elector
	schedulingObserver placement.Observer
	reconcileObserver  reconcile.Observer
}

// Option changes how a Manager is built.
type Option func(*Manager)

// WithElection makes the manager one of several that share the control plane:
// it takes part in an election on the backend (etcd), and only the elected
// leader schedules, reconciles and answers agents. Without it the manager is the
// only one and needs no backend.
func WithElection(backend election.Backend) Option {
	return func(m *Manager) {
		m.elector = election.New(election.Config{
			Backend:  backend,
			ID:       m.cfg.InstanceID,
			Key:      m.cfg.EtcdPrefix + "/leader",
			TTL:      m.cfg.ElectionTTL,
			Begin:    m.beginTerm,
			Duties:   []election.Duty{m.leaderDuty},
			OnChange: m.onLeadershipChange,
			Logger:   m.logger,
		})
	}
}

// Leadership says whether this manager leads. A manager without an election is
// its own leader in every way but the epoch, which it does not use.
func (m *Manager) Leadership() election.Status {
	if m.elector == nil {
		return election.Status{ID: m.cfg.InstanceID, Role: election.Leader}
	}
	return m.elector.Status()
}

// overcommit is the configured factors as the scheduler takes them.
func overcommit(cfg config.Manager) scheduler.Overcommit {
	return scheduler.Overcommit{CPU: cfg.CPUOvercommit, Memory: cfg.MemoryOvercommit}
}

// loopMetrics turns what the control loops report about their passes into metrics.
type loopMetrics struct{ m *metrics.Metrics }

func (l loopMetrics) SchedulingPass(d time.Duration, r placement.Report, err error) {
	l.m.ObserveSchedulingPass(d, r.Placed, r.NewReasons, err)
}

func (l loopMetrics) ReconcilePass(d time.Duration, _ reconcile.Report, err error) {
	l.m.ObserveReconcilePass(d, err)
}

func (l loopMetrics) RolloutFailed(service string) { l.m.DeploymentFailed(service) }

// New prepares the manager. TLS material is loaded eagerly so a bad
// certificate or CA bundle fails startup instead of the first connection.
//
// With TLS configured, the admin API serves HTTPS (authenticated by its bearer
// token) and the agent gRPC listener requires a client certificate signed by
// the client CA; the certificate files are re-read when they change.
func New(cfg config.Manager, st store.Store, logger *slog.Logger, opts ...Option) (*Manager, error) {
	// Silence from before this manager started is not counted against a node:
	// a manager that was down cannot tell a dead node from one it could not hear.
	timing := scheduler.Timing{SuspectAfter: cfg.NodeTTL, UnavailableAfter: cfg.NodeUnavailableAfter, Since: time.Now()}
	// With metrics off the observers stay nil interfaces, which the loops skip.
	var instruments *metrics.Metrics
	var schedulingObserver placement.Observer
	var reconcileObserver reconcile.Observer
	if cfg.MetricsEnabled {
		instruments = metrics.New(version.Version, time.Now())
		schedulingObserver, reconcileObserver = loopMetrics{instruments}, loopMetrics{instruments}
	}
	m := &Manager{
		cfg: cfg, store: st, logger: logger, timing: timing, metrics: instruments,
		schedulingObserver: schedulingObserver, reconcileObserver: reconcileObserver,
		registry: registry.New(st, logger).WithInstanceID(cfg.InstanceID),
		scheduler: placement.New(placement.Config{
			Store: st, Logger: logger, NodeTTL: cfg.NodeTTL, Timing: timing, Overcommit: overcommit(cfg),
			Interval: cfg.SchedulerInterval, InstanceID: cfg.InstanceID, Observer: schedulingObserver,
		}),
		reconciler: reconcile.New(reconcile.Config{
			Store: st, Logger: logger, Interval: cfg.ReconcileInterval, InstanceID: cfg.InstanceID, Timing: timing,
			Observer: reconcileObserver,
		}),
	}
	m.logs = logstream.NewHub(logstream.HubConfig{
		MaxStreams: cfg.LogMaxStreams, Window: cfg.LogWindowBytes, ReconnectWait: cfg.LogReconnectWait, Logger: logger,
	})
	for _, opt := range opts {
		opt(m)
	}
	if cfg.HAEnabled() && m.elector == nil {
		return nil, errors.New("high availability is configured (DOCKFLEET_ETCD_ENDPOINTS) but no election backend was given")
	}
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

// httpHandler is the administrative API, with the dashboard in front of it when
// that is enabled. The dashboard reads through the API's own handler, so what
// it shows is what an API client is allowed to see.
func (m *Manager) httpHandler() (http.Handler, error) {
	var cluster api.Cluster
	if m.elector != nil {
		cluster = m.elector // (a nil *Elector in the interface would not be nil)
	}
	admin := api.New(api.Config{
		Cluster:         cluster,
		Store:           m.store,
		Token:           m.cfg.APIToken,
		NodeTTL:         m.cfg.NodeTTL,
		Timing:          m.timing,
		NodeLease:       m.cfg.NodeLease,
		Logger:          m.logger,
		InstanceID:      m.cfg.InstanceID,
		Overcommit:      overcommit(m.cfg),
		Metrics:         m.metrics,
		Logs:            m.logs,
		LogWriteTimeout: m.cfg.LogWriteTimeout,
	})
	if !m.cfg.DashboardEnabled {
		return admin, nil
	}
	dash, err := dashboard.New(dashboard.Config{API: admin, Token: m.cfg.APIToken, Logger: m.logger, Audit: m.auditDashboard})
	if err != nil {
		return nil, fmt.Errorf("dashboard: %w", err)
	}
	root := http.NewServeMux()
	root.Handle("/dashboard", dash)
	root.Handle("/dashboard/", dash)
	root.Handle("/", admin)
	return root, nil
}

// auditDashboard records a login to or a logout from the dashboard in the audit
// trail. A failure to record it is logged and does not lock the administrator out.
func (m *Manager) auditDashboard(ctx context.Context, action, message string, details map[string]string) {
	_, err := m.store.AppendEvent(ctx, store.Event{
		At: time.Now().UTC(), InstanceID: m.cfg.InstanceID, Actor: "admin", Action: action,
		ResourceType: "dashboard", ResourceID: "session", Message: message, Details: details,
	})
	if err != nil {
		m.logger.ErrorContext(ctx, "recording a dashboard audit event failed", "action", action, "error", err)
	}
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
	if m.elector != nil {
		nodes.WithFencing(m.elector)
	}
	serverOpts := protocol.ServerOptions{
		TLS:              m.grpcTLS,
		Token:            m.cfg.AgentToken,
		HandlerTimeout:   m.cfg.RequestTimeout,
		MaxConnectionAge: maxConnectionAge,
	}
	if m.metrics != nil {
		serverOpts.Observe = func(method string, code codes.Code, d time.Duration) { m.metrics.ObserveGRPC(method, code.String(), d) }
	}
	grpcSrv, err := protocol.NewGRPCServer(nodes, serverOpts)
	if err != nil {
		closeListeners()
		return err
	}
	handler, err := m.httpHandler()
	if err != nil {
		closeListeners()
		return err
	}
	httpSrv := &http.Server{
		Handler:           handler,
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
	// The election runs on a context of its own, which ends first when the manager
	// shuts down: a leader gives up its lease before it stops serving, so another
	// manager takes over at once and the agents are never left with a leader that
	// is about to go.
	electCtx, stopElection := context.WithCancel(ctx)
	defer stopElection()
	electionDone := make(chan struct{})
	if m.elector != nil {
		go func() {
			defer close(electionDone)
			m.elector.Run(electCtx)
		}()
	} else {
		close(electionDone)
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

	stopElection()
	<-electionDone

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
