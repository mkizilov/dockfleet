package protocol

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/dockfleet/dockfleet/internal/election"
	"github.com/dockfleet/dockfleet/internal/logstream"
	dockfleetv1 "github.com/dockfleet/dockfleet/internal/protocol/dockfleet/v1"
	"github.com/dockfleet/dockfleet/internal/registry"
	"github.com/dockfleet/dockfleet/internal/store"
)

const (
	maxMessageBytes = 1 << 20

	// maxAssignmentBytes is what an agent accepts in one response: the
	// assignments of a node, environments included.
	maxAssignmentBytes = 32 << 20
	maxLabels          = 32
	maxLabelLength     = 128
	maxNameLength      = 255
	maxStatuses        = 1000
	maxStatusMessage   = 512
	maxWorkloadIDBytes = 64
	maxContainerID     = 128
	defaultHandlerWait = 10 * time.Second
)

// phaseStates maps agent-reported phases to stored workload states.
var phaseStates = map[dockfleetv1.WorkloadPhase]string{
	dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_PENDING:   store.WorkloadPending,
	dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_RUNNING:   store.WorkloadRunning,
	dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_SUCCEEDED: store.WorkloadSucceeded,
	dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_FAILED:    store.WorkloadFailed,
	dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_STOPPED:   store.WorkloadStopped,
}

// validHealth lists the health results an agent may report; empty means the
// container has no health check.
var validHealth = map[string]bool{"": true, "starting": true, "healthy": true, "unhealthy": true}

// desiredStates maps a workload's stored desired state to the instruction sent to its agent.
var desiredStates = map[string]dockfleetv1.WorkloadDesiredState{
	store.DesiredRunning: dockfleetv1.WorkloadDesiredState_WORKLOAD_DESIRED_STATE_RUNNING,
	store.DesiredStopped: dockfleetv1.WorkloadDesiredState_WORKLOAD_DESIRED_STATE_STOPPED,
}

// Registry is the manager-side node registry the service delegates to;
// *registry.Registry implements it.
type Registry interface {
	Register(ctx context.Context, n store.Node) (registry.Session, []store.Workload, error)
	Heartbeat(ctx context.Context, nodeID, sessionID string, reports []registry.StatusReport) ([]store.Workload, error)
	Deregister(ctx context.Context, nodeID, sessionID string) error
	// CheckSession reports whether the session is the node's current one and
	// the node is still enrolled, without changing anything.
	CheckSession(ctx context.Context, nodeID, sessionID string) error
}

// Fencing says whether this manager may serve agents, and under which
// control-plane epoch. A manager without one serves with epoch 0.
type Fencing interface {
	// Epoch returns the epoch to put on answers. It returns election.ErrNotLeader
	// while the manager is not the leader of its cluster. *election.Elector
	// implements it.
	Epoch() (int64, error)
}

// NodeServer implements the agent-facing NodeService.
type NodeServer struct {
	dockfleetv1.UnimplementedNodeServiceServer
	registry Registry
	interval time.Duration
	lease    time.Duration
	logger   *slog.Logger
	version  *dockfleetv1.ProtocolVersion
	fencing  Fencing

	// logs receives the container logs agents stream; nil means the manager
	// does not serve them. logCheck is how often an open log stream is checked
	// against its session, and helloWait how long a new one may take to say who it is.
	logs      *logstream.Hub
	logCheck  time.Duration
	helloWait time.Duration
}

// NewNodeServer tells registering agents to heartbeat every interval.
func NewNodeServer(reg Registry, interval time.Duration, logger *slog.Logger) *NodeServer {
	return &NodeServer{registry: reg, interval: interval, logger: logger, version: CurrentVersion(), helloWait: defaultHelloWait}
}

// WithLease makes the server grant each session a lease of d, renewed by every
// successful heartbeat. Zero, the default, grants none.
func (s *NodeServer) WithLease(d time.Duration) *NodeServer {
	s.lease = d
	return s
}

// WithFencing makes the server answer only while f allows it, and stamp the
// epoch f gives on the answers that carry instructions.
func (s *NodeServer) WithFencing(f Fencing) *NodeServer {
	s.fencing = f
	return s
}

// epoch is the epoch to answer under, or the error to answer with: NOT_LEADER
// when this manager does not lead its cluster.
func (s *NodeServer) epoch(ctx context.Context) (int64, error) {
	if s.fencing == nil {
		return 0, nil
	}
	epoch, err := s.fencing.Epoch()
	switch {
	case err == nil:
		return epoch, nil
	case errors.Is(err, election.ErrNotLeader):
		return 0, newStatusError(codes.FailedPrecondition, dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_LEADER,
			"this manager is not the leader; try another manager", s.version)
	}
	return 0, s.internal(ctx, "read the control-plane epoch", err)
}

// stillEpoch checks, after the instructions were read, that the term they are
// answered under has not ended in the meantime: an answer is never sent by a
// manager that stopped leading while it was being made.
func (s *NodeServer) stillEpoch(ctx context.Context, epoch int64) error {
	now, err := s.epoch(ctx)
	if err != nil {
		return err
	}
	if now != epoch {
		return newStatusError(codes.FailedPrecondition, dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_LEADER,
			"the control-plane epoch changed while the answer was prepared; try again", s.version)
	}
	return nil
}

// WithVersion overrides the protocol version the server speaks. It exists to
// test version negotiation; production code must not call it.
func (s *NodeServer) WithVersion(v *dockfleetv1.ProtocolVersion) *NodeServer {
	s.version = v
	return s
}

// ErrClientAuthRequired is returned when a TLS configuration would let
// clients connect without a verified certificate.
var ErrClientAuthRequired = errors.New("TLS config must require and verify client certificates (mutual TLS)")

// ServerOptions configures NewGRPCServer. It runs in one of two modes:
//
//   - Mutual TLS (TLS set): clients are authenticated by their certificate,
//     which also fixes the node identity they may use. Token is not used.
//   - Plaintext (TLS nil): development on loopback only. Clients are
//     authenticated by the shared Token and may claim any node ID. The caller
//     must bind the listener to loopback (see netutil.RequireLoopback).
type ServerOptions struct {
	// TLS enables mutual TLS. Its ClientAuth must be RequireAndVerifyClientCert.
	TLS *tls.Config
	// Token is the bearer token for plaintext mode; required there.
	Token string
	// HandlerTimeout bounds each call; zero means 10 seconds.
	HandlerTimeout time.Duration
	// MaxConnectionAge makes the server end connections after this long (zero:
	// never). Clients reconnect transparently, which makes a renewed client
	// certificate take effect on connections that would otherwise outlive it.
	MaxConnectionAge time.Duration
	// Observe, when set, is called after every unary call with its full method
	// name, the status code it ended with and how long it took, calls refused
	// for their credentials included.
	Observe func(method string, code codes.Code, d time.Duration)
	// Now overrides the clock used to check certificate validity per call (tests).
	Now func() time.Time
}

// NewGRPCServer builds the agent-facing gRPC server with a per-call deadline.
func NewGRPCServer(nodes *NodeServer, opts ServerOptions) (*grpc.Server, error) {
	wait := opts.HandlerTimeout
	if wait <= 0 {
		wait = defaultHandlerWait
	}

	var auth grpc.UnaryServerInterceptor
	var streamAuth grpc.StreamServerInterceptor
	serverOpts := []grpc.ServerOption{grpc.MaxRecvMsgSize(maxMessageBytes)}
	if opts.TLS != nil {
		if opts.TLS.ClientAuth != tls.RequireAndVerifyClientCert {
			return nil, ErrClientAuthRequired
		}
		auth = UnaryIdentityInterceptor(opts.Now)
		streamAuth = StreamIdentityInterceptor(opts.Now)
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(opts.TLS)))
	} else {
		if opts.Token == "" {
			return nil, errors.New("a bearer token is required without TLS")
		}
		auth = UnaryTokenInterceptor(opts.Token)
		streamAuth = StreamTokenInterceptor(opts.Token)
	}
	// Streams get the same authentication as calls, but no deadline: a log
	// stream lives as long as the agent's session.
	unary := []grpc.UnaryServerInterceptor{auth, unaryDeadline(wait)}
	if opts.Observe != nil {
		// First, so that it also sees the calls the authentication refuses.
		unary = append([]grpc.UnaryServerInterceptor{unaryObserve(opts.Observe)}, unary...)
	}
	serverOpts = append(serverOpts, grpc.ChainUnaryInterceptor(unary...), grpc.ChainStreamInterceptor(streamAuth))
	if opts.MaxConnectionAge > 0 {
		serverOpts = append(serverOpts, grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge:      opts.MaxConnectionAge,
			MaxConnectionAgeGrace: 10 * time.Second,
		}))
	}
	srv := grpc.NewServer(serverOpts...)
	dockfleetv1.RegisterNodeServiceServer(srv, nodes)
	return srv, nil
}

func unaryObserve(observe func(method string, code codes.Code, d time.Duration)) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		observe(info.FullMethod, status.Code(err), time.Since(start))
		return resp, err
	}
}

func unaryDeadline(d time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, cancel := context.WithTimeout(ctx, d)
		defer cancel()
		return handler(ctx, req)
	}
}

func (s *NodeServer) invalid(msg string) error {
	return newStatusError(codes.InvalidArgument, dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_INVALID_REQUEST, msg, s.version)
}

func (s *NodeServer) internal(ctx context.Context, what string, err error) error {
	s.logger.ErrorContext(ctx, what+" failed", "error", err)
	return newStatusError(codes.Internal, dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_UNSPECIFIED, what+" failed", s.version)
}

// checkIdentity binds a request to the caller's certificate: under mutual TLS
// the node_id must equal the identity in the certificate. On plaintext servers
// there is no certificate and nothing to check.
func (s *NodeServer) checkIdentity(ctx context.Context, nodeID string) error {
	id, ok := identityFromContext(ctx)
	if !ok || id == nodeID {
		return nil
	}
	s.logger.WarnContext(ctx, "request names a different node than the client certificate", "certificate_node", id, "requested_node", nodeID)
	return newStatusError(codes.PermissionDenied, dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_IDENTITY_MISMATCH, "node_id does not match the identity in the client certificate", s.version)
}

func (s *NodeServer) notEnrolled(ctx context.Context, nodeID string) error {
	s.logger.WarnContext(ctx, "rejected a node that is not enrolled", "node_id", nodeID)
	return newStatusError(codes.PermissionDenied, dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_ENROLLED, "node is not enrolled; ask an administrator to enroll it", s.version)
}

// sessionError maps registry errors to protocol errors the agent can act on.
func (s *NodeServer) sessionError(ctx context.Context, what, nodeID string, err error) error {
	switch {
	case errors.Is(err, registry.ErrNotEnrolled):
		return s.notEnrolled(ctx, nodeID)
	case errors.Is(err, registry.ErrNotRegistered):
		return newStatusError(codes.FailedPrecondition, dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_REGISTERED, "node is not registered; register again", s.version)
	case errors.Is(err, registry.ErrSessionSuperseded):
		return newStatusError(codes.FailedPrecondition, dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_SESSION_SUPERSEDED, "session was superseded; register again", s.version)
	default:
		return s.internal(ctx, what, err)
	}
}

func (s *NodeServer) RegisterNode(ctx context.Context, req *dockfleetv1.RegisterNodeRequest) (*dockfleetv1.RegisterNodeResponse, error) {
	if err := s.checkIdentity(ctx, req.GetIdentity().GetNodeId()); err != nil {
		return nil, err
	}
	epoch, err := s.epoch(ctx)
	if err != nil {
		return nil, err
	}
	if err := checkAgentVersion(s.version, req.GetProtocolVersion()); err != nil {
		s.logger.WarnContext(ctx, "registration rejected", "reason", err.Error())
		return nil, newStatusError(codes.FailedPrecondition, dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_INCOMPATIBLE_VERSION, err.Error(), s.version)
	}
	id := req.GetIdentity()
	capacity := req.GetCapacity()
	switch {
	case !store.ValidName(id.GetNodeId()):
		return nil, s.invalid("node_id must be 1-63 characters of [A-Za-z0-9._-] starting with a letter or digit")
	case id.GetName() == "" || len(id.GetName()) > maxNameLength:
		return nil, s.invalid("name must be 1-255 characters")
	case capacity.GetCpuMillis() <= 0 || capacity.GetMemoryBytes() <= 0:
		return nil, s.invalid("capacity cpu_millis and memory_bytes must be positive")
	case len(id.GetLabels()) > maxLabels:
		return nil, s.invalid("too many labels")
	}
	for k, v := range id.GetLabels() {
		if k == "" || len(k) > maxLabelLength || len(v) > maxLabelLength {
			return nil, s.invalid("label keys must be 1-128 characters and values at most 128")
		}
	}

	session, workloads, err := s.registry.Register(ctx, store.Node{
		ID:           id.GetNodeId(),
		Name:         id.GetName(),
		CPUMillis:    capacity.GetCpuMillis(),
		MemoryBytes:  capacity.GetMemoryBytes(),
		Labels:       id.GetLabels(),
		AgentVersion: id.GetAgentVersion(),
	})
	if errors.Is(err, registry.ErrNotEnrolled) {
		return nil, s.notEnrolled(ctx, id.GetNodeId())
	}
	if err != nil {
		return nil, s.internal(ctx, "register node", err)
	}
	if err := s.stillEpoch(ctx, epoch); err != nil {
		return nil, err
	}
	s.logger.InfoContext(ctx, "node registered", "node_id", id.GetNodeId(), "name", id.GetName(), "session", session.ID,
		"agent_protocol", formatVersion(req.GetProtocolVersion()), "assignments", len(workloads), "epoch", epoch)
	return &dockfleetv1.RegisterNodeResponse{
		ProtocolVersion:     s.version,
		SessionId:           session.ID,
		HeartbeatIntervalMs: s.interval.Milliseconds(),
		Assignments:         toAssignments(workloads),
		LeaseMs:             s.lease.Milliseconds(),
		ControlPlaneEpoch:   epoch,
	}, nil
}

func (s *NodeServer) Heartbeat(ctx context.Context, req *dockfleetv1.HeartbeatRequest) (*dockfleetv1.HeartbeatResponse, error) {
	if err := s.checkIdentity(ctx, req.GetNodeId()); err != nil {
		return nil, err
	}
	epoch, err := s.epoch(ctx)
	if err != nil {
		return nil, err
	}
	if len(req.GetStatuses()) > maxStatuses {
		return nil, s.invalid("too many workload statuses")
	}
	reports := make([]registry.StatusReport, 0, len(req.GetStatuses()))
	for _, st := range req.GetStatuses() {
		state, ok := phaseStates[st.GetPhase()]
		switch {
		case st.GetWorkloadId() == "" || len(st.GetWorkloadId()) > maxWorkloadIDBytes:
			return nil, s.invalid("workload_id must be 1-64 characters")
		case !ok:
			return nil, s.invalid("workload status has an unspecified or unknown phase")
		case len(st.GetMessage()) > maxStatusMessage:
			return nil, s.invalid("workload status message is too long")
		case len(st.GetContainerId()) > maxContainerID:
			return nil, s.invalid("container_id must be at most 128 characters")
		case st.GetAcknowledgedGeneration() < 0 || st.GetRestartCount() < 0:
			return nil, s.invalid("acknowledged_generation and restart_count must not be negative")
		case !validHealth[st.GetHealth()]:
			return nil, s.invalid(`health must be empty, "starting", "healthy" or "unhealthy"`)
		}
		report := registry.StatusReport{
			WorkloadID: st.GetWorkloadId(), State: state, Message: st.GetMessage(),
			ContainerID: st.GetContainerId(), OOMKilled: st.GetOomKilled(), RestartCount: int(st.GetRestartCount()),
			Health: st.GetHealth(), AckedGeneration: st.GetAcknowledgedGeneration(),
		}
		if st.ExitCode != nil {
			exit := int(*st.ExitCode)
			report.ExitCode = &exit
		}
		reports = append(reports, report)
	}
	workloads, err := s.registry.Heartbeat(ctx, req.GetNodeId(), req.GetSessionId(), reports)
	if err != nil {
		return nil, s.sessionError(ctx, "heartbeat", req.GetNodeId(), err)
	}
	if err := s.stillEpoch(ctx, epoch); err != nil {
		return nil, err
	}
	return &dockfleetv1.HeartbeatResponse{Assignments: toAssignments(workloads), ControlPlaneEpoch: epoch}, nil
}

func (s *NodeServer) DeregisterNode(ctx context.Context, req *dockfleetv1.DeregisterNodeRequest) (*dockfleetv1.DeregisterNodeResponse, error) {
	if err := s.checkIdentity(ctx, req.GetNodeId()); err != nil {
		return nil, err
	}
	if _, err := s.epoch(ctx); err != nil {
		return nil, err
	}
	if err := s.registry.Deregister(ctx, req.GetNodeId(), req.GetSessionId()); err != nil {
		return nil, s.sessionError(ctx, "deregister node", req.GetNodeId(), err)
	}
	return &dockfleetv1.DeregisterNodeResponse{}, nil
}

func toAssignments(workloads []store.Workload) []*dockfleetv1.WorkloadAssignment {
	out := make([]*dockfleetv1.WorkloadAssignment, 0, len(workloads))
	for _, w := range workloads {
		a := &dockfleetv1.WorkloadAssignment{
			WorkloadId:   w.ID,
			Name:         w.Name,
			Image:        w.Image,
			CpuMillis:    w.CPUMillis,
			MemoryBytes:  w.MemoryBytes,
			Generation:   w.Generation,
			DesiredState: desiredStates[w.DesiredState],
			Command:      w.Command,
			Env:          w.Env,
		}
		for _, p := range w.Ports {
			a.Ports = append(a.Ports, &dockfleetv1.PortMapping{
				ContainerPort: uint32(p.ContainerPort), HostPort: uint32(p.HostPort), Protocol: p.Protocol,
			})
		}
		if w.RestartPolicy != nil {
			a.RestartPolicy = &dockfleetv1.RestartPolicy{
				Condition: w.RestartPolicy.Condition, MaxRetries: uint32(w.RestartPolicy.MaxRetries),
				BackoffSeconds: uint32(w.RestartPolicy.BackoffSeconds), MaxBackoffSeconds: uint32(w.RestartPolicy.MaxBackoffSeconds),
			}
		}
		if h := w.HealthCheck; h != nil {
			a.HealthCheck = &dockfleetv1.HealthCheck{
				Command: h.Command, IntervalSeconds: uint32(h.IntervalSeconds), TimeoutSeconds: uint32(h.TimeoutSeconds),
				StartPeriodSeconds: uint32(h.StartPeriodSeconds), Retries: uint32(h.Retries),
			}
		}
		out = append(out, a)
	}
	return out
}
