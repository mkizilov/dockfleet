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

	dockfleetv1 "github.com/dockfleet/dockfleet/internal/protocol/dockfleet/v1"
	"github.com/dockfleet/dockfleet/internal/registry"
	"github.com/dockfleet/dockfleet/internal/store"
)

const (
	maxMessageBytes    = 1 << 20
	maxLabels          = 32
	maxLabelLength     = 128
	maxNameLength      = 255
	maxStatuses        = 1000
	maxStatusMessage   = 512
	maxWorkloadIDBytes = 64
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

// Registry is the manager-side node registry the service delegates to;
// *registry.Registry implements it.
type Registry interface {
	Register(ctx context.Context, n store.Node) (registry.Session, []store.Workload, error)
	Heartbeat(ctx context.Context, nodeID, sessionID string, reports []registry.StatusReport) ([]store.Workload, error)
	Deregister(ctx context.Context, nodeID, sessionID string) error
}

// NodeServer implements the agent-facing NodeService.
type NodeServer struct {
	dockfleetv1.UnimplementedNodeServiceServer
	registry Registry
	interval time.Duration
	logger   *slog.Logger
	version  *dockfleetv1.ProtocolVersion
}

// NewNodeServer tells registering agents to heartbeat every interval.
func NewNodeServer(reg Registry, interval time.Duration, logger *slog.Logger) *NodeServer {
	return &NodeServer{registry: reg, interval: interval, logger: logger, version: CurrentVersion()}
}

// WithVersion overrides the protocol version the server speaks. It exists to
// test version negotiation; production code must not call it.
func (s *NodeServer) WithVersion(v *dockfleetv1.ProtocolVersion) *NodeServer {
	s.version = v
	return s
}

// ServerOptions configures NewGRPCServer.
type ServerOptions struct {
	// Token is the bearer token every call must carry.
	Token string
	// TLS enables TLS when non-nil. Without it the caller must bind the
	// listener to loopback only (see netutil.RequireLoopback).
	TLS *tls.Config
	// HandlerTimeout bounds each call; zero means 10 seconds.
	HandlerTimeout time.Duration
}

// NewGRPCServer builds a token-authenticated gRPC server with a per-call deadline.
func NewGRPCServer(nodes *NodeServer, opts ServerOptions) *grpc.Server {
	wait := opts.HandlerTimeout
	if wait <= 0 {
		wait = defaultHandlerWait
	}
	serverOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(UnaryTokenInterceptor(opts.Token), unaryDeadline(wait)),
		grpc.MaxRecvMsgSize(maxMessageBytes),
	}
	if opts.TLS != nil {
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(opts.TLS)))
	}
	srv := grpc.NewServer(serverOpts...)
	dockfleetv1.RegisterNodeServiceServer(srv, nodes)
	return srv
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

// sessionError maps registry errors to protocol errors the agent can act on.
func (s *NodeServer) sessionError(ctx context.Context, what string, err error) error {
	switch {
	case errors.Is(err, registry.ErrNotRegistered):
		return newStatusError(codes.FailedPrecondition, dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_REGISTERED, "node is not registered; register again", s.version)
	case errors.Is(err, registry.ErrSessionSuperseded):
		return newStatusError(codes.FailedPrecondition, dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_SESSION_SUPERSEDED, "session was superseded; register again", s.version)
	default:
		return s.internal(ctx, what, err)
	}
}

func (s *NodeServer) RegisterNode(ctx context.Context, req *dockfleetv1.RegisterNodeRequest) (*dockfleetv1.RegisterNodeResponse, error) {
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
	if err != nil {
		return nil, s.internal(ctx, "register node", err)
	}
	s.logger.InfoContext(ctx, "node registered", "node_id", id.GetNodeId(), "name", id.GetName(), "session", session.ID,
		"agent_protocol", formatVersion(req.GetProtocolVersion()), "assignments", len(workloads))
	return &dockfleetv1.RegisterNodeResponse{
		ProtocolVersion:     s.version,
		SessionId:           session.ID,
		HeartbeatIntervalMs: s.interval.Milliseconds(),
		Assignments:         toAssignments(workloads),
	}, nil
}

func (s *NodeServer) Heartbeat(ctx context.Context, req *dockfleetv1.HeartbeatRequest) (*dockfleetv1.HeartbeatResponse, error) {
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
		}
		reports = append(reports, registry.StatusReport{WorkloadID: st.GetWorkloadId(), State: state, Message: st.GetMessage()})
	}
	workloads, err := s.registry.Heartbeat(ctx, req.GetNodeId(), req.GetSessionId(), reports)
	if err != nil {
		return nil, s.sessionError(ctx, "heartbeat", err)
	}
	return &dockfleetv1.HeartbeatResponse{Assignments: toAssignments(workloads)}, nil
}

func (s *NodeServer) DeregisterNode(ctx context.Context, req *dockfleetv1.DeregisterNodeRequest) (*dockfleetv1.DeregisterNodeResponse, error) {
	if err := s.registry.Deregister(ctx, req.GetNodeId(), req.GetSessionId()); err != nil {
		return nil, s.sessionError(ctx, "deregister node", err)
	}
	return &dockfleetv1.DeregisterNodeResponse{}, nil
}

func toAssignments(workloads []store.Workload) []*dockfleetv1.WorkloadAssignment {
	out := make([]*dockfleetv1.WorkloadAssignment, 0, len(workloads))
	for _, w := range workloads {
		out = append(out, &dockfleetv1.WorkloadAssignment{
			WorkloadId:  w.ID,
			Name:        w.Name,
			Image:       w.Image,
			CpuMillis:   w.CPUMillis,
			MemoryBytes: w.MemoryBytes,
		})
	}
	return out
}
