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
	"google.golang.org/grpc/status"

	dockfleetv1 "github.com/dockfleet/dockfleet/internal/protocol/dockfleet/v1"
	"github.com/dockfleet/dockfleet/internal/store"
)

const (
	maxMessageBytes = 64 << 10
	maxLabels       = 32
	maxNameLength   = 255
)

// Registry is the part of store.Store the node service needs.
type Registry interface {
	UpsertNode(ctx context.Context, n store.Node) (store.Node, error)
	Heartbeat(ctx context.Context, id string, at time.Time) error
}

// NodeServer implements the agent-facing NodeService.
type NodeServer struct {
	dockfleetv1.UnimplementedNodeServiceServer
	registry Registry
	interval time.Duration
	logger   *slog.Logger
	now      func() time.Time
}

// NewNodeServer tells registering agents to heartbeat every interval.
func NewNodeServer(registry Registry, interval time.Duration, logger *slog.Logger) *NodeServer {
	return &NodeServer{registry: registry, interval: interval, logger: logger, now: time.Now}
}

// NewGRPCServer builds a token-authenticated gRPC server, with TLS when tlsConfig is non-nil.
func NewGRPCServer(nodes *NodeServer, token string, tlsConfig *tls.Config) *grpc.Server {
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(UnaryTokenInterceptor(token)),
		grpc.MaxRecvMsgSize(maxMessageBytes),
	}
	if tlsConfig != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	srv := grpc.NewServer(opts...)
	dockfleetv1.RegisterNodeServiceServer(srv, nodes)
	return srv
}

func (s *NodeServer) RegisterNode(ctx context.Context, req *dockfleetv1.RegisterNodeRequest) (*dockfleetv1.RegisterNodeResponse, error) {
	switch {
	case !store.ValidName(req.GetNodeId()):
		return nil, status.Error(codes.InvalidArgument, "node_id must be 1-63 characters of [A-Za-z0-9._-] starting with a letter or digit")
	case req.GetName() == "" || len(req.GetName()) > maxNameLength:
		return nil, status.Error(codes.InvalidArgument, "name must be 1-255 characters")
	case req.GetCpuMillis() <= 0 || req.GetMemoryBytes() <= 0:
		return nil, status.Error(codes.InvalidArgument, "cpu_millis and memory_bytes must be positive")
	case len(req.GetLabels()) > maxLabels:
		return nil, status.Error(codes.InvalidArgument, "too many labels")
	}
	now := s.now()
	_, err := s.registry.UpsertNode(ctx, store.Node{
		ID:            req.GetNodeId(),
		Name:          req.GetName(),
		CPUMillis:     req.GetCpuMillis(),
		MemoryBytes:   req.GetMemoryBytes(),
		Labels:        req.GetLabels(),
		AgentVersion:  req.GetAgentVersion(),
		RegisteredAt:  now,
		LastHeartbeat: now,
	})
	if err != nil {
		s.logger.ErrorContext(ctx, "register node failed", "node_id", req.GetNodeId(), "error", err)
		return nil, status.Error(codes.Internal, "register node failed")
	}
	s.logger.InfoContext(ctx, "node registered", "node_id", req.GetNodeId(), "name", req.GetName())
	return &dockfleetv1.RegisterNodeResponse{HeartbeatIntervalMs: s.interval.Milliseconds()}, nil
}

func (s *NodeServer) Heartbeat(ctx context.Context, req *dockfleetv1.HeartbeatRequest) (*dockfleetv1.HeartbeatResponse, error) {
	err := s.registry.Heartbeat(ctx, req.GetNodeId(), s.now())
	switch {
	case err == nil:
		return &dockfleetv1.HeartbeatResponse{}, nil
	case errors.Is(err, store.ErrNotFound):
		return nil, status.Error(codes.NotFound, "node is not registered")
	default:
		s.logger.ErrorContext(ctx, "heartbeat failed", "node_id", req.GetNodeId(), "error", err)
		return nil, status.Error(codes.Internal, "heartbeat failed")
	}
}
