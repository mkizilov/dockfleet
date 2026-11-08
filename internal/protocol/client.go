package protocol

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	dockfleetv1 "github.com/dockfleet/dockfleet/internal/protocol/dockfleet/v1"
	"github.com/dockfleet/dockfleet/internal/netutil"
)

// ErrInsecureRemote is returned when a plaintext connection to a non-loopback
// manager is requested. Remote access requires TLS.
var ErrInsecureRemote = errors.New("refusing plaintext connection to a non-loopback address; configure TLS")

// NodeInfo describes a node as announced by its agent.
type NodeInfo struct {
	ID           string
	Name         string
	CPUMillis    int64
	MemoryBytes  int64
	Labels       map[string]string
	AgentVersion string
}

// Session is the result of a successful registration.
type Session struct {
	ID                string
	HeartbeatInterval time.Duration
	Assignments       []*dockfleetv1.WorkloadAssignment
}

// ClientConfig configures Dial.
type ClientConfig struct {
	// Addr is the manager's host:port (or any gRPC target when Dialer is set).
	Addr string
	// Token is the agent bearer token.
	Token string
	// TLS enables TLS when non-nil. Without it Addr must be loopback.
	TLS *tls.Config
	// Dialer replaces the network dialer, for example with an in-memory
	// transport in tests. The loopback rule cannot be checked for custom
	// dialers, so the caller vouches for them.
	Dialer func(ctx context.Context, addr string) (net.Conn, error)
	// Version overrides the advertised protocol version. It exists to test
	// version negotiation; leave it nil in production.
	Version *dockfleetv1.ProtocolVersion
	// DialOptions are appended last and can override the defaults.
	DialOptions []grpc.DialOption
}

// Client is the agent's connection to the manager.
type Client struct {
	conn    *grpc.ClientConn
	nodes   dockfleetv1.NodeServiceClient
	version *dockfleetv1.ProtocolVersion
}

// Dial prepares a connection to the manager. The connection is established
// lazily and re-established automatically with backoff, so an unreachable
// manager surfaces as errors on calls rather than here.
func Dial(cfg ClientConfig) (*Client, error) {
	if cfg.TLS == nil && cfg.Dialer == nil && !netutil.IsLoopbackAddr(cfg.Addr) {
		return nil, ErrInsecureRemote
	}
	creds := credentials.TransportCredentials(insecure.NewCredentials())
	if cfg.TLS != nil {
		creds = credentials.NewTLS(cfg.TLS)
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithPerRPCCredentials(tokenCredentials{token: cfg.Token, secure: cfg.TLS != nil}),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 500 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: 10 * time.Second},
			MinConnectTimeout: 5 * time.Second,
		}),
	}
	if cfg.Dialer != nil {
		opts = append(opts, grpc.WithContextDialer(cfg.Dialer))
	}
	opts = append(opts, cfg.DialOptions...)
	conn, err := grpc.NewClient(cfg.Addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("dial manager: %w", err)
	}
	version := cfg.Version
	if version == nil {
		version = CurrentVersion()
	}
	return &Client{conn: conn, nodes: dockfleetv1.NewNodeServiceClient(conn), version: version}, nil
}

// Register announces the node. A manager speaking an incompatible protocol is
// reported as an *Error with code INCOMPATIBLE_VERSION.
func (c *Client) Register(ctx context.Context, n NodeInfo) (Session, error) {
	resp, err := c.nodes.RegisterNode(ctx, &dockfleetv1.RegisterNodeRequest{
		ProtocolVersion: c.version,
		Identity: &dockfleetv1.NodeIdentity{
			NodeId:       n.ID,
			Name:         n.Name,
			Labels:       n.Labels,
			AgentVersion: n.AgentVersion,
		},
		Capacity: &dockfleetv1.NodeCapacity{CpuMillis: n.CPUMillis, MemoryBytes: n.MemoryBytes},
	})
	if err != nil {
		return Session{}, decodeError(err)
	}
	if err := checkManagerVersion(c.version, resp.GetProtocolVersion()); err != nil {
		return Session{}, &Error{
			Code:      dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_INCOMPATIBLE_VERSION,
			Message:   err.Error(),
			Supported: resp.GetProtocolVersion(),
		}
	}
	if resp.GetSessionId() == "" {
		return Session{}, errors.New("manager returned an empty session id")
	}
	return Session{
		ID:                resp.GetSessionId(),
		HeartbeatInterval: time.Duration(resp.GetHeartbeatIntervalMs()) * time.Millisecond,
		Assignments:       resp.GetAssignments(),
	}, nil
}

// Heartbeat sends liveness and workload statuses and returns the complete set
// of workloads currently assigned to the node.
func (c *Client) Heartbeat(ctx context.Context, nodeID, sessionID string, statuses []*dockfleetv1.WorkloadStatus) ([]*dockfleetv1.WorkloadAssignment, error) {
	resp, err := c.nodes.Heartbeat(ctx, &dockfleetv1.HeartbeatRequest{NodeId: nodeID, SessionId: sessionID, Statuses: statuses})
	if err != nil {
		return nil, decodeError(err)
	}
	return resp.GetAssignments(), nil
}

// Deregister ends the session; call it on clean shutdown.
func (c *Client) Deregister(ctx context.Context, nodeID, sessionID string) error {
	_, err := c.nodes.DeregisterNode(ctx, &dockfleetv1.DeregisterNodeRequest{NodeId: nodeID, SessionId: sessionID})
	return decodeError(err)
}

func (c *Client) Close() error {
	return c.conn.Close()
}
