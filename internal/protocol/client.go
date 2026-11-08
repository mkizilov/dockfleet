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

var (
	// ErrInsecureRemote is returned when a plaintext connection to a non-loopback
	// manager is requested. Remote access requires mutual TLS; there is no
	// plaintext fallback.
	ErrInsecureRemote = errors.New("refusing plaintext connection to a non-loopback address; configure mutual TLS")
	// ErrNoClientCertificate is returned for a TLS configuration that cannot
	// present a client certificate; the manager requires one.
	ErrNoClientCertificate = errors.New("TLS config has no client certificate; mutual TLS is required")
)

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
	// Lease is how long a successful heartbeat keeps the session's lease, from
	// when the agent sent it; zero when the manager grants none.
	Lease       time.Duration
	Assignments []*dockfleetv1.WorkloadAssignment
}

// ClientConfig configures Dial.
type ClientConfig struct {
	// Addr is the manager's host:port (or any gRPC target when Dialer is set).
	Addr string
	// TLS enables mutual TLS when non-nil: it must verify the manager and
	// present the agent certificate (see pki.ClientTLS). Without it Addr must
	// be loopback and Token authenticates the agent.
	TLS *tls.Config
	// Token is the bearer token for plaintext loopback development. It is not
	// sent over TLS, where the client certificate is the credential.
	Token string
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
	var opts []grpc.DialOption
	if cfg.TLS != nil {
		if len(cfg.TLS.Certificates) == 0 && cfg.TLS.GetClientCertificate == nil {
			return nil, ErrNoClientCertificate
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(cfg.TLS)))
	} else {
		opts = append(opts,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithPerRPCCredentials(tokenCredentials{token: cfg.Token}),
		)
	}
	opts = append(opts,
		// A node's assignments carry environments, so they can be larger than gRPC's 4 MiB default.
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxAssignmentBytes)),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 500 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: 10 * time.Second},
			MinConnectTimeout: 5 * time.Second,
		}),
	)
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
		Lease:             time.Duration(resp.GetLeaseMs()) * time.Millisecond,
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
