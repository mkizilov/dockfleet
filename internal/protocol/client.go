package protocol

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/dockfleet/dockfleet/internal/netutil"
	dockfleetv1 "github.com/dockfleet/dockfleet/internal/protocol/dockfleet/v1"
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
	// Epoch is the control-plane epoch of the manager that registered the node;
	// 0 when the managers have never elected a leader.
	Epoch int64
}

// ClientConfig configures Dial.
type ClientConfig struct {
	// Addr is the manager's host:port (or any gRPC target when Dialer is set).
	Addr string
	// Addrs lists the managers of a high-availability cluster, in the order to
	// try them; when set it replaces Addr. Only the leader answers, so the
	// client looks for it: a manager that says it is not the leader, or cannot
	// be reached, is skipped, and the next call starts with the one that answered.
	Addrs []string
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

type endpoint struct {
	addr  string
	conn  *grpc.ClientConn
	nodes dockfleetv1.NodeServiceClient
}

// Client is the agent's connection to the manager (or managers).
type Client struct {
	eps     []*endpoint
	cur     atomic.Int32 // the endpoint that answered last
	epoch   atomic.Int64 // the highest control-plane epoch heard
	version *dockfleetv1.ProtocolVersion
}

// Dial prepares a connection to the manager. The connection is established
// lazily and re-established automatically with backoff, so an unreachable
// manager surfaces as errors on calls rather than here.
func Dial(cfg ClientConfig) (*Client, error) {
	addrs := cfg.Addrs
	if len(addrs) == 0 {
		addrs = []string{cfg.Addr}
	}
	if cfg.TLS == nil && cfg.Dialer == nil {
		for _, addr := range addrs {
			if !netutil.IsLoopbackAddr(addr) {
				return nil, ErrInsecureRemote
			}
		}
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

	version := cfg.Version
	if version == nil {
		version = CurrentVersion()
	}
	c := &Client{version: version}
	for _, addr := range addrs {
		conn, err := grpc.NewClient(addr, opts...)
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("dial manager: %w", err)
		}
		c.eps = append(c.eps, &endpoint{addr: addr, conn: conn, nodes: dockfleetv1.NewNodeServiceClient(conn)})
	}
	return c, nil
}

// node is the current endpoint's service client.
func (c *Client) node() dockfleetv1.NodeServiceClient { return c.eps[c.cur.Load()].nodes }

// Epoch is the highest control-plane epoch this client has heard from a manager.
func (c *Client) Epoch() int64 { return c.epoch.Load() }

// Endpoint is the address of the manager the client talks to now.
func (c *Client) Endpoint() string { return c.eps[c.cur.Load()].addr }

// admit records the epoch an answer carries, or refuses the answer: an epoch
// lower than one already heard comes from a control plane that has been
// replaced, and what it says may undo what its successor decided.
func (c *Client) admit(epoch int64) error {
	for {
		seen := c.epoch.Load()
		if epoch < seen {
			return &Error{
				Code:    dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_STALE_CONTROL_PLANE,
				Message: fmt.Sprintf("the manager answers under control-plane epoch %d, but epoch %d has been heard: it has been replaced", epoch, seen),
			}
		}
		if epoch == seen || c.epoch.CompareAndSwap(seen, epoch) {
			return nil
		}
	}
}

// movesOn is whether a failed call is worth making to the next manager: this
// one is not the leader, answered for a control plane that has been replaced, or
// cannot be reached.
func movesOn(err error) bool {
	if IsNotLeader(err) || IsStaleControlPlane(err) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	}
	return false
}

// Register announces the node. A manager speaking an incompatible protocol is
// reported as an *Error with code INCOMPATIBLE_VERSION. With several managers it
// goes round them, once, until one that leads answers; the time of the call is
// shared among them.
func (c *Client) Register(ctx context.Context, n NodeInfo) (Session, error) {
	var last error
	deadline, hasDeadline := ctx.Deadline()
	first := int(c.cur.Load())
	for i := 0; i < len(c.eps); i++ {
		idx := (first + i) % len(c.eps)
		attempt, cancel := ctx, context.CancelFunc(func() {})
		if hasDeadline && len(c.eps)-i > 1 {
			attempt, cancel = context.WithTimeout(ctx, time.Until(deadline)/time.Duration(len(c.eps)-i))
		}
		session, err := c.registerAt(attempt, c.eps[idx], n)
		cancel()
		if err == nil {
			c.cur.Store(int32(idx))
			return session, nil
		}
		last = err
		if ctx.Err() != nil || !movesOn(err) {
			return Session{}, err
		}
		// The next call, if this one runs out of time, starts with the next manager.
		c.cur.Store(int32((idx + 1) % len(c.eps)))
	}
	return Session{}, last
}

func (c *Client) registerAt(ctx context.Context, ep *endpoint, n NodeInfo) (Session, error) {
	resp, err := ep.nodes.RegisterNode(ctx, &dockfleetv1.RegisterNodeRequest{
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
	if err := c.admit(resp.GetControlPlaneEpoch()); err != nil {
		return Session{}, err
	}
	return Session{
		ID:                resp.GetSessionId(),
		HeartbeatInterval: time.Duration(resp.GetHeartbeatIntervalMs()) * time.Millisecond,
		Lease:             time.Duration(resp.GetLeaseMs()) * time.Millisecond,
		Assignments:       resp.GetAssignments(),
		Epoch:             resp.GetControlPlaneEpoch(),
	}, nil
}

// Heartbeat sends liveness and workload statuses and returns the complete set
// of workloads currently assigned to the node. It goes to the manager that
// registered the node, because the session is that manager's; if that manager
// cannot be reached, the next call goes to another, which will tell the agent to
// register again. An answer from a control plane older than one already heard is
// refused, as an *Error with code STALE_CONTROL_PLANE, and its assignments are
// not returned.
func (c *Client) Heartbeat(ctx context.Context, nodeID, sessionID string, statuses []*dockfleetv1.WorkloadStatus) ([]*dockfleetv1.WorkloadAssignment, error) {
	idx := int(c.cur.Load())
	resp, err := c.eps[idx].nodes.Heartbeat(ctx, &dockfleetv1.HeartbeatRequest{NodeId: nodeID, SessionId: sessionID, Statuses: statuses})
	if err != nil {
		err = decodeError(err)
		if len(c.eps) > 1 && ctx.Err() == nil && movesOn(err) {
			c.cur.CompareAndSwap(int32(idx), int32((idx+1)%len(c.eps)))
		}
		return nil, err
	}
	if err := c.admit(resp.GetControlPlaneEpoch()); err != nil {
		return nil, err
	}
	return resp.GetAssignments(), nil
}

// Deregister ends the session; call it on clean shutdown.
func (c *Client) Deregister(ctx context.Context, nodeID, sessionID string) error {
	_, err := c.node().DeregisterNode(ctx, &dockfleetv1.DeregisterNodeRequest{NodeId: nodeID, SessionId: sessionID})
	return decodeError(err)
}

// Close closes every connection.
func (c *Client) Close() error {
	var first error
	for _, ep := range c.eps {
		if err := ep.conn.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
