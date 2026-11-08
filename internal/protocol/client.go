package protocol

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	dockfleetv1 "github.com/dockfleet/dockfleet/internal/protocol/dockfleet/v1"
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

// Client is the agent's connection to the manager.
type Client struct {
	conn  *grpc.ClientConn
	nodes dockfleetv1.NodeServiceClient
}

// Dial prepares a connection to the manager. The connection is established
// lazily, so an unreachable manager surfaces as errors on the first call.
// The bearer token is only sent over TLS unless tlsConfig is nil, in which
// case the caller must have restricted addr to loopback.
func Dial(addr, token string, tlsConfig *tls.Config, extra ...grpc.DialOption) (*Client, error) {
	creds := credentials.TransportCredentials(insecure.NewCredentials())
	if tlsConfig != nil {
		creds = credentials.NewTLS(tlsConfig)
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithPerRPCCredentials(tokenCredentials{token: token, secure: tlsConfig != nil}),
	}
	opts = append(opts, extra...)
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("dial manager: %w", err)
	}
	return &Client{conn: conn, nodes: dockfleetv1.NewNodeServiceClient(conn)}, nil
}

// RegisterNode registers the node and returns the heartbeat interval the manager asks for.
func (c *Client) RegisterNode(ctx context.Context, n NodeInfo) (time.Duration, error) {
	resp, err := c.nodes.RegisterNode(ctx, &dockfleetv1.RegisterNodeRequest{
		NodeId:       n.ID,
		Name:         n.Name,
		CpuMillis:    n.CPUMillis,
		MemoryBytes:  n.MemoryBytes,
		Labels:       n.Labels,
		AgentVersion: n.AgentVersion,
	})
	if err != nil {
		return 0, err
	}
	return time.Duration(resp.GetHeartbeatIntervalMs()) * time.Millisecond, nil
}

func (c *Client) Heartbeat(ctx context.Context, nodeID string) error {
	_, err := c.nodes.Heartbeat(ctx, &dockfleetv1.HeartbeatRequest{NodeId: nodeID})
	return err
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// IsNotRegistered reports whether err means the manager does not know the node.
func IsNotRegistered(err error) bool {
	return status.Code(err) == codes.NotFound
}
