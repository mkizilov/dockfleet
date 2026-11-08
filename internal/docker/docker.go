// Package docker is the worker-side Docker Engine adapter. Only the agent
// imports it: the manager never talks to Docker and never needs a Docker
// socket (see TestManagerSideNeverImportsDocker).
//
// The adapter manages exactly the containers DockFleet created on this node,
// recognized by labels (see LabelManaged), and refuses to touch any other
// container. Requests to create containers pass through a Policy that rejects
// privileged mode, Docker socket mounts and other dangerous settings. All use
// of the Docker SDK is confined to sdk.go behind a small internal interface,
// so everything else is tested without a Docker daemon.
package docker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"time"
)

// Client is the minimal Docker surface the agent's liveness check needs.
type Client interface {
	Ping(ctx context.Context) error
	Close() error
}

// Runtime is everything the agent may do with containers. Containers are
// addressed by workload ID, the stable identifier the manager assigns, never
// by Docker ID or name, so an operation can only ever reach a container that
// carries this node's DockFleet labels for that workload.
type Runtime interface {
	Client

	// List returns the managed containers on this node, ordered by workload ID.
	List(ctx context.Context) ([]Container, error)
	// PullImage downloads an image so a later Create does not fail with ErrImageNotFound.
	PullImage(ctx context.Context, ref string) error
	// Create validates spec against the policy and creates (but does not
	// start) its container. ErrRejected, ErrConflict and ErrImageNotFound are
	// possible failures.
	Create(ctx context.Context, spec ContainerSpec) (Container, error)
	Start(ctx context.Context, workloadID string) error
	// Stop asks the container to stop and kills it after timeout; a negative
	// timeout means 10 seconds, zero kills immediately.
	Stop(ctx context.Context, workloadID string, timeout time.Duration) error
	// Remove deletes the container and its anonymous volumes. A running
	// container is only removed with force (ErrStateConflict otherwise).
	Remove(ctx context.Context, workloadID string, force bool) error
	Inspect(ctx context.Context, workloadID string) (ContainerState, error)
	Logs(ctx context.Context, workloadID string, opts LogOptions) (Logs, error)
	// Stats takes one resource sample. The daemon samples twice, so it takes about a second.
	Stats(ctx context.Context, workloadID string) (Usage, error)
}

var (
	// ErrNotFound means no managed container of this node exists for the workload.
	ErrNotFound = errors.New("no managed container for the workload")
	// ErrConflict means the workload already has a container, or the name is
	// taken by a container DockFleet does not manage (which is left untouched).
	ErrConflict = errors.New("container already exists")
	// ErrStateConflict means the container is in the wrong state for the
	// operation, for example removing a running container without force.
	ErrStateConflict = errors.New("container is in the wrong state for this operation")
	// ErrImageNotFound means the image is not present on the node.
	ErrImageNotFound = errors.New("image not found")
	// ErrAmbiguous means more than one managed container claims the workload ID.
	ErrAmbiguous = errors.New("more than one managed container for the workload")
	// ErrInvalid means a request argument is malformed.
	ErrInvalid = errors.New("invalid argument")
)

// validateHost accepts unix:///path/to/docker.sock or tcp://127.0.0.1:port.
// Non-loopback TCP is refused because the Engine API is unauthenticated and
// grants root on the host: only the local agent may reach it.
func validateHost(host string) error {
	u, err := url.Parse(host)
	if err != nil {
		return fmt.Errorf("invalid docker host %q: %w", host, err)
	}
	switch u.Scheme {
	case "unix":
		if u.Path == "" {
			return fmt.Errorf("docker host %q has no socket path", host)
		}
	case "tcp":
		ip := net.ParseIP(u.Hostname())
		if u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return fmt.Errorf("docker host %q: only loopback TCP is allowed", host)
		}
		if u.Port() == "" {
			return fmt.Errorf("docker host %q has no port", host)
		}
	default:
		return fmt.Errorf("docker host %q: unsupported scheme %q (want unix or tcp)", host, u.Scheme)
	}
	return nil
}
