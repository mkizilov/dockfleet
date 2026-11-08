package docker

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client is the Docker Engine surface DockFleet depends on. It only checks
// reachability for now; container lifecycle calls arrive with real orchestration.
type Client interface {
	Ping(ctx context.Context) error
	Close() error
}

// Engine talks to the Docker Engine API over a unix socket or loopback TCP.
type Engine struct {
	baseURL string
	http    *http.Client
}

var _ Client = (*Engine)(nil)

// NewEngine accepts unix:///path/to/docker.sock or tcp://127.0.0.1:port.
// Non-loopback TCP is refused because the Engine API is unauthenticated.
func NewEngine(host string) (*Engine, error) {
	u, err := url.Parse(host)
	if err != nil {
		return nil, fmt.Errorf("invalid docker host %q: %w", host, err)
	}
	switch u.Scheme {
	case "unix":
		if u.Path == "" {
			return nil, fmt.Errorf("docker host %q has no socket path", host)
		}
		socket := u.Path
		transport := &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socket)
			},
		}
		return &Engine{baseURL: "http://docker", http: &http.Client{Transport: transport, Timeout: 10 * time.Second}}, nil
	case "tcp":
		ip := net.ParseIP(u.Hostname())
		if u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return nil, fmt.Errorf("docker host %q: only loopback TCP is allowed", host)
		}
		return &Engine{baseURL: "http://" + u.Host, http: &http.Client{Timeout: 10 * time.Second}}, nil
	default:
		return nil, fmt.Errorf("docker host %q: unsupported scheme %q (want unix or tcp)", host, u.Scheme)
	}
}

// Ping calls GET /_ping and expects the body "OK".
func (e *Engine) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.baseURL+"/_ping", nil)
	if err != nil {
		return err
	}
	resp, err := e.http.Do(req)
	if err != nil {
		return fmt.Errorf("docker ping: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64))
	if err != nil {
		return fmt.Errorf("docker ping: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("docker ping: unexpected status %d", resp.StatusCode)
	}
	if strings.TrimSpace(string(body)) != "OK" {
		return fmt.Errorf("docker ping: unexpected response %q", body)
	}
	return nil
}

func (e *Engine) Close() error {
	e.http.CloseIdleConnections()
	return nil
}
