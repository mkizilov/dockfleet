// Package etcd is the small part of an etcd v3 client the leader election needs:
// leases, an atomic create-if-absent of one key, and reading it. It implements
// election.Backend.
//
// It speaks etcd's JSON gateway (POST /v3/..., served by every etcd 3.4 and
// later next to the gRPC port) with the standard library's net/http, so DockFleet
// takes no dependency on the official client and its tree of modules. The cost is
// that only these few calls are there: no watches, no authentication (use mutual
// TLS on the etcd side to decide who may write) and no transactions beyond the
// one the election uses.
package etcd

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dockfleet/dockfleet/internal/election"
	"github.com/dockfleet/dockfleet/internal/netutil"
)

const (
	// maxResponseBytes bounds what is read from a reply: a leader key and a
	// header are tiny, and a server that sends more is not one to trust.
	maxResponseBytes = 1 << 20
	defaultTimeout   = 5 * time.Second
)

// ErrInsecureRemote is returned for a plaintext endpoint that is not on loopback.
var ErrInsecureRemote = errors.New("refusing a plaintext connection to a non-loopback etcd endpoint; configure TLS")

// Error is an error reported by etcd.
type Error struct {
	Status  int    // the HTTP status
	Code    int    // the gRPC status code etcd reports (5 is NotFound, 14 Unavailable)
	Message string // etcd's message
}

func (e *Error) Error() string {
	return fmt.Sprintf("etcd answered %d (code %d): %s", e.Status, e.Code, e.Message)
}

// leaseNotFound is whether etcd says the lease it was asked about is gone.
func (e *Error) leaseNotFound() bool {
	return e.Code == 5 && strings.Contains(e.Message, "lease not found")
}

// Config configures a Client.
type Config struct {
	// Endpoints are the base URLs of the etcd members, such as
	// "https://etcd-1:2379". A plaintext one must be on loopback.
	Endpoints []string
	// TLS is the client configuration for https endpoints: it must verify the
	// servers, and present a client certificate if they ask for one.
	TLS *tls.Config
	// HTTPClient replaces the HTTP client; tests use it. TLS is ignored then.
	HTTPClient *http.Client
	// Timeout bounds each request; zero means 5 seconds.
	Timeout time.Duration
}

// Client talks to an etcd cluster. It is safe for concurrent use.
type Client struct {
	endpoints []string
	http      *http.Client
	timeout   time.Duration
	cur       atomic.Int32 // the endpoint that worked last
}

var _ election.Backend = (*Client)(nil)

// New checks the endpoints and returns a client; it does not connect.
func New(cfg Config) (*Client, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("etcd: at least one endpoint is required")
	}
	c := &Client{timeout: cfg.Timeout, http: cfg.HTTPClient}
	if c.timeout <= 0 {
		c.timeout = defaultTimeout
	}
	for _, raw := range cfg.Endpoints {
		u, err := url.Parse(strings.TrimSpace(raw))
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.User != nil {
			return nil, fmt.Errorf("etcd: endpoint %q must look like http://host:port or https://host:port", raw)
		}
		if u.Scheme == "http" && cfg.HTTPClient == nil && !netutil.IsLoopbackAddr(u.Host) {
			return nil, fmt.Errorf("etcd: endpoint %q: %w", raw, ErrInsecureRemote)
		}
		if u.Scheme == "https" && cfg.HTTPClient == nil && cfg.TLS == nil {
			return nil, fmt.Errorf("etcd: endpoint %q is https but no TLS configuration was given", raw)
		}
		c.endpoints = append(c.endpoints, u.Scheme+"://"+u.Host)
	}
	if c.http == nil {
		c.http = &http.Client{Transport: &http.Transport{
			TLSClientConfig:     cfg.TLS,
			MaxIdleConnsPerHost: 2,
			IdleConnTimeout:     30 * time.Second,
		}}
	}
	return c, nil
}

// Close drops idle connections.
func (c *Client) Close() { c.http.CloseIdleConnections() }

// call POSTs a JSON request and decodes the JSON reply. It tries the members in
// turn, beginning with the one that worked last, and moves on when one cannot be
// reached or says it is unavailable; an answer that etcd means (a lease that
// is gone, say) is returned as it is.
func (c *Client) call(ctx context.Context, path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	var last error
	start := int(c.cur.Load())
	for i := 0; i < len(c.endpoints); i++ {
		idx := (start + i) % len(c.endpoints)
		err := c.once(ctx, c.endpoints[idx]+path, body, out)
		if err == nil {
			c.cur.Store(int32(idx))
			return nil
		}
		last = err
		var ee *Error
		if ctx.Err() != nil || (errors.As(err, &ee) && ee.Status < 500) {
			return err
		}
	}
	return last
}

func (c *Client) once(ctx context.Context, target string, body []byte, out any) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return err
	}
	if len(raw) > maxResponseBytes {
		return fmt.Errorf("etcd: reply from %s is larger than %d bytes", target, maxResponseBytes)
	}
	if resp.StatusCode != http.StatusOK {
		e := &Error{Status: resp.StatusCode, Message: strings.TrimSpace(string(raw))}
		var body struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Error   string `json:"error"`
		}
		if json.Unmarshal(raw, &body) == nil {
			e.Code, e.Message = body.Code, cmpOr(body.Message, body.Error, e.Message)
		}
		if len(e.Message) > 200 {
			e.Message = e.Message[:200]
		}
		return e
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("etcd: reply from %s is not the expected JSON: %w", target, err)
	}
	return nil
}

func cmpOr(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// seconds is a duration as the whole seconds etcd counts leases in, at least one.
func seconds(d time.Duration) int64 { return max(1, int64((d+time.Second-1)/time.Second)) }

func parseInt(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// Grant creates a lease of about ttl (etcd counts whole seconds).
func (c *Client) Grant(ctx context.Context, ttl time.Duration) (election.Lease, error) {
	var out struct {
		ID    string `json:"ID"`
		TTL   string `json:"TTL"`
		Error string `json:"error"`
	}
	if err := c.call(ctx, "/v3/lease/grant", map[string]string{"TTL": strconv.FormatInt(seconds(ttl), 10)}, &out); err != nil {
		return election.Lease{}, err
	}
	if out.Error != "" || out.ID == "" || parseInt(out.ID) == 0 {
		return election.Lease{}, fmt.Errorf("etcd: lease grant failed: %q", out.Error)
	}
	granted := parseInt(out.TTL)
	if granted <= 0 {
		return election.Lease{}, errors.New("etcd: lease grant returned no ttl")
	}
	return election.Lease{ID: out.ID, TTL: time.Duration(granted) * time.Second}, nil
}

// KeepAlive renews the lease and returns the time it has left.
func (c *Client) KeepAlive(ctx context.Context, l election.Lease) (time.Duration, error) {
	var out struct {
		Result struct {
			TTL string `json:"TTL"`
		} `json:"result"`
	}
	if err := c.call(ctx, "/v3/lease/keepalive", map[string]string{"ID": l.ID}, &out); err != nil {
		var ee *Error
		if errors.As(err, &ee) && ee.leaseNotFound() {
			return 0, election.ErrLeaseExpired
		}
		return 0, err
	}
	// etcd answers a lease that is gone with no ttl at all.
	if ttl := parseInt(out.Result.TTL); ttl > 0 {
		return time.Duration(ttl) * time.Second, nil
	}
	return 0, election.ErrLeaseExpired
}

// Revoke ends the lease and deletes the keys attached to it.
func (c *Client) Revoke(ctx context.Context, l election.Lease) error {
	var out struct{}
	err := c.call(ctx, "/v3/lease/revoke", map[string]string{"ID": l.ID}, &out)
	var ee *Error
	if errors.As(err, &ee) && ee.leaseNotFound() {
		return nil
	}
	return err
}

type kv struct {
	Key            string `json:"key"`
	Value          string `json:"value"`
	CreateRevision string `json:"create_revision"`
}

func holderOf(k kv) (election.Holder, error) {
	v, err := base64.StdEncoding.DecodeString(k.Value)
	if err != nil {
		return election.Holder{}, fmt.Errorf("etcd: the leadership key holds a value that is not base64: %w", err)
	}
	return election.Holder{ID: string(v), Revision: parseInt(k.CreateRevision)}, nil
}

// Claim creates key=id attached to the lease unless the key exists, in one
// transaction. Either way it reports who holds the key afterwards.
func (c *Client) Claim(ctx context.Context, key, id string, l election.Lease) (election.Claim, error) {
	req := map[string]any{
		"compare": []map[string]string{{"target": "CREATE", "key": b64(key), "result": "EQUAL", "create_revision": "0"}},
		"success": []map[string]any{{"request_put": map[string]string{"key": b64(key), "value": b64(id), "lease": l.ID}}},
		"failure": []map[string]any{{"request_range": map[string]string{"key": b64(key)}}},
	}
	var out struct {
		Header    struct{ Revision string } `json:"header"`
		Succeeded bool                      `json:"succeeded"`
		Responses []struct {
			Range struct {
				Kvs []kv `json:"kvs"`
			} `json:"response_range"`
		} `json:"responses"`
	}
	if err := c.call(ctx, "/v3/kv/txn", req, &out); err != nil {
		var ee *Error
		if errors.As(err, &ee) && ee.leaseNotFound() {
			return election.Claim{}, election.ErrLeaseExpired
		}
		return election.Claim{}, err
	}
	if out.Succeeded {
		// The transaction wrote once, at the revision in its header: that is the
		// revision at which the key was created.
		return election.Claim{Won: true, Holder: election.Holder{ID: id, Revision: parseInt(out.Header.Revision)}}, nil
	}
	if len(out.Responses) == 0 || len(out.Responses[0].Range.Kvs) == 0 {
		return election.Claim{}, errors.New("etcd: the claim did not succeed and the key was not returned")
	}
	h, err := holderOf(out.Responses[0].Range.Kvs[0])
	if err != nil {
		return election.Claim{}, err
	}
	return election.Claim{Holder: h}, nil
}

// Observe reads the key.
func (c *Client) Observe(ctx context.Context, key string) (election.Holder, bool, error) {
	var out struct {
		Kvs []kv `json:"kvs"`
	}
	if err := c.call(ctx, "/v3/kv/range", map[string]string{"key": b64(key)}, &out); err != nil {
		return election.Holder{}, false, err
	}
	if len(out.Kvs) == 0 {
		return election.Holder{}, false, nil
	}
	h, err := holderOf(out.Kvs[0])
	return h, err == nil, err
}
