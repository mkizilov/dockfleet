package api

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"github.com/dockfleet/dockfleet/internal/store"
)

// The audit trail has two parts, both structured. Every change an administrator
// makes is an event in the store, written in the same transaction as the change
// (what changed, and from which request); and every request that could change
// something leaves one "audit" log record whatever came of it: accepted,
// rejected as invalid or out of date, denied for want of credentials, or failed.
// The two share the request ID, which the response also carries in X-Request-ID,
// so an event can be found from a log line and the other way round.

// Keys of the details an audited event carries besides what the action adds.
const (
	detailRequestID = "request_id"
	detailRemote    = "remote"
)

type auditKey struct{}

// requestInfo is what the middleware knows about a request.
type requestInfo struct {
	ID string
	// Remote is the address the request came from: the peer of the connection,
	// not a forwarding header, which anyone can write.
	Remote string
	// Actor is who the request authenticated as; empty until it has.
	Actor string
}

func infoFrom(ctx context.Context) *requestInfo {
	info, _ := ctx.Value(auditKey{}).(*requestInfo)
	return info
}

// newRequestInfo takes the request ID the client sent, if it is a sensible one
// (so a proxy or the dashboard can carry its own through), and makes one if not.
func newRequestInfo(r *http.Request) *requestInfo {
	id := r.Header.Get("X-Request-ID")
	if !validRequestID(id) {
		id = store.NewID()[:16]
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return &requestInfo{ID: id, Remote: host}
}

func validRequestID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for _, c := range id {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

// changesState is whether a request with this method can change anything.
func changesState(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	}
	return false
}

// routeOf is the pattern the request matched, without its method: a bounded set
// of values, unlike the path. A request that matched none is "unmatched".
func routeOf(r *http.Request) string {
	pattern := r.Pattern
	if pattern == "" {
		return "unmatched"
	}
	if _, path, ok := strings.Cut(pattern, " "); ok {
		return path
	}
	return pattern
}

// logAudit writes the audit record of a request that could change something.
// It names the route, not the query or the body, which can hold secrets.
func (s *server) logAudit(r *http.Request, info *requestInfo, status int) {
	actor := info.Actor
	if actor == "" {
		actor = "anonymous"
	}
	outcome, level := "accepted", slog.LevelInfo
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		outcome, level = "denied", slog.LevelWarn
	case status >= 500:
		outcome, level = "failed", slog.LevelError
	case status >= 400:
		outcome, level = "rejected", slog.LevelInfo
	}
	s.logger.Log(r.Context(), level, "audit",
		"request_id", info.ID, "actor", actor, "action", r.Method+" "+routeOf(r), "path", r.URL.Path,
		"status", status, "outcome", outcome, "remote", info.Remote, "instance_id", s.instanceID)
}
