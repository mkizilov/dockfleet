package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// healthState is what the agent knows about its own connection to the control
// plane, for the health endpoint. The agent registers with a manager (the leader
// of a cluster, when there are several) and keeps a heartbeat going; it is
// healthy while that holds.
type healthState struct {
	mu       sync.Mutex
	up       bool
	last     time.Time
	interval time.Duration
}

// registered records a successful registration; the session's heartbeat
// interval says how long a silence is too long.
func (h *healthState) registered(now time.Time, interval time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.up, h.last, h.interval = true, now, interval
}

// beat records a heartbeat the manager accepted.
func (h *healthState) beat(now time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.last = now
}

// down records that the agent has no session: it is registering again, or has stopped.
func (h *healthState) down() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.up = false
}

func (h *healthState) snapshot() (up bool, last time.Time, interval time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.up, h.last, h.interval
}

// Readiness is the answer of GET /readyz.
type Readiness struct {
	Ready  bool   `json:"ready"`
	NodeID string `json:"node_id"`
	// Registered is whether the agent holds a session with a manager.
	Registered bool `json:"registered"`
	// LastHeartbeatAgeMS is how long ago a manager last accepted a heartbeat.
	LastHeartbeatAgeMS int64 `json:"last_heartbeat_age_ms,omitempty"`
	// Reason says why the agent is not ready.
	Reason string `json:"reason,omitempty"`
}

// enginePing bounds the question to the container engine.
const enginePing = 2 * time.Second

// Readiness reports whether the agent can do its job now: it holds a session
// with a manager that accepted a heartbeat recently (three heartbeat intervals
// and one call, so a single lost heartbeat does not make it unready), and the
// container engine answers.
func (a *Agent) Readiness(ctx context.Context) Readiness {
	out := Readiness{NodeID: a.cfg.Node.ID}
	up, last, interval := a.health.snapshot()
	out.Registered = up
	if !up {
		out.Reason = "not registered with a manager: none is reachable, or none leads"
		return out
	}
	age := a.runner.now().Sub(last)
	out.LastHeartbeatAgeMS = age.Milliseconds()
	if interval <= 0 {
		interval = defaultHeartbeat
	}
	if limit := 3*interval + a.cfg.RPCTimeout; age > limit {
		out.Reason = fmt.Sprintf("no heartbeat was accepted for %s (limit %s)", age.Round(time.Millisecond), limit)
		return out
	}
	pingCtx, cancel := context.WithTimeout(ctx, enginePing)
	defer cancel()
	if err := a.cfg.Docker.Ping(pingCtx); err != nil {
		out.Reason = "the container engine does not answer: " + err.Error()
		return out
	}
	out.Ready = true
	return out
}

// HealthHandler serves the agent's health: GET /healthz (the process is up and
// serving, always 200) and GET /readyz (200 when Readiness says ready, else 503
// with the reason). Both are read-only and carry nothing secret, and the agent
// binds them to loopback only.
func (a *Agent) HealthHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeHealth(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		got := a.Readiness(r.Context())
		status := http.StatusOK
		if !got.Ready {
			status = http.StatusServiceUnavailable
		}
		writeHealth(w, status, got)
	})
	return mux
}

func writeHealth(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
