package api

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dockfleet/dockfleet/internal/scheduler"
	"github.com/dockfleet/dockfleet/internal/store"
)

const (
	maxBodyBytes   = 1 << 20
	maxImageLength = 255
	readyTimeout   = 2 * time.Second
)

// Config wires the administrative API.
type Config struct {
	Store   store.Store
	Token   string
	NodeTTL time.Duration
	Logger  *slog.Logger
	// Now defaults to time.Now; tests override it.
	Now func() time.Time
}

type server struct {
	store   store.Store
	token   []byte
	nodeTTL time.Duration
	logger  *slog.Logger
	now     func() time.Time
	// placeMu serializes placement so concurrent requests cannot overcommit a
	// node. It is sufficient for a single manager instance only.
	placeMu sync.Mutex
}

// New returns the administrative HTTP handler. /healthz and /readyz are open;
// everything under /v1/ requires "Authorization: Bearer <token>".
func New(cfg Config) http.Handler {
	s := &server{
		store:   cfg.Store,
		token:   []byte("Bearer " + cfg.Token),
		nodeTTL: cfg.NodeTTL,
		logger:  cfg.Logger,
		now:     cfg.Now,
	}
	if s.now == nil {
		s.now = time.Now
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.Handle("GET /v1/nodes", s.auth(s.listNodes))
	mux.Handle("GET /v1/nodes/{id}", s.auth(s.getNode))
	mux.Handle("POST /v1/workloads", s.auth(s.createWorkload))
	mux.Handle("GET /v1/workloads", s.auth(s.listWorkloads))
	mux.Handle("GET /v1/workloads/{id}", s.auth(s.getWorkload))
	return s.logRequests(mux)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (s *server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := s.now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.logger.InfoContext(r.Context(), "http request",
			"method", r.Method, "path", r.URL.Path, "status", rec.status, "duration", s.now().Sub(start))
	})
}

func (s *server) auth(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := []byte(r.Header.Get("Authorization"))
		if subtle.ConstantTimeCompare(got, s.token) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, http.StatusUnauthorized, "invalid or missing credentials")
			return
		}
		next(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func (s *server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	s.logger.ErrorContext(r.Context(), "request failed", "method", r.Method, "path", r.URL.Path, "error", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}

func (s *server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		s.logger.WarnContext(r.Context(), "readiness check failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type nodeView struct {
	store.Node
	Status string `json:"status"`
}

func (s *server) view(n store.Node) nodeView {
	status := "not_ready"
	if scheduler.Ready(n, s.now(), s.nodeTTL) {
		status = "ready"
	}
	return nodeView{Node: n, Status: status}
}

func (s *server) listNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := s.store.ListNodes(r.Context())
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	views := make([]nodeView, 0, len(nodes))
	for _, n := range nodes {
		views = append(views, s.view(n))
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodes": views})
}

func (s *server) getNode(w http.ResponseWriter, r *http.Request) {
	n, err := s.store.GetNode(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "node not found")
	case err != nil:
		s.internalError(w, r, err)
	default:
		writeJSON(w, http.StatusOK, s.view(n))
	}
}

type createWorkloadRequest struct {
	Name         string            `json:"name"`
	Image        string            `json:"image"`
	CPUMillis    int64             `json:"cpu_millis"`
	MemoryBytes  int64             `json:"memory_bytes"`
	NodeSelector map[string]string `json:"node_selector"`
}

func (req createWorkloadRequest) validate() string {
	switch {
	case !store.ValidName(req.Name):
		return "name must be 1-63 characters of [A-Za-z0-9._-] starting with a letter or digit"
	case req.Image == "" || len(req.Image) > maxImageLength || strings.ContainsAny(req.Image, " \t\r\n"):
		return "image must be 1-255 characters without whitespace"
	case req.CPUMillis <= 0 || req.MemoryBytes <= 0:
		return "cpu_millis and memory_bytes must be positive"
	}
	return ""
}

func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// createWorkload records a placement decision. Nothing is started on the node yet.
func (s *server) createWorkload(w http.ResponseWriter, r *http.Request) {
	var req createWorkloadRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "request body must be a valid JSON workload")
		return
	}
	if msg := req.validate(); msg != "" {
		writeError(w, http.StatusBadRequest, msg)
		return
	}

	s.placeMu.Lock()
	defer s.placeMu.Unlock()

	nodes, err := s.store.ListNodes(r.Context())
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	workloads, err := s.store.ListWorkloads(r.Context())
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	target, err := scheduler.Place(
		scheduler.Request{CPUMillis: req.CPUMillis, MemoryBytes: req.MemoryBytes, NodeSelector: req.NodeSelector},
		scheduler.Candidates(nodes, workloads, s.now(), s.nodeTTL),
	)
	if errors.Is(err, scheduler.ErrNoCapacity) {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	id, err := newID()
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	created, err := s.store.CreateWorkload(r.Context(), store.Workload{
		ID:          id,
		Name:        req.Name,
		Image:       req.Image,
		CPUMillis:   req.CPUMillis,
		MemoryBytes: req.MemoryBytes,
		NodeID:      target.ID,
		State:       store.WorkloadScheduled,
		CreatedAt:   s.now().UTC(),
	})
	switch {
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, "a workload with this name already exists")
	case err != nil:
		s.internalError(w, r, err)
	default:
		w.Header().Set("Location", "/v1/workloads/"+created.ID)
		writeJSON(w, http.StatusCreated, created)
	}
}

func (s *server) listWorkloads(w http.ResponseWriter, r *http.Request) {
	workloads, err := s.store.ListWorkloads(r.Context())
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"workloads": workloads})
}

func (s *server) getWorkload(w http.ResponseWriter, r *http.Request) {
	wl, err := s.store.GetWorkload(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "workload not found")
	case err != nil:
		s.internalError(w, r, err)
	default:
		writeJSON(w, http.StatusOK, wl)
	}
}
