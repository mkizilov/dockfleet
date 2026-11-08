package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dockfleet/dockfleet/internal/placement"
	"github.com/dockfleet/dockfleet/internal/scheduler"
	"github.com/dockfleet/dockfleet/internal/store"
)

const (
	maxBodyBytes   = 1 << 20
	maxImageLength = 255
	maxDescription = 200
	readyTimeout   = 2 * time.Second
)

// Config wires the administrative API.
type Config struct {
	Store   store.Store
	Token   string
	NodeTTL time.Duration
	// Timing says when a silent node is suspect and when it is unavailable. The
	// zero value gives suspect after NodeTTL and never unavailable.
	Timing scheduler.Timing
	// NodeLease is how long a heartbeat keeps a node's lease; zero when the
	// manager grants none.
	NodeLease time.Duration
	Logger    *slog.Logger
	// InstanceID names this manager in the audit events it records.
	InstanceID string
	// Overcommit is how far nodes may be promised beyond their capacity when a
	// workload is placed; the zero value is none.
	Overcommit scheduler.Overcommit
	// Now defaults to time.Now; tests override it.
	Now func() time.Time
}

type server struct {
	store      store.Store
	token      []byte
	nodeTTL    time.Duration
	timing     scheduler.Timing
	nodeLease  time.Duration
	logger     *slog.Logger
	instanceID string
	overcommit scheduler.Overcommit
	now        func() time.Time
}

// New returns the administrative HTTP handler. /healthz and /readyz are open;
// everything under /v1/ requires "Authorization: Bearer <token>".
func New(cfg Config) http.Handler {
	s := &server{
		store:      cfg.Store,
		token:      []byte("Bearer " + cfg.Token),
		nodeTTL:    cfg.NodeTTL,
		timing:     cfg.Timing,
		nodeLease:  cfg.NodeLease,
		logger:     cfg.Logger,
		instanceID: cfg.InstanceID,
		overcommit: cfg.Overcommit,
		now:        cfg.Now,
	}
	if s.now == nil {
		s.now = time.Now
	}
	if s.timing.SuspectAfter <= 0 {
		s.timing.SuspectAfter = cfg.NodeTTL
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.Handle("GET /v1/nodes", s.auth(s.listNodes))
	mux.Handle("GET /v1/nodes/{id}", s.auth(s.getNode))
	mux.Handle("POST /v1/workloads", s.auth(s.createWorkload))
	mux.Handle("GET /v1/workloads", s.auth(s.listWorkloads))
	mux.Handle("GET /v1/workloads/{id}", s.auth(s.getWorkload))
	mux.Handle("POST /v1/enrollments", s.auth(s.createEnrollment))
	mux.Handle("GET /v1/enrollments", s.auth(s.listEnrollments))
	mux.Handle("GET /v1/enrollments/{id}", s.auth(s.getEnrollment))
	mux.Handle("DELETE /v1/enrollments/{id}", s.auth(s.revokeEnrollment))
	mux.Handle("GET /v1/events", s.auth(s.listEvents))
	mux.Handle("POST /v1/services", s.auth(s.createService))
	mux.Handle("GET /v1/services", s.auth(s.listServices))
	mux.Handle("GET /v1/services/{name}", s.auth(s.getService))
	mux.Handle("PUT /v1/services/{name}", s.auth(s.updateService))
	mux.Handle("DELETE /v1/services/{name}", s.auth(s.deleteService))
	mux.Handle("GET /v1/services/{name}/revisions", s.auth(s.listServiceRevisions))
	mux.Handle("GET /v1/services/{name}/scale", s.auth(s.getScale))
	mux.Handle("PUT /v1/services/{name}/scale", s.auth(s.putScale))
	return s.logRequests(mux)
}

// adminActor is who the audit trail names for requests that carry the admin
// token. There is a single shared token today; per-user identities would
// replace this.
const adminActor = "admin"

// event builds an audit event for an administrative action.
func (s *server) event(action, resourceType, resourceID, message string, details map[string]string) store.Event {
	return store.Event{
		At: s.now().UTC(), InstanceID: s.instanceID, Actor: adminActor, Action: action,
		ResourceType: resourceType, ResourceID: resourceID, Message: message, Details: details,
	}
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
			s.logger.WarnContext(r.Context(), "rejected an unauthenticated request", "method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr)
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, http.StatusUnauthorized, "invalid or missing credentials")
			return
		}
		next(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	// Responses can carry service environments, so nothing may cache them.
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
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

type resources struct {
	CPUMillis   int64 `json:"cpu_millis"`
	MemoryBytes int64 `json:"memory_bytes"`
}

type allocation struct {
	resources
	Workloads int `json:"workloads"`
}

// nodeView is a node with what the scheduler sees of it: whether it can take
// new replicas and why not, how much of it may be promised, and how much has
// been promised to workloads that are meant to run.
//
// Health is the node's state from how long it has been silent: healthy,
// suspect (no new replicas, the ones it has stay) or unavailable (its replicas
// are started elsewhere). It is computed on every read, so it is current even
// when the reconciler has not recorded the change yet; HealthSince is when the
// reconciler did, and is left out until it has. LeaseExpiresAt is when the
// node's lease runs out unless it heartbeats, which is when an agent that fences
// itself stops its containers.
type nodeView struct {
	store.Node
	Status         string     `json:"status"`
	StatusReason   string     `json:"status_reason,omitempty"`
	Health         string     `json:"health"`
	HealthReason   string     `json:"health_reason,omitempty"`
	HealthSince    *time.Time `json:"health_since,omitempty"`
	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty"`
	Allocatable    resources  `json:"allocatable"`
	Allocated      allocation `json:"allocated"`
}

func (s *server) view(n store.Node, used scheduler.Used) nodeView {
	now := s.now()
	v := nodeView{Node: n, Status: "ready", StatusReason: scheduler.Unavailability(n, now, s.nodeTTL)}
	if v.StatusReason != "" {
		v.Status = "not_ready"
	}
	v.Health = s.timing.Health(n, now)
	v.HealthReason = s.timing.HealthReason(v.Health)
	if n.Health == v.Health && !n.HealthSince.IsZero() {
		since := n.HealthSince.UTC()
		v.HealthSince = &since
	}
	if s.nodeLease > 0 && !n.LastHeartbeat.IsZero() && n.LastHeartbeat.Year() > 1 {
		expires := n.LastHeartbeat.Add(s.nodeLease).UTC()
		v.LeaseExpiresAt = &expires
	}
	v.Allocatable.CPUMillis, v.Allocatable.MemoryBytes = s.overcommit.Allocatable(n.CPUMillis, n.MemoryBytes)
	v.Allocated = allocation{resources: resources{CPUMillis: used.CPUMillis, MemoryBytes: used.MemoryBytes}, Workloads: used.Workloads}
	return v
}

func (s *server) listNodes(w http.ResponseWriter, r *http.Request) {
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
	used := scheduler.UsedByNode(workloads)
	views := make([]nodeView, 0, len(nodes))
	for _, n := range nodes {
		views = append(views, s.view(n, used[n.ID]))
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodes": views})
}

func (s *server) getNode(w http.ResponseWriter, r *http.Request) {
	n, err := s.store.GetNode(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "node not found")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	workloads, err := s.store.ListWorkloadsForNode(r.Context(), n.ID)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, s.view(n, scheduler.UsedByNode(workloads)[n.ID]))
}

// unschedulableError is a placement the scheduler could not make.
type unschedulableError struct {
	u *scheduler.Unschedulable
}

func (e *unschedulableError) Error() string { return e.u.Message }

// writeUnschedulable answers 409 with the reason, and for each node why it was ruled out.
func writeUnschedulable(w http.ResponseWriter, u *scheduler.Unschedulable) {
	type nodeReason struct {
		NodeID  string `json:"node_id"`
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	nodes := make([]nodeReason, 0, len(u.Nodes))
	for _, n := range u.Nodes {
		nodes = append(nodes, nodeReason(n))
	}
	writeJSON(w, http.StatusConflict, map[string]any{"error": u.Message, "code": u.Code, "nodes": nodes})
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

// createWorkload records a placement decision (desired state). Nothing is
// started on the node yet. Reading the cluster, choosing a node, saving the
// workload and recording the event are one transaction under the cluster-wide
// placement lock, so concurrent requests cannot overcommit a node, even when
// they reach different managers sharing the database.
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

	id := store.NewID()
	var created store.Workload
	err := s.store.InTx(r.Context(), func(tx store.Tx) error {
		ctx := r.Context()
		if err := tx.LockPlacement(ctx); err != nil {
			return err
		}
		now := s.now().UTC()
		cluster, err := placement.ReadCluster(ctx, tx, now, s.nodeTTL)
		if err != nil {
			return err
		}
		// The same scheduler that places service replicas decides here.
		decision := scheduler.Schedule(scheduler.Input{
			Nodes: cluster.Nodes, Assignments: cluster.Assignments, Overcommit: s.overcommit,
			Replicas: []scheduler.Replica{{
				Service: req.Name, CPUMillis: req.CPUMillis, MemoryBytes: req.MemoryBytes, NodeSelector: req.NodeSelector,
			}},
		})[0]
		if !decision.Placed() {
			return &unschedulableError{decision.Unschedulable}
		}
		if created, err = tx.CreateWorkload(ctx, store.Workload{
			ID: id, Name: req.Name, Image: req.Image, CPUMillis: req.CPUMillis, MemoryBytes: req.MemoryBytes,
			NodeID: decision.NodeID, DesiredState: store.DesiredRunning, CreatedAt: now,
		}); err != nil {
			return err
		}
		_, err = tx.AppendEvent(ctx, s.event("workload.created", "workload", id, "", map[string]string{"name": req.Name, "node_id": decision.NodeID, "image": req.Image}))
		return err
	})
	var unschedulable *unschedulableError
	switch {
	case errors.As(err, &unschedulable):
		writeUnschedulable(w, unschedulable.u)
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, "a workload with this name already exists")
	case err != nil:
		s.internalError(w, r, err)
	default:
		w.Header().Set("Location", "/v1/workloads/"+created.ID)
		writeJSON(w, http.StatusCreated, created)
	}
}

type createEnrollmentRequest struct {
	NodeID      string `json:"node_id"`
	Description string `json:"description"`
}

// createEnrollment explicitly allows a node ID to register. The node still has
// to authenticate (its client certificate must carry this ID).
func (s *server) createEnrollment(w http.ResponseWriter, r *http.Request) {
	var req createEnrollmentRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "request body must be a valid JSON enrollment")
		return
	}
	if !store.ValidName(req.NodeID) {
		writeError(w, http.StatusBadRequest, "node_id must be 1-63 characters of [A-Za-z0-9._-] starting with a letter or digit")
		return
	}
	if len(req.Description) > maxDescription {
		writeError(w, http.StatusBadRequest, "description must be at most 200 characters")
		return
	}
	var created store.Enrollment
	err := s.store.InTx(r.Context(), func(tx store.Tx) error {
		var err error
		if created, err = tx.CreateEnrollment(r.Context(), store.Enrollment{NodeID: req.NodeID, Description: req.Description, CreatedAt: s.now().UTC()}); err != nil {
			return err
		}
		_, err = tx.AppendEvent(r.Context(), s.event("enrollment.created", "node", req.NodeID, req.Description, nil))
		return err
	})
	switch {
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, "node is already enrolled")
	case err != nil:
		s.internalError(w, r, err)
	default:
		s.logger.InfoContext(r.Context(), "node enrolled", "node_id", created.NodeID)
		w.Header().Set("Location", "/v1/enrollments/"+created.NodeID)
		writeJSON(w, http.StatusCreated, created)
	}
}

func (s *server) listEnrollments(w http.ResponseWriter, r *http.Request) {
	enrollments, err := s.store.ListEnrollments(r.Context())
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"enrollments": enrollments})
}

func (s *server) getEnrollment(w http.ResponseWriter, r *http.Request) {
	e, err := s.store.GetEnrollment(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "enrollment not found")
	case err != nil:
		s.internalError(w, r, err)
	default:
		writeJSON(w, http.StatusOK, e)
	}
}

// revokeEnrollment blocks a node from registering. Its running session ends
// at its next heartbeat. Revoking twice is harmless.
func (s *server) revokeEnrollment(w http.ResponseWriter, r *http.Request) {
	var e store.Enrollment
	err := s.store.InTx(r.Context(), func(tx store.Tx) error {
		ctx, id := r.Context(), r.PathValue("id")
		var err error
		if e, err = tx.GetEnrollment(ctx, id); err != nil {
			return err
		}
		if !e.Active() {
			return nil // already revoked: nothing changes, so nothing is recorded
		}
		if e, err = tx.RevokeEnrollment(ctx, id, s.now().UTC()); err != nil {
			return err
		}
		_, err = tx.AppendEvent(ctx, s.event("enrollment.revoked", "node", id, "", nil))
		return err
	})
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "enrollment not found")
	case err != nil:
		s.internalError(w, r, err)
	default:
		s.logger.InfoContext(r.Context(), "enrollment revoked", "node_id", e.NodeID)
		writeJSON(w, http.StatusOK, e)
	}
}

// listEvents returns the audit trail, newest first. Query parameters:
// resource_type, resource_id, before (an event ID, for paging) and limit.
func (s *server) listEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.EventFilter{ResourceType: q.Get("resource_type"), ResourceID: q.Get("resource_id")}
	if raw := q.Get("before"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 0 {
			writeError(w, http.StatusBadRequest, "before must be a non-negative integer")
			return
		}
		f.BeforeID = v
	}
	if raw := q.Get("limit"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 1 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		f.Limit = v
	}
	events, err := s.store.ListEvents(r.Context(), f)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
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
