package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/scheduler"
	"github.com/dockfleet/dockfleet/internal/store"
)

const token = "api-token-0123456789"

var fixedNow = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func newTestHandler(t *testing.T) (http.Handler, *store.Memory) {
	t.Helper()
	st := store.NewMemory()
	h := New(Config{
		Store:   st,
		Token:   token,
		NodeTTL: 30 * time.Second,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:     func() time.Time { return fixedNow },
	})
	return h, st
}

func addNode(t *testing.T, st *store.Memory, id string, cpu int64, heartbeat time.Time, labels map[string]string) {
	t.Helper()
	_, err := st.UpsertNode(context.Background(), store.Node{
		ID: id, Name: id, CPUMillis: cpu, MemoryBytes: 4 << 30, Labels: labels,
		RegisteredAt: heartbeat, LastHeartbeat: heartbeat,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func do(h http.Handler, method, path, body string, authed bool) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if authed {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decode(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("response is not JSON: %v: %q", err, rec.Body.String())
	}
}

func TestHealthAndReadinessAreOpen(t *testing.T) {
	h, _ := newTestHandler(t)
	for _, path := range []string{"/healthz", "/readyz"} {
		if rec := do(h, http.MethodGet, path, "", false); rec.Code != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", path, rec.Code)
		}
	}
}

func TestV1RequiresToken(t *testing.T) {
	h, _ := newTestHandler(t)
	for _, path := range []string{"/v1/nodes", "/v1/nodes/x", "/v1/workloads", "/v1/workloads/x", "/v1/events"} {
		rec := do(h, http.MethodGet, path, "", false)
		if rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") != "Bearer" {
			t.Errorf("GET %s without token = %d, want 401 with challenge", path, rec.Code)
		}
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/nodes", nil)
	req.Header.Set("Authorization", "Bearer wrong-token-0123456789")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("wrong token = %d, want 401", rec.Code)
	}
	if rec := do(h, http.MethodPost, "/v1/workloads", `{}`, false); rec.Code != http.StatusUnauthorized {
		t.Errorf("POST without token = %d, want 401", rec.Code)
	}
}

func TestListAndGetNodes(t *testing.T) {
	h, st := newTestHandler(t)
	addNode(t, st, "fresh", 1000, fixedNow.Add(-5*time.Second), nil)
	addNode(t, st, "stale", 1000, fixedNow.Add(-time.Hour), nil)

	rec := do(h, http.MethodGet, "/v1/nodes", "", true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var list struct {
		Nodes []struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"nodes"`
	}
	decode(t, rec, &list)
	if len(list.Nodes) != 2 || list.Nodes[0].ID != "fresh" || list.Nodes[0].Status != "ready" ||
		list.Nodes[1].ID != "stale" || list.Nodes[1].Status != "not_ready" {
		t.Fatalf("unexpected nodes: %+v", list.Nodes)
	}

	rec = do(h, http.MethodGet, "/v1/nodes/fresh", "", true)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":"ready"`) {
		t.Fatalf("GET node = %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(h, http.MethodGet, "/v1/nodes/ghost", "", true); rec.Code != http.StatusNotFound {
		t.Fatalf("GET missing node = %d, want 404", rec.Code)
	}
}

// Every node view says how long the node has been silent in the terms that
// matter to a replica on it: healthy, suspect (it gets no new replicas but keeps
// its own) or unavailable (its replicas are started elsewhere), and when its
// lease runs out. The state is computed when it is asked for, so it is current
// even before the reconciler has recorded the change.
func TestNodesShowTheirHealthAndLease(t *testing.T) {
	st := store.NewMemory()
	h := New(Config{
		Store: st, Token: token, NodeTTL: 30 * time.Second, NodeLease: time.Minute,
		Timing: scheduler.Timing{SuspectAfter: 30 * time.Second, UnavailableAfter: 2 * time.Minute},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return fixedNow },
	})
	addNode(t, st, "alive", 1000, fixedNow.Add(-5*time.Second), nil)
	addNode(t, st, "gone", 1000, fixedNow.Add(-time.Hour), nil)
	addNode(t, st, "left", 1000, fixedNow.Add(-time.Minute), nil)
	addNode(t, st, "quiet", 1000, fixedNow.Add(-time.Minute), nil)
	ctx := context.Background()
	if err := st.DeregisterNode(ctx, "left", fixedNow.Add(-10*time.Second)); err != nil {
		t.Fatal(err)
	}
	// The reconciler has recorded the loss of "gone" but not yet the silence of "quiet".
	recorded := fixedNow.Add(-58 * time.Minute)
	if err := st.SetNodeHealth(ctx, "gone", store.NodeUnavailable, recorded); err != nil {
		t.Fatal(err)
	}

	rec := do(h, http.MethodGet, "/v1/nodes", "", true)
	var list struct {
		Nodes []struct {
			ID             string     `json:"id"`
			Status         string     `json:"status"`
			Health         string     `json:"health"`
			HealthReason   string     `json:"health_reason"`
			HealthSince    *time.Time `json:"health_since"`
			LeaseExpiresAt *time.Time `json:"lease_expires_at"`
		} `json:"nodes"`
	}
	decode(t, rec, &list)
	if rec.Code != http.StatusOK || len(list.Nodes) != 4 {
		t.Fatalf("GET /v1/nodes = %d %s", rec.Code, rec.Body.String())
	}
	byID := map[string]int{}
	for i, n := range list.Nodes {
		byID[n.ID] = i
	}
	get := func(id string) int { return byID[id] }

	alive := list.Nodes[get("alive")]
	if alive.Health != store.NodeHealthy || alive.Status != "ready" || alive.HealthReason != "" ||
		alive.LeaseExpiresAt == nil || !alive.LeaseExpiresAt.Equal(fixedNow.Add(-5*time.Second).Add(time.Minute)) ||
		alive.HealthSince == nil || !alive.HealthSince.Equal(fixedNow.Add(-5*time.Second)) {
		t.Fatalf("alive = %+v", alive)
	}
	quiet := list.Nodes[get("quiet")]
	if quiet.Health != store.NodeSuspect || quiet.Status != "not_ready" || !strings.Contains(quiet.HealthReason, "30s") || quiet.HealthSince != nil {
		t.Fatalf("quiet = %+v; the state is computed now, and HealthSince waits for the reconciler", quiet)
	}
	gone := list.Nodes[get("gone")]
	if gone.Health != store.NodeUnavailable || !strings.Contains(gone.HealthReason, "2m0s") || gone.HealthSince == nil || !gone.HealthSince.Equal(recorded) {
		t.Fatalf("gone = %+v", gone)
	}
	left := list.Nodes[get("left")]
	if left.Health != store.NodeSuspect || left.LeaseExpiresAt != nil {
		t.Fatalf("left = %+v; an agent that said goodbye holds no lease", left)
	}

	one := do(h, http.MethodGet, "/v1/nodes/gone", "", true)
	if one.Code != http.StatusOK || !strings.Contains(one.Body.String(), `"health":"unavailable"`) {
		t.Fatalf("GET node = %d %s", one.Code, one.Body.String())
	}
}

// Without timings of its own the API still calls a node suspect once it has missed
// its heartbeats, and never calls it unavailable.
func TestNodeHealthDefaultsToTheHeartbeatLimit(t *testing.T) {
	h, st := newTestHandler(t)
	addNode(t, st, "fresh", 1000, fixedNow.Add(-5*time.Second), nil)
	addNode(t, st, "stale", 1000, fixedNow.Add(-24*time.Hour), nil)
	var list struct {
		Nodes []struct {
			ID     string `json:"id"`
			Health string `json:"health"`
		} `json:"nodes"`
	}
	decode(t, do(h, http.MethodGet, "/v1/nodes", "", true), &list)
	if len(list.Nodes) != 2 || list.Nodes[0].Health != store.NodeHealthy || list.Nodes[1].Health != store.NodeSuspect {
		t.Fatalf("nodes = %+v", list.Nodes)
	}
}

func TestCreateWorkloadSchedulesOnReadyNode(t *testing.T) {
	h, st := newTestHandler(t)
	addNode(t, st, "stale-big", 64000, fixedNow.Add(-time.Hour), nil)
	addNode(t, st, "small", 1000, fixedNow, nil)
	addNode(t, st, "large", 4000, fixedNow, nil)

	rec := do(h, http.MethodPost, "/v1/workloads", `{"name":"web","image":"nginx:1.27","cpu_millis":500,"memory_bytes":134217728}`, true)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var created store.Workload
	decode(t, rec, &created)
	if created.NodeID != "large" || created.DesiredState != store.DesiredRunning || created.Phase() != store.WorkloadScheduled || created.Observed != nil || created.ID == "" {
		t.Fatalf("unexpected workload: %+v", created)
	}
	// Desired and observed state are separate fields in the API too.
	if body := rec.Body.String(); !strings.Contains(body, `"desired_state":"running"`) || !strings.Contains(body, `"observed":null`) {
		t.Errorf("response should carry desired_state and an empty observed: %s", body)
	}
	if loc := rec.Header().Get("Location"); loc != "/v1/workloads/"+created.ID {
		t.Errorf("Location = %q", loc)
	}

	rec = do(h, http.MethodGet, "/v1/workloads/"+created.ID, "", true)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET workload = %d", rec.Code)
	}
	rec = do(h, http.MethodGet, "/v1/workloads", "", true)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"name":"web"`) {
		t.Fatalf("list workloads = %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(h, http.MethodGet, "/v1/workloads/ghost", "", true); rec.Code != http.StatusNotFound {
		t.Fatalf("GET missing workload = %d, want 404", rec.Code)
	}
}

func TestCreateWorkloadAccountsForExistingPlacements(t *testing.T) {
	h, st := newTestHandler(t)
	addNode(t, st, "only", 1000, fixedNow, nil)
	body := func(name string) string {
		return `{"name":"` + name + `","image":"x","cpu_millis":600,"memory_bytes":1024}`
	}
	if rec := do(h, http.MethodPost, "/v1/workloads", body("one"), true); rec.Code != http.StatusCreated {
		t.Fatalf("first = %d: %s", rec.Code, rec.Body.String())
	}
	if rec := do(h, http.MethodPost, "/v1/workloads", body("two"), true); rec.Code != http.StatusConflict {
		t.Fatalf("overcommit = %d, want 409: %s", rec.Code, rec.Body.String())
	}
}

func TestCreateWorkloadNoReadyNodes(t *testing.T) {
	h, st := newTestHandler(t)
	addNode(t, st, "stale", 4000, fixedNow.Add(-time.Hour), nil)
	rec := do(h, http.MethodPost, "/v1/workloads", `{"name":"w","image":"x","cpu_millis":1,"memory_bytes":1}`, true)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
}

func TestCreateWorkloadNodeSelector(t *testing.T) {
	h, st := newTestHandler(t)
	addNode(t, st, "plain", 8000, fixedNow, map[string]string{"zone": "a"})
	addNode(t, st, "gpu", 1000, fixedNow, map[string]string{"zone": "b"})
	rec := do(h, http.MethodPost, "/v1/workloads", `{"name":"w","image":"x","cpu_millis":1,"memory_bytes":1,"node_selector":{"zone":"b"}}`, true)
	var created store.Workload
	decode(t, rec, &created)
	if rec.Code != http.StatusCreated || created.NodeID != "gpu" {
		t.Fatalf("status = %d, workload = %+v", rec.Code, created)
	}
}

func TestCreateWorkloadDuplicateName(t *testing.T) {
	h, st := newTestHandler(t)
	addNode(t, st, "n", 4000, fixedNow, nil)
	body := `{"name":"dup","image":"x","cpu_millis":1,"memory_bytes":1}`
	if rec := do(h, http.MethodPost, "/v1/workloads", body, true); rec.Code != http.StatusCreated {
		t.Fatalf("first = %d", rec.Code)
	}
	if rec := do(h, http.MethodPost, "/v1/workloads", body, true); rec.Code != http.StatusConflict {
		t.Fatalf("duplicate = %d, want 409", rec.Code)
	}
}

func TestCreateWorkloadValidation(t *testing.T) {
	h, st := newTestHandler(t)
	addNode(t, st, "n", 4000, fixedNow, nil)
	bodies := map[string]string{
		"malformed json":   `{`,
		"unknown field":    `{"name":"w","image":"x","cpu_millis":1,"memory_bytes":1,"privileged":true}`,
		"bad name":         `{"name":"a b","image":"x","cpu_millis":1,"memory_bytes":1}`,
		"missing image":    `{"name":"w","cpu_millis":1,"memory_bytes":1}`,
		"image whitespace": `{"name":"w","image":"x y","cpu_millis":1,"memory_bytes":1}`,
		"zero cpu":         `{"name":"w","image":"x","cpu_millis":0,"memory_bytes":1}`,
		"negative memory":  `{"name":"w","image":"x","cpu_millis":1,"memory_bytes":-1}`,
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			if rec := do(h, http.MethodPost, "/v1/workloads", body, true); rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestOversizedBodyRejected(t *testing.T) {
	h, _ := newTestHandler(t)
	body := `{"name":"w","image":"` + strings.Repeat("x", maxBodyBytes) + `","cpu_millis":1,"memory_bytes":1}`
	if rec := do(h, http.MethodPost, "/v1/workloads", body, true); rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestEnrollmentLifecycle(t *testing.T) {
	h, st := newTestHandler(t)

	if rec := do(h, http.MethodPost, "/v1/enrollments", `{"node_id":"worker-1"}`, false); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated enrollment = %d, want 401", rec.Code)
	}

	rec := do(h, http.MethodPost, "/v1/enrollments", `{"node_id":"worker-1","description":"rack 4"}`, true)
	if rec.Code != http.StatusCreated || rec.Header().Get("Location") != "/v1/enrollments/worker-1" {
		t.Fatalf("enroll = %d %s (Location %q)", rec.Code, rec.Body.String(), rec.Header().Get("Location"))
	}
	var created store.Enrollment
	decode(t, rec, &created)
	if created.NodeID != "worker-1" || created.Description != "rack 4" || !created.CreatedAt.Equal(fixedNow) || !created.Active() {
		t.Fatalf("unexpected enrollment: %+v", created)
	}
	if e, err := st.GetEnrollment(context.Background(), "worker-1"); err != nil || !e.Active() {
		t.Fatalf("enrollment was not stored: %+v, %v", e, err)
	}

	if rec := do(h, http.MethodPost, "/v1/enrollments", `{"node_id":"worker-1"}`, true); rec.Code != http.StatusConflict {
		t.Fatalf("duplicate enrollment = %d, want 409", rec.Code)
	}
	if rec := do(h, http.MethodGet, "/v1/enrollments/worker-1", "", true); rec.Code != http.StatusOK {
		t.Fatalf("GET enrollment = %d", rec.Code)
	}
	rec = do(h, http.MethodGet, "/v1/enrollments", "", true)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"node_id":"worker-1"`) {
		t.Fatalf("list enrollments = %d %s", rec.Code, rec.Body.String())
	}

	rec = do(h, http.MethodDelete, "/v1/enrollments/worker-1", "", true)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"revoked_at"`) {
		t.Fatalf("revoke = %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(h, http.MethodDelete, "/v1/enrollments/worker-1", "", true); rec.Code != http.StatusOK {
		t.Fatalf("revoking twice = %d, want 200", rec.Code)
	}
	if e, _ := st.GetEnrollment(context.Background(), "worker-1"); e.Active() {
		t.Fatalf("enrollment must be revoked: %+v", e)
	}
	if rec := do(h, http.MethodPost, "/v1/enrollments", `{"node_id":"worker-1"}`, true); rec.Code != http.StatusCreated {
		t.Fatalf("re-enrolling a revoked node = %d, want 201", rec.Code)
	}
}

func TestEnrollmentValidationAndUnknownNodes(t *testing.T) {
	h, _ := newTestHandler(t)
	bodies := map[string]string{
		"malformed json":     `{`,
		"unknown field":      `{"node_id":"w","admin":true}`,
		"missing node id":    `{}`,
		"bad node id":        `{"node_id":"../etc"}`,
		"long description":   `{"node_id":"w","description":"` + strings.Repeat("d", 201) + `"}`,
		"node id with space": `{"node_id":"a b"}`,
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			if rec := do(h, http.MethodPost, "/v1/enrollments", body, true); rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
			}
		})
	}
	if rec := do(h, http.MethodGet, "/v1/enrollments/ghost", "", true); rec.Code != http.StatusNotFound {
		t.Fatalf("GET unknown enrollment = %d, want 404", rec.Code)
	}
	if rec := do(h, http.MethodDelete, "/v1/enrollments/ghost", "", true); rec.Code != http.StatusNotFound {
		t.Fatalf("revoking an unknown enrollment = %d, want 404", rec.Code)
	}
	if rec := do(h, http.MethodDelete, "/v1/enrollments/ghost", "", false); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated revoke = %d, want 401", rec.Code)
	}
}

type eventList struct {
	Events []store.Event `json:"events"`
}

func TestAdministrativeActionsAreRecordedAsEvents(t *testing.T) {
	h, st := newTestHandler(t)
	addNode(t, st, "n", 4000, fixedNow, nil)

	do(h, http.MethodPost, "/v1/enrollments", `{"node_id":"worker-1","description":"rack 4"}`, true)
	created := do(h, http.MethodPost, "/v1/workloads", `{"name":"web","image":"nginx","cpu_millis":1,"memory_bytes":1}`, true)
	var wl store.Workload
	decode(t, created, &wl)
	do(h, http.MethodDelete, "/v1/enrollments/worker-1", "", true)
	do(h, http.MethodDelete, "/v1/enrollments/worker-1", "", true) // revoking again changes nothing

	var all eventList
	decode(t, do(h, http.MethodGet, "/v1/events", "", true), &all)
	var actions []string
	for _, e := range all.Events {
		actions = append(actions, e.Action)
		if e.Actor != "admin" || e.At.IsZero() {
			t.Errorf("event lacks its actor or time: %+v", e)
		}
	}
	want := []string{"enrollment.revoked", "workload.created", "enrollment.created"} // newest first
	if strings.Join(actions, ",") != strings.Join(want, ",") {
		t.Fatalf("actions = %v, want %v (a repeated revocation must not be recorded twice)", actions, want)
	}
	if all.Events[1].ResourceType != "workload" || all.Events[1].ResourceID != wl.ID || all.Events[1].Details["node_id"] != "n" {
		t.Errorf("workload event = %+v", all.Events[1])
	}

	var one eventList
	decode(t, do(h, http.MethodGet, "/v1/events?resource_type=node&resource_id=worker-1&limit=1", "", true), &one)
	if len(one.Events) != 1 || one.Events[0].Action != "enrollment.revoked" {
		t.Fatalf("filtered events = %+v", one.Events)
	}
	var older eventList
	decode(t, do(h, http.MethodGet, "/v1/events?resource_type=node&before="+strconv.FormatInt(one.Events[0].ID, 10), "", true), &older)
	if len(older.Events) != 1 || older.Events[0].Action != "enrollment.created" {
		t.Fatalf("events before the newest = %+v", older.Events)
	}

	for _, query := range []string{"limit=0", "limit=x", "before=-1", "before=x"} {
		if rec := do(h, http.MethodGet, "/v1/events?"+query, "", true); rec.Code != http.StatusBadRequest {
			t.Errorf("GET /v1/events?%s = %d, want 400", query, rec.Code)
		}
	}
}

func TestRefusedRequestsLeaveNoEvents(t *testing.T) {
	h, st := newTestHandler(t)
	addNode(t, st, "n", 1000, fixedNow, nil)
	do(h, http.MethodPost, "/v1/enrollments", `{"node_id":"worker-1"}`, true)
	// A duplicate enrollment, a workload that fits nowhere, and an unknown revocation.
	do(h, http.MethodPost, "/v1/enrollments", `{"node_id":"worker-1"}`, true)
	do(h, http.MethodPost, "/v1/workloads", `{"name":"big","image":"x","cpu_millis":9000,"memory_bytes":1}`, true)
	do(h, http.MethodDelete, "/v1/enrollments/ghost", "", true)

	events, err := st.ListEvents(context.Background(), store.EventFilter{})
	if err != nil || len(events) != 1 || events[0].Action != "enrollment.created" {
		t.Fatalf("events = %+v, %v; only the one successful change may be recorded", events, err)
	}
}

func TestPlacementAccountsForDesiredNotObservedState(t *testing.T) {
	h, st := newTestHandler(t)
	ctx := context.Background()
	addNode(t, st, "only", 1000, fixedNow, nil)
	body := func(name string) string {
		return `{"name":"` + name + `","image":"x","cpu_millis":800,"memory_bytes":1024}`
	}
	rec := do(h, http.MethodPost, "/v1/workloads", body("one"), true)
	var first store.Workload
	decode(t, rec, &first)

	// Observed failure does not free the capacity the workload is meant to have...
	if err := st.RecordObservations(ctx, "only", []store.Observation{{WorkloadID: first.ID, State: store.WorkloadFailed, ObservedAt: fixedNow}}); err != nil {
		t.Fatal(err)
	}
	if rec := do(h, http.MethodPost, "/v1/workloads", body("two"), true); rec.Code != http.StatusConflict {
		t.Fatalf("second = %d, want 409: a failed container is still desired to run", rec.Code)
	}
	// ...but asking for it to be stopped does.
	if _, err := st.SetWorkloadDesiredState(ctx, first.ID, store.DesiredStopped, fixedNow); err != nil {
		t.Fatal(err)
	}
	if rec := do(h, http.MethodPost, "/v1/workloads", body("two"), true); rec.Code != http.StatusCreated {
		t.Fatalf("second after stopping the first = %d: %s", rec.Code, rec.Body.String())
	}
}

func TestWorkloadResponsesShowObservedStateSeparately(t *testing.T) {
	h, st := newTestHandler(t)
	addNode(t, st, "n", 4000, fixedNow, nil)
	var wl store.Workload
	decode(t, do(h, http.MethodPost, "/v1/workloads", `{"name":"web","image":"x","cpu_millis":1,"memory_bytes":1}`, true), &wl)
	exit := 137
	if err := st.RecordObservations(context.Background(), "n", []store.Observation{{
		WorkloadID: wl.ID, State: store.WorkloadFailed, Message: "oom", ExitCode: &exit, OOMKilled: true, ObservedAt: fixedNow,
	}}); err != nil {
		t.Fatal(err)
	}

	var got store.Workload
	decode(t, do(h, http.MethodGet, "/v1/workloads/"+wl.ID, "", true), &got)
	if got.DesiredState != store.DesiredRunning || got.Observed == nil || got.Observed.State != store.WorkloadFailed ||
		got.Observed.ExitCode == nil || *got.Observed.ExitCode != 137 || !got.Observed.OOMKilled || got.Phase() != store.WorkloadFailed {
		t.Fatalf("workload = %+v / %+v", got, got.Observed)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	h, _ := newTestHandler(t)
	if rec := do(h, http.MethodDelete, "/v1/nodes", "", true); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}
