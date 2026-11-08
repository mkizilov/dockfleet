package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
	for _, path := range []string{"/v1/nodes", "/v1/nodes/x", "/v1/workloads", "/v1/workloads/x"} {
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
	if created.NodeID != "large" || created.State != store.WorkloadScheduled || created.ID == "" {
		t.Fatalf("unexpected workload: %+v", created)
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

func TestMethodNotAllowed(t *testing.T) {
	h, _ := newTestHandler(t)
	if rec := do(h, http.MethodDelete, "/v1/nodes", "", true); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}
