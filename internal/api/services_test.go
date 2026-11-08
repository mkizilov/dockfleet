package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/store"
)

// The service API tests are one suite that takes a store factory. They run
// against the in-memory store always, and against PostgreSQL under the
// `integration` build tag (services_postgres_test.go), so both back ends must
// behave the same through the HTTP interface.

const minimalService = `{"name":"web","replicas":2,"spec":{"image":"nginx:1.27","cpu_millis":500,"memory_bytes":134217728}}`

type serviceEnv struct {
	t     *testing.T
	h     http.Handler
	st    store.Store
	clock time.Time
}

func newServiceEnv(t *testing.T, st store.Store) *serviceEnv {
	t.Helper()
	e := &serviceEnv{t: t, st: st, clock: fixedNow}
	e.h = New(Config{
		Store:      st,
		Token:      token,
		NodeTTL:    30 * time.Second,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		InstanceID: "test-manager",
		Now:        func() time.Time { return e.clock },
	})
	return e
}

// request sends an authenticated request. Header pairs (name, value, ...) are
// added afterwards and may replace the credentials.
func (e *serviceEnv) request(method, path, body string, headers ...string) *httptest.ResponseRecorder {
	e.t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	e.h.ServeHTTP(rec, req)
	return rec
}

// run is t.Run for a subtest that uses the same store: while it runs, the
// env's helpers report failures to the subtest's t, not to its parent's.
func (e *serviceEnv) run(t *testing.T, name string, fn func(t *testing.T)) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		parent := e.t
		e.t = t
		defer func() { e.t = parent }()
		fn(t)
	})
}

func (e *serviceEnv) create(body string) serviceView {
	e.t.Helper()
	rec := e.request(http.MethodPost, "/v1/services", body)
	if rec.Code != http.StatusCreated {
		e.t.Fatalf("create = %d: %s", rec.Code, rec.Body.String())
	}
	var v serviceView
	decode(e.t, rec, &v)
	return v
}

func (e *serviceEnv) get(name string) serviceView {
	e.t.Helper()
	rec := e.request(http.MethodGet, "/v1/services/"+name, "")
	if rec.Code != http.StatusOK {
		e.t.Fatalf("get %s = %d: %s", name, rec.Code, rec.Body.String())
	}
	var v serviceView
	decode(e.t, rec, &v)
	return v
}

// events returns the recorded actions, oldest first.
func (e *serviceEnv) events() []string {
	e.t.Helper()
	list, err := e.st.ListEvents(context.Background(), store.EventFilter{})
	if err != nil {
		e.t.Fatal(err)
	}
	var actions []string
	for i := len(list) - 1; i >= 0; i-- {
		actions = append(actions, list[i].Action)
	}
	return actions
}

func (e *serviceEnv) wantEvents(want ...string) {
	e.t.Helper()
	if got := e.events(); !reflect.DeepEqual(got, want) && !(len(got) == 0 && len(want) == 0) {
		e.t.Fatalf("events = %v, want %v", got, want)
	}
}

func (e *serviceEnv) wantNoServices() {
	e.t.Helper()
	list, err := e.st.ListServices(context.Background(), true)
	if err != nil || len(list) != 0 {
		e.t.Fatalf("services = %+v, %v; want none", list, err)
	}
	e.wantEvents()
}

type serviceTest struct {
	name string
	run  func(t *testing.T, e *serviceEnv)
}

var serviceTests = []serviceTest{
	{"authentication", testServiceAuthentication},
	{"create and inspect", testServiceCreateAndGet},
	{"full definition", testServiceFullDefinition},
	{"list", testServiceList},
	{"unknown services", testServiceNotFound},
	{"duplicate names", testServiceDuplicateName},
	{"update", testServiceUpdate},
	{"equivalent definitions are not changes", testServiceUpdateIsCanonical},
	{"read, edit, write back", testServiceRoundTrip},
	{"optimistic locking", testServiceOptimisticLocking},
	{"delete", testServiceDelete},
	{"revisions", testServiceRevisions},
	{"validation", testServiceValidation},
	{"unsafe options", testServiceUnsafeOptions},
	{"request limits and secrets", testServiceRequestLimits},
	{"defining a service schedules nothing", testServiceDoesNotSchedule},
	{"status shows the scheduler's assignments", testServiceStatusShowsAssignments},
	{"status explains an unschedulable replica", testServiceStatusExplainsWhyNot},
	{"status names the reason for every node", testServiceStatusNamesEveryReason},
	{"status across revisions, scaling and deletion", testServiceStatusAcrossRevisionsScalingAndDeletion},
	{"standalone workloads use the same scheduler", testWorkloadsUseTheSameScheduler},
	{"the node view explains availability", testNodeViewExplainsAvailability},
	{"events", testServiceEvents},
}

// runServiceAPITests runs the suite, each test on an empty store from newStore.
func runServiceAPITests(t *testing.T, newStore func(t *testing.T) store.Store) {
	t.Helper()
	for _, tt := range serviceTests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t, newServiceEnv(t, newStore(t)))
		})
	}
}

func TestServiceAPIWithMemoryStore(t *testing.T) {
	runServiceAPITests(t, func(*testing.T) store.Store { return store.NewMemory() })
}

func testServiceAuthentication(t *testing.T, e *serviceEnv) {
	e.create(minimalService)
	routes := []struct{ method, path, body string }{
		{http.MethodPost, "/v1/services", minimalService},
		{http.MethodGet, "/v1/services", ""},
		{http.MethodGet, "/v1/services/web", ""},
		{http.MethodPut, "/v1/services/web", `{"replicas":9,"spec":{"image":"nginx:2","cpu_millis":1,"memory_bytes":134217728}}`},
		{http.MethodDelete, "/v1/services/web", ""},
		{http.MethodGet, "/v1/services/web/revisions", ""},
	}
	credentials := []string{
		"",
		"Bearer",
		"Bearer ",
		"Bearer wrong-token-0123456789",
		"bearer " + token,
		"BEARER " + token,
		"Basic " + token,
		token,
		"Bearer " + token + " ",
		"Bearer  " + token,
		"Bearer " + token[:len(token)-1],
		"Bearer " + token + "x",
	}
	for _, route := range routes {
		for _, cred := range credentials {
			rec := e.request(route.method, route.path, route.body, "Authorization", cred)
			if rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") != "Bearer" {
				t.Errorf("%s %s with %q = %d, want 401 with a Bearer challenge", route.method, route.path, cred, rec.Code)
			}
			if strings.Contains(rec.Body.String(), "nginx") {
				t.Errorf("%s %s with %q leaked service data: %s", route.method, route.path, cred, rec.Body.String())
			}
		}
	}
	// A token in the query string is not a credential.
	if rec := e.request(http.MethodGet, "/v1/services?token="+token, "", "Authorization", ""); rec.Code != http.StatusUnauthorized {
		t.Errorf("token in the query string = %d, want 401", rec.Code)
	}

	// None of the rejected requests changed anything.
	v := e.get("web")
	if v.Replicas != 2 || v.Version != 1 || v.DeletedAt != nil || v.Spec.Image != "nginx:1.27" {
		t.Fatalf("service after rejected requests = %+v", v)
	}
	e.wantEvents("service.created")

	// And the right credentials still work on every route.
	for _, route := range routes[1:3] {
		if rec := e.request(route.method, route.path, route.body); rec.Code != http.StatusOK {
			t.Errorf("%s %s with the token = %d", route.method, route.path, rec.Code)
		}
	}
}

func testServiceCreateAndGet(t *testing.T, e *serviceEnv) {
	rec := e.request(http.MethodPost, "/v1/services", minimalService)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create = %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Location") != "/v1/services/web" || rec.Header().Get("ETag") != `"1"` {
		t.Errorf("Location = %q, ETag = %q", rec.Header().Get("Location"), rec.Header().Get("ETag"))
	}
	if rec.Header().Get("Cache-Control") != "no-store" || rec.Header().Get("Content-Type") != "application/json" {
		t.Errorf("response headers = %v", rec.Header())
	}
	var created serviceView
	decode(t, rec, &created)
	want := store.ServiceSpec{
		Image: "nginx:1.27", CPUMillis: 500, MemoryBytes: 134217728,
		RestartPolicy: &store.RestartPolicy{Condition: store.RestartOnFailure, MaxRetries: 5},
	}
	if len(created.ID) != 32 || created.Name != "web" || created.Replicas != 2 || created.Revision != 1 || created.Version != 1 ||
		!created.CreatedAt.Equal(fixedNow) || !created.UpdatedAt.Equal(fixedNow) || created.DeletedAt != nil {
		t.Fatalf("created = %+v", created)
	}
	if !reflect.DeepEqual(created.Spec, want) {
		t.Fatalf("spec = %+v\nwant %+v (the restart policy default is filled in)", created.Spec, want)
	}

	rec = e.request(http.MethodGet, "/v1/services/web", "")
	var got serviceView
	decode(t, rec, &got)
	if rec.Header().Get("ETag") != `"1"` || rec.Header().Get("Cache-Control") != "no-store" || !reflect.DeepEqual(got, created) {
		t.Fatalf("GET = %+v (ETag %q), want what POST returned", got, rec.Header().Get("ETag"))
	}

	// The desired specification is what the store holds.
	svc, err := e.st.GetServiceByName(context.Background(), "web")
	if err != nil || svc.ID != created.ID || svc.Replicas != 2 {
		t.Fatalf("stored service = %+v, %v", svc, err)
	}
	rev, err := e.st.GetRevision(context.Background(), svc.ID, 1)
	if err != nil || !reflect.DeepEqual(rev.Spec, want) || rev.CreatedBy != "admin" || rev.SpecHash != want.Hash() {
		t.Fatalf("stored revision = %+v, %v", rev, err)
	}
	e.wantEvents("service.created")
	events, _ := e.st.ListEvents(context.Background(), store.EventFilter{})
	if ev := events[0]; ev.Actor != "admin" || ev.InstanceID != "test-manager" || ev.ResourceType != "service" || ev.ResourceID != created.ID ||
		ev.Details["name"] != "web" || ev.Details["image"] != "nginx:1.27" || ev.Details["replicas"] != "2" {
		t.Fatalf("event = %+v", ev)
	}
}

// fullImage is pinned by digest, to exercise the whole image reference grammar.
var fullImage = "registry.example.com:5000/team/api@sha256:" + strings.Repeat("a", 64)

var fullService = `{
  "name": "api",
  "replicas": 3,
  "spec": {
    "image": "` + fullImage + `",
    "command": ["/bin/api", "--port", "8080"],
    "env": {"MODE": "prod", "EMPTY": ""},
    "cpu_millis": 1500,
    "memory_bytes": 536870912,
    "ports": [
      {"container_port": 8080, "host_port": 18080},
      {"container_port": 53, "protocol": "udp"},
      {"container_port": 80, "protocol": "tcp"}
    ],
    "health_check": {"command": ["curl", "-f", "http://localhost:8080/healthz"], "interval_seconds": 10, "timeout_seconds": 2, "start_period_seconds": 15, "retries": 4},
    "restart_policy": {"condition": "on-failure", "max_retries": 3},
    "node_selector": {"zone": "a", "disk": "ssd"}
  }
}`

func testServiceFullDefinition(t *testing.T, e *serviceEnv) {
	want := store.ServiceSpec{
		Image:       fullImage,
		Command:     []string{"/bin/api", "--port", "8080"},
		Env:         map[string]string{"MODE": "prod", "EMPTY": ""},
		CPUMillis:   1500,
		MemoryBytes: 536870912,
		// Sorted by container port, with the protocol defaulted to tcp.
		Ports: []store.ServicePort{
			{ContainerPort: 53, Protocol: "udp"},
			{ContainerPort: 80, Protocol: "tcp"},
			{ContainerPort: 8080, Protocol: "tcp", HostPort: 18080},
		},
		HealthCheck:   &store.HealthCheck{Command: []string{"curl", "-f", "http://localhost:8080/healthz"}, IntervalSeconds: 10, TimeoutSeconds: 2, StartPeriodSeconds: 15, Retries: 4},
		RestartPolicy: &store.RestartPolicy{Condition: store.RestartOnFailure, MaxRetries: 3},
		NodeSelector:  map[string]string{"zone": "a", "disk": "ssd"},
	}
	created := e.create(fullService)
	if !reflect.DeepEqual(created.Spec, want) {
		t.Fatalf("created spec = %+v\nwant %+v", created.Spec, want)
	}
	if got := e.get("api"); !reflect.DeepEqual(got.Spec, want) || got.Replicas != 3 {
		t.Fatalf("stored spec = %+v\nwant %+v", got.Spec, want)
	}

	// Defaults for the optional parts of a health check and the three restart conditions.
	e.create(`{"name":"defaults","replicas":1,"spec":{"image":"x","cpu_millis":1,"memory_bytes":134217728,"health_check":{"command":["true"]},"restart_policy":{"condition":"on-failure"}}}`)
	d := e.get("defaults").Spec
	if !reflect.DeepEqual(d.HealthCheck, &store.HealthCheck{Command: []string{"true"}, IntervalSeconds: 30, TimeoutSeconds: 5, StartPeriodSeconds: 0, Retries: 3}) ||
		!reflect.DeepEqual(d.RestartPolicy, &store.RestartPolicy{Condition: store.RestartOnFailure, MaxRetries: 5}) {
		t.Fatalf("defaults = %+v / %+v", d.HealthCheck, d.RestartPolicy)
	}
	for _, condition := range []string{"no", "always"} {
		name := "restart-" + condition
		e.create(`{"name":"` + name + `","replicas":0,"spec":{"image":"x","cpu_millis":1,"memory_bytes":134217728,"restart_policy":{"condition":"` + condition + `"}}}`)
		if rp := e.get(name).Spec.RestartPolicy; rp == nil || rp.Condition != condition || rp.MaxRetries != 0 {
			t.Errorf("restart policy %q = %+v", condition, rp)
		}
	}
}

func testServiceList(t *testing.T, e *serviceEnv) {
	rec := e.request(http.MethodGet, "/v1/services", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"services":[]`) {
		t.Fatalf("empty list = %d %s; want an empty array, not null", rec.Code, rec.Body.String())
	}
	for _, name := range []string{"charlie", "alpha", "bravo"} {
		e.create(strings.Replace(minimalService, `"web"`, `"`+name+`"`, 1))
	}
	var list struct {
		Services []serviceView `json:"services"`
	}
	decode(t, e.request(http.MethodGet, "/v1/services", ""), &list)
	if len(list.Services) != 3 || list.Services[0].Name != "alpha" || list.Services[1].Name != "bravo" || list.Services[2].Name != "charlie" {
		t.Fatalf("list = %+v, want alphabetical order", list.Services)
	}
	for _, s := range list.Services {
		if s.Spec.Image != "nginx:1.27" || s.Revision != 1 || s.ID == "" {
			t.Errorf("list entry lacks its definition: %+v", s)
		}
	}

	// The list shows each service's current revision.
	e.request(http.MethodPut, "/v1/services/alpha", `{"replicas":2,"spec":{"image":"nginx:1.28","cpu_millis":500,"memory_bytes":134217728}}`)
	decode(t, e.request(http.MethodGet, "/v1/services", ""), &list)
	if list.Services[0].Revision != 2 || list.Services[0].Spec.Image != "nginx:1.28" || list.Services[1].Revision != 1 {
		t.Fatalf("list after a deployment = %+v", list.Services)
	}

	if rec := e.request(http.MethodGet, "/v1/services?include_deleted=maybe", ""); rec.Code != http.StatusBadRequest {
		t.Errorf("include_deleted=maybe = %d, want 400", rec.Code)
	}
}

func testServiceNotFound(t *testing.T, e *serviceEnv) {
	created := e.create(minimalService)
	for _, tt := range []struct{ method, path, body string }{
		{http.MethodGet, "/v1/services/ghost", ""},
		{http.MethodPut, "/v1/services/ghost", `{"replicas":1,"spec":{"image":"x","cpu_millis":1,"memory_bytes":134217728}}`},
		{http.MethodDelete, "/v1/services/ghost", ""},
		{http.MethodGet, "/v1/services/ghost/revisions", ""},
		// Services are addressed by name; the ID is not a name.
		{http.MethodGet, "/v1/services/" + created.ID, ""},
		{http.MethodDelete, "/v1/services/" + created.ID, ""},
	} {
		if rec := e.request(tt.method, tt.path, tt.body); rec.Code != http.StatusNotFound {
			t.Errorf("%s %s = %d, want 404: %s", tt.method, tt.path, rec.Code, rec.Body.String())
		}
	}
	if v := e.get("web"); v.Version != 1 || v.DeletedAt != nil {
		t.Fatalf("a request for another service changed web: %+v", v)
	}
}

func testServiceDuplicateName(t *testing.T, e *serviceEnv) {
	first := e.create(minimalService)
	other := strings.Replace(minimalService, `"replicas":2`, `"replicas":7`, 1)
	if rec := e.request(http.MethodPost, "/v1/services", other); rec.Code != http.StatusConflict {
		t.Fatalf("duplicate = %d, want 409: %s", rec.Code, rec.Body.String())
	}
	if v := e.get("web"); v.ID != first.ID || v.Replicas != 2 {
		t.Fatalf("the existing service was changed by the rejected duplicate: %+v", v)
	}
	e.wantEvents("service.created")
}

func put(e *serviceEnv, name, body string, headers ...string) *httptest.ResponseRecorder {
	return e.request(http.MethodPut, "/v1/services/"+name, body, headers...)
}

func serviceBodyWith(image string, replicas string) string {
	return `{"replicas":` + replicas + `,"spec":{"image":"` + image + `","cpu_millis":500,"memory_bytes":134217728}}`
}

func testServiceUpdate(t *testing.T, e *serviceEnv) {
	created := e.create(minimalService)

	// The same definition again: nothing changes and nothing is recorded.
	e.clock = e.clock.Add(time.Minute)
	rec := put(e, "web", serviceBodyWith("nginx:1.27", "2"))
	var same serviceView
	decode(t, rec, &same)
	if rec.Code != http.StatusOK || same.Version != 1 || same.Revision != 1 || !same.UpdatedAt.Equal(fixedNow) {
		t.Fatalf("unchanged PUT = %d %+v", rec.Code, same)
	}
	e.wantEvents("service.created")

	// Only the replica count: a scaling, not a new revision.
	e.clock = e.clock.Add(time.Minute)
	rec = put(e, "web", serviceBodyWith("nginx:1.27", "5"))
	var scaled serviceView
	decode(t, rec, &scaled)
	if rec.Code != http.StatusOK || scaled.Replicas != 5 || scaled.Revision != 1 || scaled.Version != 2 || !scaled.UpdatedAt.Equal(e.clock) || scaled.ID != created.ID {
		t.Fatalf("scaling PUT = %d %+v", rec.Code, scaled)
	}
	if rec.Header().Get("ETag") != `"2"` {
		t.Errorf("ETag = %q", rec.Header().Get("ETag"))
	}

	// Only the spec: a new deployment revision.
	rec = put(e, "web", serviceBodyWith("nginx:1.28", "5"))
	var deployed serviceView
	decode(t, rec, &deployed)
	if rec.Code != http.StatusOK || deployed.Revision != 2 || deployed.Version != 3 || deployed.Replicas != 5 || deployed.Spec.Image != "nginx:1.28" {
		t.Fatalf("deploying PUT = %d %+v", rec.Code, deployed)
	}

	// Both at once, scaling to zero.
	rec = put(e, "web", serviceBodyWith("nginx:1.29", "0"))
	var both serviceView
	decode(t, rec, &both)
	if rec.Code != http.StatusOK || both.Revision != 3 || both.Version != 5 || both.Replicas != 0 || both.Spec.Image != "nginx:1.29" {
		t.Fatalf("PUT changing both = %d %+v", rec.Code, both)
	}

	e.wantEvents("service.created", "service.scaled", "service.deployed", "service.deployed", "service.scaled")
	events, _ := e.st.ListEvents(context.Background(), store.EventFilter{ResourceType: "service"})
	// Newest first: scaled 5 -> 0, deployed 2 -> 3, deployed 1 -> 2, scaled 2 -> 5.
	if events[0].Details["from_replicas"] != "5" || events[0].Details["to_replicas"] != "0" ||
		events[1].Details["from_revision"] != "2" || events[1].Details["to_revision"] != "3" || events[1].Details["image"] != "nginx:1.29" ||
		events[3].Details["from_replicas"] != "2" || events[3].Details["to_replicas"] != "5" {
		t.Fatalf("event details = %+v", events)
	}

	// The name may be repeated in the body but not changed.
	if rec := put(e, "web", `{"name":"web","replicas":0,"spec":{"image":"nginx:1.29","cpu_millis":500,"memory_bytes":134217728}}`); rec.Code != http.StatusOK {
		t.Errorf("PUT with the same name = %d: %s", rec.Code, rec.Body.String())
	}
	if rec := put(e, "web", `{"name":"renamed","replicas":0,"spec":{"image":"nginx:1.29","cpu_millis":500,"memory_bytes":134217728}}`); rec.Code != http.StatusBadRequest ||
		!strings.Contains(rec.Body.String(), `"field":"name"`) {
		t.Errorf("PUT with another name = %d: %s", rec.Code, rec.Body.String())
	}
	if _, err := e.st.GetServiceByName(context.Background(), "renamed"); err == nil {
		t.Error("a service was renamed")
	}
}

func testServiceUpdateIsCanonical(t *testing.T, e *serviceEnv) {
	e.create(`{"name":"web","replicas":1,"spec":{"image":"nginx","cpu_millis":100,"memory_bytes":134217728,
		"env":{"A":"1","B":"2"},
		"ports":[{"container_port":80},{"container_port":443}],
		"health_check":{"command":["true"]},
		"node_selector":{"zone":"a"}}}`)

	// The same definition spelled differently: another order, defaults written
	// out, empty optional values, an explicit null.
	rec := put(e, "web", `{"replicas":1,"spec":{"image":"nginx","cpu_millis":100,"memory_bytes":134217728,
		"command":[],
		"env":{"B":"2","A":"1"},
		"ports":[{"container_port":443,"protocol":"tcp","host_port":0},{"container_port":80,"protocol":"tcp"}],
		"health_check":{"command":["true"],"interval_seconds":30,"timeout_seconds":5,"start_period_seconds":0,"retries":3},
		"restart_policy":{"condition":"on-failure","max_retries":5},
		"node_selector":{"zone":"a"}}}`)
	var v serviceView
	decode(t, rec, &v)
	if rec.Code != http.StatusOK || v.Version != 1 || v.Revision != 1 {
		t.Fatalf("equivalent PUT = %d %+v; want no change", rec.Code, v)
	}
	rec = put(e, "web", `{"replicas":1,"spec":{"image":"nginx","cpu_millis":100,"memory_bytes":134217728,"env":{"A":"1","B":"2"},"ports":[{"container_port":80},{"container_port":443}],"health_check":{"command":["true"]},"restart_policy":null,"node_selector":{"zone":"a"}}}`)
	decode(t, rec, &v)
	if rec.Code != http.StatusOK || v.Version != 1 {
		t.Fatalf("PUT with a null restart policy = %d %+v; want the default, no change", rec.Code, v)
	}
	e.wantEvents("service.created")

	// A real difference in one of the new fields is a new revision.
	rec = put(e, "web", `{"replicas":1,"spec":{"image":"nginx","cpu_millis":100,"memory_bytes":134217728,"env":{"A":"1","B":"2"},"ports":[{"container_port":80},{"container_port":443}],"health_check":{"command":["true"],"retries":4},"node_selector":{"zone":"a"}}}`)
	decode(t, rec, &v)
	if rec.Code != http.StatusOK || v.Revision != 2 || v.Spec.HealthCheck.Retries != 4 {
		t.Fatalf("PUT changing a health check = %d %+v", rec.Code, v)
	}
}

func specJSON(spec store.ServiceSpec) string {
	b, err := json.Marshal(spec)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func withImage(spec store.ServiceSpec, image string) store.ServiceSpec {
	spec.Image = image
	return spec
}

// What GET returns is a valid PUT body: reading a service, changing a field and
// writing it back works, and writing it back unchanged is not a change.
func testServiceRoundTrip(t *testing.T, e *serviceEnv) {
	created := e.create(fullService)
	before := e.get("api")

	rec := put(e, "api", `{"replicas":3,"spec":`+specJSON(before.Spec)+`}`)
	var same serviceView
	decode(t, rec, &same)
	if rec.Code != http.StatusOK || !reflect.DeepEqual(same, before) {
		t.Fatalf("writing back what was read = %d %+v, want no change from %+v", rec.Code, same, before)
	}

	rec = put(e, "api", `{"name":"api","replicas":3,"spec":`+specJSON(withImage(before.Spec, "registry.example.com:5000/team/api:v2"))+`}`)
	var changed serviceView
	decode(t, rec, &changed)
	if rec.Code != http.StatusOK || changed.Revision != 2 || changed.Spec.Image != "registry.example.com:5000/team/api:v2" || changed.ID != created.ID {
		t.Fatalf("editing one field = %d %+v", rec.Code, changed)
	}
	// Everything else in the spec is carried over.
	changed.Spec.Image = before.Spec.Image
	if !reflect.DeepEqual(changed.Spec, before.Spec) {
		t.Fatalf("editing the image altered other fields:\n%+v\n%+v", changed.Spec, before.Spec)
	}
	e.wantEvents("service.created", "service.deployed")
}

func testServiceOptimisticLocking(t *testing.T, e *serviceEnv) {
	e.create(minimalService) // version 1
	scale := func(replicas string, headers ...string) *httptest.ResponseRecorder {
		return put(e, "web", serviceBodyWith("nginx:1.27", replicas), headers...)
	}

	if rec := scale("3", "If-Match", `"1"`); rec.Code != http.StatusOK || rec.Header().Get("ETag") != `"2"` {
		t.Fatalf("PUT with the current version = %d (ETag %q): %s", rec.Code, rec.Header().Get("ETag"), rec.Body.String())
	}
	// A client that still holds version 1 loses, even when its change would be a no-op.
	for _, replicas := range []string{"9", "3"} {
		rec := scale(replicas, "If-Match", `"1"`)
		if rec.Code != http.StatusPreconditionFailed {
			t.Fatalf("stale PUT (replicas %s) = %d, want 412: %s", replicas, rec.Code, rec.Body.String())
		}
	}
	if v := e.get("web"); v.Replicas != 3 || v.Version != 2 {
		t.Fatalf("a rejected PUT changed the service: %+v", v)
	}
	e.wantEvents("service.created", "service.scaled")

	// * and an absent header mean "whatever is there"; a bare number is accepted too.
	// The first of these changes the service (to version 3); the others repeat it as no-ops.
	for _, header := range [][]string{{"If-Match", "*"}, nil, {"If-Match", "3"}} {
		if rec := scale("4", header...); rec.Code != http.StatusOK {
			t.Fatalf("PUT with %v = %d: %s", header, rec.Code, rec.Body.String())
		}
	}
	if v := e.get("web"); v.Replicas != 4 || v.Version != 3 {
		t.Fatalf("service = %+v", v)
	}

	for _, bad := range []string{"abc", "0", "-1", `"1","2"`, `W/"x"`, "1.5"} {
		if rec := scale("5", "If-Match", bad); rec.Code != http.StatusBadRequest {
			t.Errorf("If-Match %q = %d, want 400", bad, rec.Code)
		}
	}

	// The same rules apply to deletion.
	current := e.get("web")
	if rec := e.request(http.MethodDelete, "/v1/services/web", "", "If-Match", `"1"`); rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale DELETE = %d, want 412", rec.Code)
	}
	if v := e.get("web"); v.DeletedAt != nil || v.Version != current.Version {
		t.Fatalf("a rejected DELETE changed the service: %+v", v)
	}
	if rec := e.request(http.MethodDelete, "/v1/services/web", "", "If-Match", etag(current.Version)); rec.Code != http.StatusOK {
		t.Fatalf("DELETE with the current version = %d: %s", rec.Code, rec.Body.String())
	}
}

func testServiceDelete(t *testing.T, e *serviceEnv) {
	created := e.create(minimalService)
	e.clock = e.clock.Add(time.Hour)
	rec := e.request(http.MethodDelete, "/v1/services/web", "")
	var deleted serviceView
	decode(t, rec, &deleted)
	if rec.Code != http.StatusOK || deleted.ID != created.ID || deleted.DeletedAt == nil || !deleted.DeletedAt.Equal(e.clock) ||
		deleted.Replicas != 0 || deleted.Version != 2 || deleted.Spec.Image != "nginx:1.27" {
		t.Fatalf("DELETE = %d %+v", rec.Code, deleted)
	}

	// Gone by name and from the default listing; kept in the history.
	for _, tt := range []struct{ method, path string }{
		{http.MethodGet, "/v1/services/web"},
		{http.MethodDelete, "/v1/services/web"},
		{http.MethodGet, "/v1/services/web/revisions"},
	} {
		if rec := e.request(tt.method, tt.path, ""); rec.Code != http.StatusNotFound {
			t.Errorf("%s %s after deletion = %d, want 404", tt.method, tt.path, rec.Code)
		}
	}
	if rec := put(e, "web", serviceBodyWith("nginx:1.27", "9")); rec.Code != http.StatusNotFound {
		t.Errorf("PUT after deletion = %d, want 404", rec.Code)
	}
	var list struct {
		Services []serviceView `json:"services"`
	}
	decode(t, e.request(http.MethodGet, "/v1/services", ""), &list)
	if len(list.Services) != 0 {
		t.Fatalf("live list = %+v", list.Services)
	}
	decode(t, e.request(http.MethodGet, "/v1/services?include_deleted=true", ""), &list)
	if len(list.Services) != 1 || list.Services[0].ID != created.ID || list.Services[0].DeletedAt == nil {
		t.Fatalf("list with deleted = %+v", list.Services)
	}

	// The name can be used again, by a new service with a new identity.
	reborn := e.create(minimalService)
	if reborn.ID == created.ID || reborn.Revision != 1 || reborn.Version != 1 || reborn.DeletedAt != nil {
		t.Fatalf("re-created service = %+v", reborn)
	}
	decode(t, e.request(http.MethodGet, "/v1/services?include_deleted=true", ""), &list)
	if len(list.Services) != 2 {
		t.Fatalf("list with deleted = %+v, want both services", list.Services)
	}
	e.wantEvents("service.created", "service.deleted", "service.created")
}

func testServiceRevisions(t *testing.T, e *serviceEnv) {
	e.create(minimalService)
	put(e, "web", serviceBodyWith("nginx:1.28", "2"))
	put(e, "web", serviceBodyWith("nginx:1.28", "6")) // scaling adds no revision
	put(e, "web", serviceBodyWith("nginx:1.27", "6")) // a rollback is a new revision

	rec := e.request(http.MethodGet, "/v1/services/web/revisions", "")
	var list struct {
		Revisions []store.Revision `json:"revisions"`
	}
	decode(t, rec, &list)
	if rec.Code != http.StatusOK || len(list.Revisions) != 3 {
		t.Fatalf("revisions = %d %+v", rec.Code, list.Revisions)
	}
	for i, wantImage := range []string{"nginx:1.27", "nginx:1.28", "nginx:1.27"} {
		r := list.Revisions[i]
		if r.Revision != i+1 || r.Spec.Image != wantImage || r.CreatedBy != "admin" || r.SpecHash != r.Spec.Hash() {
			t.Errorf("revision %d = %+v", i+1, r)
		}
	}
	if list.Revisions[0].SpecHash != list.Revisions[2].SpecHash {
		t.Error("a rollback must record the old spec unchanged")
	}
	if v := e.get("web"); v.Revision != 3 || v.Replicas != 6 {
		t.Fatalf("service = %+v", v)
	}
}

func testServiceDoesNotSchedule(t *testing.T, e *serviceEnv) {
	ctx := context.Background()
	// A healthy node with plenty of room: if anything scheduled, this is where it would land.
	if _, err := e.st.UpsertNode(ctx, store.Node{ID: "node-1", Name: "node-1", CPUMillis: 64000, MemoryBytes: 64 << 30, RegisteredAt: fixedNow, LastHeartbeat: fixedNow}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.st.CreateEnrollment(ctx, store.Enrollment{NodeID: "node-1", CreatedAt: fixedNow}); err != nil {
		t.Fatal(err)
	}

	created := e.create(strings.Replace(minimalService, `"replicas":2`, `"replicas":10`, 1))
	put(e, "web", serviceBodyWith("nginx:1.28", "20"))
	e.request(http.MethodDelete, "/v1/services/web", "")

	if all, err := e.st.ListWorkloads(ctx); err != nil || len(all) != 0 {
		t.Fatalf("workloads = %+v, %v; defining services must not place anything", all, err)
	}
	if replicas, err := e.st.ListWorkloadsForService(ctx, created.ID); err != nil || len(replicas) != 0 {
		t.Fatalf("replicas = %+v, %v", replicas, err)
	}
	if node, err := e.st.GetNode(ctx, "node-1"); err != nil || !node.LastHeartbeat.Equal(fixedNow) {
		t.Fatalf("node = %+v, %v", node, err)
	}
}

func testServiceEvents(t *testing.T, e *serviceEnv) {
	e.create(`{"name":"web","replicas":1,"spec":{"image":"nginx","cpu_millis":100,"memory_bytes":134217728,"env":{"DB_PASSWORD":"hunter2-secret"}}}`)
	put(e, "web", `{"replicas":2,"spec":{"image":"nginx:2","cpu_millis":100,"memory_bytes":134217728,"env":{"DB_PASSWORD":"hunter2-secret"}}}`)
	e.request(http.MethodDelete, "/v1/services/web", "")

	rec := e.request(http.MethodGet, "/v1/events?resource_type=service", "")
	var list struct {
		Events []store.Event `json:"events"`
	}
	decode(t, rec, &list)
	if len(list.Events) != 4 {
		t.Fatalf("events = %+v", list.Events)
	}
	if strings.Contains(rec.Body.String(), "hunter2") {
		t.Fatalf("an event contains an environment value: %s", rec.Body.String())
	}
	for _, ev := range list.Events {
		if ev.Actor != "admin" || ev.ResourceType != "service" || ev.ResourceID == "" || ev.Details["name"] != "web" {
			t.Errorf("event = %+v", ev)
		}
	}
}
