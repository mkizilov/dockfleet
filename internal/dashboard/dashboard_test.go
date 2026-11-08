package dashboard_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/api"
	"github.com/dockfleet/dockfleet/internal/dashboard"
	"github.com/dockfleet/dockfleet/internal/scheduler"
	"github.com/dockfleet/dockfleet/internal/store"
)

const (
	adminToken = "admin-token-0123456789"
	browser    = "203.0.113.9:4444"
)

var t0 = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

type seenRequest struct {
	Method, Path, Remote, ID, Auth string
}

type audit struct{ Action, Message, Remote string }

// env is a real API on an in-memory store with a dashboard in front of it.
type env struct {
	t      *testing.T
	st     *store.Memory
	api    http.Handler // the API itself, for seeding
	dash   *dashboard.Dashboard
	now    time.Time
	apiErr bool

	mu     sync.Mutex
	seen   []seenRequest
	audits []audit
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{t: t, st: store.NewMemory(), now: t0}
	e.api = api.New(api.Config{
		Store: e.st, Token: adminToken, NodeTTL: 30 * time.Second, Logger: quiet(), InstanceID: "test-manager",
		Timing: scheduler.Timing{SuspectAfter: 30 * time.Second, UnavailableAfter: 2 * time.Minute},
		Now:    func() time.Time { return e.now },
	})
	// What the dashboard sees of the API is recorded here, and the API can be made to fail.
	recording := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.mu.Lock()
		e.seen = append(e.seen, seenRequest{r.Method, r.URL.RequestURI(), r.RemoteAddr, r.Header.Get("X-Request-ID"), r.Header.Get("Authorization")})
		down := e.apiErr
		e.mu.Unlock()
		if down {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"error":"internal error: connection to 10.0.0.5:5432 refused"}`)
			return
		}
		e.api.ServeHTTP(w, r)
	})
	d, err := dashboard.New(dashboard.Config{
		API: recording, Token: adminToken, Logger: quiet(), Now: func() time.Time { return e.now },
		Audit: func(_ context.Context, action, message string, details map[string]string) {
			e.mu.Lock()
			defer e.mu.Unlock()
			e.audits = append(e.audits, audit{action, message, details["remote"]})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	e.dash = d
	return e
}

func (e *env) do(method, target, body string, mods ...func(*http.Request)) *httptest.ResponseRecorder {
	e.t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.RemoteAddr = browser
	if body != "" {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	for _, m := range mods {
		m(req)
	}
	rec := httptest.NewRecorder()
	e.dash.ServeHTTP(rec, req)
	return rec
}

func bearer(token string) func(*http.Request) {
	return func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) }
}

func withCookie(c *http.Cookie) func(*http.Request) { return func(r *http.Request) { r.AddCookie(c) } }

// get fetches a page as an administrator with the bearer token.
func (e *env) get(target string) *httptest.ResponseRecorder {
	e.t.Helper()
	return e.do(http.MethodGet, target, "", bearer(adminToken))
}

// seedAPI sends a request to the API itself, as an administrator.
func (e *env) seedAPI(method, path, body string) *httptest.ResponseRecorder {
	e.t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+adminToken)
	rec := httptest.NewRecorder()
	e.api.ServeHTTP(rec, req)
	return rec
}

func (e *env) node(id string, beat time.Duration, labels map[string]string) {
	e.t.Helper()
	_, err := e.st.UpsertNode(context.Background(), store.Node{
		ID: id, Name: id, CPUMillis: 4000, MemoryBytes: 8 << 30, Labels: labels,
		RegisteredAt: t0.Add(-time.Hour), LastHeartbeat: t0.Add(-beat), AgentVersion: "test-1",
	})
	if err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) service(name string, replicas int, image string, cpu int) string {
	e.t.Helper()
	body := fmt.Sprintf(`{"name":%q,"replicas":%d,"spec":{"image":%q,"cpu_millis":%d,"memory_bytes":134217728}}`, name, replicas, image, cpu)
	if rec := e.seedAPI(http.MethodPost, "/v1/services", body); rec.Code != http.StatusCreated {
		e.t.Fatalf("create %s: %d %s", name, rec.Code, rec.Body)
	}
	svc, err := e.st.GetServiceByName(context.Background(), name)
	if err != nil {
		e.t.Fatal(err)
	}
	return svc.ID
}

// replica places a replica of a service on a node, with what its agent reported.
func (e *env) replica(serviceID, id, node string, index int, state, message string, restarts int) {
	e.t.Helper()
	ctx := context.Background()
	_, err := e.st.CreateWorkload(ctx, store.Workload{
		ID: id, Name: "web-r1-" + id, Image: "nginx:1.27", CPUMillis: 500, MemoryBytes: 128 << 20, NodeID: node,
		ServiceID: serviceID, ServiceRevision: 1, ReplicaIndex: index, DesiredState: store.DesiredRunning, Generation: 1, CreatedAt: t0,
	})
	if err != nil {
		e.t.Fatal(err)
	}
	obs := store.Observation{WorkloadID: id, State: state, Message: message, ContainerID: "c-" + id, RestartCount: restarts, AckedGeneration: 1, ObservedAt: t0}
	if state == store.WorkloadFailed {
		exit := 1
		obs.ExitCode = &exit
	}
	if err := e.st.RecordObservations(ctx, node, []store.Observation{obs}); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) event(action, resourceType, resourceID, message string) {
	e.t.Helper()
	_, err := e.st.AppendEvent(context.Background(), store.Event{
		At: t0.Add(-time.Minute), InstanceID: "test-manager", Actor: "reconciler", Action: action,
		ResourceType: resourceType, ResourceID: resourceID, Message: message,
	})
	if err != nil {
		e.t.Fatal(err)
	}
}

// seedCluster makes a small fleet: one node of each health, a service with a
// running and a failed replica, and a service no node can take.
func (e *env) seedCluster() (webID, apiID string) {
	e.node("node-a", 10*time.Second, map[string]string{"zone": "a"})
	e.node("node-b", time.Minute, map[string]string{"zone": "b"})
	e.node("node-c", 5*time.Minute, nil)
	webID = e.service("web", 3, "nginx:1.27", 500)
	apiID = e.service("api", 2, "api:2", 8000)
	e.replica(webID, "w1", "node-a", 1, store.WorkloadRunning, "", 0)
	e.replica(webID, "w2", "node-a", 2, store.WorkloadRunning, "", 0)
	e.replica(webID, "w3", "node-b", 3, store.WorkloadFailed, "exited with code 1", 3)
	_, err := e.st.ReplaceUnschedulable(context.Background(), []store.Unschedulable{{
		ServiceID: apiID, Revision: 1, ReplicaIndex: 1, Code: "insufficient_resources", Message: "no node has 8000m of CPU free",
		Nodes: []store.NodeReason{{NodeID: "node-a", Code: "insufficient_cpu", Message: "node-a has 3000m free"}},
		Since: t0.Add(-time.Minute),
	}}, t0)
	if err != nil {
		e.t.Fatal(err)
	}
	e.event("node.suspect", "node", "node-b", "node node-b is suspect")
	return webID, apiID
}

// login logs in with the token and returns the session cookie.
func (e *env) login() *http.Cookie {
	e.t.Helper()
	rec := e.do(http.MethodPost, "/dashboard/login", url.Values{"token": {adminToken}}.Encode())
	if rec.Code != http.StatusSeeOther {
		e.t.Fatalf("login = %d: %s", rec.Code, rec.Body)
	}
	for _, c := range rec.Result().Cookies() {
		if c.Name == "dockfleet_session" {
			return c
		}
	}
	e.t.Fatal("login set no session cookie")
	return nil
}

func contains(t *testing.T, body string, wants ...string) {
	t.Helper()
	for _, w := range wants {
		if !strings.Contains(body, w) {
			t.Errorf("the page lacks %q:\n%s", w, body)
		}
	}
}

func lacks(t *testing.T, body string, unwanted ...string) {
	t.Helper()
	for _, u := range unwanted {
		if strings.Contains(body, u) {
			t.Errorf("the page has %q:\n%s", u, body)
		}
	}
}

// section is the part of a page from one marker up to the next.
func section(t *testing.T, body, from, to string) string {
	t.Helper()
	i := strings.Index(body, from)
	j := strings.Index(body, to)
	if i < 0 || j < i {
		t.Fatalf("no section from %q to %q in:\n%s", from, to, body)
	}
	return body[i:j]
}

func TestNewRefusesWhatCannotWork(t *testing.T) {
	ok := http.NotFoundHandler()
	if _, err := dashboard.New(dashboard.Config{Token: adminToken}); err == nil {
		t.Error("a dashboard without an API was built")
	}
	if _, err := dashboard.New(dashboard.Config{API: ok, Token: "short"}); err == nil {
		t.Error("a dashboard with a token too short to be a credential was built")
	}
	if _, err := dashboard.New(dashboard.Config{API: ok, Token: adminToken}); err != nil {
		t.Errorf("a valid configuration was refused (are the templates valid?): %v", err)
	}
}

func TestPagesNeedALoginAndLeakNothingWithout(t *testing.T) {
	e := newEnv(t)
	e.seedCluster()
	pages := []string{"/dashboard", "/dashboard/", "/dashboard/events", "/dashboard/services/web", "/dashboard/nodes/node-a", "/dashboard/events?resource_type=node"}
	for _, page := range pages {
		rec := e.do(http.MethodGet, page, "")
		if rec.Code != http.StatusSeeOther || !strings.HasPrefix(rec.Header().Get("Location"), "/dashboard/login?next=") {
			t.Errorf("GET %s without credentials = %d, Location %q; want a redirect to the login page", page, rec.Code, rec.Header().Get("Location"))
		}
		// (The redirect's own body names the page it came from, so look for what only the pages hold.)
		lacks(t, rec.Body.String(), "node-b", "node-c", "nginx", "service.created", "exited", "unschedulable")
	}
	if loc := e.do(http.MethodGet, "/dashboard/events?resource_type=node", "").Header().Get("Location"); loc != "/dashboard/login?next="+url.QueryEscape("/dashboard/events?resource_type=node") {
		t.Errorf("Location = %q; the login page must know where to go back to", loc)
	}
	if len(e.seen) != 0 {
		t.Fatalf("the API was called %d times for requests that were not logged in: %+v", len(e.seen), e.seen)
	}

	// A token in the URL is not a credential.
	rec := e.do(http.MethodGet, "/dashboard?token="+adminToken, "")
	if rec.Code != http.StatusSeeOther {
		t.Errorf("a token in the query string = %d, want the redirect to the login page", rec.Code)
	}
}

func TestBearerCredentials(t *testing.T) {
	e := newEnv(t)
	e.seedCluster()
	if rec := e.get("/dashboard"); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "node-a") {
		t.Fatalf("the admin token = %d", rec.Code)
	}
	for _, auth := range []string{"Bearer wrong-token-0123456789", "Bearer " + adminToken + "x", "Bearer", "Basic " + adminToken, adminToken, "bearer " + adminToken} {
		rec := e.do(http.MethodGet, "/dashboard", "", func(r *http.Request) { r.Header.Set("Authorization", auth) })
		if rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") != "Bearer" {
			t.Errorf("Authorization %q = %d, WWW-Authenticate %q; want 401 with a challenge, not a redirect", auth, rec.Code, rec.Header().Get("WWW-Authenticate"))
		}
		lacks(t, rec.Body.String(), "node-a", "web")
	}
	// The wrong bearer header does not fall back to a valid cookie: it is a wrong credential.
	cookie := e.login()
	rec := e.do(http.MethodGet, "/dashboard", "", withCookie(cookie), bearer("wrong-token-0123456789"))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("a wrong bearer header beside a good cookie = %d, want 401", rec.Code)
	}
}

func TestLogin(t *testing.T) {
	e := newEnv(t)
	e.seedCluster()

	form := e.do(http.MethodGet, "/dashboard/login", "")
	if form.Code != http.StatusOK {
		t.Fatalf("login page = %d", form.Code)
	}
	contains(t, form.Body.String(), `type="password"`, `action="/dashboard/login"`)
	lacks(t, form.Body.String(), `http-equiv="refresh"`) // a page with a form must not reload under the typist

	t.Run("a wrong token is refused", func(t *testing.T) {
		rec := e.do(http.MethodPost, "/dashboard/login", url.Values{"token": {"not-the-token-0123456789"}}.Encode())
		if rec.Code != http.StatusUnauthorized || len(rec.Result().Cookies()) != 0 {
			t.Fatalf("status %d, cookies %v; want 401 and no cookie", rec.Code, rec.Result().Cookies())
		}
		contains(t, rec.Body.String(), "That token was not accepted.")
		lacks(t, rec.Body.String(), adminToken, "not-the-token-0123456789")
	})

	t.Run("a token in the URL or in a header does not log in", func(t *testing.T) {
		for _, rec := range []*httptest.ResponseRecorder{
			e.do(http.MethodPost, "/dashboard/login?token="+adminToken, ""),
			e.do(http.MethodPost, "/dashboard/login", "", bearer(adminToken)),
			e.do(http.MethodPost, "/dashboard/login", url.Values{"other": {adminToken}}.Encode()),
		} {
			if rec.Code != http.StatusUnauthorized || len(rec.Result().Cookies()) != 0 {
				t.Errorf("status %d, cookies %v; want 401 and no cookie", rec.Code, rec.Result().Cookies())
			}
		}
	})

	t.Run("the right token gives a cookie that holds no secret and is locked down", func(t *testing.T) {
		rec := e.do(http.MethodPost, "/dashboard/login", url.Values{"token": {adminToken}, "next": {"/dashboard/events?resource_type=node"}}.Encode())
		if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/dashboard/events?resource_type=node" {
			t.Fatalf("status %d, Location %q", rec.Code, rec.Header().Get("Location"))
		}
		cookies := rec.Result().Cookies()
		if len(cookies) != 1 {
			t.Fatalf("cookies = %v", cookies)
		}
		c := cookies[0]
		if c.Name != "dockfleet_session" || !c.HttpOnly || c.SameSite != http.SameSiteStrictMode || c.Path != "/dashboard" || c.MaxAge != int((8*time.Hour)/time.Second) {
			t.Errorf("cookie = %+v; want HttpOnly, SameSite=Strict, Path=/dashboard, 8 hours", c)
		}
		if c.Secure {
			t.Error("the cookie is Secure on a plain HTTP request, so a browser would not send it back")
		}
		if strings.Contains(c.Value, adminToken) || strings.Contains(c.Value, "admin") {
			t.Errorf("the cookie %q holds the token", c.Value)
		}
		if page := e.do(http.MethodGet, "/dashboard", "", withCookie(c)); page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "node-a") {
			t.Errorf("a page with the session = %d", page.Code)
		}
	})

	t.Run("the cookie is Secure over TLS", func(t *testing.T) {
		rec := e.do(http.MethodPost, "/dashboard/login", url.Values{"token": {adminToken}}.Encode(), func(r *http.Request) { r.TLS = &tls.ConnectionState{} })
		if cs := rec.Result().Cookies(); len(cs) != 1 || !cs[0].Secure {
			t.Errorf("cookies = %+v; want one that is Secure", cs)
		}
	})
}

func TestSessionsAreSignedAndExpire(t *testing.T) {
	e := newEnv(t)
	e.seedCluster()
	good := e.login()
	if rec := e.do(http.MethodGet, "/dashboard", "", withCookie(good)); rec.Code != http.StatusOK {
		t.Fatalf("a fresh session = %d", rec.Code)
	}
	parts := strings.Split(good.Value, ".")
	if len(parts) != 3 {
		t.Fatalf("cookie value %q", good.Value)
	}
	flipped := parts[2][:len(parts[2])-1] + map[bool]string{true: "1", false: "0"}[strings.HasSuffix(parts[2], "0")]
	// A key derived from another token: a cookie from a manager that has a different token.
	other, err := dashboard.New(dashboard.Config{API: http.NotFoundHandler(), Token: "another-admin-token-0123", Now: func() time.Time { return e.now }})
	if err != nil {
		t.Fatal(err)
	}
	loginRec := httptest.NewRecorder()
	loginReq := httptest.NewRequest(http.MethodPost, "/dashboard/login", strings.NewReader(url.Values{"token": {"another-admin-token-0123"}}.Encode()))
	loginReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	other.ServeHTTP(loginRec, loginReq)
	foreign := loginRec.Result().Cookies()
	if len(foreign) != 1 {
		t.Fatalf("the other dashboard's login set %v", foreign)
	}

	for name, value := range map[string]string{
		"empty":                    "",
		"garbage":                  "garbage",
		"no signature":             "v1." + parts[1],
		"a changed signature":      "v1." + parts[1] + "." + flipped,
		"a later expiry":           "v1.99999999999." + parts[2],
		"an unsigned future":       "v1.99999999999.",
		"another version":          "v2." + parts[1] + "." + parts[2],
		"extra parts":              good.Value + ".x",
		"signed with another key":  foreign[0].Value,
		"a negative expiry":        "v1.-1." + parts[2],
		"an expiry that is no int": "v1.soon." + parts[2],
	} {
		rec := e.do(http.MethodGet, "/dashboard", "", withCookie(&http.Cookie{Name: "dockfleet_session", Value: value}))
		if rec.Code != http.StatusSeeOther {
			t.Errorf("%s: %d, want the redirect to the login page", name, rec.Code)
		}
		lacks(t, rec.Body.String(), "node-a")
	}

	e.now = e.now.Add(8*time.Hour - time.Second)
	if rec := e.do(http.MethodGet, "/dashboard", "", withCookie(good)); rec.Code != http.StatusOK {
		t.Errorf("a session just before its expiry = %d", rec.Code)
	}
	e.now = e.now.Add(2 * time.Second)
	if rec := e.do(http.MethodGet, "/dashboard", "", withCookie(good)); rec.Code != http.StatusSeeOther {
		t.Errorf("an expired session = %d, want the redirect to the login page", rec.Code)
	}
}

func TestLoginRedirectsOnlyWithinTheDashboard(t *testing.T) {
	e := newEnv(t)
	for next, want := range map[string]string{
		"/dashboard/services/web":        "/dashboard/services/web",
		"/dashboard?x=1":                 "/dashboard?x=1",
		"/dashboard":                     "/dashboard",
		"":                               "/dashboard",
		"https://evil.example/":          "/dashboard",
		"//evil.example/dashboard":       "/dashboard",
		"/v1/services":                   "/dashboard",
		"/dashboardx":                    "/dashboard",
		"/dashboard\\evil":               "/dashboard",
		"/dashboard/login":               "/dashboard",
		"/dashboard/\r\nSet-Cookie: x=1": "/dashboard",
		"javascript:alert(1)":            "/dashboard",
	} {
		rec := e.do(http.MethodPost, "/dashboard/login", url.Values{"token": {adminToken}, "next": {next}}.Encode())
		if got := rec.Header().Get("Location"); rec.Code != http.StatusSeeOther || got != want {
			t.Errorf("next %q: %d, Location %q, want %q", next, rec.Code, got, want)
		}
	}
	page := e.do(http.MethodGet, "/dashboard/login?next="+url.QueryEscape("https://evil.example/"), "")
	lacks(t, page.Body.String(), "evil.example")
}

func TestLoginAttemptsAreThrottled(t *testing.T) {
	e := newEnv(t)
	wrong := url.Values{"token": {"not-the-token-0123456789"}}.Encode()
	for i := 0; i < 10; i++ {
		if rec := e.do(http.MethodPost, "/dashboard/login", wrong); rec.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d = %d, want 401", i+1, rec.Code)
		}
	}
	// Beyond the limit even the right token waits.
	for _, body := range []string{wrong, url.Values{"token": {adminToken}}.Encode()} {
		rec := e.do(http.MethodPost, "/dashboard/login", body)
		if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "60" || len(rec.Result().Cookies()) != 0 {
			t.Fatalf("after ten failures: %d, Retry-After %q, cookies %v", rec.Code, rec.Header().Get("Retry-After"), rec.Result().Cookies())
		}
	}
	// Another address is not held back by it.
	rec := e.do(http.MethodPost, "/dashboard/login", url.Values{"token": {adminToken}}.Encode(), func(r *http.Request) { r.RemoteAddr = "198.51.100.7:1000" })
	if rec.Code != http.StatusSeeOther {
		t.Errorf("another address = %d, want it to log in", rec.Code)
	}
	e.now = e.now.Add(61 * time.Second)
	if rec := e.do(http.MethodPost, "/dashboard/login", url.Values{"token": {adminToken}}.Encode()); rec.Code != http.StatusSeeOther {
		t.Errorf("after the window = %d, want the login to work again", rec.Code)
	}
}

func TestLoginAndLogoutRefuseOtherSites(t *testing.T) {
	e := newEnv(t)
	cookie := e.login()
	body := url.Values{"token": {adminToken}}.Encode()
	attacks := map[string]func(*http.Request){
		"another origin":      func(r *http.Request) { r.Header.Set("Origin", "https://evil.example") },
		"a cross-site fetch":  func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") },
		"a same-site fetch":   func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "same-site") },
		"an origin with port": func(r *http.Request) { r.Header.Set("Origin", "http://example.com:8080") },
	}
	for name, attack := range attacks {
		if rec := e.do(http.MethodPost, "/dashboard/login", body, attack); rec.Code != http.StatusForbidden || len(rec.Result().Cookies()) != 0 {
			t.Errorf("login from %s = %d, cookies %v; want 403 and no cookie", name, rec.Code, rec.Result().Cookies())
		}
		if rec := e.do(http.MethodPost, "/dashboard/logout", "", withCookie(cookie), attack); rec.Code != http.StatusForbidden {
			t.Errorf("logout from %s = %d, want 403", name, rec.Code)
		}
	}
	same := func(r *http.Request) {
		r.Header.Set("Origin", "http://example.com")
		r.Header.Set("Sec-Fetch-Site", "same-origin")
	}
	if rec := e.do(http.MethodPost, "/dashboard/login", body, same); rec.Code != http.StatusSeeOther {
		t.Errorf("login from the same origin = %d", rec.Code)
	}
}

func TestLogoutClearsTheSessionAndIsAudited(t *testing.T) {
	e := newEnv(t)
	cookie := e.login()
	rec := e.do(http.MethodPost, "/dashboard/logout", "", withCookie(cookie))
	if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/dashboard/login" {
		t.Fatalf("logout = %d, Location %q", rec.Code, rec.Header().Get("Location"))
	}
	cleared := rec.Result().Cookies()
	if len(cleared) != 1 || cleared[0].Name != "dockfleet_session" || cleared[0].Value != "" || cleared[0].MaxAge >= 0 || cleared[0].Path != "/dashboard" {
		t.Errorf("logout cookies = %+v; want the session cleared", cleared)
	}
	// Logging out without a session changes nothing and records nothing.
	e.do(http.MethodPost, "/dashboard/logout", "")

	e.mu.Lock()
	defer e.mu.Unlock()
	want := []audit{
		{"dashboard.login", "an administrator logged in to the dashboard", "203.0.113.9"},
		{"dashboard.logout", "an administrator logged out of the dashboard", "203.0.113.9"},
	}
	if fmt.Sprint(e.audits) != fmt.Sprint(want) {
		t.Fatalf("audit records = %+v, want %+v", e.audits, want)
	}
}

func TestFailedLoginsAreNotInTheAuditTrailButTheDashboardIsReadOnly(t *testing.T) {
	e := newEnv(t)
	e.do(http.MethodPost, "/dashboard/login", url.Values{"token": {"not-the-token-0123456789"}}.Encode())
	if len(e.audits) != 0 {
		t.Fatalf("a refused login was recorded as an administrative change: %+v", e.audits)
	}
	// Nothing but reading is offered: every method but GET on a page is refused by the router.
	e.seedCluster()
	for _, page := range []string{"/dashboard", "/dashboard/events", "/dashboard/services/web", "/dashboard/nodes/node-a"} {
		for _, method := range []string{http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodPost} {
			rec := e.do(method, page, "", bearer(adminToken))
			if rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s %s = %d, want 405", method, page, rec.Code)
			}
		}
	}
}

func TestEveryResponseCarriesTheSecurityHeaders(t *testing.T) {
	e := newEnv(t)
	e.seedCluster()
	cookie := e.login()
	responses := map[string]*httptest.ResponseRecorder{
		"a page":            e.do(http.MethodGet, "/dashboard", "", withCookie(cookie)),
		"a redirect":        e.do(http.MethodGet, "/dashboard", ""),
		"the login page":    e.do(http.MethodGet, "/dashboard/login", ""),
		"an error":          e.do(http.MethodGet, "/dashboard/services/nope", "", withCookie(cookie)),
		"a refusal":         e.do(http.MethodGet, "/dashboard", "", bearer("wrong-token-0123456789")),
		"a method refusal":  e.do(http.MethodDelete, "/dashboard", ""),
		"the stylesheet":    e.do(http.MethodGet, "/dashboard/static/style.css", ""),
		"an unknown path":   e.do(http.MethodGet, "/dashboard/nothing-here", ""),
		"an empty bad page": e.do(http.MethodGet, "/dashboard/events?before=-1", "", withCookie(cookie)),
	}
	for name, rec := range responses {
		h := rec.Header()
		csp := h.Get("Content-Security-Policy")
		if !strings.Contains(csp, "default-src 'none'") || !strings.Contains(csp, "frame-ancestors 'none'") || strings.Contains(csp, "unsafe") || strings.Contains(csp, "script-src") {
			t.Errorf("%s: Content-Security-Policy = %q", name, csp)
		}
		if h.Get("X-Content-Type-Options") != "nosniff" || h.Get("X-Frame-Options") != "DENY" || h.Get("Referrer-Policy") != "no-referrer" {
			t.Errorf("%s: headers = %v", name, h)
		}
		if name != "the stylesheet" && h.Get("Cache-Control") != "no-store" {
			t.Errorf("%s: Cache-Control = %q, want no-store", name, h.Get("Cache-Control"))
		}
	}
	// The stylesheet is public: it holds nothing, and the login page needs it.
	css := responses["the stylesheet"]
	if css.Code != http.StatusOK || !strings.HasPrefix(css.Header().Get("Content-Type"), "text/css") || strings.Contains(css.Body.String(), "node-") {
		t.Errorf("stylesheet = %d %q", css.Code, css.Header().Get("Content-Type"))
	}
}

func TestOverviewShowsTheStateOfTheCluster(t *testing.T) {
	e := newEnv(t)
	e.seedCluster()
	rec := e.get("/dashboard")
	if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("overview = %d %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	body := rec.Body.String()

	// The summary.
	contains(t, body,
		`<span class="badge ok">1 healthy</span>`, `<span class="badge warn">1 suspect</span>`, `<span class="badge bad">1 unavailable</span>`,
		"2 running of 5 desired", `<span class="badge bad">2 with problems</span>`,
		"1.5 CPU of 12 CPU", "384 MiB of 24 GiB",
		`http-equiv="refresh" content="10"`,
	)

	// The nodes, those that need attention first, each with its health, heartbeat and allocation.
	nodes := section(t, body, "<h2>Nodes</h2>", "<h2>Services</h2>")
	a, b, c := strings.Index(nodes, ">node-a<"), strings.Index(nodes, ">node-b<"), strings.Index(nodes, ">node-c<")
	if a < 0 || b < 0 || c < 0 || !(c < b && b < a) {
		t.Errorf("node order: a=%d b=%d c=%d; want unavailable, suspect, healthy", a, b, c)
	}
	contains(t, nodes,
		`<span class="badge ok">healthy</span>`, `<span class="badge warn">suspect</span>`, `<span class="badge bad">unavailable</span>`,
		"10s ago", "1m ago", "5m ago",
		"1 CPU of 4 CPU", "256 MiB of 8 GiB", "0.5 CPU of 4 CPU", "128 MiB of 8 GiB",
		`<progress value="1000" max="4000">`, `href="/dashboard/nodes/node-a"`,
	)

	// The services, with their scaling and rollout state and what is wrong with them.
	services := section(t, body, "<h2>Services</h2>", "<h2>Recent events</h2>")
	contains(t, services,
		`href="/dashboard/services/web"`, `<code>nginx:1.27</code>`, "2 of 3", `<span class="badge bad">degraded</span>`, `<span class="badge bad">1 failed</span>`,
		`href="/dashboard/services/api"`, "0 of 2", `<span class="badge bad">blocked</span>`, `<span class="badge warn">1 unschedulable</span>`,
	)

	// Recent events.
	events := section(t, body, "<h2>Recent events</h2>", "</main>")
	contains(t, events, "service.created", "node.suspect", "node node-b is suspect", `href="/dashboard/nodes/node-b"`, `href="/dashboard/events"`)
	contains(t, events, "request_id=", "remote=") // the audit context of the change
}

func TestAnEmptyClusterSaysSo(t *testing.T) {
	e := newEnv(t)
	body := e.get("/dashboard").Body.String()
	contains(t, body, "No node has registered yet.", "No service is defined.", "No events.", "0 running of 0 desired", "no problems")
}

func TestServicePageShowsReplicasHealthAndWhyOneWaits(t *testing.T) {
	e := newEnv(t)
	e.seedCluster()

	web := e.get("/dashboard/services/web")
	if web.Code != http.StatusOK {
		t.Fatalf("service page = %d: %s", web.Code, web.Body)
	}
	body := web.Body.String()
	contains(t, body,
		"<h2>web ", `<span class="badge bad">degraded</span>`, "<code>nginx:1.27</code> (revision 1)", "0.5 CPU, 128 MiB", "2 running of 3 wanted",
		`href="/dashboard/nodes/node-a"`, `href="/dashboard/nodes/node-b"`,
		`<span class="badge ok">running</span>`, `<span class="badge bad">failed</span>`, "exited with code 1",
		`<span class="badge warn">suspect</span>`, // the failed replica's node
		// Its restarts, its exit code and its message, in that order.
		"<td>3</td>\n<td>1</td>\n<td>exited with code 1</td>",
		"<h3>Events</h3>", "service.created",
	)
	if rows := strings.Count(body, `href="/dashboard/nodes/node-`); rows < 3 {
		t.Errorf("%d replica rows link to a node, want 3", rows)
	}
	lacks(t, body, "Waiting for a node")

	api := e.get("/dashboard/services/api").Body.String()
	contains(t, api,
		`<span class="badge bad">blocked</span>`, "0 running of 2 wanted", "No replica has been assigned to a node.",
		"<h3>Waiting for a node</h3>", `<span class="badge warn">insufficient_resources</span>`, "no node has 8000m of CPU free", "node-a: node-a has 3000m free",
	)

	if rec := e.get("/dashboard/services/nobody"); rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "service not found") {
		t.Errorf("an unknown service = %d: %s", rec.Code, rec.Body)
	}
}

func TestNodePageShowsTheNodeAndWhatRunsOnIt(t *testing.T) {
	e := newEnv(t)
	e.seedCluster()
	rec := e.get("/dashboard/nodes/node-b")
	if rec.Code != http.StatusOK {
		t.Fatalf("node page = %d: %s", rec.Code, rec.Body)
	}
	contains(t, rec.Body.String(),
		"<h2>node-b ", `<span class="badge warn">suspect</span>`, "<code>zone=b</code>", "1m ago", "test-1",
		"0.5 CPU allocated of 4 CPU allocatable (4 CPU declared)", "128 MiB allocated of 8 GiB allocatable (8 GiB declared)",
		"<h3>Workloads on this node</h3>", "web-r1-w3", `href="/dashboard/services/web"`, `<span class="badge bad">failed</span>`, "exited with code 1",
		"node.suspect", "node node-b is suspect",
	)
	lacks(t, rec.Body.String(), "web-r1-w1", "web-r1-w2") // those run on node-a

	contains(t, e.get("/dashboard/nodes/node-c").Body.String(), "No workload is placed on this node.", `<span class="badge bad">unavailable</span>`)
	if rec := e.get("/dashboard/nodes/nobody"); rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "node not found") {
		t.Errorf("an unknown node = %d: %s", rec.Code, rec.Body)
	}
}

func TestEventsArePagedAndFiltered(t *testing.T) {
	e := newEnv(t)
	for i := 1; i <= 120; i++ {
		kind, id := "node", fmt.Sprintf("n%d", i)
		if i%2 == 0 {
			kind = "service"
		}
		e.event("test.event", kind, id, fmt.Sprintf("event number %d", i))
	}

	first := e.get("/dashboard/events").Body.String()
	contains(t, first, "event number 120", "event number 71", `href="/dashboard/events?before=71"`)
	lacks(t, first, "event number 70<", "event number 121")
	if strings.Index(first, "event number 120") > strings.Index(first, "event number 119") {
		t.Error("events are not newest first")
	}

	second := e.get("/dashboard/events?before=71").Body.String()
	contains(t, second, "event number 70", "event number 21", `href="/dashboard/events?before=21"`)
	lacks(t, second, "event number 71<", "event number 20<")

	last := e.get("/dashboard/events?before=21").Body.String()
	contains(t, last, "event number 20<", "event number 1<")
	lacks(t, last, "Older events")

	// A filter, kept across pages.
	filtered := e.get("/dashboard/events?resource_type=node").Body.String()
	contains(t, filtered, "event number 119", `value="node"`, "&amp;resource_type=node")
	lacks(t, filtered, "event number 120<")
	contains(t, e.get("/dashboard/events?resource_type=service&resource_id=n2").Body.String(), "event number 2<")
	lacks(t, e.get("/dashboard/events?resource_type=service&resource_id=n2").Body.String(), "event number 4<")

	for _, bad := range []string{
		"resource_type=Bad%20Type", "resource_type=" + strings.Repeat("a", 33), "resource_id=a%2Fb", "resource_id=" + strings.Repeat("a", 65),
		"before=0", "before=-3", "before=x", "before=99999999999999999999",
	} {
		rec := e.get("/dashboard/events?" + bad)
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "Invalid ") {
			t.Errorf("events?%s = %d, want a 400 page", bad, rec.Code)
		}
	}
	if len(e.seen) == 0 {
		t.Fatal("no request reached the API")
	}
	for _, s := range e.seen {
		if strings.Contains(s.Path, "Bad") || strings.Contains(s.Path, "before=-") || strings.Contains(s.Path, "before=0") {
			t.Errorf("an invalid filter was passed on to the API: %s", s.Path)
		}
	}
}

// What a container or an agent can write is shown as text, never as markup.
func TestPagesEscapeWhatTheClusterReports(t *testing.T) {
	e := newEnv(t)
	evil := `<script>alert(1)</script>`
	_, err := e.st.UpsertNode(context.Background(), store.Node{ID: "node-x", Name: evil, CPUMillis: 1000, MemoryBytes: 1 << 30,
		Labels: map[string]string{"zone": `"><img src=x onerror=alert(2)>`}, RegisteredAt: t0, LastHeartbeat: t0.Add(-time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	web := e.service("web", 1, "nginx:1.27", 100)
	e.replica(web, "w1", "node-x", 1, store.WorkloadFailed, `</td><svg onload=alert(3)>`, 0)
	e.event("test.event", "node", "node-x", `<img src=x onerror=alert(4)>`)

	for _, page := range []string{"/dashboard", "/dashboard/nodes/node-x", "/dashboard/services/web", "/dashboard/events"} {
		body := e.get(page).Body.String()
		lacks(t, body, "<script>alert", "<img", "<svg", "onerror=alert(2)>")
	}
	contains(t, e.get("/dashboard").Body.String(), "&lt;script&gt;alert(1)&lt;/script&gt;")
	contains(t, e.get("/dashboard/nodes/node-x").Body.String(), "&lt;img src=x onerror=alert(2)&gt;")
	contains(t, e.get("/dashboard/services/web").Body.String(), "&lt;/td&gt;&lt;svg onload=alert(3)&gt;")
	contains(t, e.get("/dashboard/events").Body.String(), "&lt;img src=x onerror=alert(4)&gt;")
}

func TestWhenTheAPIFailsThePageSaysSoWithoutDetails(t *testing.T) {
	e := newEnv(t)
	e.seedCluster()
	e.mu.Lock()
	e.apiErr = true
	e.mu.Unlock()
	for _, page := range []string{"/dashboard", "/dashboard/events", "/dashboard/services/web", "/dashboard/nodes/node-a"} {
		rec := e.get(page)
		if rec.Code != http.StatusBadGateway {
			t.Errorf("%s = %d, want 502", page, rec.Code)
		}
		contains(t, rec.Body.String(), "could not read the manager")
		lacks(t, rec.Body.String(), "10.0.0.5", "5432", "connection", "internal error")
	}
}

func TestThePagesAreMadeOfReadsOfTheAPIOnBehalfOfTheBrowser(t *testing.T) {
	e := newEnv(t)
	e.seedCluster()
	cookie := e.login()
	for _, page := range []string{"/dashboard", "/dashboard/services/web", "/dashboard/nodes/node-a", "/dashboard/events?resource_type=node"} {
		if rec := e.do(http.MethodGet, page, "", withCookie(cookie)); rec.Code != http.StatusOK {
			t.Fatalf("%s = %d", page, rec.Code)
		}
	}
	e.mu.Lock()
	seen := append([]seenRequest(nil), e.seen...)
	e.mu.Unlock()
	if len(seen) < 8 {
		t.Fatalf("only %d API calls: %+v", len(seen), seen)
	}
	ids := map[string]bool{}
	for _, s := range seen {
		switch {
		case s.Method != http.MethodGet:
			t.Errorf("the dashboard sent %s %s to the API; it only reads", s.Method, s.Path)
		case !strings.HasPrefix(s.Path, "/v1/"):
			t.Errorf("the dashboard called %s, which is not the API", s.Path)
		case s.Auth != "Bearer "+adminToken:
			t.Errorf("%s was sent with %q", s.Path, s.Auth)
		case s.Remote != browser:
			t.Errorf("%s was sent as from %q; the API's audit records must name the browser (%s)", s.Path, s.Remote, browser)
		case !strings.HasPrefix(s.ID, "dash-") || ids[s.ID]:
			t.Errorf("%s has the request ID %q; each call needs its own", s.Path, s.ID)
		}
		ids[s.ID] = true
	}
	// The browser never sees the token, in the pages or in the cookie.
	lacks(t, e.get("/dashboard").Body.String(), adminToken)
	if strings.Contains(cookie.Value, adminToken) {
		t.Error("the cookie holds the token")
	}
}
