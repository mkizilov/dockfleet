package api

import (
	"go/ast"
	"go/parser"
	gotoken "go/token"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// openRoutes are the only routes that answer without the admin token: liveness
// and readiness, which an orchestrator's health check asks and which say nothing
// but "ok", "ready" or "store unavailable".
var openRoutes = map[string]bool{"GET /healthz": true, "GET /readyz": true}

// registeredRoutes reads the patterns the API registers from its source, and
// whether each is wrapped in the authentication check. Reading the source is the
// point: a route added later, in any file of the package, is found without
// anyone remembering to list it in a test.
func registeredRoutes(t *testing.T) (authed, open []string) {
	t.Helper()
	fset := gotoken.NewFileSet()
	files, err := parser.ParseDir(fset, ".", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, pkg := range files {
		for name, file := range pkg.Files {
			if strings.HasSuffix(name, "_test.go") {
				continue
			}
			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || (sel.Sel.Name != "Handle" && sel.Sel.Name != "HandleFunc") || len(call.Args) != 2 {
					return true
				}
				if id, ok := sel.X.(*ast.Ident); !ok || id.Name != "mux" {
					return true
				}
				lit, ok := call.Args[0].(*ast.BasicLit)
				if !ok || lit.Kind != gotoken.STRING {
					t.Errorf("%s: a route pattern that is not a string literal cannot be checked", fset.Position(call.Pos()))
					return true
				}
				pattern, _ := strconv.Unquote(lit.Value)
				wrapped := false
				if inner, ok := call.Args[1].(*ast.CallExpr); ok {
					if f, ok := inner.Fun.(*ast.SelectorExpr); ok && f.Sel.Name == "auth" {
						wrapped = true
					}
				}
				if wrapped {
					authed = append(authed, pattern)
				} else {
					open = append(open, pattern)
				}
				return true
			})
		}
	}
	sort.Strings(authed)
	sort.Strings(open)
	return authed, open
}

func newRequest(method, path string) *http.Request {
	return httptest.NewRequest(method, path, strings.NewReader("{}"))
}

func serve(h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// Every route except the two health checks must go through the authentication
// check, in the source, and must refuse a request without the right token, at
// run time. A new route that forgets the check fails here.
func TestEveryRouteExceptHealthRequiresTheAdminToken(t *testing.T) {
	authed, open := registeredRoutes(t)
	if len(authed) < 20 {
		t.Fatalf("found only %d authenticated routes in the source (%v); the test is not looking at the API", len(authed), authed)
	}
	for _, pattern := range open {
		if !openRoutes[pattern] {
			t.Errorf("route %q is registered without the authentication check; only %v may be open", pattern, openRoutes)
		}
	}
	for pattern := range openRoutes {
		found := false
		for _, o := range open {
			found = found || o == pattern
		}
		if !found {
			t.Errorf("the open route %q is not registered any more; update openRoutes", pattern)
		}
	}

	h, _ := newTestHandler(t)
	for _, pattern := range authed {
		method, path, ok := strings.Cut(pattern, " ")
		if !ok {
			t.Fatalf("pattern %q has no method", pattern)
		}
		path = strings.NewReplacer("{id}", "x", "{name}", "x").Replace(path)
		for _, auth := range []string{"", "Bearer", "Bearer wrong-token-0123456789", "bearer " + token, "Basic " + token, token, "Bearer " + token + "x", "Bearer  " + token} {
			req := newRequest(method, path)
			if auth != "" {
				req.Header.Set("Authorization", auth)
			}
			rec := serve(h, req)
			if rec.Code == http.StatusNotFound && path == "/metrics" {
				continue // not registered while metrics are off
			}
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("%s with Authorization %q = %d, want 401", pattern, auth, rec.Code)
			}
			if got := rec.Header().Get("WWW-Authenticate"); got == "" && rec.Code == http.StatusUnauthorized {
				t.Errorf("%s: a 401 without WWW-Authenticate", pattern)
			}
		}
		// A token in the URL is not a token.
		req := newRequest(method, path+"?token="+token+"&access_token="+token)
		if rec := serve(h, req); rec.Code != http.StatusUnauthorized && !(rec.Code == http.StatusNotFound && path == "/metrics") {
			t.Errorf("%s with the token in the query string = %d, want 401", pattern, rec.Code)
		}
		// With the token the route is reachable: whatever it answers, it is not 401 or 403.
		req = newRequest(method, path)
		req.Header.Set("Authorization", "Bearer "+token)
		if rec := serve(h, req); rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden {
			t.Errorf("%s with the right token = %d", pattern, rec.Code)
		}
	}
}
