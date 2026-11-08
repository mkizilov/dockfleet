package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type def = map[string]any

func baseDef() def {
	return def{"name": "web", "replicas": 2, "spec": def{"image": "nginx:1.27", "cpu_millis": 500, "memory_bytes": 134217728}}
}

func specOf(d def) def { return d["spec"].(def) }

func render(t *testing.T, d def) string {
	t.Helper()
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

type problemResponse struct {
	Error   string    `json:"error"`
	Details []problem `json:"details"`
}

// wantProblems checks that the response is a 400 that names each expected field
// (a field matches when it starts with the expected path) and returns it.
func wantProblems(t *testing.T, rec *httptest.ResponseRecorder, fields ...string) problemResponse {
	t.Helper()
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
	var resp problemResponse
	decode(t, rec, &resp)
	if resp.Error != "invalid service definition" || len(resp.Details) == 0 {
		t.Fatalf("response = %s", rec.Body.String())
	}
	for _, want := range fields {
		found := false
		for _, d := range resp.Details {
			if d.Message == "" {
				t.Errorf("problem without a message: %+v", d)
			}
			if (want == "" && d.Field == "") || (want != "" && strings.HasPrefix(d.Field, want)) {
				found = true
			}
		}
		if !found {
			t.Errorf("no problem reported for %q in %s", want, rec.Body.String())
		}
	}
	return resp
}

func manyEnv(n int) map[string]any {
	env := map[string]any{}
	for i := 0; i < n; i++ {
		env[fmt.Sprintf("VAR_%d", i)] = "x"
	}
	return env
}

func manyPorts(n int) []any {
	ports := make([]any, 0, n)
	for i := 0; i < n; i++ {
		ports = append(ports, def{"container_port": 1000 + i})
	}
	return ports
}

func manyArgs(n int) []any {
	args := make([]any, 0, n)
	for i := 0; i < n; i++ {
		args = append(args, "x")
	}
	return args
}

func testServiceValidation(t *testing.T, e *serviceEnv) {
	cases := []struct {
		name   string
		mutate func(d def)
		fields []string
	}{
		// name
		{"missing name", func(d def) { delete(d, "name") }, []string{"name"}},
		{"empty name", func(d def) { d["name"] = "" }, []string{"name"}},
		{"name with a space", func(d def) { d["name"] = "a b" }, []string{"name"}},
		{"name with a path", func(d def) { d["name"] = "../x" }, []string{"name"}},
		{"name starting with a dash", func(d def) { d["name"] = "-web" }, []string{"name"}},
		{"name too long", func(d def) { d["name"] = strings.Repeat("a", 64) }, []string{"name"}},
		{"name is a number", func(d def) { d["name"] = 5 }, []string{"name"}},

		// replicas
		{"missing replicas", func(d def) { delete(d, "replicas") }, []string{"replicas"}},
		{"null replicas", func(d def) { d["replicas"] = nil }, []string{"replicas"}},
		{"negative replicas", func(d def) { d["replicas"] = -1 }, []string{"replicas"}},
		{"too many replicas", func(d def) { d["replicas"] = 1001 }, []string{"replicas"}},
		{"replicas as a string", func(d def) { d["replicas"] = "3" }, []string{"replicas"}},
		{"fractional replicas", func(d def) { d["replicas"] = 1.5 }, []string{"replicas"}},

		// spec
		{"missing spec", func(d def) { delete(d, "spec") }, []string{"spec"}},
		{"null spec", func(d def) { d["spec"] = nil }, []string{"spec"}},
		{"spec as a string", func(d def) { d["spec"] = "nginx" }, []string{"spec"}},
		{"spec as an array", func(d def) { d["spec"] = []any{} }, []string{"spec"}},

		// image
		{"missing image", func(d def) { delete(specOf(d), "image") }, []string{"spec.image"}},
		{"empty image", func(d def) { specOf(d)["image"] = "" }, []string{"spec.image"}},
		{"upper-case repository", func(d def) { specOf(d)["image"] = "NGINX" }, []string{"spec.image"}},
		{"image with a space", func(d def) { specOf(d)["image"] = "nginx latest" }, []string{"spec.image"}},
		{"image that looks like a flag", func(d def) { specOf(d)["image"] = "--privileged" }, []string{"spec.image"}},
		{"image that is a URL", func(d def) { specOf(d)["image"] = "http://evil.example/x" }, []string{"spec.image"}},
		{"image with an empty tag", func(d def) { specOf(d)["image"] = "nginx:" }, []string{"spec.image"}},
		{"image with a short digest", func(d def) { specOf(d)["image"] = "nginx@sha256:abc" }, []string{"spec.image"}},
		{"image too long", func(d def) { specOf(d)["image"] = strings.Repeat("a", 256) }, []string{"spec.image"}},
		{"image is a number", func(d def) { specOf(d)["image"] = 5 }, []string{"spec.image"}},

		// resources
		{"missing cpu limit", func(d def) { delete(specOf(d), "cpu_millis") }, []string{"spec.cpu_millis"}},
		{"zero cpu limit", func(d def) { specOf(d)["cpu_millis"] = 0 }, []string{"spec.cpu_millis"}},
		{"negative cpu limit", func(d def) { specOf(d)["cpu_millis"] = -5 }, []string{"spec.cpu_millis"}},
		{"cpu limit too high", func(d def) { specOf(d)["cpu_millis"] = 1_000_001 }, []string{"spec.cpu_millis"}},
		{"cpu limit as a string", func(d def) { specOf(d)["cpu_millis"] = "1" }, []string{"spec.cpu_millis"}},
		{"fractional cpu limit", func(d def) { specOf(d)["cpu_millis"] = 0.5 }, []string{"spec.cpu_millis"}},
		{"missing memory limit", func(d def) { delete(specOf(d), "memory_bytes") }, []string{"spec.memory_bytes"}},
		{"zero memory limit", func(d def) { specOf(d)["memory_bytes"] = 0 }, []string{"spec.memory_bytes"}},
		{"memory limit below 6 MiB", func(d def) { specOf(d)["memory_bytes"] = 1 << 20 }, []string{"spec.memory_bytes"}},
		{"memory limit above 1 TiB", func(d def) { specOf(d)["memory_bytes"] = (1 << 40) + 1 }, []string{"spec.memory_bytes"}},

		// command and environment
		{"too many command arguments", func(d def) { specOf(d)["command"] = manyArgs(129) }, []string{"spec.command"}},
		{"command with a NUL", func(d def) { specOf(d)["command"] = []any{"sh", "a\x00b"} }, []string{"spec.command[1]"}},
		{"command starting with an empty program", func(d def) { specOf(d)["command"] = []any{""} }, []string{"spec.command"}},
		{"command as a string", func(d def) { specOf(d)["command"] = "ls -l" }, []string{"spec.command"}},
		{"env name starting with a digit", func(d def) { specOf(d)["env"] = def{"1BAD": "x"} }, []string{"spec.env"}},
		{"env name with an equals sign", func(d def) { specOf(d)["env"] = def{"A=B": "x"} }, []string{"spec.env"}},
		{"env name with a space", func(d def) { specOf(d)["env"] = def{"A B": "x"} }, []string{"spec.env"}},
		{"empty env name", func(d def) { specOf(d)["env"] = def{"": "x"} }, []string{"spec.env"}},
		{"env value with a NUL", func(d def) { specOf(d)["env"] = def{"A": "x\x00y"} }, []string{"spec.env"}},
		{"env value too long", func(d def) { specOf(d)["env"] = def{"A": strings.Repeat("x", 32<<10+1)} }, []string{"spec.env"}},
		{"too many env variables", func(d def) { specOf(d)["env"] = manyEnv(129) }, []string{"spec.env"}},
		{"env value is a number", func(d def) { specOf(d)["env"] = def{"A": 123} }, []string{"spec.env"}},
		{"env as an array", func(d def) { specOf(d)["env"] = []any{"A=1"} }, []string{"spec.env"}},

		// ports
		{"port without a number", func(d def) { specOf(d)["ports"] = []any{def{"protocol": "tcp"}} }, []string{"spec.ports[0].container_port"}},
		{"port zero", func(d def) { specOf(d)["ports"] = []any{def{"container_port": 0}} }, []string{"spec.ports[0].container_port"}},
		{"port above 65535", func(d def) { specOf(d)["ports"] = []any{def{"container_port": 65536}} }, []string{"spec.ports[0].container_port"}},
		{"port as a string", func(d def) { specOf(d)["ports"] = []any{def{"container_port": "80"}} }, []string{"spec.ports"}},
		{"unknown protocol", func(d def) { specOf(d)["ports"] = []any{def{"container_port": 80, "protocol": "sctp"}} }, []string{"spec.ports[0].protocol"}},
		{"upper-case protocol", func(d def) { specOf(d)["ports"] = []any{def{"container_port": 80, "protocol": "TCP"}} }, []string{"spec.ports[0].protocol"}},
		{"privileged host port", func(d def) { specOf(d)["ports"] = []any{def{"container_port": 80, "host_port": 80}} }, []string{"spec.ports[0].host_port"}},
		{"host port above 65535", func(d def) { specOf(d)["ports"] = []any{def{"container_port": 80, "host_port": 70000}} }, []string{"spec.ports[0].host_port"}},
		{"negative host port", func(d def) { specOf(d)["ports"] = []any{def{"container_port": 80, "host_port": -1}} }, []string{"spec.ports[0].host_port"}},
		{"same port twice", func(d def) { specOf(d)["ports"] = []any{def{"container_port": 80}, def{"container_port": 80}} }, []string{"spec.ports[1].container_port"}},
		{"same host port twice", func(d def) {
			specOf(d)["ports"] = []any{def{"container_port": 80, "host_port": 8080}, def{"container_port": 81, "host_port": 8080}}
		}, []string{"spec.ports[1].host_port"}},
		{"too many ports", func(d def) { specOf(d)["ports"] = manyPorts(33) }, []string{"spec.ports"}},
		{"ports as a string", func(d def) { specOf(d)["ports"] = "80" }, []string{"spec.ports"}},
		{"port entry as a string", func(d def) { specOf(d)["ports"] = []any{"80"} }, []string{"spec.ports"}},

		// health check
		{"health check without a command", func(d def) { specOf(d)["health_check"] = def{} }, []string{"spec.health_check.command"}},
		{"health check with a shell string", func(d def) { specOf(d)["health_check"] = def{"command": []any{"CMD-SHELL", "curl localhost"}} }, []string{"spec.health_check.command"}},
		{"health check with a CMD marker", func(d def) { specOf(d)["health_check"] = def{"command": []any{"CMD", "curl"}} }, []string{"spec.health_check.command"}},
		{"health check disabled with NONE", func(d def) { specOf(d)["health_check"] = def{"command": []any{"NONE"}} }, []string{"spec.health_check.command"}},
		{"health check with an empty program", func(d def) { specOf(d)["health_check"] = def{"command": []any{""}} }, []string{"spec.health_check.command"}},
		{"health check with too many arguments", func(d def) { specOf(d)["health_check"] = def{"command": manyArgs(33)} }, []string{"spec.health_check.command"}},
		{"negative interval", func(d def) { specOf(d)["health_check"] = def{"command": []any{"true"}, "interval_seconds": -1} }, []string{"spec.health_check.interval_seconds"}},
		{"interval above an hour", func(d def) { specOf(d)["health_check"] = def{"command": []any{"true"}, "interval_seconds": 3601} }, []string{"spec.health_check.interval_seconds"}},
		{"timeout above 300 seconds", func(d def) { specOf(d)["health_check"] = def{"command": []any{"true"}, "timeout_seconds": 301} }, []string{"spec.health_check.timeout_seconds"}},
		{"negative start period", func(d def) { specOf(d)["health_check"] = def{"command": []any{"true"}, "start_period_seconds": -1} }, []string{"spec.health_check.start_period_seconds"}},
		{"start period above an hour", func(d def) { specOf(d)["health_check"] = def{"command": []any{"true"}, "start_period_seconds": 3601} }, []string{"spec.health_check.start_period_seconds"}},
		{"more than ten retries", func(d def) { specOf(d)["health_check"] = def{"command": []any{"true"}, "retries": 11} }, []string{"spec.health_check.retries"}},
		{"docker-style test field", func(d def) { specOf(d)["health_check"] = def{"test": []any{"CMD", "true"}} }, []string{"spec.health_check.test"}},
		{"health check as a string", func(d def) { specOf(d)["health_check"] = "curl localhost" }, []string{"spec.health_check"}},

		// restart policy
		{"restart policy without a condition", func(d def) { specOf(d)["restart_policy"] = def{} }, []string{"spec.restart_policy.condition"}},
		{"unknown restart condition", func(d def) { specOf(d)["restart_policy"] = def{"condition": "sometimes"} }, []string{"spec.restart_policy.condition"}},
		{"unless-stopped is not supported", func(d def) { specOf(d)["restart_policy"] = def{"condition": "unless-stopped"} }, []string{"spec.restart_policy.condition"}},
		{"upper-case restart condition", func(d def) { specOf(d)["restart_policy"] = def{"condition": "ALWAYS"} }, []string{"spec.restart_policy.condition"}},
		{"retries with no restart", func(d def) { specOf(d)["restart_policy"] = def{"condition": "no", "max_retries": 3} }, []string{"spec.restart_policy.max_retries"}},
		{"retries with always restart", func(d def) { specOf(d)["restart_policy"] = def{"condition": "always", "max_retries": 1} }, []string{"spec.restart_policy.max_retries"}},
		{"negative retries", func(d def) { specOf(d)["restart_policy"] = def{"condition": "on-failure", "max_retries": -1} }, []string{"spec.restart_policy.max_retries"}},
		{"more than 100 retries", func(d def) { specOf(d)["restart_policy"] = def{"condition": "on-failure", "max_retries": 101} }, []string{"spec.restart_policy.max_retries"}},
		{"restart policy as a string", func(d def) { specOf(d)["restart_policy"] = "always" }, []string{"spec.restart_policy"}},

		// node selector
		{"selector key with a space", func(d def) { specOf(d)["node_selector"] = def{"bad key": "x"} }, []string{"spec.node_selector"}},
		{"empty selector key", func(d def) { specOf(d)["node_selector"] = def{"": "x"} }, []string{"spec.node_selector"}},
		{"selector value too long", func(d def) { specOf(d)["node_selector"] = def{"zone": strings.Repeat("a", 129)} }, []string{"spec.node_selector"}},
		{"selector value with a control character", func(d def) { specOf(d)["node_selector"] = def{"zone": "a\x07"} }, []string{"spec.node_selector"}},
		{"too many selector labels", func(d def) {
			sel := def{}
			for i := 0; i < 17; i++ {
				sel[fmt.Sprintf("k%d", i)] = "v"
			}
			specOf(d)["node_selector"] = sel
		}, []string{"spec.node_selector"}},
		{"selector as a string", func(d def) { specOf(d)["node_selector"] = "zone=a" }, []string{"spec.node_selector"}},

		// unsupported and read-only fields
		{"unknown top-level field", func(d def) { d["colour"] = "red" }, []string{"colour"}},
		{"unknown spec field", func(d def) { specOf(d)["entrypoint"] = []any{"sh"} }, []string{"spec.entrypoint"}},
		{"compose-style restart field", func(d def) { specOf(d)["restart"] = "always" }, []string{"spec.restart"}},
		{"labels", func(d def) { specOf(d)["labels"] = def{"io.dockfleet.managed": "false"} }, []string{"spec.labels"}},
		{"unknown port field", func(d def) { specOf(d)["ports"] = []any{def{"container_port": 80, "published": 80}} }, []string{"spec.ports[0].published"}},
		{"unknown restart field", func(d def) { specOf(d)["restart_policy"] = def{"condition": "no", "delay": 5} }, []string{"spec.restart_policy.delay"}},
		{"read-only id", func(d def) { d["id"] = "abc" }, []string{"id"}},
		{"read-only version", func(d def) { d["version"] = 3 }, []string{"version"}},
		{"read-only revision", func(d def) { d["revision"] = 3 }, []string{"revision"}},
		{"read-only timestamps", func(d def) { d["created_at"] = "2026-01-01T00:00:00Z"; d["deleted_at"] = nil }, []string{"created_at", "deleted_at"}},
	}
	for _, tt := range cases {
		e.run(t, tt.name, func(t *testing.T) {
			d := baseDef()
			tt.mutate(d)
			wantProblems(t, e.request(http.MethodPost, "/v1/services", render(t, d)), tt.fields...)
			e.wantNoServices()
		})
	}

	e.run(t, "every problem is reported at once", func(t *testing.T) {
		d := baseDef()
		d["name"] = "a b"
		d["replicas"] = -1
		specOf(d)["image"] = "NGINX"
		specOf(d)["cpu_millis"] = 0
		specOf(d)["ports"] = []any{def{"container_port": 0, "protocol": "x"}}
		resp := wantProblems(t, e.request(http.MethodPost, "/v1/services", render(t, d)),
			"name", "replicas", "spec.image", "spec.cpu_millis", "spec.ports[0].container_port", "spec.ports[0].protocol")
		if len(resp.Details) != 6 {
			t.Errorf("%d problems reported, want 6: %+v", len(resp.Details), resp.Details)
		}
		// The order is stable, so clients and tests can rely on it.
		again := wantProblems(t, e.request(http.MethodPost, "/v1/services", render(t, d)))
		if fmt.Sprint(resp.Details) != fmt.Sprint(again.Details) {
			t.Errorf("problems differ between identical requests:\n%v\n%v", resp.Details, again.Details)
		}
	})

	e.run(t, "an empty object lists everything that is required", func(t *testing.T) {
		wantProblems(t, e.request(http.MethodPost, "/v1/services", `{}`), "name", "replicas", "spec")
		e.wantNoServices()
	})

	e.run(t, "updates use the same rules and change nothing", func(t *testing.T) {
		e.create(minimalService)
		for name, mutate := range map[string]func(d def){
			"bad image":       func(d def) { specOf(d)["image"] = "NGINX" },
			"missing replica": func(d def) { delete(d, "replicas") },
			"privileged":      func(d def) { specOf(d)["privileged"] = true },
			"renamed":         func(d def) { d["name"] = "other" },
			"zero cpu":        func(d def) { specOf(d)["cpu_millis"] = 0 },
		} {
			d := baseDef()
			mutate(d)
			rec := e.request(http.MethodPut, "/v1/services/web", render(t, d))
			if rec.Code != http.StatusBadRequest {
				t.Errorf("%s: PUT = %d, want 400: %s", name, rec.Code, rec.Body.String())
			}
		}
		if v := e.get("web"); v.Version != 1 || v.Replicas != 2 || v.Spec.Image != "nginx:1.27" {
			t.Fatalf("a rejected update changed the service: %+v", v)
		}
		e.wantEvents("service.created")
	})
}

// The API has no field for the Docker options that weaken a container's
// isolation from its host, and says so by name rather than ignoring them.
func testServiceUnsafeOptions(t *testing.T, e *serviceEnv) {
	realistic := []struct {
		name  string
		field string
		value any
	}{
		{"privileged mode", "privileged", true},
		{"the Docker socket as a volume", "volumes", []any{"/var/run/docker.sock:/var/run/docker.sock"}},
		{"a bind mount of the host root", "binds", []any{"/:/host"}},
		{"a mounts list", "mounts", []any{def{"type": "bind", "source": "/", "target": "/host"}}},
		{"host networking", "network_mode", "host"},
		{"extra capabilities", "cap_add", []any{"SYS_ADMIN"}},
		{"host devices", "devices", []any{"/dev/mem"}},
		{"the host PID namespace", "pid_mode", "host"},
		{"the host IPC namespace", "ipc_mode", "host"},
		{"the host UTS namespace", "uts_mode", "host"},
		{"the host user namespace", "userns_mode", "host"},
		{"a disabled seccomp profile", "security_opt", []any{"seccomp=unconfined"}},
		{"kernel parameters", "sysctls", def{"kernel.core_pattern": "|/x"}},
	}
	for _, tt := range realistic {
		e.run(t, tt.name, func(t *testing.T) {
			d := baseDef()
			specOf(d)[tt.field] = tt.value
			resp := wantProblems(t, e.request(http.MethodPost, "/v1/services", render(t, d)), "spec."+tt.field)
			if !strings.Contains(resp.Details[0].Message, "isolation") {
				t.Errorf("message = %q; it should explain that DockFleet refuses this on purpose", resp.Details[0].Message)
			}
			e.wantNoServices()
		})
	}

	// Every unsafe option is refused wherever it appears, at the top level, in the
	// spec and in the nested objects, and for updates too.
	e.create(minimalService)
	for key := range unsafeFields {
		for _, where := range []string{"top", "spec", "port", "health_check", "restart_policy"} {
			d := baseDef()
			switch where {
			case "top":
				d[key] = true
			case "spec":
				specOf(d)[key] = true
			case "port":
				specOf(d)["ports"] = []any{def{"container_port": 80, key: true}}
			case "health_check":
				specOf(d)["health_check"] = def{"command": []any{"true"}, key: true}
			case "restart_policy":
				specOf(d)["restart_policy"] = def{"condition": "no", key: true}
			}
			for _, rec := range []*httptest.ResponseRecorder{
				e.request(http.MethodPost, "/v1/services", render(t, d)),
				e.request(http.MethodPut, "/v1/services/web", render(t, d)),
			} {
				resp := wantProblems(t, rec)
				if len(resp.Details) != 1 || !strings.HasSuffix(resp.Details[0].Field, key) || !strings.Contains(resp.Details[0].Message, "isolation") {
					t.Errorf("%s in %s: details = %+v", key, where, resp.Details)
				}
			}
		}
	}
	if v := e.get("web"); v.Version != 1 {
		t.Fatalf("a rejected update changed the service: %+v", v)
	}
	e.wantEvents("service.created")
}

func testServiceRequestLimits(t *testing.T, e *serviceEnv) {
	for name, body := range map[string]string{
		"empty body":          "",
		"whitespace only":     "  \n",
		"null":                "null",
		"an array":            "[]",
		"a string":            `"web"`,
		"a number":            "5",
		"truncated JSON":      `{"name":"web"`,
		"two objects":         minimalService + minimalService,
		"trailing text":       minimalService + " x",
		"not JSON":            "name=web&replicas=2",
		"a JSON comment":      `{"name":"web"} // comment`,
		"duplicate-looking":   `{"name":"web","name":"web","replicas":2}`,
		"an object with null": `{"name":null,"replicas":null,"spec":null}`,
	} {
		e.run(t, name, func(t *testing.T) {
			wantProblems(t, e.request(http.MethodPost, "/v1/services", body))
			e.wantNoServices()
		})
	}

	e.run(t, "a body over the limit", func(t *testing.T) {
		body := `{"name":"web","replicas":1,"spec":{"image":"x","env":{"A":"` + strings.Repeat("x", maxBodyBytes) + `"}}}`
		if rec := e.request(http.MethodPost, "/v1/services", body); rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want 413", rec.Code)
		}
		e.wantNoServices()
	})

	// Environment values can be secrets, so no error repeats them.
	e.run(t, "errors never echo environment values", func(t *testing.T) {
		const secret = "top-secret-value-9f3a"
		for name, env := range map[string]def{
			"bad name":       {"BAD NAME": secret},
			"bad name again": {"1BAD": secret},
			"NUL in value":   {"OK": secret + "\x00"},
			"value too long": {"OK": secret + strings.Repeat("x", 32<<10)},
		} {
			d := baseDef()
			specOf(d)["env"] = env
			rec := e.request(http.MethodPost, "/v1/services", render(t, d))
			wantProblems(t, rec, "spec.env")
			if strings.Contains(rec.Body.String(), secret) {
				t.Errorf("%s: the response repeats the value: %s", name, rec.Body.String())
			}
		}
	})

	e.run(t, "responses are not cacheable", func(t *testing.T) {
		e.create(minimalService)
		for _, path := range []string{"/v1/services", "/v1/services/web", "/v1/services/web/revisions", "/v1/services/ghost"} {
			rec := e.request(http.MethodGet, path, "")
			if rec.Header().Get("Cache-Control") != "no-store" || rec.Header().Get("X-Content-Type-Options") != "nosniff" {
				t.Errorf("GET %s headers = %v", path, rec.Header())
			}
		}
	})

	e.run(t, "other methods are not allowed", func(t *testing.T) {
		for _, tt := range []struct{ method, path string }{
			{http.MethodPatch, "/v1/services/web"},
			{http.MethodPost, "/v1/services/web"},
			{http.MethodDelete, "/v1/services"},
			{http.MethodPut, "/v1/services"},
			{http.MethodPost, "/v1/services/web/revisions"},
		} {
			if rec := e.request(tt.method, tt.path, "{}"); rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s %s = %d, want 405", tt.method, tt.path, rec.Code)
			}
		}
	})
}
