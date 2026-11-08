package api

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestValidImage(t *testing.T) {
	digest := "sha256:" + strings.Repeat("ab", 32)
	valid := []string{
		"nginx", "nginx:1.27", "library/nginx", "library/nginx:1.27", "docker.io/library/nginx:1.27",
		"registry.example.com:5000/team/app:v1.2.3", "localhost:5000/app", "localhost/app", "1.2.3.4:5000/app",
		"ghcr.io/org/app@" + digest, "nginx@" + digest, "nginx:1.27@" + digest,
		"my_app", "my-app", "a__b", "a.b", "a---b", "app:Tag_1.2-rc1", "a/b/c/d/e", "my-registry/app",
	}
	invalid := []string{
		"", "NGINX", "Nginx:1", "Library/nginx", "nginx latest", " nginx", "nginx ", "-nginx", "--privileged",
		"nginx:", ":1", "nginx::1", "nginx:1:2", "nginx@sha256:short", "nginx@md5:" + strings.Repeat("a", 32),
		"nginx@" + digest + "@" + digest, "http://x/y", "nginx/", "/nginx", "a//b", "a/./b", "a/../b",
		"ngi\x00nx", "nginx\n", "a_", "_a", "a-", ".a", "a..b", "a-.b", "a:" + strings.Repeat("t", 129),
		"-registry.example.com/app", "registry..example.com/app", "host:123456/app", strings.Repeat("a", 256),
	}
	for _, ref := range valid {
		if !validImage(ref) {
			t.Errorf("validImage(%q) = false, want true", ref)
		}
	}
	for _, ref := range invalid {
		if validImage(ref) {
			t.Errorf("validImage(%q) = true, want false", ref)
		}
	}
}

func TestParseIfMatch(t *testing.T) {
	tests := []struct {
		header  string
		version int64
		ok      bool
	}{
		{"", 0, true}, {"*", 0, true}, {" * ", 0, true},
		{`"1"`, 1, true}, {`"42"`, 42, true}, {"7", 7, true}, {` "3" `, 3, true},
		{"0", 0, false}, {"-1", 0, false}, {"abc", 0, false}, {`"1","2"`, 0, false}, {`W/"1"`, 0, false}, {"1.5", 0, false}, {`""`, 0, false},
	}
	for _, tt := range tests {
		version, ok := parseIfMatch(tt.header)
		if version != tt.version || ok != tt.ok {
			t.Errorf("parseIfMatch(%q) = %d, %v; want %d, %v", tt.header, version, ok, tt.version, tt.ok)
		}
	}
}

func parse(t *testing.T, body string) serviceInput {
	t.Helper()
	in, probs := parseService([]byte(body), true, "")
	if len(probs) > 0 {
		t.Fatalf("parseService(%s) = %+v", body, probs)
	}
	return in
}

// Definitions that mean the same thing must normalize to the same spec, or an
// unchanged definition would be recorded as a new deployment.
func TestEquivalentDefinitionsNormalizeAlike(t *testing.T) {
	base := parse(t, `{"name":"w","replicas":1,"spec":{"image":"nginx","cpu_millis":1,"memory_bytes":134217728,
		"env":{"A":"1","B":"2"},"ports":[{"container_port":80},{"container_port":53,"protocol":"udp"}]}}`)

	equivalents := []string{
		// another order, explicit defaults, empty optional values
		`{"name":"w","replicas":1,"spec":{"image":"nginx","cpu_millis":1,"memory_bytes":134217728,"command":[],"node_selector":{},
			"env":{"B":"2","A":"1"},"ports":[{"container_port":53,"protocol":"udp","host_port":0},{"container_port":80,"protocol":"tcp"}],
			"restart_policy":{"condition":"on-failure","max_retries":5}}}`,
		`{"name":"w","replicas":1,"spec":{"restart_policy":null,"health_check":null,"image":"nginx","cpu_millis":1,"memory_bytes":134217728,
			"env":{"A":"1","B":"2"},"ports":[{"container_port":80},{"container_port":53,"protocol":"udp"}]}}`,
	}
	for _, body := range equivalents {
		in := parse(t, body)
		if in.spec.Hash() != base.spec.Hash() || !reflect.DeepEqual(in.spec, base.spec) {
			t.Errorf("not normalized alike:\n%+v\n%+v", in.spec, base.spec)
		}
	}

	different := []string{
		`{"name":"w","replicas":1,"spec":{"image":"nginx","cpu_millis":1,"memory_bytes":134217728,"env":{"A":"1"},"ports":[{"container_port":80},{"container_port":53,"protocol":"udp"}]}}`,
		`{"name":"w","replicas":1,"spec":{"image":"nginx","cpu_millis":1,"memory_bytes":134217728,"env":{"A":"1","B":"2"},"ports":[{"container_port":80},{"container_port":53}]}}`,
		`{"name":"w","replicas":1,"spec":{"image":"nginx","cpu_millis":1,"memory_bytes":134217728,"env":{"A":"1","B":"2"},"ports":[{"container_port":80},{"container_port":53,"protocol":"udp"}],"restart_policy":{"condition":"always"}}}`,
	}
	for _, body := range different {
		if in := parse(t, body); in.spec.Hash() == base.spec.Hash() {
			t.Errorf("a different definition normalized to the same spec: %s", body)
		}
	}
}

func TestParseServiceReturnsTheNormalizedDefinition(t *testing.T) {
	in := parse(t, `{"name":"w","replicas":1,"spec":{"image":"nginx","cpu_millis":1,"memory_bytes":134217728,"command":["a"],"env":{"A":"1"},"node_selector":{"z":"a"}}}`)
	if in.name != "w" || in.replicas != 1 || in.spec.Image != "nginx" || in.spec.Command[0] != "a" || in.spec.Env["A"] != "1" || in.spec.NodeSelector["z"] != "a" {
		t.Fatalf("parsed = %+v", in)
	}
	if in.spec.Ports != nil || in.spec.HealthCheck != nil || in.spec.RestartPolicy == nil {
		t.Fatalf("ports %v, health check %v, restart policy %v; only the restart policy has a default", in.spec.Ports, in.spec.HealthCheck, in.spec.RestartPolicy)
	}
}

func TestProblemsAreOrderedAndComplete(t *testing.T) {
	body := `{"zeta":1,"alpha":2,"spec":{"privileged":true,"colour":"x","image":"NGINX","cpu_millis":1,"memory_bytes":134217728}}`
	_, first := parseService([]byte(body), true, "")
	_, second := parseService([]byte(body), true, "")
	var fields []string
	for _, p := range first {
		fields = append(fields, p.Field)
	}
	// Unknown fields are reported in alphabetical order, per object, and stop the
	// semantic checks (the image is not judged while the shape is wrong).
	want := []string{"alpha", "zeta", "spec.colour", "spec.privileged"}
	if !sort.StringsAreSorted(fields[:2]) || !reflect.DeepEqual(fields, want) {
		t.Fatalf("fields = %v, want %v", fields, want)
	}
	if fmt.Sprint(first) != fmt.Sprint(second) {
		t.Fatalf("problems differ between identical requests: %v / %v", first, second)
	}
}

func TestPathNameMustMatchOnUpdate(t *testing.T) {
	body := []byte(`{"name":"other","replicas":1,"spec":{"image":"nginx","cpu_millis":1,"memory_bytes":134217728}}`)
	if _, probs := parseService(body, false, "web"); len(probs) != 1 || probs[0].Field != "name" {
		t.Fatalf("problems = %+v", probs)
	}
	if in, probs := parseService(body, false, "other"); len(probs) != 0 || in.name != "" {
		t.Fatalf("a matching name is accepted and ignored: %+v %+v", in, probs)
	}
}
