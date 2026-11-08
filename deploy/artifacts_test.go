// Package deploy has no code: it holds what a deployment is made of (compose
// files, examples) and the tests that keep those files true. Images cannot be
// built where these tests run, so the checks here are on the files themselves:
// that the pins agree, that the examples load through the real configuration
// code, and that the hardening they promise is still in them.
package deploy

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/dockfleet/dockfleet/internal/config"
)

func read(t *testing.T, rel string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func matchOne(t *testing.T, re, text, what string) string {
	t.Helper()
	m := regexp.MustCompile(re).FindStringSubmatch(text)
	if m == nil {
		t.Fatalf("%s not found", what)
	}
	return m[1]
}

// The Dockerfile builds with the same generators as `make generate`, and with the
// Go release the module is written for.
func TestDockerfilePinsAgreeWithTheMakefileAndGoMod(t *testing.T) {
	docker, makefile, gomod := read(t, "Dockerfile"), read(t, "Makefile"), read(t, "go.mod")
	for _, p := range []struct{ arg, make string }{
		{"BUF_VERSION", "BUF_VERSION"},
		{"PROTOC_GEN_GO_VERSION", "PROTOC_GEN_GO_VERSION"},
		{"PROTOC_GEN_GO_GRPC_VERSION", "PROTOC_GEN_GO_GRPC_VERSION"},
	} {
		inDocker := matchOne(t, `(?m)^ARG `+p.arg+`=(\S+)$`, docker, "ARG "+p.arg)
		inMake := matchOne(t, `(?m)^`+p.make+`\s*:=\s*(\S+)$`, makefile, p.make+" in the Makefile")
		if inDocker != inMake {
			t.Errorf("%s is %s in the Dockerfile and %s in the Makefile", p.arg, inDocker, inMake)
		}
	}
	goVersion := matchOne(t, `(?m)^ARG GO_VERSION=(\S+)$`, docker, "ARG GO_VERSION")
	modGo := matchOne(t, `(?m)^go (\d+\.\d+(?:\.\d+)?)$`, gomod, "the go directive")
	// go.mod names the exact release (it carries the standard-library fixes the binaries
	// need), and the image builds with that release; a bare 1.26 would accept any patch.
	if strings.Count(modGo, ".") != 2 || goVersion != modGo {
		t.Errorf("the image builds with Go %s but go.mod says go %s; both must name the same release, patch included", goVersion, modGo)
	}
	if strings.Count(docker, "golang:${GO_VERSION}") != 1 {
		t.Error("the build stage must use the pinned Go release")
	}
}

// stage returns the instructions of one stage of the Dockerfile.
func stage(t *testing.T, docker, name string) []string {
	t.Helper()
	var out []string
	in := false
	for _, line := range strings.Split(docker, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "FROM ") {
			in = strings.HasSuffix(trimmed, " AS "+name)
		}
		if in && trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			out = append(out, trimmed)
		}
	}
	if len(out) == 0 {
		t.Fatalf("no stage %q in the Dockerfile", name)
	}
	return out
}

func TestImagesRunUnprivilegedAndCheckTheirOwnHealth(t *testing.T) {
	docker := read(t, "Dockerfile")
	for name, ports := range map[string][]string{"manager": {"21280", "21281"}, "agent": nil} {
		text := strings.Join(stage(t, docker, name), "\n")
		if !regexp.MustCompile(`(?m)^FROM \$\{RUNTIME_IMAGE\} AS `).MatchString(text) {
			t.Errorf("%s: the runtime stage must start from the RUNTIME_IMAGE argument", name)
		}
		if user := matchOne(t, `(?m)^USER (\S+)$`, text, name+" USER"); user == "0" || user == "root" || strings.HasPrefix(user, "0:") || !regexp.MustCompile(`^[1-9][0-9]*(:[1-9][0-9]*)?$`).MatchString(user) {
			t.Errorf("%s: USER %s must be a numeric, non-root user", name, user)
		}
		binary := "/usr/local/bin/dockfleet-" + name
		if !strings.Contains(text, `ENTRYPOINT ["`+binary+`"]`) {
			t.Errorf("%s: ENTRYPOINT must be the binary in exec form", name)
		}
		if !strings.Contains(text, `CMD ["`+binary+`", "healthcheck"]`) || !strings.Contains(text, "HEALTHCHECK ") {
			t.Errorf("%s: HEALTHCHECK must run the binary's own healthcheck", name)
		}
		for _, banned := range []string{"ADD ", "--privileged", "curl ", "wget ", "apt-get", "sudo", "chmod 777", "COPY . "} {
			if strings.Contains(text, banned) {
				t.Errorf("%s: the stage contains %q", name, banned)
			}
		}
		if got := regexp.MustCompile(`(?m)^EXPOSE (.+)$`).FindStringSubmatch(text); (got == nil) != (len(ports) == 0) || (got != nil && strings.Join(strings.Fields(got[1]), " ") != strings.Join(ports, " ")) {
			t.Errorf("%s: EXPOSE %v, want %v", name, got, ports)
		}
	}
	// No credential is baked into any layer through ENV or ARG.
	for _, line := range strings.Split(docker, "\n") {
		l := strings.ToUpper(strings.TrimSpace(line))
		if (strings.HasPrefix(l, "ENV ") || strings.HasPrefix(l, "ARG ")) && regexp.MustCompile(`(TOKEN|PASSWORD|SECRET|PRIVATE|DATABASE_URL)`).MatchString(l) {
			t.Errorf("the Dockerfile sets a credential-looking variable: %q", line)
		}
	}
	// The build copies named directories, and the context excludes everything else.
	build := strings.Join(stage(t, docker, "build"), "\n")
	for _, copied := range regexp.MustCompile(`(?m)^COPY (\S+)`).FindAllStringSubmatch(build, -1) {
		if copied[1] == "--from=build" || copied[1] == "." {
			t.Errorf("COPY %s: copy named paths", copied[1])
		}
	}
	ignore := read(t, ".dockerignore")
	if !regexp.MustCompile(`(?m)^\*$`).MatchString(ignore) {
		t.Error(".dockerignore must start by excluding everything")
	}
	for _, want := range []string{".dev-pki", "*.key", "*.pem", "*.env", "_test.go"} {
		if !strings.Contains(ignore, want) {
			t.Errorf(".dockerignore does not exclude %s", want)
		}
	}
}

// envFile reads KEY=value lines. With uncomment, lines commented out as
// `#KEY=value` (the optional settings) count too.
func envFile(t *testing.T, rel string, uncomment bool) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, line := range strings.Split(read(t, rel), "\n") {
		line = strings.TrimSpace(line)
		if uncomment && strings.HasPrefix(line, "#DOCKFLEET_") {
			line = line[1:]
		}
		if !strings.HasPrefix(line, "DOCKFLEET_") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("%s: %q is not KEY=value", rel, line)
		}
		out[k] = v
	}
	return out
}

// substituteFiles points every credential file at a real temporary file, which
// is what the container runtime or systemd provides at run time.
func substituteFiles(t *testing.T, env map[string]string) {
	t.Helper()
	dir := t.TempDir()
	for k, v := range env {
		if !strings.HasSuffix(k, "_FILE") || !strings.HasPrefix(v, "/run/secrets/") {
			continue
		}
		p := filepath.Join(dir, filepath.Base(v))
		content := "a-credential-for-" + strings.ToLower(k) + "-0123456789"
		if strings.Contains(k, "DATABASE_URL") {
			content = "postgres://u:p@db.internal:5432/dockfleet"
		}
		if err := os.WriteFile(p, []byte(content+"\n"), 0o400); err != nil {
			t.Fatal(err)
		}
		env[k] = p
	}
}

// The examples are real configuration: they load through the same code the
// binaries use, as written and with every optional setting switched on.
func TestExampleConfigurationLoads(t *testing.T) {
	for _, uncomment := range []bool{false, true} {
		m := envFile(t, "deploy/examples/manager.env.example", uncomment)
		substituteFiles(t, m)
		cfg, err := config.LoadManager(func(k string) string { return m[k] })
		if err != nil {
			t.Fatalf("manager.env.example (optional settings on: %v): %v", uncomment, err)
		}
		if !cfg.TLSEnabled() || cfg.AgentToken != "" {
			t.Errorf("the example manager must use mutual TLS and no shared agent token: %+v", cfg)
		}
		if cfg.HAEnabled() != uncomment {
			t.Errorf("high availability is %v with the optional settings %v", cfg.HAEnabled(), uncomment)
		}

		w := envFile(t, "deploy/examples/worker.env.example", uncomment)
		a, err := config.LoadAgent(func(k string) string { return w[k] })
		if err != nil {
			t.Fatalf("worker.env.example (optional settings on: %v): %v", uncomment, err)
		}
		if !a.TLSEnabled() || a.Token != "" || len(a.ManagerAddrs) < 2 || !a.FenceOnLeaseLoss || a.HealthAddr == "" {
			t.Errorf("the example worker must use mutual TLS, no token, several managers, fencing and a health endpoint: %+v", a)
		}
		if uncomment && (len(a.AllowedBindRoots) == 0) {
			t.Error("the optional container policy settings did not load")
		}
	}
}

var variableName = regexp.MustCompile(`DOCKFLEET_[A-Z0-9_]+`)

func variablesIn(t *testing.T, files ...string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, f := range files {
		for _, v := range variableName.FindAllString(read(t, f), -1) {
			out[strings.TrimSuffix(v, "_")] = true
		}
	}
	return out
}

// An example that sets a variable nothing reads is a typo that silently does
// nothing; a variable the code reads that the README does not mention is an
// undocumented setting.
func TestEveryVariableIsRealAndDocumented(t *testing.T) {
	code := variablesIn(t, "internal/config/config.go", "cmd/manager/main.go", "cmd/agent/main.go")
	// Credentials may also be given as files; the code builds the _FILE name.
	for _, name := range []string{"DOCKFLEET_API_TOKEN", "DOCKFLEET_AGENT_TOKEN", "DOCKFLEET_DATABASE_URL"} {
		code[name+"_FILE"] = true
	}
	for _, rel := range []string{"deploy/examples/manager.env.example", "deploy/examples/worker.env.example"} {
		for name := range envFile(t, rel, true) {
			if !code[name] {
				t.Errorf("%s sets %s, which nothing reads", rel, name)
			}
		}
	}
	for _, rel := range []string{"deploy/examples/compose.manager.yml"} {
		for name := range variablesIn(t, rel) {
			if !code[name] && !strings.HasPrefix(name, "DOCKFLEET_PUBLISH") && !strings.HasPrefix(name, "DOCKFLEET_MANAGER_IMAGE") && !strings.HasPrefix(name, "DOCKFLEET_AGENT_IMAGE") {
				t.Errorf("%s mentions %s, which nothing reads", rel, name)
			}
		}
	}
	readme := read(t, "README.md") + read(t, "docs/deployment.md")
	var missing []string
	for name := range code {
		if name == "DOCKFLEET_TEST_DATABASE_URL" || strings.HasPrefix(name, "DOCKFLEET_TEST_") {
			continue
		}
		if !strings.Contains(readme, name) {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("variables the code reads that neither README.md nor docs/deployment.md mention: %v", missing)
	}
}

func fileExists(t *testing.T, rel string) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join("..", rel))
	return err == nil
}

func lines(text string) map[string]bool {
	out := map[string]bool{}
	for _, l := range strings.Split(text, "\n") {
		out[strings.TrimSpace(l)] = true
	}
	return out
}

func TestComposeExamplesKeepTheirHardening(t *testing.T) {
	for _, rel := range []string{"deploy/examples/compose.manager.yml"} {
		text := read(t, rel)
		l := lines(text)
		for _, want := range []string{"read_only: true", "cap_drop: [ALL]", "- no-new-privileges:true", `user: "65532:65532"`, "restart: unless-stopped"} {
			if !l[want] {
				t.Errorf("%s lacks %q", rel, want)
			}
		}
		for _, banned := range []string{"privileged:", "network_mode: host", "pid: host", "ipc: host", "cap_add", "userns_mode"} {
			for _, line := range strings.Split(text, "\n") {
				if strings.HasPrefix(strings.TrimSpace(line), banned) {
					t.Errorf("%s contains %q", rel, banned)
				}
			}
		}
		if !strings.Contains(text, "healthcheck:") || !strings.Contains(text, `"healthcheck"]`) {
			t.Errorf("%s has no health check", rel)
		}
		if !strings.Contains(text, "stop_grace_period:") {
			t.Errorf("%s does not give the process time to shut down", rel)
		}
	}
	manager := read(t, "deploy/examples/compose.manager.yml")
	if strings.Contains(manager, "docker.sock") && !strings.Contains(manager, "# ") {
		t.Error("the manager has no business with the Docker socket")
	}
	for _, line := range strings.Split(manager, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "#") && strings.Contains(line, "docker.sock") {
			t.Error("the manager compose file mounts the Docker socket")
		}
	}
	for _, want := range []string{"secrets:", "dockfleet_api_token", "dockfleet_database_url"} {
		if !strings.Contains(manager, want) {
			t.Errorf("the manager compose file does not provide %s as a secret", want)
		}
	}
	if fileExists(t, "deploy/examples/compose.worker.yml") {
		t.Error("a Compose file for the agent would have to mount the Docker socket; run the agent as a host service")
	}
}

func TestSystemdUnitsKeepTheirSandbox(t *testing.T) {
	for _, rel := range []string{"deploy/examples/systemd/dockfleet-manager.service", "deploy/examples/systemd/dockfleet-agent.service"} {
		l := lines(read(t, rel))
		for _, want := range []string{
			"NoNewPrivileges=yes", "CapabilityBoundingSet=", "ProtectSystem=strict", "ProtectHome=yes", "PrivateTmp=yes", "PrivateDevices=yes",
			"RestrictSUIDSGID=yes", "MemoryDenyWriteExecute=yes", "LockPersonality=yes", "SystemCallErrorNumber=EPERM", "Restart=on-failure", "ReadOnlyPaths=/etc/dockfleet",
		} {
			if !l[want] {
				t.Errorf("%s lacks %s", rel, want)
			}
		}
		for line := range l {
			if strings.HasPrefix(line, "User=") && (line == "User=root" || line == "User=") {
				t.Errorf("%s runs as root", rel)
			}
			if strings.HasPrefix(line, "Environment=") && regexp.MustCompile(`(?i)(TOKEN|PASSWORD|DATABASE_URL)=[^%]`).MatchString(line) {
				t.Errorf("%s puts a credential in the environment: %s", rel, line)
			}
		}
	}
	manager := lines(read(t, "deploy/examples/systemd/dockfleet-manager.service"))
	for _, want := range []string{"LoadCredential=api-token:/etc/dockfleet/secrets/api-token", "Environment=DOCKFLEET_API_TOKEN_FILE=%d/api-token", "RestrictAddressFamilies=AF_INET AF_INET6"} {
		if !manager[want] {
			t.Errorf("the manager unit lacks %s", want)
		}
	}
	agent := lines(read(t, "deploy/examples/systemd/dockfleet-agent.service"))
	if !agent["SupplementaryGroups=docker"] || !agent["User=dockfleet-agent"] {
		t.Error("the agent unit must run as its own user, in the docker group and no other")
	}
}

// The CI workflow runs every kind of check this repository has; a check that is
// dropped from it is a check nobody runs.
func TestCIRunsEveryKindOfCheck(t *testing.T) {
	ci := read(t, ".github/workflows/ci.yml")
	for _, job := range []string{"lint", "unit", "cross-build", "integration", "etcd", "e2e", "docker", "images", "vulnerabilities"} {
		if !regexp.MustCompile(`(?m)^  ` + job + `:$`).MatchString(ci) {
			t.Errorf("ci.yml has no %q job", job)
		}
	}
	for _, step := range []string{"make check", "make test-e2e", "-tags integration", "make test-etcd", "make test-docker", "make docker-build", "make cross-build", "make tidy-check", "make vuln"} {
		if !strings.Contains(ci, step) {
			t.Errorf("ci.yml never runs %q", step)
		}
	}
	if !strings.Contains(ci, "permissions:\n  contents: read") {
		t.Error("ci.yml must run with read-only repository permissions")
	}
	for _, line := range strings.Split(ci, "\n") {
		if strings.Contains(line, "uses:") && !regexp.MustCompile(`@(v\d+|[0-9a-f]{40})`).MatchString(line) {
			t.Errorf("an action is not pinned: %s", strings.TrimSpace(line))
		}
		if strings.Contains(line, "pull_request_target") {
			t.Error("ci.yml uses pull_request_target, which runs untrusted code with repository secrets")
		}
	}
}
