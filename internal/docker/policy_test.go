package docker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func validSpec() ContainerSpec {
	return ContainerSpec{
		WorkloadID:   "w-1",
		WorkloadName: "web",
		Image:        "nginx:1.27",
		Command:      []string{"nginx", "-g", "daemon off;"},
		Env:          map[string]string{"MODE": "test"},
		CPUMillis:    500,
		MemoryBytes:  128 << 20,
		Ports:        []PortMapping{{ContainerPort: 80, HostPort: 8080}},
		Mounts:       []Mount{{Type: MountTmpfs, Target: "/tmp"}},
	}
}

// policyWith returns a policy whose symlink resolution is replaced by links
// (path -> real path), so tests need no files.
func policyWith(t *testing.T, cfg PolicyConfig, links map[string]string) *Policy {
	t.Helper()
	p, err := NewPolicy(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p.evalSymlinks = func(path string) (string, error) {
		if real, ok := links[path]; ok {
			return real, nil
		}
		return path, nil
	}
	return p
}

func wantRejected(t *testing.T, err error, field string) {
	t.Helper()
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("error = %v, want one wrapping ErrRejected", err)
	}
	if !strings.Contains(err.Error(), field+":") {
		t.Fatalf("error %q should name field %s", err, field)
	}
}

func TestDefaultPolicyAcceptsAnOrdinarySpec(t *testing.T) {
	if err := DefaultPolicy().Validate(validSpec()); err != nil {
		t.Fatalf("a plain spec must be accepted: %v", err)
	}
}

func TestDefaultPolicyRejectsDangerousSettings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ContainerSpec)
		field  string
	}{
		{"privileged mode", func(s *ContainerSpec) { s.Privileged = true }, "Privileged"},
		{"added capability", func(s *ContainerSpec) { s.CapAdd = []string{"NET_BIND_SERVICE"} }, "CapAdd"},
		{"sys_admin", func(s *ContainerSpec) { s.CapAdd = []string{"SYS_ADMIN"} }, "CapAdd"},
		{"cap_all with prefix", func(s *ContainerSpec) { s.CapAdd = []string{"cap_all"} }, "CapAdd"},
		{"bogus capability name", func(s *ContainerSpec) { s.CapAdd = []string{"net bind"} }, "CapAdd"},
		{"host devices", func(s *ContainerSpec) { s.Devices = []string{"/dev/mem"} }, "Devices"},
		{"host pid namespace", func(s *ContainerSpec) { s.PidMode = "host" }, "PidMode"},
		{"host ipc namespace", func(s *ContainerSpec) { s.IpcMode = "host" }, "IpcMode"},
		{"host uts namespace", func(s *ContainerSpec) { s.UTSMode = "host" }, "UTSMode"},
		{"host user namespace", func(s *ContainerSpec) { s.UsernsMode = "host" }, "UsernsMode"},
		{"seccomp unconfined", func(s *ContainerSpec) { s.SecurityOpt = []string{"seccomp=unconfined"} }, "SecurityOpt"},
		{"host network", func(s *ContainerSpec) { s.NetworkMode = "host" }, "NetworkMode"},
		{"container network", func(s *ContainerSpec) { s.NetworkMode = "container:other" }, "NetworkMode"},
		{"public port", func(s *ContainerSpec) { s.Ports[0].HostIP = "0.0.0.0" }, "Ports"},
		{"remote host address", func(s *ContainerSpec) { s.Ports[0].HostIP = "10.1.2.3" }, "Ports"},
		{"host address not an IP", func(s *ContainerSpec) { s.Ports[0].HostIP = "localhost" }, "Ports"},
		{"port zero", func(s *ContainerSpec) { s.Ports[0].ContainerPort = 0 }, "Ports"},
		{"protocol", func(s *ContainerSpec) { s.Ports[0].Protocol = "sctp" }, "Ports"},
		{"too many ports", func(s *ContainerSpec) { s.Ports = make([]PortMapping, maxPorts+1) }, "Ports"},
		{"no cpu limit", func(s *ContainerSpec) { s.CPUMillis = 0 }, "CPUMillis"},
		{"no memory limit", func(s *ContainerSpec) { s.MemoryBytes = 0 }, "MemoryBytes"},
		{"tiny memory", func(s *ContainerSpec) { s.MemoryBytes = 1 << 20 }, "MemoryBytes"},
		{"negative pids", func(s *ContainerSpec) { s.PidsLimit = -1 }, "PidsLimit"},
		{"huge pids", func(s *ContainerSpec) { s.PidsLimit = maxPidsLimit + 1 }, "PidsLimit"},
		{"empty workload id", func(s *ContainerSpec) { s.WorkloadID = "" }, "WorkloadID"},
		{"workload id with slash", func(s *ContainerSpec) { s.WorkloadID = "a/b" }, "WorkloadID"},
		{"bad workload name", func(s *ContainerSpec) { s.WorkloadName = "a b" }, "WorkloadName"},
		{"empty image", func(s *ContainerSpec) { s.Image = "" }, "Image"},
		{"image with space", func(s *ContainerSpec) { s.Image = "nginx latest" }, "Image"},
		{"image looking like a flag", func(s *ContainerSpec) { s.Image = "--privileged" }, "Image"},
		{"bad env name", func(s *ContainerSpec) { s.Env = map[string]string{"A=B": "x"} }, "Env"},
		{"env value with NUL", func(s *ContainerSpec) { s.Env = map[string]string{"A": "x\x00y"} }, "Env"},
		{"too much command", func(s *ContainerSpec) { s.Command = make([]string, maxCommandArgs+1) }, "Command"},
		{"bad user", func(s *ContainerSpec) { s.User = "root; id" }, "User"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := validSpec()
			tt.mutate(&spec)
			wantRejected(t, DefaultPolicy().Validate(spec), tt.field)
		})
	}
}

func validHealth() *HealthCheck {
	return &HealthCheck{Command: []string{"curl", "-f", "http://localhost/"}, Interval: 10 * time.Second, Timeout: 2 * time.Second, StartPeriod: 5 * time.Second, Retries: 3}
}

func TestHealthChecksAreBounded(t *testing.T) {
	spec := validSpec()
	spec.HealthCheck = validHealth()
	if err := DefaultPolicy().Validate(spec); err != nil {
		t.Fatalf("an ordinary health check must be accepted: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*HealthCheck)
	}{
		{"no command", func(h *HealthCheck) { h.Command = nil }},
		{"too many arguments", func(h *HealthCheck) { h.Command = make([]string, maxHealthArgs+1) }},
		{"an argument that is too long", func(h *HealthCheck) { h.Command = []string{strings.Repeat("a", maxHealthArgBytes+1)} }},
		{"an argument with NUL", func(h *HealthCheck) { h.Command = []string{"a\x00b"} }},
		{"a shell string", func(h *HealthCheck) { h.Command = []string{"CMD-SHELL", "curl -f localhost || exit 1"} }},
		{"docker's own CMD prefix", func(h *HealthCheck) { h.Command = []string{"CMD", "true"} }},
		{"disabling the check", func(h *HealthCheck) { h.Command = []string{"NONE"} }},
		{"no interval", func(h *HealthCheck) { h.Interval = 0 }},
		{"an interval beyond an hour", func(h *HealthCheck) { h.Interval = 2 * time.Hour }},
		{"no timeout", func(h *HealthCheck) { h.Timeout = 0 }},
		{"a timeout beyond five minutes", func(h *HealthCheck) { h.Timeout = 10 * time.Minute }},
		{"a negative start period", func(h *HealthCheck) { h.StartPeriod = -time.Second }},
		{"a start period beyond an hour", func(h *HealthCheck) { h.StartPeriod = 2 * time.Hour }},
		{"no retries", func(h *HealthCheck) { h.Retries = 0 }},
		{"too many retries", func(h *HealthCheck) { h.Retries = maxHealthRetries + 1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := validSpec()
			spec.HealthCheck = validHealth()
			tt.mutate(spec.HealthCheck)
			wantRejected(t, DefaultPolicy().Validate(spec), "HealthCheck")
		})
	}
}

func TestPolicyReportsEveryViolation(t *testing.T) {
	spec := validSpec()
	spec.Privileged = true
	spec.NetworkMode = "host"
	spec.CPUMillis = 0
	err := DefaultPolicy().Validate(spec)
	for _, field := range []string{"Privileged", "NetworkMode", "CPUMillis"} {
		wantRejected(t, err, field)
	}
}

func TestBindMountsAreDeniedByDefault(t *testing.T) {
	spec := validSpec()
	spec.Mounts = []Mount{{Type: MountBind, Source: "/srv/data", Target: "/data"}}
	err := DefaultPolicy().Validate(spec)
	wantRejected(t, err, "Mounts")
	if !strings.Contains(err.Error(), "not allowed by this agent's policy") {
		t.Errorf("unexpected reason: %v", err)
	}
}

func TestDockerSocketAndHostSystemPathsCanNeverBeMounted(t *testing.T) {
	// Even a policy that allows binds under /, /var, /run ... is refused at construction.
	for _, root := range []string{"/", "/var", "/var/run", "/run", "/etc", "/proc", "/var/lib", "/var/lib/docker", "/dev"} {
		if _, err := NewPolicy(PolicyConfig{AllowedBindRoots: []string{root}}); err == nil {
			t.Errorf("bind root %q must be refused", root)
		}
	}

	policy := policyWith(t, PolicyConfig{AllowedBindRoots: []string{"/srv", "/tmp/dockfleet"}}, map[string]string{
		"/srv/sneaky":            "/var/run/docker.sock",
		"/srv/to-run":            "/run",
		"/srv/link-to-etc":       "/etc/shadow",
		"/srv/outside":           "/home/user/secret",
		"/srv/docker-sock-alias": "/srv/data/docker.sock",
	})
	sources := map[string]string{
		"the docker socket":                 "/var/run/docker.sock",
		"the socket under /run":             "/run/docker.sock",
		"containerd socket":                 "/run/containerd/containerd.sock",
		"any socket":                        "/srv/data/app.sock",
		"a directory containing the socket": "/var/run",
		"the filesystem root":               "/",
		"etc":                               "/etc",
		"a file in etc":                     "/etc/passwd",
		"proc":                              "/proc/1/root",
		"docker's data root":                "/var/lib/docker/volumes",
		"a symlink to the socket":           "/srv/sneaky",
		"a symlink to /run":                 "/srv/to-run",
		"a symlink into etc":                "/srv/link-to-etc",
		"a symlink leaving the root":        "/srv/outside",
		"a symlink to a socket in the root": "/srv/docker-sock-alias",
		"a path outside the roots":          "/home/user",
		"a relative path":                   "srv/data",
		"an unclean path":                   "/srv/../var/run/docker.sock",
		"an unclean path inside the root":   "/srv/data/../data",
		"empty":                             "",
	}
	for name, source := range sources {
		t.Run(name, func(t *testing.T) {
			spec := validSpec()
			spec.Mounts = []Mount{{Type: MountBind, Source: source, Target: "/data"}}
			wantRejected(t, policy.Validate(spec), "Mounts")
		})
	}
}

func TestBindMountsInsideAllowedRootsAreAccepted(t *testing.T) {
	policy := policyWith(t, PolicyConfig{AllowedBindRoots: []string{"/srv/dockfleet"}}, nil)
	spec := validSpec()
	spec.Mounts = []Mount{
		{Type: MountBind, Source: "/srv/dockfleet", Target: "/data"},
		{Type: MountBind, Source: "/srv/dockfleet/cache", Target: "/cache", ReadOnly: true},
	}
	if err := policy.Validate(spec); err != nil {
		t.Fatalf("binds inside an allowed root must be accepted: %v", err)
	}
	spec.Mounts = []Mount{{Type: MountBind, Source: "/srv/dockfleet-other", Target: "/data"}}
	wantRejected(t, policy.Validate(spec), "Mounts")
}

func TestSymlinksAreResolvedOnTheRealFilesystem(t *testing.T) {
	root := t.TempDir()
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(resolvedRoot, "escape")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := os.Mkdir(filepath.Join(resolvedRoot, "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	policy, err := NewPolicy(PolicyConfig{AllowedBindRoots: []string{resolvedRoot}})
	if err != nil {
		t.Fatal(err)
	}

	spec := validSpec()
	spec.Mounts = []Mount{{Type: MountBind, Source: filepath.Join(resolvedRoot, "data"), Target: "/data"}}
	if err := policy.Validate(spec); err != nil {
		t.Fatalf("a real directory inside the root must be accepted: %v", err)
	}
	spec.Mounts = []Mount{{Type: MountBind, Source: filepath.Join(resolvedRoot, "escape"), Target: "/data"}}
	wantRejected(t, policy.Validate(spec), "Mounts")
}

func TestMountTargetsAndTypes(t *testing.T) {
	policy := policyWith(t, PolicyConfig{AllowedBindRoots: []string{"/srv"}}, nil)
	bad := map[string]Mount{
		"relative target":        {Type: MountTmpfs, Target: "tmp"},
		"root target":            {Type: MountTmpfs, Target: "/"},
		"unclean target":         {Type: MountTmpfs, Target: "/tmp/../etc"},
		"socket target":          {Type: MountTmpfs, Target: "/var/run/docker.sock"},
		"unknown type":           {Type: "npipe", Target: "/x"},
		"tmpfs with source":      {Type: MountTmpfs, Source: "/srv", Target: "/x"},
		"negative tmpfs size":    {Type: MountTmpfs, Target: "/x", SizeBytes: -1},
		"volume not namespaced":  {Type: MountVolume, Source: "other-volume", Target: "/x"},
		"volume path":            {Type: MountVolume, Source: "/var/lib/docker", Target: "/x"},
		"empty volume":           {Type: MountVolume, Target: "/x"},
		"volume with traversal":  {Type: MountVolume, Source: "dockfleet-../x", Target: "/x"},
		"empty bind source":      {Type: MountBind, Target: "/x"},
		"bind source is a flag":  {Type: MountBind, Source: "-v", Target: "/x"},
		"volume target is a dot": {Type: MountVolume, Source: "dockfleet-data", Target: "."},
	}
	for name, m := range bad {
		t.Run(name, func(t *testing.T) {
			spec := validSpec()
			spec.Mounts = []Mount{m}
			wantRejected(t, policy.Validate(spec), "Mounts")
		})
	}

	spec := validSpec()
	spec.Mounts = []Mount{
		{Type: MountVolume, Source: "dockfleet-data", Target: "/var/data"},
		{Type: MountTmpfs, Target: "/run/app", SizeBytes: 1 << 20},
		{Type: MountBind, Source: "/srv/app", Target: "/app", ReadOnly: true},
	}
	if err := policy.Validate(spec); err != nil {
		t.Fatalf("valid mounts rejected: %v", err)
	}
	spec.Mounts = make([]Mount, maxMounts+1)
	for i := range spec.Mounts {
		spec.Mounts[i] = Mount{Type: MountTmpfs, Target: fmt.Sprintf("/t%d", i)}
	}
	wantRejected(t, policy.Validate(spec), "Mounts")
}

func TestAllowedCapabilities(t *testing.T) {
	policy, err := NewPolicy(PolicyConfig{AllowedCapabilities: []string{"net_bind_service", "CAP_CHOWN"}})
	if err != nil {
		t.Fatal(err)
	}
	spec := validSpec()
	spec.CapAdd = []string{"NET_BIND_SERVICE", "cap_chown"}
	if err := policy.Validate(spec); err != nil {
		t.Fatalf("allowed capabilities rejected: %v", err)
	}
	spec.CapAdd = []string{"NET_BIND_SERVICE", "SETUID"}
	wantRejected(t, policy.Validate(spec), "CapAdd")

	for _, c := range []string{"SYS_ADMIN", "ALL", "cap_net_admin", "SYS_PTRACE", "SYS_MODULE", "DAC_READ_SEARCH", "MKNOD", "bad name"} {
		if _, err := NewPolicy(PolicyConfig{AllowedCapabilities: []string{c}}); err == nil {
			t.Errorf("capability %q must be refused even when the operator asks for it", c)
		}
	}
}

func TestPublicPortsNeedExplicitPermission(t *testing.T) {
	spec := validSpec()
	spec.Ports = []PortMapping{{ContainerPort: 80, HostPort: 8080, HostIP: "0.0.0.0"}}
	wantRejected(t, DefaultPolicy().Validate(spec), "Ports")

	policy, err := NewPolicy(PolicyConfig{AllowPublicPorts: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.Validate(spec); err != nil {
		t.Fatalf("public ports must be accepted once allowed: %v", err)
	}
	for _, ip := range []string{"127.0.0.1", "::1", ""} {
		spec.Ports[0].HostIP = ip
		if err := DefaultPolicy().Validate(spec); err != nil {
			t.Errorf("loopback address %q must be accepted: %v", ip, err)
		}
	}
}

func TestNewPolicyRejectsBadRoots(t *testing.T) {
	for _, root := range []string{"", "relative/dir", "/srv/../etc", "/srv/", "/srv//data"} {
		if _, err := NewPolicy(PolicyConfig{AllowedBindRoots: []string{root}}); err == nil {
			t.Errorf("bind root %q must be refused", root)
		}
	}
}
