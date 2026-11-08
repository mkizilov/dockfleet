package docker

import (
	"errors"
	"fmt"
	"net"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// ErrRejected is wrapped by every error that says a ContainerSpec violates the
// policy. Nothing reaches Docker for a rejected spec.
var ErrRejected = errors.New("container spec rejected by policy")

// PolicyError explains one violation.
type PolicyError struct {
	Field  string
	Reason string
}

func (e *PolicyError) Error() string { return e.Field + ": " + e.Reason }

func (e *PolicyError) Is(target error) bool { return target == ErrRejected }

var (
	idPattern         = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$`)
	envKeyPattern     = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	volumePattern     = regexp.MustCompile(`^dockfleet-[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`)
	userPattern       = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,63}(:[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,63})?$`)
	capabilityPattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)
)

const (
	maxCommandArgs = 128
	maxArgBytes    = 4096
	maxEnvEntries  = 128
	maxEnvValue    = 32 << 10
	maxPorts       = 32
	maxMounts      = 16
	maxPidsLimit   = 32768
	minMemoryBytes = 6 << 20
	maxCPUMillis   = 1_000_000
	maxImageLength = 255

	maxHealthArgs        = 32
	maxHealthArgBytes    = 1024
	maxHealthRetries     = 10
	maxHealthInterval    = time.Hour
	maxHealthTimeout     = 5 * time.Minute
	maxHealthStartPeriod = time.Hour
)

// deniedCapabilities can never be granted, whatever the policy allows. They
// either give control over the host kernel or defeat container isolation.
var deniedCapabilities = map[string]bool{
	"ALL": true, "SYS_ADMIN": true, "SYS_PTRACE": true, "SYS_MODULE": true,
	"SYS_RAWIO": true, "SYS_BOOT": true, "SYS_TIME": true, "SYS_RESOURCE": true,
	"DAC_READ_SEARCH": true, "NET_ADMIN": true, "NET_RAW": true, "MKNOD": true,
	"SETFCAP": true, "MAC_ADMIN": true, "MAC_OVERRIDE": true, "AUDIT_CONTROL": true,
	"BPF": true, "PERFMON": true, "SYSLOG": true, "LINUX_IMMUTABLE": true,
	"CHECKPOINT_RESTORE": true, "WAKE_ALARM": true, "BLOCK_SUSPEND": true,
}

// sensitiveHostPaths must never be bind-mounted, nor may any directory that
// contains one (mounting /var/run exposes /var/run/docker.sock). Together with
// the .sock rule this keeps the Docker socket and the host's configuration and
// runtime state out of containers.
var sensitiveHostPaths = []string{
	"/", "/etc", "/proc", "/sys", "/dev", "/boot", "/root",
	"/run", "/var/run", "/var/lib/docker", "/var/lib/containerd", "/var/lib/kubelet",
}

// PolicyConfig relaxes the default policy. The zero value is the strictest.
type PolicyConfig struct {
	// AllowedBindRoots lists absolute host directories that bind mounts may
	// live under. Empty means bind mounts are not allowed at all.
	AllowedBindRoots []string
	// AllowedCapabilities lists capabilities containers may add. Capabilities
	// in the always-denied set are refused here too.
	AllowedCapabilities []string
	// AllowPublicPorts lets containers publish ports on non-loopback addresses.
	AllowPublicPorts bool
}

// Policy decides which container specs the adapter accepts. Create it with
// NewPolicy or DefaultPolicy.
type Policy struct {
	bindRoots        []string
	capabilities     map[string]bool
	allowPublicPorts bool
	evalSymlinks     func(string) (string, error)
}

// DefaultPolicy is the strictest policy: no privileged mode, no added
// capabilities, no devices, no host namespaces, no bind mounts, no non-loopback
// published ports.
func DefaultPolicy() *Policy {
	return &Policy{capabilities: map[string]bool{}, evalSymlinks: filepath.EvalSymlinks}
}

// NewPolicy validates cfg and builds a policy. It refuses to allow anything
// that is dangerous regardless of configuration.
func NewPolicy(cfg PolicyConfig) (*Policy, error) {
	p := DefaultPolicy()
	p.allowPublicPorts = cfg.AllowPublicPorts
	var errs []error
	for _, root := range cfg.AllowedBindRoots {
		switch {
		case !path.IsAbs(root) || path.Clean(root) != root:
			errs = append(errs, fmt.Errorf("bind root %q must be an absolute, clean path", root))
		case sensitivePath(root):
			errs = append(errs, fmt.Errorf("bind root %q is, contains or sits inside a host path that must never be mounted", root))
		default:
			p.bindRoots = append(p.bindRoots, root)
		}
	}
	for _, c := range cfg.AllowedCapabilities {
		name := normalizeCapability(c)
		switch {
		case !capabilityPattern.MatchString(name):
			errs = append(errs, fmt.Errorf("capability %q is not a valid name", c))
		case deniedCapabilities[name]:
			errs = append(errs, fmt.Errorf("capability %s can never be allowed", name))
		default:
			p.capabilities[name] = true
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	sort.Strings(p.bindRoots)
	return p, nil
}

func normalizeCapability(c string) string {
	return strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(c)), "CAP_")
}

func isSocketName(p string) bool {
	base := path.Base(p)
	return strings.HasSuffix(base, ".sock") || strings.Contains(base, "docker.sock")
}

// sensitivePath reports whether mounting p would expose a sensitive host
// location: p is one, lies inside one, contains one, or names a socket.
func sensitivePath(p string) bool {
	if isSocketName(p) {
		return true
	}
	for _, s := range sensitiveHostPaths {
		if p == s || strings.HasPrefix(p, s+"/") {
			return true
		}
		if strings.HasPrefix(s, strings.TrimSuffix(p, "/")+"/") {
			return true
		}
	}
	return false
}

func within(p, root string) bool {
	return p == root || strings.HasPrefix(p, root+"/")
}

// Validate returns nil when spec is acceptable, otherwise an error wrapping
// ErrRejected that lists every violation.
func (p *Policy) Validate(spec ContainerSpec) error {
	var errs []error
	reject := func(field, format string, args ...any) {
		errs = append(errs, &PolicyError{Field: field, Reason: fmt.Sprintf(format, args...)})
	}

	if !idPattern.MatchString(spec.WorkloadID) {
		reject("WorkloadID", "must be 1-63 characters of [A-Za-z0-9._-] starting with a letter or digit")
	}
	if spec.WorkloadName != "" && !idPattern.MatchString(spec.WorkloadName) {
		reject("WorkloadName", "must be 1-63 characters of [A-Za-z0-9._-] starting with a letter or digit")
	}
	if spec.Image == "" || len(spec.Image) > maxImageLength || strings.HasPrefix(spec.Image, "-") || strings.ContainsAny(spec.Image, " \t\r\n\x00") {
		reject("Image", "must be 1-%d characters without whitespace", maxImageLength)
	}
	if len(spec.Command) > maxCommandArgs {
		reject("Command", "at most %d arguments", maxCommandArgs)
	}
	for _, arg := range spec.Command {
		if len(arg) > maxArgBytes || strings.Contains(arg, "\x00") {
			reject("Command", "arguments must be at most %d bytes and contain no NUL", maxArgBytes)
			break
		}
	}
	p.checkEnv(spec.Env, reject)
	if spec.User != "" && !userPattern.MatchString(spec.User) {
		reject("User", "must be a user name or numeric ID, optionally followed by :group")
	}

	if spec.CPUMillis < 1 || spec.CPUMillis > maxCPUMillis {
		reject("CPUMillis", "must be between 1 and %d: containers always run with a CPU limit", maxCPUMillis)
	}
	if spec.MemoryBytes < minMemoryBytes {
		reject("MemoryBytes", "must be at least %d bytes: containers always run with a memory limit", minMemoryBytes)
	}
	if spec.PidsLimit < 0 || spec.PidsLimit > maxPidsLimit {
		reject("PidsLimit", "must be between 0 (default %d) and %d", defaultPidsLimit, maxPidsLimit)
	}

	if spec.Privileged {
		reject("Privileged", "privileged mode is never allowed")
	}
	p.checkCapabilities(spec.CapAdd, reject)
	if len(spec.Devices) > 0 {
		reject("Devices", "host devices cannot be passed to containers")
	}
	for field, mode := range map[string]string{"PidMode": spec.PidMode, "IpcMode": spec.IpcMode, "UTSMode": spec.UTSMode, "UsernsMode": spec.UsernsMode} {
		if mode != "" {
			reject(field, "sharing a namespace with the host or another container (%q) is not allowed", mode)
		}
	}
	if len(spec.SecurityOpt) > 0 {
		reject("SecurityOpt", "security options cannot be overridden: containers always run with the daemon's seccomp/AppArmor profiles and no-new-privileges")
	}
	switch spec.NetworkMode {
	case "", "bridge", "none":
	default:
		reject("NetworkMode", "%q is not allowed; use bridge or none (host and container networks expose the host)", spec.NetworkMode)
	}

	p.checkPorts(spec.Ports, reject)
	p.checkMounts(spec.Mounts, reject)
	checkHealth(spec.HealthCheck, reject)
	return errors.Join(errs...)
}

// checkHealth bounds a health check. Its command runs inside the container with
// the container's own privileges, so it adds no access, but an unbounded one
// could keep the daemon busy.
func checkHealth(h *HealthCheck, reject rejectFunc) {
	if h == nil {
		return
	}
	switch {
	case len(h.Command) == 0 || len(h.Command) > maxHealthArgs:
		reject("HealthCheck", "the command needs 1-%d arguments", maxHealthArgs)
	case h.Command[0] == "CMD" || h.Command[0] == "CMD-SHELL" || h.Command[0] == "NONE":
		reject("HealthCheck", "the command must start with the program to run, not %q: only the exec form is supported", h.Command[0])
	}
	for _, arg := range h.Command {
		if len(arg) > maxHealthArgBytes || strings.Contains(arg, "\x00") {
			reject("HealthCheck", "arguments must be at most %d bytes and contain no NUL", maxHealthArgBytes)
			break
		}
	}
	if h.Interval < time.Second || h.Interval > maxHealthInterval {
		reject("HealthCheck", "the interval must be between 1s and %s", maxHealthInterval)
	}
	if h.Timeout < time.Second || h.Timeout > maxHealthTimeout {
		reject("HealthCheck", "the timeout must be between 1s and %s", maxHealthTimeout)
	}
	if h.StartPeriod < 0 || h.StartPeriod > maxHealthStartPeriod {
		reject("HealthCheck", "the start period must be between 0 and %s", maxHealthStartPeriod)
	}
	if h.Retries < 1 || h.Retries > maxHealthRetries {
		reject("HealthCheck", "retries must be between 1 and %d", maxHealthRetries)
	}
}

type rejectFunc func(field, format string, args ...any)

func (p *Policy) checkEnv(env map[string]string, reject rejectFunc) {
	if len(env) > maxEnvEntries {
		reject("Env", "at most %d variables", maxEnvEntries)
	}
	for k, v := range env {
		if !envKeyPattern.MatchString(k) {
			reject("Env", "variable name %q is invalid", k)
		}
		if len(v) > maxEnvValue || strings.Contains(v, "\x00") {
			reject("Env", "value of %s must be at most %d bytes and contain no NUL", k, maxEnvValue)
		}
	}
}

func (p *Policy) checkCapabilities(caps []string, reject rejectFunc) {
	for _, c := range caps {
		name := normalizeCapability(c)
		switch {
		case !capabilityPattern.MatchString(name):
			reject("CapAdd", "%q is not a valid capability name", c)
		case deniedCapabilities[name]:
			reject("CapAdd", "capability %s is never allowed", name)
		case !p.capabilities[name]:
			reject("CapAdd", "capability %s is not allowed by this agent's policy", name)
		}
	}
}

func (p *Policy) checkPorts(ports []PortMapping, reject rejectFunc) {
	if len(ports) > maxPorts {
		reject("Ports", "at most %d published ports", maxPorts)
	}
	for _, m := range ports {
		if m.ContainerPort == 0 {
			reject("Ports", "container port must be between 1 and 65535")
		}
		switch m.Protocol {
		case "", "tcp", "udp":
		default:
			reject("Ports", "protocol %q must be tcp or udp", m.Protocol)
		}
		ip := net.ParseIP(m.hostIP())
		switch {
		case ip == nil:
			reject("Ports", "host address %q is not an IP address", m.HostIP)
		case !ip.IsLoopback() && !p.allowPublicPorts:
			reject("Ports", "publishing on %s is not allowed; only loopback addresses are (the default is 127.0.0.1)", ip)
		}
	}
}

func (p *Policy) checkMounts(mounts []Mount, reject rejectFunc) {
	if len(mounts) > maxMounts {
		reject("Mounts", "at most %d mounts", maxMounts)
	}
	for _, m := range mounts {
		if !path.IsAbs(m.Target) || path.Clean(m.Target) != m.Target || m.Target == "/" {
			reject("Mounts", "target %q must be an absolute, clean path other than /", m.Target)
		} else if isSocketName(m.Target) {
			reject("Mounts", "target %q looks like a socket path", m.Target)
		}
		switch m.Type {
		case MountBind:
			p.checkBind(m, reject)
		case MountVolume:
			if !volumePattern.MatchString(m.Source) {
				reject("Mounts", "volume %q must be named dockfleet-<name> so unrelated volumes stay untouched", m.Source)
			}
		case MountTmpfs:
			if m.Source != "" {
				reject("Mounts", "a tmpfs mount has no source")
			}
			if m.SizeBytes < 0 {
				reject("Mounts", "tmpfs size must not be negative")
			}
		default:
			reject("Mounts", "type %q must be bind, volume or tmpfs", m.Type)
		}
	}
}

func (p *Policy) checkBind(m Mount, reject rejectFunc) {
	if !path.IsAbs(m.Source) || path.Clean(m.Source) != m.Source {
		reject("Mounts", "bind source %q must be an absolute, clean path", m.Source)
		return
	}
	// Symlinks are the classic way around path checks, so check where the
	// path really leads as well as what it says.
	candidates := []string{m.Source}
	if p.evalSymlinks != nil {
		if resolved, err := p.evalSymlinks(m.Source); err == nil && resolved != m.Source {
			candidates = append(candidates, resolved)
		}
	}
	for _, c := range candidates {
		if sensitivePath(c) {
			reject("Mounts", "bind source %q reaches %q, which must never be mounted (host system paths and Docker/containerd sockets)", m.Source, c)
			return
		}
	}
	if len(p.bindRoots) == 0 {
		reject("Mounts", "bind mounts are not allowed by this agent's policy")
		return
	}
	for _, c := range candidates {
		allowed := false
		for _, root := range p.bindRoots {
			if within(c, root) {
				allowed = true
				break
			}
		}
		if !allowed {
			reject("Mounts", "bind source %q (%q) is outside the allowed roots %v", m.Source, c, p.bindRoots)
			return
		}
	}
}
