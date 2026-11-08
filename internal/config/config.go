package config

import (
	"errors"
	"fmt"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/dockfleet/dockfleet/internal/netutil"
	"github.com/dockfleet/dockfleet/internal/store"
)

const (
	minTokenLength = 16
	maxLabels      = 32
)

// Getenv looks up an environment variable; os.Getenv satisfies it.
type Getenv func(string) string

// Manager is the manager process configuration. Secrets (tokens, database
// URL) must never be logged.
type Manager struct {
	LogLevel          string
	LogFormat         string
	APIAddr           string
	GRPCAddr          string
	APIToken          string
	AgentToken        string
	Store             string
	DatabaseURL       string
	TLSCertFile       string
	TLSKeyFile        string
	TLSClientCAFile   string
	HeartbeatInterval time.Duration
	NodeTTL           time.Duration
	RequestTimeout    time.Duration
	ShutdownTimeout   time.Duration
}

// TLSEnabled reports whether the manager serves TLS. TLS always means mutual
// TLS on the gRPC listener; there is no mode without client certificates.
func (m Manager) TLSEnabled() bool {
	return m.TLSCertFile != ""
}

// Agent is the agent process configuration. Token must never be logged.
type Agent struct {
	LogLevel      string
	LogFormat     string
	ManagerAddr   string
	Token         string
	TLSCAFile     string
	TLSCertFile   string
	TLSKeyFile    string
	TLSServerName string
	NodeID        string
	NodeName      string
	CPUMillis     int64
	MemoryBytes   int64
	Labels        map[string]string
	DockerHost    string
	RPCTimeout    time.Duration

	// Container policy relaxations; empty means the strictest policy. The
	// docker package validates them (they never allow the always-denied set).
	AllowedBindRoots    []string
	AllowedCapabilities []string
}

// TLSEnabled reports whether the agent connects with mutual TLS. Without it
// the agent only talks to a loopback manager, in plaintext, with Token.
func (a Agent) TLSEnabled() bool {
	return a.TLSCAFile != ""
}

// LoadManager reads and validates manager configuration. Defaults are secure:
// loopback listeners, a mandatory admin token, and mutual TLS required for any
// other bind address. Without TLS the agent token authenticates agents, on
// loopback only; with TLS the client certificate does and the agent token is
// ignored.
func LoadManager(get Getenv) (Manager, error) {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	var c Manager
	c.LogLevel = env(get, "DOCKFLEET_LOG_LEVEL", "info")
	c.LogFormat = env(get, "DOCKFLEET_LOG_FORMAT", "json")
	c.APIAddr = env(get, "DOCKFLEET_API_ADDR", "127.0.0.1:21280")
	c.GRPCAddr = env(get, "DOCKFLEET_GRPC_ADDR", "127.0.0.1:21281")
	c.APIToken = env(get, "DOCKFLEET_API_TOKEN", "")
	c.AgentToken = env(get, "DOCKFLEET_AGENT_TOKEN", "")
	c.Store = env(get, "DOCKFLEET_STORE", "postgres")
	c.DatabaseURL = env(get, "DOCKFLEET_DATABASE_URL", "")
	c.TLSCertFile = env(get, "DOCKFLEET_TLS_CERT_FILE", "")
	c.TLSKeyFile = env(get, "DOCKFLEET_TLS_KEY_FILE", "")
	c.TLSClientCAFile = env(get, "DOCKFLEET_TLS_CLIENT_CA_FILE", "")
	c.HeartbeatInterval = duration(get, "DOCKFLEET_HEARTBEAT_INTERVAL", 10*time.Second, &errs)
	c.NodeTTL = duration(get, "DOCKFLEET_NODE_TTL", 30*time.Second, &errs)
	c.RequestTimeout = duration(get, "DOCKFLEET_REQUEST_TIMEOUT", 10*time.Second, &errs)
	c.ShutdownTimeout = duration(get, "DOCKFLEET_SHUTDOWN_TIMEOUT", 15*time.Second, &errs)

	switch c.Store {
	case "postgres":
		if c.DatabaseURL == "" {
			fail("DOCKFLEET_DATABASE_URL is required when DOCKFLEET_STORE=postgres")
		}
	case "memory":
	default:
		fail("DOCKFLEET_STORE must be postgres or memory, got %q", c.Store)
	}

	anyTLS := c.TLSCertFile != "" || c.TLSKeyFile != "" || c.TLSClientCAFile != ""
	if anyTLS && (c.TLSCertFile == "" || c.TLSKeyFile == "" || c.TLSClientCAFile == "") {
		fail("DOCKFLEET_TLS_CERT_FILE, DOCKFLEET_TLS_KEY_FILE and DOCKFLEET_TLS_CLIENT_CA_FILE must be set together: TLS means mutual TLS, there is no mode without client certificates")
	}

	if len(c.APIToken) < minTokenLength {
		fail("DOCKFLEET_API_TOKEN must be set to at least %d characters", minTokenLength)
	}
	if !anyTLS {
		if len(c.AgentToken) < minTokenLength {
			fail("DOCKFLEET_AGENT_TOKEN must be set to at least %d characters when TLS is not configured", minTokenLength)
		}
		if c.APIToken != "" && c.APIToken == c.AgentToken {
			fail("DOCKFLEET_API_TOKEN and DOCKFLEET_AGENT_TOKEN must differ")
		}
	}

	if !anyTLS {
		for _, a := range []struct{ name, addr string }{
			{"DOCKFLEET_API_ADDR", c.APIAddr},
			{"DOCKFLEET_GRPC_ADDR", c.GRPCAddr},
		} {
			if !netutil.IsLoopbackAddr(a.addr) {
				fail("%s=%q is not a loopback address; configure mutual TLS (DOCKFLEET_TLS_CERT_FILE, DOCKFLEET_TLS_KEY_FILE, DOCKFLEET_TLS_CLIENT_CA_FILE) or bind to loopback", a.name, a.addr)
			}
		}
	}

	if c.NodeTTL < 2*c.HeartbeatInterval {
		fail("DOCKFLEET_NODE_TTL (%s) must be at least twice DOCKFLEET_HEARTBEAT_INTERVAL (%s)", c.NodeTTL, c.HeartbeatInterval)
	}

	if err := errors.Join(errs...); err != nil {
		return Manager{}, err
	}
	return c, nil
}

// LoadAgent reads and validates agent configuration.
func LoadAgent(get Getenv) (Agent, error) {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	var c Agent
	c.LogLevel = env(get, "DOCKFLEET_LOG_LEVEL", "info")
	c.LogFormat = env(get, "DOCKFLEET_LOG_FORMAT", "json")
	c.ManagerAddr = env(get, "DOCKFLEET_MANAGER_ADDR", "127.0.0.1:21281")
	c.Token = env(get, "DOCKFLEET_AGENT_TOKEN", "")
	c.TLSCAFile = env(get, "DOCKFLEET_TLS_CA_FILE", "")
	c.TLSCertFile = env(get, "DOCKFLEET_TLS_CERT_FILE", "")
	c.TLSKeyFile = env(get, "DOCKFLEET_TLS_KEY_FILE", "")
	c.TLSServerName = env(get, "DOCKFLEET_TLS_SERVER_NAME", "")
	c.DockerHost = env(get, "DOCKFLEET_DOCKER_HOST", "unix:///var/run/docker.sock")
	c.RPCTimeout = duration(get, "DOCKFLEET_RPC_TIMEOUT", 5*time.Second, &errs)
	c.CPUMillis = positiveInt(get, "DOCKFLEET_NODE_CPU_MILLIS", int64(runtime.NumCPU())*1000, &errs)
	// Declared capacity, not measured: host memory detection is future work.
	c.MemoryBytes = positiveInt(get, "DOCKFLEET_NODE_MEMORY_MB", 1024, &errs) << 20

	c.NodeID = env(get, "DOCKFLEET_NODE_ID", "")
	if c.NodeID == "" {
		host, err := os.Hostname()
		if err != nil {
			fail("DOCKFLEET_NODE_ID is unset and the hostname is unavailable: %v", err)
		}
		c.NodeID = host
	}
	if !store.ValidName(c.NodeID) {
		fail("node ID %q is invalid; set DOCKFLEET_NODE_ID to 1-63 characters of [A-Za-z0-9._-] starting with a letter or digit", c.NodeID)
	}
	c.NodeName = env(get, "DOCKFLEET_NODE_NAME", c.NodeID)

	labels, err := parseLabels(get("DOCKFLEET_NODE_LABELS"))
	if err != nil {
		fail("DOCKFLEET_NODE_LABELS: %v", err)
	}
	c.Labels = labels

	c.AllowedBindRoots = splitList(get("DOCKFLEET_ALLOWED_BIND_ROOTS"))
	for _, root := range c.AllowedBindRoots {
		if !path.IsAbs(root) {
			fail("DOCKFLEET_ALLOWED_BIND_ROOTS entry %q must be an absolute path", root)
		}
	}
	c.AllowedCapabilities = splitList(get("DOCKFLEET_ALLOWED_CAPABILITIES"))

	anyTLS := c.TLSCAFile != "" || c.TLSCertFile != "" || c.TLSKeyFile != ""
	if anyTLS && (c.TLSCAFile == "" || c.TLSCertFile == "" || c.TLSKeyFile == "") {
		fail("DOCKFLEET_TLS_CA_FILE, DOCKFLEET_TLS_CERT_FILE and DOCKFLEET_TLS_KEY_FILE must be set together: the manager requires mutual TLS")
	}
	if !anyTLS {
		if len(c.Token) < minTokenLength {
			fail("DOCKFLEET_AGENT_TOKEN must be set to at least %d characters when TLS is not configured", minTokenLength)
		}
		if !netutil.IsLoopbackAddr(c.ManagerAddr) {
			fail("DOCKFLEET_MANAGER_ADDR=%q is not a loopback address; configure mutual TLS (DOCKFLEET_TLS_CA_FILE, DOCKFLEET_TLS_CERT_FILE, DOCKFLEET_TLS_KEY_FILE): plaintext is never used for remote managers", c.ManagerAddr)
		}
	}

	if err := errors.Join(errs...); err != nil {
		return Agent{}, err
	}
	return c, nil
}

func env(get Getenv, key, def string) string {
	if v := strings.TrimSpace(get(key)); v != "" {
		return v
	}
	return def
}

func duration(get Getenv, key string, def time.Duration, errs *[]error) time.Duration {
	raw := strings.TrimSpace(get(key))
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		*errs = append(*errs, fmt.Errorf("%s must be a positive duration such as 10s, got %q", key, raw))
		return def
	}
	return d
}

func positiveInt(get Getenv, key string, def int64, errs *[]error) int64 {
	raw := strings.TrimSpace(get(key))
	if raw == "" {
		return def
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v <= 0 || v > 1<<40 {
		*errs = append(*errs, fmt.Errorf("%s must be a positive integer, got %q", key, raw))
		return def
	}
	return v
}

// splitList splits a comma-separated list, dropping blanks.
func splitList(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func parseLabels(raw string) (map[string]string, error) {
	labels := map[string]string{}
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		k, v, ok := strings.Cut(pair, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			return nil, fmt.Errorf("invalid label %q, want key=value", pair)
		}
		labels[k] = strings.TrimSpace(v)
	}
	if len(labels) > maxLabels {
		return nil, fmt.Errorf("at most %d labels are allowed", maxLabels)
	}
	return labels, nil
}
