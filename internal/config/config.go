package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

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
	HeartbeatInterval time.Duration
	NodeTTL           time.Duration
	ShutdownTimeout   time.Duration
}

// Agent is the agent process configuration. Token must never be logged.
type Agent struct {
	LogLevel    string
	LogFormat   string
	ManagerAddr string
	Token       string
	TLSCAFile   string
	NodeID      string
	NodeName    string
	CPUMillis   int64
	MemoryBytes int64
	Labels      map[string]string
	DockerHost  string
}

// LoadManager reads and validates manager configuration. Defaults are secure:
// loopback listeners, mandatory tokens, and TLS required for any other bind address.
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
	c.HeartbeatInterval = duration(get, "DOCKFLEET_HEARTBEAT_INTERVAL", 10*time.Second, &errs)
	c.NodeTTL = duration(get, "DOCKFLEET_NODE_TTL", 30*time.Second, &errs)
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

	for _, t := range []struct{ name, value string }{
		{"DOCKFLEET_API_TOKEN", c.APIToken},
		{"DOCKFLEET_AGENT_TOKEN", c.AgentToken},
	} {
		if len(t.value) < minTokenLength {
			fail("%s must be set to at least %d characters", t.name, minTokenLength)
		}
	}
	if c.APIToken != "" && c.APIToken == c.AgentToken {
		fail("DOCKFLEET_API_TOKEN and DOCKFLEET_AGENT_TOKEN must differ")
	}

	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		fail("DOCKFLEET_TLS_CERT_FILE and DOCKFLEET_TLS_KEY_FILE must be set together")
	}
	if c.TLSCertFile == "" {
		for _, a := range []struct{ name, addr string }{
			{"DOCKFLEET_API_ADDR", c.APIAddr},
			{"DOCKFLEET_GRPC_ADDR", c.GRPCAddr},
		} {
			if !isLoopbackAddr(a.addr) {
				fail("%s=%q is not a loopback address; configure TLS or bind to loopback", a.name, a.addr)
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
	c.DockerHost = env(get, "DOCKFLEET_DOCKER_HOST", "unix:///var/run/docker.sock")
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

	if len(c.Token) < minTokenLength {
		fail("DOCKFLEET_AGENT_TOKEN must be set to at least %d characters", minTokenLength)
	}
	if c.TLSCAFile == "" && !isLoopbackAddr(c.ManagerAddr) {
		fail("DOCKFLEET_MANAGER_ADDR=%q is not a loopback address; set DOCKFLEET_TLS_CA_FILE", c.ManagerAddr)
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

func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
