package config

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
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
	// InstanceID names this manager process in audit events and leases, so
	// several managers can share one database. It is unique per process
	// unless set.
	InstanceID        string
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

	// SchedulerInterval is the time between scheduling passes; zero (only
	// possible outside this loader) turns the scheduler off.
	SchedulerInterval time.Duration
	// NodeLease is how long an agent's lease lasts after each heartbeat it
	// sends. An agent that fences itself stops its containers when the lease
	// has run out.
	NodeLease time.Duration
	// NodeUnavailableAfter is how long a node may be silent before it is given
	// up on and its replicas are started on other nodes. It is longer than the
	// lease, so an agent that fences itself has stopped by then.
	NodeUnavailableAfter time.Duration
	// ReconcileInterval is the time between passes that compare services with
	// what agents report and stop or remove the workloads that should go; zero
	// (only possible outside this loader) turns the reconciler off.
	ReconcileInterval time.Duration
	// CPUOvercommit and MemoryOvercommit are how many times a node's capacity may
	// be promised to workloads. 1 means never more than the node has.
	CPUOvercommit    float64
	MemoryOvercommit float64

	// Container logs. LogMaxStreams is how many log queries may be open at once,
	// LogWindowBytes the most that is buffered for one query (agents are held
	// back by flow control beyond it), LogReconnectWait how long a following
	// query waits for an agent whose connection dropped, and LogWriteTimeout how
	// long a response waits for a client that takes no data before it is closed.
	LogMaxStreams    int
	LogWindowBytes   int
	LogReconnectWait time.Duration
	LogWriteTimeout  time.Duration

	// MetricsEnabled serves GET /metrics in the Prometheus text format (admin
	// token required) and records request, scheduling and reconciliation metrics.
	// DashboardEnabled serves the administrative dashboard under /dashboard.
	MetricsEnabled   bool
	DashboardEnabled bool

	// High availability. With EtcdEndpoints set, several managers share the
	// PostgreSQL database and elect one leader through a lease in etcd; only the
	// leader schedules and reconciles. Without it the manager is the only one
	// and needs no etcd. ElectionTTL is how long the leader's lease lasts
	// without being renewed, so how long a failover takes at most (plus a third).
	// An https endpoint is reached with mutual TLS: the CA that signed etcd's
	// certificate, and this manager's own certificate and key. Plaintext is only
	// for etcd on loopback.
	EtcdEndpoints []string
	EtcdCAFile    string
	EtcdCertFile  string
	EtcdKeyFile   string
	EtcdPrefix    string
	ElectionTTL   time.Duration
}

// HAEnabled reports whether the manager takes part in a leader election.
func (m Manager) HAEnabled() bool { return len(m.EtcdEndpoints) > 0 }

// EtcdTLSEnabled reports whether any etcd endpoint is reached over TLS.
func (m Manager) EtcdTLSEnabled() bool {
	for _, e := range m.EtcdEndpoints {
		if strings.HasPrefix(e, "https://") {
			return true
		}
	}
	return false
}

// TLSEnabled reports whether the manager serves TLS. TLS always means mutual
// TLS on the gRPC listener; there is no mode without client certificates.
func (m Manager) TLSEnabled() bool {
	return m.TLSCertFile != ""
}

// Agent is the agent process configuration. Token must never be logged.
type Agent struct {
	LogLevel    string
	LogFormat   string
	ManagerAddr string
	// ManagerAddrs are the managers to talk to: DOCKFLEET_MANAGER_ADDRS when a
	// high-availability cluster is configured, otherwise just ManagerAddr. Only
	// the leader of a cluster answers, and the agent finds it.
	ManagerAddrs  []string
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

	// FenceOnLeaseLoss makes the agent stop its containers when the manager's
	// lease on the node runs out (no heartbeat was accepted for that long), so
	// that a replica the manager has started elsewhere is not also running here.
	// Turn it off to keep containers running through a long manager outage, at
	// the price of duplicate replicas after a failover.
	FenceOnLeaseLoss bool

	// HealthAddr is where the agent serves GET /healthz and /readyz, which the
	// image's HEALTHCHECK asks. It is always a loopback address, because the
	// endpoint is for the machine (or container) the agent runs in; "" means off.
	HealthAddr string

	// LogRequests is how many log requests the agent serves at once for the
	// manager; 0 turns serving container logs off.
	LogRequests int

	// Container policy relaxations; empty means the strictest policy. The
	// docker package validates them (they never allow the always-denied set).
	AllowedBindRoots    []string
	AllowedCapabilities []string
}

// DefaultAPIAddr is where the manager serves its API (and its health
// endpoints) when DOCKFLEET_API_ADDR is not set.
const DefaultAPIAddr = "127.0.0.1:21280"

// DefaultAgentHealthAddr is where the agent serves its health endpoint when
// DOCKFLEET_HEALTH_ADDR is not set.
const DefaultAgentHealthAddr = "127.0.0.1:21282"

// AgentHealthAddr is the address of the agent's health endpoint, or "" when
// DOCKFLEET_HEALTH_ADDR is "off". The `healthcheck` subcommand reads it the same
// way the agent does.
func AgentHealthAddr(get Getenv) string {
	switch v := strings.TrimSpace(get("DOCKFLEET_HEALTH_ADDR")); v {
	case "":
		return DefaultAgentHealthAddr
	case "off":
		return ""
	default:
		return v
	}
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
	c.InstanceID = env(get, "DOCKFLEET_INSTANCE_ID", "manager-"+store.NewID()[:8])
	if !store.ValidName(c.InstanceID) {
		fail("DOCKFLEET_INSTANCE_ID %q is invalid; use 1-63 characters of [A-Za-z0-9._-] starting with a letter or digit", c.InstanceID)
	}
	c.LogLevel = env(get, "DOCKFLEET_LOG_LEVEL", "info")
	c.LogFormat = env(get, "DOCKFLEET_LOG_FORMAT", "json")
	c.APIAddr = env(get, "DOCKFLEET_API_ADDR", DefaultAPIAddr)
	c.GRPCAddr = env(get, "DOCKFLEET_GRPC_ADDR", "127.0.0.1:21281")
	c.APIToken = secret(get, "DOCKFLEET_API_TOKEN", &errs)
	c.AgentToken = secret(get, "DOCKFLEET_AGENT_TOKEN", &errs)
	c.Store = env(get, "DOCKFLEET_STORE", "postgres")
	c.DatabaseURL = secret(get, "DOCKFLEET_DATABASE_URL", &errs)
	c.TLSCertFile = env(get, "DOCKFLEET_TLS_CERT_FILE", "")
	c.TLSKeyFile = env(get, "DOCKFLEET_TLS_KEY_FILE", "")
	c.TLSClientCAFile = env(get, "DOCKFLEET_TLS_CLIENT_CA_FILE", "")
	c.HeartbeatInterval = duration(get, "DOCKFLEET_HEARTBEAT_INTERVAL", 10*time.Second, &errs)
	c.NodeTTL = duration(get, "DOCKFLEET_NODE_TTL", 30*time.Second, &errs)
	c.NodeLease = duration(get, "DOCKFLEET_NODE_LEASE", time.Minute, &errs)
	c.NodeUnavailableAfter = duration(get, "DOCKFLEET_NODE_UNAVAILABLE_AFTER", 2*time.Minute, &errs)
	c.RequestTimeout = duration(get, "DOCKFLEET_REQUEST_TIMEOUT", 10*time.Second, &errs)
	c.ShutdownTimeout = duration(get, "DOCKFLEET_SHUTDOWN_TIMEOUT", 15*time.Second, &errs)
	c.SchedulerInterval = duration(get, "DOCKFLEET_SCHEDULER_INTERVAL", 5*time.Second, &errs)
	c.ReconcileInterval = duration(get, "DOCKFLEET_RECONCILE_INTERVAL", 5*time.Second, &errs)
	c.CPUOvercommit = overcommit(get, "DOCKFLEET_SCHEDULER_CPU_OVERCOMMIT", &errs)
	c.MemoryOvercommit = overcommit(get, "DOCKFLEET_SCHEDULER_MEMORY_OVERCOMMIT", &errs)
	c.LogMaxStreams = intInRange(get, "DOCKFLEET_LOG_MAX_STREAMS", 32, 1, 1000, &errs)
	c.LogWindowBytes = intInRange(get, "DOCKFLEET_LOG_WINDOW_KIB", 256, 64, 16384, &errs) << 10
	c.LogReconnectWait = duration(get, "DOCKFLEET_LOG_RECONNECT_WAIT", 30*time.Second, &errs)
	c.LogWriteTimeout = duration(get, "DOCKFLEET_LOG_WRITE_TIMEOUT", 30*time.Second, &errs)
	c.MetricsEnabled = boolean(get, "DOCKFLEET_METRICS_ENABLED", true, &errs)
	c.DashboardEnabled = boolean(get, "DOCKFLEET_DASHBOARD_ENABLED", true, &errs)
	c.EtcdEndpoints = splitList(get("DOCKFLEET_ETCD_ENDPOINTS"))
	c.EtcdCAFile = env(get, "DOCKFLEET_ETCD_CA_FILE", "")
	c.EtcdCertFile = env(get, "DOCKFLEET_ETCD_CERT_FILE", "")
	c.EtcdKeyFile = env(get, "DOCKFLEET_ETCD_KEY_FILE", "")
	c.EtcdPrefix = env(get, "DOCKFLEET_ETCD_PREFIX", "/dockfleet")
	c.ElectionTTL = duration(get, "DOCKFLEET_ELECTION_TTL", 15*time.Second, &errs)

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
	// A lease that a single lost heartbeat can end would stop containers for nothing, and a node
	// must not be given up on before its agent has fenced itself, or the replica would run twice.
	if c.NodeLease < 2*c.HeartbeatInterval {
		fail("DOCKFLEET_NODE_LEASE (%s) must be at least twice DOCKFLEET_HEARTBEAT_INTERVAL (%s)", c.NodeLease, c.HeartbeatInterval)
	}
	if c.NodeUnavailableAfter < c.NodeLease+c.HeartbeatInterval {
		fail("DOCKFLEET_NODE_UNAVAILABLE_AFTER (%s) must be at least DOCKFLEET_NODE_LEASE plus DOCKFLEET_HEARTBEAT_INTERVAL (%s), so that an agent has fenced itself before its replicas are started elsewhere", c.NodeUnavailableAfter, c.NodeLease+c.HeartbeatInterval)
	}
	if c.NodeUnavailableAfter <= c.NodeTTL {
		fail("DOCKFLEET_NODE_UNAVAILABLE_AFTER (%s) must be longer than DOCKFLEET_NODE_TTL (%s): a node is suspect before it is unavailable", c.NodeUnavailableAfter, c.NodeTTL)
	}

	loadHA(get, &c, fail)

	if err := errors.Join(errs...); err != nil {
		return Manager{}, err
	}
	return c, nil
}

// Bounds of the election TTL. etcd counts leases in whole seconds, and a TTL
// of a few seconds would let an ordinary pause of the leader's process end its term.
const (
	minElectionTTL = 5 * time.Second
	maxElectionTTL = 5 * time.Minute
	maxEtcdPrefix  = 128
)

// loadHA validates the high-availability settings. Everything here is only
// checked when DOCKFLEET_ETCD_ENDPOINTS is set: a single manager ignores it.
func loadHA(get Getenv, c *Manager, fail func(string, ...any)) {
	if !c.HAEnabled() {
		for _, name := range []string{"DOCKFLEET_ETCD_CA_FILE", "DOCKFLEET_ETCD_CERT_FILE", "DOCKFLEET_ETCD_KEY_FILE"} {
			if get(name) != "" {
				fail("%s is set but DOCKFLEET_ETCD_ENDPOINTS is not: set the endpoints to turn on high availability, or unset the etcd settings", name)
			}
		}
		return
	}
	if c.Store != "postgres" {
		fail("DOCKFLEET_ETCD_ENDPOINTS needs DOCKFLEET_STORE=postgres: managers that elect a leader must share their state, and the in-memory store is private to one process")
	}
	if get("DOCKFLEET_INSTANCE_ID") == "" {
		fail("DOCKFLEET_INSTANCE_ID must be set, unique per manager, when DOCKFLEET_ETCD_ENDPOINTS is set: it names the leader in the cluster, the audit trail and the metrics")
	}
	if c.ElectionTTL < minElectionTTL || c.ElectionTTL > maxElectionTTL {
		fail("DOCKFLEET_ELECTION_TTL (%s) must be between %s and %s", c.ElectionTTL, minElectionTTL, maxElectionTTL)
	}
	// A failover takes up to four thirds of the TTL (the lease must lapse, then a
	// follower's next campaign wins), and the agents notice a heartbeat later. All
	// of it has to fit in the lease the agents fence themselves by, or every
	// change of leader would stop the containers of the fleet.
	if c.ElectionTTL >= minElectionTTL && c.NodeLease < 2*c.ElectionTTL+c.HeartbeatInterval {
		fail("DOCKFLEET_NODE_LEASE (%s) must be at least twice DOCKFLEET_ELECTION_TTL plus DOCKFLEET_HEARTBEAT_INTERVAL (%s), so that agents find a new leader before their lease runs out and they stop their containers", c.NodeLease, 2*c.ElectionTTL+c.HeartbeatInterval)
	}
	badChar := func(r rune) bool { return r <= ' ' || r == 0x7f }
	if len(c.EtcdPrefix) < 2 || !strings.HasPrefix(c.EtcdPrefix, "/") || strings.HasSuffix(c.EtcdPrefix, "/") || len(c.EtcdPrefix) > maxEtcdPrefix || strings.ContainsFunc(c.EtcdPrefix, badChar) {
		fail("DOCKFLEET_ETCD_PREFIX %q must be a path such as /dockfleet: it starts with /, does not end with one, has no spaces or control characters, and is at most %d characters", c.EtcdPrefix, maxEtcdPrefix)
	}

	secure := false
	seen := map[string]bool{}
	for _, raw := range c.EtcdEndpoints {
		u, err := url.Parse(raw)
		switch {
		case err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.User != nil:
			fail("DOCKFLEET_ETCD_ENDPOINTS entry %q must look like https://host:2379 (or http://127.0.0.1:2379 for development)", raw)
			continue
		case u.Scheme == "http" && !netutil.IsLoopbackAddr(u.Host):
			fail("DOCKFLEET_ETCD_ENDPOINTS entry %q is plaintext to a host that is not loopback; use https with DOCKFLEET_ETCD_CA_FILE (and a client certificate): there is no plaintext fallback", raw)
		}
		if u.Scheme == "https" {
			secure = true
		}
		if seen[u.Host] {
			fail("DOCKFLEET_ETCD_ENDPOINTS lists %s twice", u.Host)
		}
		seen[u.Host] = true
	}
	files := []string{c.EtcdCAFile, c.EtcdCertFile, c.EtcdKeyFile}
	anyFile := files[0] != "" || files[1] != "" || files[2] != ""
	switch {
	case secure && (files[0] == "" || files[1] == "" || files[2] == ""):
		fail("an https etcd endpoint needs DOCKFLEET_ETCD_CA_FILE, DOCKFLEET_ETCD_CERT_FILE and DOCKFLEET_ETCD_KEY_FILE together: managers reach etcd with mutual TLS, because whoever can write the leadership key decides who leads")
	case !secure && anyFile:
		fail("DOCKFLEET_ETCD_CA_FILE, DOCKFLEET_ETCD_CERT_FILE and DOCKFLEET_ETCD_KEY_FILE apply to https endpoints, and none of the endpoints is one")
	}
}

// LoadAgent reads and validates agent configuration.
func LoadAgent(get Getenv) (Agent, error) {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	var c Agent
	c.LogLevel = env(get, "DOCKFLEET_LOG_LEVEL", "info")
	c.LogFormat = env(get, "DOCKFLEET_LOG_FORMAT", "json")
	c.ManagerAddr = env(get, "DOCKFLEET_MANAGER_ADDR", "127.0.0.1:21281")
	c.ManagerAddrs = splitList(get("DOCKFLEET_MANAGER_ADDRS"))
	if len(c.ManagerAddrs) == 0 {
		c.ManagerAddrs = []string{c.ManagerAddr}
	}
	c.Token = secret(get, "DOCKFLEET_AGENT_TOKEN", &errs)
	c.TLSCAFile = env(get, "DOCKFLEET_TLS_CA_FILE", "")
	c.TLSCertFile = env(get, "DOCKFLEET_TLS_CERT_FILE", "")
	c.TLSKeyFile = env(get, "DOCKFLEET_TLS_KEY_FILE", "")
	c.TLSServerName = env(get, "DOCKFLEET_TLS_SERVER_NAME", "")
	c.DockerHost = env(get, "DOCKFLEET_DOCKER_HOST", "unix:///var/run/docker.sock")
	c.RPCTimeout = duration(get, "DOCKFLEET_RPC_TIMEOUT", 5*time.Second, &errs)
	c.FenceOnLeaseLoss = boolean(get, "DOCKFLEET_FENCE_ON_LEASE_LOSS", true, &errs)
	c.HealthAddr = AgentHealthAddr(get)
	if c.HealthAddr != "" && !netutil.IsLoopbackAddr(c.HealthAddr) {
		fail("DOCKFLEET_HEALTH_ADDR=%q is not a loopback address; the health endpoint is for the machine the agent runs on (use \"off\" to turn it off)", c.HealthAddr)
	}
	c.LogRequests = intInRange(get, "DOCKFLEET_LOG_MAX_REQUESTS", 16, 0, 256, &errs)
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
		for _, addr := range c.ManagerAddrs {
			if !netutil.IsLoopbackAddr(addr) {
				fail("manager address %q is not a loopback address; configure mutual TLS (DOCKFLEET_TLS_CA_FILE, DOCKFLEET_TLS_CERT_FILE, DOCKFLEET_TLS_KEY_FILE): plaintext is never used for remote managers", addr)
			}
		}
	}
	seenAddr := map[string]bool{}
	for _, addr := range c.ManagerAddrs {
		if _, _, err := net.SplitHostPort(addr); err != nil {
			fail("manager address %q must look like host:port", addr)
		}
		if seenAddr[addr] {
			fail("manager address %q is listed twice", addr)
		}
		seenAddr[addr] = true
	}

	if err := errors.Join(errs...); err != nil {
		return Agent{}, err
	}
	return c, nil
}

// maxSecretFile bounds a credential read from a file: a token or a database
// URL is a few hundred bytes, and a file far larger is not one.
const maxSecretFile = 8 << 10

// secret reads a credential from the variable NAME, or from the file named by
// NAME_FILE. A file is how Docker and Kubernetes secrets and systemd credentials
// are delivered, and keeps the value out of the process environment, where
// `docker inspect`, /proc/<pid>/environ and crash reports show it. Setting both
// is an error, because the winner would be a guess. The file is read once, at
// startup; surrounding whitespace (the newline at the end) is dropped. Errors
// name the file and never what is in it.
func secret(get Getenv, name string, errs *[]error) string {
	direct, file := get(name), get(name+"_FILE")
	fail := func(format string, args ...any) string {
		*errs = append(*errs, fmt.Errorf(format, args...))
		return ""
	}
	switch {
	case file == "":
		return direct
	case direct != "":
		return fail("%s and %s_FILE are both set; set only one", name, name)
	case !path.IsAbs(file):
		return fail("%s_FILE=%q must be an absolute path", name, file)
	}
	f, err := os.Open(file)
	if err != nil {
		return fail("%s_FILE: %v", name, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return fail("%s_FILE: %v", name, err)
	}
	if !info.Mode().IsRegular() {
		return fail("%s_FILE=%q is not a regular file", name, file)
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxSecretFile+1))
	if err != nil {
		return fail("%s_FILE: %v", name, err)
	}
	if len(raw) > maxSecretFile {
		return fail("%s_FILE=%q is larger than %d bytes", name, file, maxSecretFile)
	}
	value := strings.TrimSpace(string(raw))
	if value == "" {
		return fail("%s_FILE=%q is empty", name, file)
	}
	return value
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

func boolean(get Getenv, key string, def bool, errs *[]error) bool {
	raw := strings.TrimSpace(get(key))
	if raw == "" {
		return def
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s must be true or false, got %q", key, raw))
		return def
	}
	return v
}

// MaxOvercommit is the largest overcommit factor the scheduler accepts.
const MaxOvercommit = 8.0

// overcommit reads an overcommit factor: a number from 1 (never promise a node
// more than it has, the default) to MaxOvercommit. Raising it is a deliberate
// choice, so a value below 1, a non-number and anything absurd are errors, not
// quietly corrected.
func overcommit(get Getenv, key string, errs *[]error) float64 {
	raw := strings.TrimSpace(get(key))
	if raw == "" {
		return 1
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 1 || v > MaxOvercommit {
		*errs = append(*errs, fmt.Errorf("%s must be a number from 1 (no overcommit) to %g, got %q", key, MaxOvercommit, raw))
		return 1
	}
	return v
}

// intInRange reads a whole number from lo to hi.
func intInRange(get Getenv, key string, def, lo, hi int, errs *[]error) int {
	raw := strings.TrimSpace(get(key))
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < lo || v > hi {
		*errs = append(*errs, fmt.Errorf("%s must be a whole number from %d to %d, got %q", key, lo, hi, raw))
		return def
	}
	return v
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
