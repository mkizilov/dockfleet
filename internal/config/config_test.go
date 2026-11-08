package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/store"
)

func getenv(vars map[string]string) Getenv {
	return func(k string) string { return vars[k] }
}

func managerEnv() map[string]string {
	return map[string]string{
		"DOCKFLEET_API_TOKEN":    "api-token-0123456789",
		"DOCKFLEET_AGENT_TOKEN":  "agent-token-0123456789",
		"DOCKFLEET_DATABASE_URL": "postgres://u:p@127.0.0.1:5432/db",
	}
}

func TestLoadManagerDefaults(t *testing.T) {
	c, err := LoadManager(getenv(managerEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if c.APIAddr != "127.0.0.1:21280" || c.GRPCAddr != "127.0.0.1:21281" {
		t.Errorf("unexpected default addresses: %q %q", c.APIAddr, c.GRPCAddr)
	}
	if c.Store != "postgres" || c.LogFormat != "json" || c.LogLevel != "info" {
		t.Errorf("unexpected defaults: %+v", c)
	}
	if c.HeartbeatInterval != 10*time.Second || c.NodeTTL != 30*time.Second || c.ShutdownTimeout != 15*time.Second || c.RequestTimeout != 10*time.Second {
		t.Errorf("unexpected durations: %+v", c)
	}
}

func TestLoadManagerRejectsInsecureOrInvalid(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(map[string]string)
		wantErr string
	}{
		{"missing api token", func(m map[string]string) { delete(m, "DOCKFLEET_API_TOKEN") }, "DOCKFLEET_API_TOKEN"},
		{"short api token", func(m map[string]string) { m["DOCKFLEET_API_TOKEN"] = "short" }, "DOCKFLEET_API_TOKEN"},
		{"blank api token", func(m map[string]string) { m["DOCKFLEET_API_TOKEN"] = "   " }, "DOCKFLEET_API_TOKEN"},
		{"empty host api bind without tls", func(m map[string]string) { m["DOCKFLEET_API_ADDR"] = ":21280" }, "loopback"},
		{"ipv6 wildcard api bind without tls", func(m map[string]string) { m["DOCKFLEET_API_ADDR"] = "[::]:21280" }, "loopback"},
		{"hostname api bind without tls", func(m map[string]string) { m["DOCKFLEET_API_ADDR"] = "manager.example.com:21280" }, "loopback"},
		{"short agent token", func(m map[string]string) { m["DOCKFLEET_AGENT_TOKEN"] = "short" }, "DOCKFLEET_AGENT_TOKEN"},
		{"same tokens", func(m map[string]string) { m["DOCKFLEET_AGENT_TOKEN"] = m["DOCKFLEET_API_TOKEN"] }, "must differ"},
		{"missing database url", func(m map[string]string) { delete(m, "DOCKFLEET_DATABASE_URL") }, "DOCKFLEET_DATABASE_URL"},
		{"bad store", func(m map[string]string) { m["DOCKFLEET_STORE"] = "sqlite" }, "DOCKFLEET_STORE"},
		{"public bind without tls", func(m map[string]string) { m["DOCKFLEET_API_ADDR"] = "0.0.0.0:8080" }, "loopback"},
		{"empty host bind without tls", func(m map[string]string) { m["DOCKFLEET_GRPC_ADDR"] = ":9000" }, "loopback"},
		{"cert without key", func(m map[string]string) { m["DOCKFLEET_TLS_CERT_FILE"] = "c.pem" }, "set together"},
		{"tls without client ca", func(m map[string]string) {
			m["DOCKFLEET_TLS_CERT_FILE"], m["DOCKFLEET_TLS_KEY_FILE"] = "c.pem", "k.pem"
		}, "no mode without client certificates"},
		{"client ca alone", func(m map[string]string) { m["DOCKFLEET_TLS_CLIENT_CA_FILE"] = "ca.pem" }, "set together"},
		{"public bind names mutual tls", func(m map[string]string) { m["DOCKFLEET_GRPC_ADDR"] = "0.0.0.0:9000" }, "mutual TLS"},
		{"bad duration", func(m map[string]string) { m["DOCKFLEET_NODE_TTL"] = "soon" }, "DOCKFLEET_NODE_TTL"},
		{"ttl too small", func(m map[string]string) { m["DOCKFLEET_NODE_TTL"] = "15s" }, "twice"},
		{"bad request timeout", func(m map[string]string) { m["DOCKFLEET_REQUEST_TIMEOUT"] = "0s" }, "DOCKFLEET_REQUEST_TIMEOUT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vars := managerEnv()
			tt.mutate(vars)
			_, err := LoadManager(getenv(vars))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}

func TestLoadManagerErrorsDoNotLeakSecrets(t *testing.T) {
	vars := managerEnv()
	vars["DOCKFLEET_STORE"] = "sqlite"
	_, err := LoadManager(getenv(vars))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, secret := range []string{vars["DOCKFLEET_API_TOKEN"], vars["DOCKFLEET_AGENT_TOKEN"], vars["DOCKFLEET_DATABASE_URL"]} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error leaks a secret: %v", err)
		}
	}
}

func tlsManagerEnv() map[string]string {
	vars := managerEnv()
	delete(vars, "DOCKFLEET_DATABASE_URL")
	delete(vars, "DOCKFLEET_AGENT_TOKEN")
	vars["DOCKFLEET_STORE"] = "memory"
	vars["DOCKFLEET_TLS_CERT_FILE"] = "c.pem"
	vars["DOCKFLEET_TLS_KEY_FILE"] = "k.pem"
	vars["DOCKFLEET_TLS_CLIENT_CA_FILE"] = "ca.pem"
	return vars
}

func TestLoadManagerAllowsPublicBindWithMutualTLS(t *testing.T) {
	vars := tlsManagerEnv()
	vars["DOCKFLEET_API_ADDR"] = "0.0.0.0:8443"
	vars["DOCKFLEET_GRPC_ADDR"] = "0.0.0.0:9443"
	c, err := LoadManager(getenv(vars))
	if err != nil {
		t.Fatal(err)
	}
	if !c.TLSEnabled() || c.TLSClientCAFile != "ca.pem" {
		t.Errorf("unexpected TLS settings: %+v", c)
	}
}

func TestLoadManagerStillRequiresTheAdminTokenWithTLS(t *testing.T) {
	vars := tlsManagerEnv()
	delete(vars, "DOCKFLEET_API_TOKEN")
	if _, err := LoadManager(getenv(vars)); err == nil || !strings.Contains(err.Error(), "DOCKFLEET_API_TOKEN") {
		t.Fatalf("error = %v, want the admin token to stay mandatory", err)
	}
}

func TestLoadManagerWithoutTLSIsPlaintextOnLoopback(t *testing.T) {
	c, err := LoadManager(getenv(managerEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if c.TLSEnabled() {
		t.Error("TLS must be off by default")
	}
}

func agentEnv() map[string]string {
	return map[string]string{
		"DOCKFLEET_AGENT_TOKEN": "agent-token-0123456789",
		"DOCKFLEET_NODE_ID":     "node-1",
	}
}

func TestLoadAgent(t *testing.T) {
	vars := agentEnv()
	vars["DOCKFLEET_NODE_LABELS"] = "zone=a, tier = gold"
	vars["DOCKFLEET_NODE_MEMORY_MB"] = "2048"
	vars["DOCKFLEET_NODE_CPU_MILLIS"] = "3000"
	c, err := LoadAgent(getenv(vars))
	if err != nil {
		t.Fatal(err)
	}
	if c.NodeID != "node-1" || c.NodeName != "node-1" {
		t.Errorf("unexpected node identity: %q %q", c.NodeID, c.NodeName)
	}
	if c.MemoryBytes != 2048<<20 || c.CPUMillis != 3000 {
		t.Errorf("unexpected capacity: %d %d", c.MemoryBytes, c.CPUMillis)
	}
	if c.Labels["zone"] != "a" || c.Labels["tier"] != "gold" || len(c.Labels) != 2 {
		t.Errorf("unexpected labels: %v", c.Labels)
	}
	if c.ManagerAddr != "127.0.0.1:21281" || c.DockerHost != "unix:///var/run/docker.sock" || c.RPCTimeout != 5*time.Second {
		t.Errorf("unexpected defaults: %+v", c)
	}
}

func TestLoadAgentRejectsInvalid(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(map[string]string)
		wantErr string
	}{
		{"missing token", func(m map[string]string) { delete(m, "DOCKFLEET_AGENT_TOKEN") }, "DOCKFLEET_AGENT_TOKEN"},
		{"bad node id", func(m map[string]string) { m["DOCKFLEET_NODE_ID"] = "bad id" }, "invalid"},
		{"bad label", func(m map[string]string) { m["DOCKFLEET_NODE_LABELS"] = "novalue" }, "DOCKFLEET_NODE_LABELS"},
		{"bad memory", func(m map[string]string) { m["DOCKFLEET_NODE_MEMORY_MB"] = "-5" }, "DOCKFLEET_NODE_MEMORY_MB"},
		{"bad rpc timeout", func(m map[string]string) { m["DOCKFLEET_RPC_TIMEOUT"] = "fast" }, "DOCKFLEET_RPC_TIMEOUT"},
		{"remote manager without CA", func(m map[string]string) { m["DOCKFLEET_MANAGER_ADDR"] = "manager.example.com:443" }, "DOCKFLEET_TLS_CA_FILE"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vars := agentEnv()
			tt.mutate(vars)
			_, err := LoadAgent(getenv(vars))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}

func TestLoadManagerSchedulerSettings(t *testing.T) {
	c, err := LoadManager(getenv(managerEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if c.SchedulerInterval != 5*time.Second || c.CPUOvercommit != 1 || c.MemoryOvercommit != 1 {
		t.Fatalf("defaults: interval %s, CPU overcommit %v, memory overcommit %v; nothing is oversubscribed unless asked", c.SchedulerInterval, c.CPUOvercommit, c.MemoryOvercommit)
	}

	vars := managerEnv()
	vars["DOCKFLEET_SCHEDULER_INTERVAL"] = "2s"
	vars["DOCKFLEET_SCHEDULER_CPU_OVERCOMMIT"] = "2.5"
	vars["DOCKFLEET_SCHEDULER_MEMORY_OVERCOMMIT"] = "1"
	c, err = LoadManager(getenv(vars))
	if err != nil || c.SchedulerInterval != 2*time.Second || c.CPUOvercommit != 2.5 || c.MemoryOvercommit != 1 {
		t.Fatalf("explicit settings: %+v, %v", c, err)
	}

	for _, tt := range []struct{ key, value string }{
		{"DOCKFLEET_SCHEDULER_INTERVAL", "0s"},
		{"DOCKFLEET_SCHEDULER_INTERVAL", "soon"},
		{"DOCKFLEET_SCHEDULER_CPU_OVERCOMMIT", "0.5"},
		{"DOCKFLEET_SCHEDULER_CPU_OVERCOMMIT", "0"},
		{"DOCKFLEET_SCHEDULER_CPU_OVERCOMMIT", "-2"},
		{"DOCKFLEET_SCHEDULER_CPU_OVERCOMMIT", "8.5"},
		{"DOCKFLEET_SCHEDULER_CPU_OVERCOMMIT", "lots"},
		{"DOCKFLEET_SCHEDULER_CPU_OVERCOMMIT", "NaN"},
		{"DOCKFLEET_SCHEDULER_CPU_OVERCOMMIT", "Inf"},
		{"DOCKFLEET_SCHEDULER_MEMORY_OVERCOMMIT", "0.9"},
		{"DOCKFLEET_SCHEDULER_MEMORY_OVERCOMMIT", "100"},
	} {
		vars := managerEnv()
		vars[tt.key] = tt.value
		if _, err := LoadManager(getenv(vars)); err == nil || !strings.Contains(err.Error(), tt.key) {
			t.Errorf("%s=%q: error = %v, want one naming the setting", tt.key, tt.value, err)
		}
	}

	// The boundaries themselves are fine.
	for _, v := range []string{"1", "1.0", "8"} {
		vars := managerEnv()
		vars["DOCKFLEET_SCHEDULER_CPU_OVERCOMMIT"] = v
		if _, err := LoadManager(getenv(vars)); err != nil {
			t.Errorf("overcommit %s rejected: %v", v, err)
		}
	}
}

func TestLoadManagerReconcileInterval(t *testing.T) {
	c, err := LoadManager(getenv(managerEnv()))
	if err != nil || c.ReconcileInterval != 5*time.Second {
		t.Fatalf("default: %s, %v", c.ReconcileInterval, err)
	}
	vars := managerEnv()
	vars["DOCKFLEET_RECONCILE_INTERVAL"] = "750ms"
	if c, err = LoadManager(getenv(vars)); err != nil || c.ReconcileInterval != 750*time.Millisecond {
		t.Fatalf("explicit: %s, %v", c.ReconcileInterval, err)
	}
	for _, bad := range []string{"0s", "-1s", "soon"} {
		vars := managerEnv()
		vars["DOCKFLEET_RECONCILE_INTERVAL"] = bad
		if _, err := LoadManager(getenv(vars)); err == nil || !strings.Contains(err.Error(), "DOCKFLEET_RECONCILE_INTERVAL") {
			t.Errorf("%q: error = %v, want one naming the setting", bad, err)
		}
	}
}

func TestLoadManagerNodeFailureTimings(t *testing.T) {
	c, err := LoadManager(getenv(managerEnv()))
	if err != nil || c.NodeLease != time.Minute || c.NodeUnavailableAfter != 2*time.Minute {
		t.Fatalf("defaults: lease %s, unavailable after %s, %v", c.NodeLease, c.NodeUnavailableAfter, err)
	}
	vars := managerEnv()
	vars["DOCKFLEET_NODE_LEASE"], vars["DOCKFLEET_NODE_UNAVAILABLE_AFTER"] = "45s", "5m"
	if c, err = LoadManager(getenv(vars)); err != nil || c.NodeLease != 45*time.Second || c.NodeUnavailableAfter != 5*time.Minute {
		t.Fatalf("explicit: lease %s, unavailable after %s, %v", c.NodeLease, c.NodeUnavailableAfter, err)
	}

	tests := []struct {
		name    string
		mutate  func(map[string]string)
		wantErr string
	}{
		{"unparsable lease", func(m map[string]string) { m["DOCKFLEET_NODE_LEASE"] = "long" }, "DOCKFLEET_NODE_LEASE"},
		{"zero unavailable limit", func(m map[string]string) { m["DOCKFLEET_NODE_UNAVAILABLE_AFTER"] = "0s" }, "DOCKFLEET_NODE_UNAVAILABLE_AFTER"},
		// One lost heartbeat must not end a lease.
		{"lease shorter than two heartbeats", func(m map[string]string) { m["DOCKFLEET_NODE_LEASE"] = "15s" }, "at least twice"},
		// Otherwise a replica could run on the lost node and on a new one.
		{"given up on before the lease can end", func(m map[string]string) { m["DOCKFLEET_NODE_UNAVAILABLE_AFTER"] = "65s" }, "fenced itself"},
		{"given up on exactly when the lease ends", func(m map[string]string) {
			m["DOCKFLEET_NODE_LEASE"], m["DOCKFLEET_NODE_UNAVAILABLE_AFTER"] = "60s", "60s"
		}, "fenced itself"},
		{"unavailable before suspect", func(m map[string]string) {
			m["DOCKFLEET_NODE_TTL"], m["DOCKFLEET_NODE_LEASE"], m["DOCKFLEET_NODE_UNAVAILABLE_AFTER"] = "3m", "30s", "90s"
		}, "longer than DOCKFLEET_NODE_TTL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vars := managerEnv()
			tt.mutate(vars)
			if _, err := LoadManager(getenv(vars)); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}

func TestLoadAgentFencing(t *testing.T) {
	c, err := LoadAgent(getenv(agentEnv()))
	if err != nil || !c.FenceOnLeaseLoss {
		t.Fatalf("fencing is on by default: %+v, %v", c, err)
	}
	for value, want := range map[string]bool{"false": false, "0": false, "true": true, "1": true} {
		vars := agentEnv()
		vars["DOCKFLEET_FENCE_ON_LEASE_LOSS"] = value
		if c, err := LoadAgent(getenv(vars)); err != nil || c.FenceOnLeaseLoss != want {
			t.Errorf("%q: fencing = %v, %v", value, c.FenceOnLeaseLoss, err)
		}
	}
	vars := agentEnv()
	vars["DOCKFLEET_FENCE_ON_LEASE_LOSS"] = "sometimes"
	if _, err := LoadAgent(getenv(vars)); err == nil || !strings.Contains(err.Error(), "DOCKFLEET_FENCE_ON_LEASE_LOSS") {
		t.Errorf("error = %v, want one naming the setting", err)
	}
}

func TestLoadManagerInstanceID(t *testing.T) {
	first, err := LoadManager(getenv(managerEnv()))
	if err != nil {
		t.Fatal(err)
	}
	second, err := LoadManager(getenv(managerEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(first.InstanceID, "manager-") || !store.ValidName(first.InstanceID) || first.InstanceID == second.InstanceID {
		t.Errorf("default instance IDs must be valid and differ per process: %q, %q", first.InstanceID, second.InstanceID)
	}

	vars := managerEnv()
	vars["DOCKFLEET_INSTANCE_ID"] = "manager-eu-1"
	c, err := LoadManager(getenv(vars))
	if err != nil || c.InstanceID != "manager-eu-1" {
		t.Fatalf("explicit instance ID = %q, %v", c.InstanceID, err)
	}
	vars["DOCKFLEET_INSTANCE_ID"] = "bad id"
	if _, err := LoadManager(getenv(vars)); err == nil || !strings.Contains(err.Error(), "DOCKFLEET_INSTANCE_ID") {
		t.Errorf("an invalid instance ID must be rejected, got %v", err)
	}
}

func TestLoadAgentContainerPolicy(t *testing.T) {
	c, err := LoadAgent(getenv(agentEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.AllowedBindRoots) != 0 || len(c.AllowedCapabilities) != 0 {
		t.Errorf("the default policy allows nothing extra: %+v", c)
	}

	vars := agentEnv()
	vars["DOCKFLEET_ALLOWED_BIND_ROOTS"] = "/srv/dockfleet, /mnt/data,"
	vars["DOCKFLEET_ALLOWED_CAPABILITIES"] = "NET_BIND_SERVICE,CHOWN"
	c, err = LoadAgent(getenv(vars))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.AllowedBindRoots) != 2 || c.AllowedBindRoots[0] != "/srv/dockfleet" || c.AllowedBindRoots[1] != "/mnt/data" || len(c.AllowedCapabilities) != 2 {
		t.Errorf("unexpected policy settings: %+v", c)
	}

	vars["DOCKFLEET_ALLOWED_BIND_ROOTS"] = "relative/dir"
	if _, err := LoadAgent(getenv(vars)); err == nil || !strings.Contains(err.Error(), "DOCKFLEET_ALLOWED_BIND_ROOTS") {
		t.Errorf("a relative bind root must be rejected, got %v", err)
	}
}

func TestManagerConfigHasNoDockerSettings(t *testing.T) {
	// The manager must never need Docker access; its configuration must not offer any.
	vars := managerEnv()
	vars["DOCKFLEET_DOCKER_HOST"] = "unix:///var/run/docker.sock"
	c, err := LoadManager(getenv(vars))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(fmt.Sprintf("%+v", c)), "docker") {
		t.Errorf("manager configuration mentions Docker: %+v", c)
	}
}

func TestLoadAgentRemoteManagerWithMutualTLS(t *testing.T) {
	vars := agentEnv()
	delete(vars, "DOCKFLEET_AGENT_TOKEN")
	vars["DOCKFLEET_MANAGER_ADDR"] = "manager.example.com:443"
	vars["DOCKFLEET_TLS_CA_FILE"] = "ca.pem"
	vars["DOCKFLEET_TLS_CERT_FILE"] = "agent.pem"
	vars["DOCKFLEET_TLS_KEY_FILE"] = "agent.key"
	vars["DOCKFLEET_TLS_SERVER_NAME"] = "manager.example.com"
	c, err := LoadAgent(getenv(vars))
	if err != nil {
		t.Fatal(err)
	}
	if !c.TLSEnabled() || c.TLSCertFile != "agent.pem" || c.TLSKeyFile != "agent.key" || c.TLSServerName != "manager.example.com" {
		t.Errorf("unexpected TLS settings: %+v", c)
	}
}

func TestLoadAgentTLSNeedsAllThreeFiles(t *testing.T) {
	for _, set := range [][]string{
		{"DOCKFLEET_TLS_CA_FILE"},
		{"DOCKFLEET_TLS_CERT_FILE"},
		{"DOCKFLEET_TLS_CA_FILE", "DOCKFLEET_TLS_CERT_FILE"},
		{"DOCKFLEET_TLS_CERT_FILE", "DOCKFLEET_TLS_KEY_FILE"},
	} {
		vars := agentEnv()
		for _, k := range set {
			vars[k] = "x.pem"
		}
		if _, err := LoadAgent(getenv(vars)); err == nil || !strings.Contains(err.Error(), "set together") {
			t.Errorf("%v: error = %v, want a 'set together' error", set, err)
		}
	}
}

func TestLoadAgentRemoteManagerNeverFallsBackToPlaintext(t *testing.T) {
	vars := agentEnv()
	vars["DOCKFLEET_MANAGER_ADDR"] = "manager.example.com:443"
	_, err := LoadAgent(getenv(vars))
	if err == nil || !strings.Contains(err.Error(), "plaintext is never used") {
		t.Fatalf("error = %v, want a refusal of plaintext to a remote manager", err)
	}
}

func TestLoadManagerLogSettings(t *testing.T) {
	c, err := LoadManager(getenv(managerEnv()))
	if err != nil || c.LogMaxStreams != 32 || c.LogWindowBytes != 256<<10 || c.LogReconnectWait != 30*time.Second || c.LogWriteTimeout != 30*time.Second {
		t.Fatalf("defaults: %+v, %v", c, err)
	}
	vars := managerEnv()
	vars["DOCKFLEET_LOG_MAX_STREAMS"] = "5"
	vars["DOCKFLEET_LOG_WINDOW_KIB"] = "64"
	vars["DOCKFLEET_LOG_RECONNECT_WAIT"] = "2s"
	vars["DOCKFLEET_LOG_WRITE_TIMEOUT"] = "500ms"
	c, err = LoadManager(getenv(vars))
	if err != nil || c.LogMaxStreams != 5 || c.LogWindowBytes != 64<<10 || c.LogReconnectWait != 2*time.Second || c.LogWriteTimeout != 500*time.Millisecond {
		t.Fatalf("configured: %+v, %v", c, err)
	}
	for _, bad := range []struct{ key, value string }{
		{"DOCKFLEET_LOG_MAX_STREAMS", "0"},
		{"DOCKFLEET_LOG_MAX_STREAMS", "1001"},
		{"DOCKFLEET_LOG_MAX_STREAMS", "many"},
		{"DOCKFLEET_LOG_WINDOW_KIB", "63"},
		{"DOCKFLEET_LOG_WINDOW_KIB", "16385"},
		{"DOCKFLEET_LOG_WINDOW_KIB", "-1"},
		{"DOCKFLEET_LOG_RECONNECT_WAIT", "0s"},
		{"DOCKFLEET_LOG_RECONNECT_WAIT", "soon"},
		{"DOCKFLEET_LOG_WRITE_TIMEOUT", "-5s"},
	} {
		vars := managerEnv()
		vars[bad.key] = bad.value
		if _, err := LoadManager(getenv(vars)); err == nil || !strings.Contains(err.Error(), bad.key) {
			t.Errorf("%s=%s: error = %v, want one naming the setting", bad.key, bad.value, err)
		}
	}
}

func TestLoadAgentLogSettings(t *testing.T) {
	c, err := LoadAgent(getenv(agentEnv()))
	if err != nil || c.LogRequests != 16 {
		t.Fatalf("default: %+v, %v", c, err)
	}
	for value, want := range map[string]int{"0": 0, "1": 1, "256": 256} {
		vars := agentEnv()
		vars["DOCKFLEET_LOG_MAX_REQUESTS"] = value
		if c, err := LoadAgent(getenv(vars)); err != nil || c.LogRequests != want {
			t.Errorf("%q: %d, %v", value, c.LogRequests, err)
		}
	}
	for _, value := range []string{"-1", "257", "lots", "1.5"} {
		vars := agentEnv()
		vars["DOCKFLEET_LOG_MAX_REQUESTS"] = value
		if _, err := LoadAgent(getenv(vars)); err == nil || !strings.Contains(err.Error(), "DOCKFLEET_LOG_MAX_REQUESTS") {
			t.Errorf("%q: error = %v, want one naming the setting", value, err)
		}
	}
}

func TestLoadManagerObservabilitySettings(t *testing.T) {
	c, err := LoadManager(getenv(managerEnv()))
	if err != nil || !c.MetricsEnabled || !c.DashboardEnabled {
		t.Fatalf("metrics and the dashboard are on by default (both need the admin token): %+v, %v", c, err)
	}
	for value, want := range map[string]bool{"false": false, "0": false, "true": true, "1": true} {
		vars := managerEnv()
		vars["DOCKFLEET_METRICS_ENABLED"] = value
		vars["DOCKFLEET_DASHBOARD_ENABLED"] = value
		if c, err := LoadManager(getenv(vars)); err != nil || c.MetricsEnabled != want || c.DashboardEnabled != want {
			t.Errorf("%q: %+v, %v", value, c, err)
		}
	}
	for _, key := range []string{"DOCKFLEET_METRICS_ENABLED", "DOCKFLEET_DASHBOARD_ENABLED"} {
		vars := managerEnv()
		vars[key] = "sometimes"
		if _, err := LoadManager(getenv(vars)); err == nil || !strings.Contains(err.Error(), key) {
			t.Errorf("%s=sometimes: error = %v, want one naming the setting", key, err)
		}
	}
}

func haEnv() map[string]string {
	m := managerEnv()
	m["DOCKFLEET_ETCD_ENDPOINTS"] = "http://127.0.0.1:2379"
	m["DOCKFLEET_INSTANCE_ID"] = "manager-1"
	return m
}

// Without etcd a manager is the only one: nothing about the cluster is needed,
// and leader election is off.
func TestSingleManagerNeedsNoEtcd(t *testing.T) {
	c, err := LoadManager(getenv(managerEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if c.HAEnabled() || len(c.EtcdEndpoints) != 0 {
		t.Fatalf("high availability is on without endpoints: %+v", c)
	}
	// A random instance ID is fine for a manager that is alone.
	if !strings.HasPrefix(c.InstanceID, "manager-") {
		t.Errorf("instance ID = %q", c.InstanceID)
	}
	mem := managerEnv()
	mem["DOCKFLEET_STORE"] = "memory"
	if c, err := LoadManager(getenv(mem)); err != nil || c.HAEnabled() {
		t.Errorf("the in-memory store with a single manager = %+v, %v", c, err)
	}
}

func TestLoadManagerHighAvailability(t *testing.T) {
	c, err := LoadManager(getenv(haEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if !c.HAEnabled() || len(c.EtcdEndpoints) != 1 || c.EtcdEndpoints[0] != "http://127.0.0.1:2379" || c.EtcdPrefix != "/dockfleet" || c.ElectionTTL != 15*time.Second || c.InstanceID != "manager-1" {
		t.Fatalf("unexpected HA settings: %+v", c)
	}

	secure := haEnv()
	secure["DOCKFLEET_ETCD_ENDPOINTS"] = "https://etcd-1.example.com:2379, https://etcd-2.example.com:2379"
	secure["DOCKFLEET_ETCD_CA_FILE"] = "/etc/dockfleet/etcd-ca.pem"
	secure["DOCKFLEET_ETCD_CERT_FILE"] = "/etc/dockfleet/etcd.pem"
	secure["DOCKFLEET_ETCD_KEY_FILE"] = "/etc/dockfleet/etcd-key.pem"
	secure["DOCKFLEET_ETCD_PREFIX"] = "/prod/dockfleet"
	secure["DOCKFLEET_ELECTION_TTL"] = "30s"
	secure["DOCKFLEET_NODE_LEASE"] = "90s"
	secure["DOCKFLEET_NODE_UNAVAILABLE_AFTER"] = "3m"
	c, err = LoadManager(getenv(secure))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.EtcdEndpoints) != 2 || c.EtcdCAFile == "" || c.EtcdPrefix != "/prod/dockfleet" || c.ElectionTTL != 30*time.Second {
		t.Fatalf("unexpected secure HA settings: %+v", c)
	}

	if !c.EtcdTLSEnabled() || haCfg(t).EtcdTLSEnabled() {
		t.Errorf("TLS enabled: %v for https endpoints, %v for a loopback http one", c.EtcdTLSEnabled(), haCfg(t).EtcdTLSEnabled())
	}
}

func haCfg(t *testing.T) Manager {
	t.Helper()
	c, err := LoadManager(getenv(haEnv()))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestLoadManagerHighAvailabilityRejectsUnsafeSettings(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(map[string]string)
		wantErr string
	}{
		{"the in-memory store", func(m map[string]string) { m["DOCKFLEET_STORE"] = "memory" }, "DOCKFLEET_STORE=postgres"},
		{"no instance ID", func(m map[string]string) { delete(m, "DOCKFLEET_INSTANCE_ID") }, "DOCKFLEET_INSTANCE_ID must be set"},
		{"plaintext etcd on another host", func(m map[string]string) { m["DOCKFLEET_ETCD_ENDPOINTS"] = "http://etcd.example.com:2379" }, "plaintext"},
		{"plaintext etcd on a private address", func(m map[string]string) { m["DOCKFLEET_ETCD_ENDPOINTS"] = "http://10.0.0.5:2379" }, "plaintext"},
		{"an endpoint that is no URL", func(m map[string]string) { m["DOCKFLEET_ETCD_ENDPOINTS"] = "etcd:2379" }, "must look like"},
		{"an endpoint with a path", func(m map[string]string) { m["DOCKFLEET_ETCD_ENDPOINTS"] = "http://127.0.0.1:2379/v3" }, "must look like"},
		{"an endpoint with credentials", func(m map[string]string) { m["DOCKFLEET_ETCD_ENDPOINTS"] = "http://user:pw@127.0.0.1:2379" }, "must look like"},
		{"the same endpoint twice", func(m map[string]string) {
			m["DOCKFLEET_ETCD_ENDPOINTS"] = "http://127.0.0.1:2379,http://127.0.0.1:2379"
		}, "twice"},
		{"a TTL too short", func(m map[string]string) { m["DOCKFLEET_ELECTION_TTL"] = "2s" }, "DOCKFLEET_ELECTION_TTL"},
		{"a TTL too long", func(m map[string]string) { m["DOCKFLEET_ELECTION_TTL"] = "1h" }, "DOCKFLEET_ELECTION_TTL"},
		{"a TTL that is no duration", func(m map[string]string) { m["DOCKFLEET_ELECTION_TTL"] = "soon" }, "DOCKFLEET_ELECTION_TTL"},
		{"a lease a failover does not fit in", func(m map[string]string) { m["DOCKFLEET_ELECTION_TTL"] = "30s" }, "DOCKFLEET_NODE_LEASE"},
		{"a prefix without a slash", func(m map[string]string) { m["DOCKFLEET_ETCD_PREFIX"] = "dockfleet" }, "DOCKFLEET_ETCD_PREFIX"},
		{"a prefix with a trailing slash", func(m map[string]string) { m["DOCKFLEET_ETCD_PREFIX"] = "/dockfleet/" }, "DOCKFLEET_ETCD_PREFIX"},
		{"the root as the prefix", func(m map[string]string) { m["DOCKFLEET_ETCD_PREFIX"] = "/" }, "DOCKFLEET_ETCD_PREFIX"},
		{"a prefix with a space", func(m map[string]string) { m["DOCKFLEET_ETCD_PREFIX"] = "/dock fleet" }, "DOCKFLEET_ETCD_PREFIX"},
		{"https without any TLS files", func(m map[string]string) { m["DOCKFLEET_ETCD_ENDPOINTS"] = "https://etcd.example.com:2379" }, "mutual TLS"},
		{"https without a client certificate", func(m map[string]string) {
			m["DOCKFLEET_ETCD_ENDPOINTS"] = "https://etcd.example.com:2379"
			m["DOCKFLEET_ETCD_CA_FILE"] = "/etc/dockfleet/etcd-ca.pem"
		}, "mutual TLS"},
		{"https with a certificate and no key", func(m map[string]string) {
			m["DOCKFLEET_ETCD_ENDPOINTS"] = "https://etcd.example.com:2379"
			m["DOCKFLEET_ETCD_CA_FILE"] = "/etc/dockfleet/etcd-ca.pem"
			m["DOCKFLEET_ETCD_CERT_FILE"] = "/etc/dockfleet/etcd.pem"
		}, "mutual TLS"},
		{"a mix of https without TLS files and loopback http", func(m map[string]string) {
			m["DOCKFLEET_ETCD_ENDPOINTS"] = "http://127.0.0.1:2379,https://etcd.example.com:2379"
		}, "mutual TLS"},
		{"TLS files for a plaintext endpoint", func(m map[string]string) { m["DOCKFLEET_ETCD_CA_FILE"] = "/etc/dockfleet/etcd-ca.pem" }, "apply to https endpoints"},
		{"etcd TLS files without endpoints", func(m map[string]string) {
			delete(m, "DOCKFLEET_ETCD_ENDPOINTS")
			m["DOCKFLEET_ETCD_CA_FILE"] = "/etc/dockfleet/etcd-ca.pem"
		}, "DOCKFLEET_ETCD_ENDPOINTS is not"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vars := haEnv()
			tt.mutate(vars)
			_, err := LoadManager(getenv(vars))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}

// A lease just long enough for a failover is accepted, one second short is not.
func TestLoadManagerHighAvailabilityLeaseBound(t *testing.T) {
	for _, tt := range []struct {
		lease string
		ok    bool
	}{{"40s", true}, {"39s", false}, {"5m", true}} {
		vars := haEnv()
		vars["DOCKFLEET_NODE_LEASE"] = tt.lease // 2*15s election TTL + 10s heartbeat = 40s
		vars["DOCKFLEET_NODE_UNAVAILABLE_AFTER"] = "6m"
		_, err := LoadManager(getenv(vars))
		if (err == nil) != tt.ok {
			t.Errorf("lease %s: error = %v, want ok=%v", tt.lease, err, tt.ok)
		}
	}
}

func TestLoadAgentManagerAddresses(t *testing.T) {
	single, err := LoadAgent(getenv(agentEnv()))
	if err != nil || len(single.ManagerAddrs) != 1 || single.ManagerAddrs[0] != "127.0.0.1:21281" {
		t.Fatalf("default = %+v, %v", single.ManagerAddrs, err)
	}

	vars := agentEnv()
	vars["DOCKFLEET_MANAGER_ADDRS"] = "127.0.0.1:21281, 127.0.0.1:21282,[::1]:21283"
	c, err := LoadAgent(getenv(vars))
	if err != nil || len(c.ManagerAddrs) != 3 || c.ManagerAddrs[1] != "127.0.0.1:21282" {
		t.Fatalf("list = %+v, %v", c.ManagerAddrs, err)
	}

	for name, tt := range map[string]struct{ addrs, want string }{
		"a remote address without TLS": {"127.0.0.1:21281,manager-2.example.com:21281", "not a loopback address"},
		"a duplicate":                  {"127.0.0.1:21281,127.0.0.1:21281", "listed twice"},
		"no port":                      {"127.0.0.1", "host:port"},
	} {
		bad := agentEnv()
		bad["DOCKFLEET_MANAGER_ADDRS"] = tt.addrs
		if _, err := LoadAgent(getenv(bad)); err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%s: error = %v, want it to mention %q", name, err, tt.want)
		}
	}

	// With mutual TLS, remote managers are fine.
	tls := agentEnv()
	delete(tls, "DOCKFLEET_AGENT_TOKEN")
	tls["DOCKFLEET_TLS_CA_FILE"], tls["DOCKFLEET_TLS_CERT_FILE"], tls["DOCKFLEET_TLS_KEY_FILE"] = "/ca.pem", "/agent.pem", "/agent-key.pem"
	tls["DOCKFLEET_MANAGER_ADDRS"] = "manager-1.example.com:21281,manager-2.example.com:21281"
	if c, err := LoadAgent(getenv(tls)); err != nil || len(c.ManagerAddrs) != 2 {
		t.Errorf("remote managers with mutual TLS = %+v, %v", c.ManagerAddrs, err)
	}
}

// writeSecret writes a credential file the way Docker, Kubernetes or systemd
// would deliver it.
func writeSecret(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o400); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCredentialsCanComeFromFiles(t *testing.T) {
	vars := map[string]string{
		"DOCKFLEET_API_TOKEN_FILE":    writeSecret(t, "api", "api-token-from-a-file-0123\n"),
		"DOCKFLEET_AGENT_TOKEN_FILE":  writeSecret(t, "agent", "  agent-token-from-a-file-0123 \r\n\n"),
		"DOCKFLEET_DATABASE_URL_FILE": writeSecret(t, "db", "postgres://u:p@127.0.0.1:5432/db\n"),
	}
	c, err := LoadManager(getenv(vars))
	if err != nil {
		t.Fatal(err)
	}
	if c.APIToken != "api-token-from-a-file-0123" || c.AgentToken != "agent-token-from-a-file-0123" || c.DatabaseURL != "postgres://u:p@127.0.0.1:5432/db" {
		t.Fatalf("credentials = %q %q %q; whitespace around a file's content is not part of it", c.APIToken, c.AgentToken, c.DatabaseURL)
	}

	a, err := LoadAgent(getenv(map[string]string{"DOCKFLEET_AGENT_TOKEN_FILE": vars["DOCKFLEET_AGENT_TOKEN_FILE"], "DOCKFLEET_NODE_ID": "node-1"}))
	if err != nil || a.Token != "agent-token-from-a-file-0123" {
		t.Fatalf("agent token = %q, %v", a.Token, err)
	}

	// The variable alone still works, as before.
	if c, err := LoadManager(getenv(managerEnv())); err != nil || c.APIToken != "api-token-0123456789" {
		t.Fatalf("credentials from the environment = %+v, %v", c, err)
	}
}

func TestCredentialFilesAreChecked(t *testing.T) {
	dir := t.TempDir()
	link := filepath.Join(dir, "link")
	target := writeSecret(t, "target", "api-token-via-a-symlink-0123")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if c, err := LoadManager(getenv(withFiles(managerEnv(), "DOCKFLEET_API_TOKEN", link))); err != nil || c.APIToken != "api-token-via-a-symlink-0123" {
		t.Fatalf("a symlink to a secret (how Kubernetes mounts them) = %q, %v", c.APIToken, err)
	}

	tests := []struct {
		name    string
		mutate  func(map[string]string)
		wantErr string
	}{
		{"both the variable and the file", func(m map[string]string) { m["DOCKFLEET_API_TOKEN_FILE"] = target }, "both set"},
		{"a relative path", func(m map[string]string) {
			delete(m, "DOCKFLEET_API_TOKEN")
			m["DOCKFLEET_API_TOKEN_FILE"] = "secrets/api"
		}, "absolute"},
		{"a missing file", func(m map[string]string) {
			delete(m, "DOCKFLEET_API_TOKEN")
			m["DOCKFLEET_API_TOKEN_FILE"] = filepath.Join(dir, "missing")
		}, "DOCKFLEET_API_TOKEN_FILE"},
		{"a directory", func(m map[string]string) { delete(m, "DOCKFLEET_API_TOKEN"); m["DOCKFLEET_API_TOKEN_FILE"] = dir }, "not a regular file"},
		{"an empty file", func(m map[string]string) {
			delete(m, "DOCKFLEET_API_TOKEN")
			m["DOCKFLEET_API_TOKEN_FILE"] = writeSecret(t, "empty", " \n")
		}, "is empty"},
		{"a file too large to be a credential", func(m map[string]string) {
			delete(m, "DOCKFLEET_API_TOKEN")
			m["DOCKFLEET_API_TOKEN_FILE"] = writeSecret(t, "big", strings.Repeat("x", maxSecretFile+1))
		}, "larger than"},
		{"a token from a file that is too short", func(m map[string]string) {
			delete(m, "DOCKFLEET_API_TOKEN")
			m["DOCKFLEET_API_TOKEN_FILE"] = writeSecret(t, "short", "short")
		}, "at least"},
		{"the same token for both roles", func(m map[string]string) {
			delete(m, "DOCKFLEET_API_TOKEN")
			delete(m, "DOCKFLEET_AGENT_TOKEN")
			m["DOCKFLEET_API_TOKEN_FILE"] = target
			m["DOCKFLEET_AGENT_TOKEN_FILE"] = target
		}, "must differ"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vars := managerEnv()
			tt.mutate(vars)
			_, err := LoadManager(getenv(vars))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want one mentioning %q", err, tt.wantErr)
			}
		})
	}
}

// The error for a bad credential file names the file and never what it holds.
func TestCredentialFileErrorsDoNotLeakTheContent(t *testing.T) {
	const leak = "super-secret-value-must-not-appear-0123456789"
	vars := managerEnv()
	delete(vars, "DOCKFLEET_API_TOKEN")
	vars["DOCKFLEET_API_TOKEN_FILE"] = writeSecret(t, "tooshort", leak[:10])
	vars["DOCKFLEET_AGENT_TOKEN_FILE"] = writeSecret(t, "agent", leak)
	delete(vars, "DOCKFLEET_AGENT_TOKEN")
	vars["DOCKFLEET_AGENT_TOKEN"] = leak // and one in the variable, together with its file
	_, err := LoadManager(getenv(vars))
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), leak) || strings.Contains(err.Error(), leak[:10]) {
		t.Fatalf("the error shows a credential: %v", err)
	}
}

func withFiles(m map[string]string, name, file string) map[string]string {
	delete(m, name)
	m[name+"_FILE"] = file
	return m
}

func TestAgentHealthAddress(t *testing.T) {
	for _, tt := range []struct{ value, want string }{
		{"", DefaultAgentHealthAddr},
		{"  ", DefaultAgentHealthAddr},
		{"127.0.0.1:21290", "127.0.0.1:21290"},
		{"[::1]:21290", "[::1]:21290"},
		{"off", ""},
	} {
		vars := agentEnv()
		vars["DOCKFLEET_HEALTH_ADDR"] = tt.value
		c, err := LoadAgent(getenv(vars))
		if err != nil || c.HealthAddr != tt.want || AgentHealthAddr(getenv(vars)) != tt.want {
			t.Errorf("DOCKFLEET_HEALTH_ADDR=%q: HealthAddr = %q, %v; want %q", tt.value, c.HealthAddr, err, tt.want)
		}
	}
	// The endpoint is for the machine the agent runs on: it is never opened to the network.
	for _, bad := range []string{"0.0.0.0:21282", ":21282", "10.0.0.5:21282", "example.com:21282", "[::]:21282"} {
		vars := agentEnv()
		vars["DOCKFLEET_HEALTH_ADDR"] = bad
		if _, err := LoadAgent(getenv(vars)); err == nil || !strings.Contains(err.Error(), "DOCKFLEET_HEALTH_ADDR") {
			t.Errorf("DOCKFLEET_HEALTH_ADDR=%q: error = %v, want it refused", bad, err)
		}
	}
}
