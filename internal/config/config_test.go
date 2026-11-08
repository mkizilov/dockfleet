package config

import (
	"fmt"
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
