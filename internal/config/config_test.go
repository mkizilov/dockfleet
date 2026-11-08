package config

import (
	"strings"
	"testing"
	"time"
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
		{"short agent token", func(m map[string]string) { m["DOCKFLEET_AGENT_TOKEN"] = "short" }, "DOCKFLEET_AGENT_TOKEN"},
		{"same tokens", func(m map[string]string) { m["DOCKFLEET_AGENT_TOKEN"] = m["DOCKFLEET_API_TOKEN"] }, "must differ"},
		{"missing database url", func(m map[string]string) { delete(m, "DOCKFLEET_DATABASE_URL") }, "DOCKFLEET_DATABASE_URL"},
		{"bad store", func(m map[string]string) { m["DOCKFLEET_STORE"] = "sqlite" }, "DOCKFLEET_STORE"},
		{"public bind without tls", func(m map[string]string) { m["DOCKFLEET_API_ADDR"] = "0.0.0.0:8080" }, "loopback"},
		{"empty host bind without tls", func(m map[string]string) { m["DOCKFLEET_GRPC_ADDR"] = ":9000" }, "loopback"},
		{"cert without key", func(m map[string]string) { m["DOCKFLEET_TLS_CERT_FILE"] = "c.pem" }, "set together"},
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

func TestLoadManagerAllowsPublicBindWithTLSAndMemoryStore(t *testing.T) {
	vars := managerEnv()
	delete(vars, "DOCKFLEET_DATABASE_URL")
	vars["DOCKFLEET_STORE"] = "memory"
	vars["DOCKFLEET_API_ADDR"] = "0.0.0.0:8443"
	vars["DOCKFLEET_TLS_CERT_FILE"] = "c.pem"
	vars["DOCKFLEET_TLS_KEY_FILE"] = "k.pem"
	if _, err := LoadManager(getenv(vars)); err != nil {
		t.Fatal(err)
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

func TestLoadAgentRemoteManagerWithCA(t *testing.T) {
	vars := agentEnv()
	vars["DOCKFLEET_MANAGER_ADDR"] = "manager.example.com:443"
	vars["DOCKFLEET_TLS_CA_FILE"] = "ca.pem"
	if _, err := LoadAgent(getenv(vars)); err != nil {
		t.Fatal(err)
	}
}
