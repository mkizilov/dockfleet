//go:build e2e

package e2e

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/pki"
)

// The admin API over TLS: every route but the two health checks needs the
// token, whatever the request looks like otherwise, and the endpoint accepts
// nothing but TLS 1.3.
func TestAdminAPIRefusesWhatIsNotAuthenticatedTLS(t *testing.T) {
	c := newCluster(t, clusterOpts{manualOnly: true})
	m := c.managers[0]
	for _, path := range []string{"/v1/nodes", "/v1/services", "/v1/workloads", "/v1/events", "/v1/enrollments", "/v1/cluster", "/metrics", "/v1/services/web/logs"} {
		for _, token := range []string{"", "wrong-token-0123456789-abcdef", apiToken + "x", strings.ToUpper(apiToken)} {
			anon := newAPIClient(t, m.api, c.roots, token)
			if code, raw, err := anon.try(http.MethodGet, path, ""); err != nil || code != http.StatusUnauthorized {
				t.Errorf("GET %s with token %q = %d %v %s, want 401", path, token, code, err, raw)
			}
		}
	}
	anon := newAPIClient(t, m.api, c.roots, "")
	for _, path := range []string{"/healthz", "/readyz"} {
		if code, _, err := anon.try(http.MethodGet, path, ""); err != nil || code != http.StatusOK {
			t.Errorf("GET %s = %d %v, want 200 without a token", path, code, err)
		}
	}
	// The dashboard sends an anonymous visitor to log in; it shows nothing.
	if code, raw, err := anon.try(http.MethodGet, "/dashboard", ""); err != nil || code == http.StatusOK || strings.Contains(string(raw), "node-") {
		t.Errorf("GET /dashboard without a login = %d %v", code, err)
	}
	// A token in the query string is not a token.
	if code, _, _ := anon.try(http.MethodGet, "/v1/nodes?token="+apiToken, ""); code != http.StatusUnauthorized {
		t.Errorf("a token in the query = %d, want 401", code)
	}

	// TLS only: plain HTTP to the TLS port gets nothing, and TLS 1.2 is refused.
	resp, err := http.Get("http://" + m.api + "/v1/nodes")
	if err == nil {
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK || strings.Contains(string(raw), `"nodes"`) {
			t.Errorf("plain HTTP to the TLS port was answered: %d %s", resp.StatusCode, raw)
		}
	}
	conn, err := tls.Dial("tcp", m.api, &tls.Config{RootCAs: c.roots, ServerName: managerTLSDNS, MaxVersion: tls.VersionTLS12})
	if err == nil {
		_ = conn.Close()
		t.Error("the admin API accepted TLS 1.2")
	}
	// A client that does not trust the CA cannot talk to it either.
	other := x509.NewCertPool()
	if conn, err := tls.Dial("tcp", m.api, &tls.Config{RootCAs: other, ServerName: managerTLSDNS}); err == nil {
		_ = conn.Close()
		t.Error("a client with no trust in the CA connected")
	}
}

// probeGRPC opens a connection to the agents' port with the given TLS
// configuration and says what the server did with it: refused it (during or
// right after the handshake, where TLS 1.3 reports a rejected client
// certificate) or kept it open waiting for the HTTP/2 that follows.
func probeGRPC(addr string, cfg *tls.Config) (kept bool, err error) {
	d := net.Dialer{Timeout: 3 * time.Second}
	conn, err := tls.DialWithDialer(&d, "tcp", addr, cfg)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(1500 * time.Millisecond))
	n, err := conn.Read(make([]byte, 1))
	var ne net.Error
	switch {
	case n > 0:
		return true, nil // the server spoke: its HTTP/2 settings, to a client it admitted
	case errors.As(err, &ne) && ne.Timeout():
		return true, nil // nothing was said: the server is waiting for us
	}
	return false, err
}

// The agents' port admits only a client whose certificate chains to the CA the
// manager trusts, speaks TLS 1.3, and is not plaintext.
func TestAgentPortRequiresMutualTLS(t *testing.T) {
	c := newCluster(t, clusterOpts{manualOnly: true})
	m := c.managers[0]
	c.pki("issue-agent", "-dir", c.pkiDir, "-node-id", "probe")
	certFile, keyFile := c.agentCert("probe")
	good, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}

	// A CA that is not the one the manager trusts, and a certificate from it for the same node ID.
	otherCA, err := pki.NewCA("another CA", time.Now().Add(-time.Hour), time.Now().Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	issued, err := otherCA.IssueNode(pki.NodeRequest{NodeID: "probe", NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := tls.X509KeyPair(issued.CertPEM, issued.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}

	base := func() *tls.Config {
		return &tls.Config{RootCAs: c.roots, ServerName: managerTLSDNS, MinVersion: tls.VersionTLS13, NextProtos: []string{"h2"}}
	}
	t.Run("a certificate from the trusted CA is admitted", func(t *testing.T) {
		cfg := base()
		cfg.Certificates = []tls.Certificate{good}
		if kept, err := probeGRPC(m.grpc, cfg); !kept {
			t.Fatalf("the positive control was refused: %v", err)
		}
	})
	for name, mutate := range map[string]func(*tls.Config){
		"no client certificate":         func(cfg *tls.Config) {},
		"a certificate from another CA": func(cfg *tls.Config) { cfg.Certificates = []tls.Certificate{foreign} },
		"TLS 1.2": func(cfg *tls.Config) {
			cfg.Certificates = []tls.Certificate{good}
			cfg.MinVersion, cfg.MaxVersion = tls.VersionTLS12, tls.VersionTLS12
		},
		"a server certificate for another name": func(cfg *tls.Config) {
			cfg.Certificates = []tls.Certificate{good}
			cfg.ServerName = "other.example.test"
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := base()
			mutate(cfg)
			if kept, err := probeGRPC(m.grpc, cfg); kept {
				t.Fatal("the connection was admitted")
			} else if err == nil {
				t.Fatal("the connection ended without saying why")
			}
		})
	}
	t.Run("plaintext", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", m.grpc, 3*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		_, _ = conn.Write([]byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"))
		reply, _ := io.ReadAll(conn)
		if strings.HasPrefix(string(reply), "HTTP/") || strings.Contains(string(reply), "nodes") {
			t.Fatalf("plaintext was answered: %q", reply)
		}
	})
}

// What the agent refuses to do, what the manager refuses to admit, and that no
// credential reaches a log.
func TestAgentAdmissionAndRefusals(t *testing.T) {
	c := newCluster(t, clusterOpts{
		manualOnly: true,
		nodes:      []nodeSpec{{id: "node-1"}, {id: "node-x", unenrolled: true}},
	})
	api := c.managers[0].client
	newAgent := func(index int, n nodeSpec, extra ...string) *agentProc {
		a := c.newAgent(n, index)
		a.extra = extra
		c.agents[n.id] = a
		return a
	}

	// A valid certificate is not enough: the node has to be enrolled. Until it is, the agent
	// keeps trying (and says why); enrolling it needs no restart.
	x := newAgent(1, nodeSpec{id: "node-x"})
	x.start()
	x.waitNotReady(10 * time.Second)
	eventually(t, 20*time.Second, "the agent to say it is not enrolled", func() (bool, string) {
		return strings.Contains(x.p.logs(), "not enrolled"), tailLines(x.p.logs(), 5)
	})
	if _, ok := api.nodeHealth("node-x"); ok {
		t.Error("a node that is not enrolled has a record")
	}
	c.enroll("node-x")
	x.waitReady(60 * time.Second)
	api.waitNode(20*time.Second, "node-x", "healthy")
	api.must(http.MethodDelete, "/v1/enrollments/node-x", "", http.StatusOK, nil)
	x.waitNotReady(20 * time.Second)
	if !x.p.running() { // revoking an enrollment makes the agent wait, not exit
		t.Errorf("the agent exited after its enrollment was revoked: %d", x.p.exitCode())
	}
	x.p.terminate(5 * time.Second)

	// An agent whose certificate is for another node refuses to start.
	cert, key := c.agentCert("node-x")
	wrongID := newAgent(2, nodeSpec{id: "node-1"}, "DOCKFLEET_TLS_CERT_FILE="+cert, "DOCKFLEET_TLS_KEY_FILE="+key)
	wrongID.start()
	if !wrongID.p.wait(15 * time.Second) {
		t.Fatal("an agent started with the certificate of another node")
	}
	if code := wrongID.p.exitCode(); code == 0 || !strings.Contains(wrongID.p.logs(), "must match") {
		t.Errorf("exit %d: %s", code, wrongID.p.logs())
	}

	// An agent that does not trust the manager's CA never registers, and does not fall back to plaintext.
	c.pki("init-ca", "-dir", filepath.Join(c.dir, "otherpki"))
	stranger := newAgent(1, nodeSpec{id: "node-1"}, "DOCKFLEET_TLS_CA_FILE="+filepath.Join(c.dir, "otherpki", "ca.pem"))
	stranger.start()
	time.Sleep(2 * time.Second)
	if code, _ := stranger.readyz(); code == http.StatusOK {
		t.Error("an agent that does not trust the manager's CA is ready")
	}
	if _, ok := api.nodeHealth("node-1"); ok {
		t.Error("an agent that does not trust the manager registered")
	}
	stranger.p.terminate(5 * time.Second)

	// A private key that others can read is refused.
	_, keyOf1 := c.agentCert("node-1")
	if err := os.Chmod(keyOf1, 0o644); err != nil {
		t.Fatal(err)
	}
	loose := newAgent(1, nodeSpec{id: "node-1"})
	loose.start()
	if !loose.p.wait(15 * time.Second) {
		t.Fatal("an agent started with a world-readable private key")
	}
	if loose.p.exitCode() == 0 {
		t.Errorf("exit 0 with a world-readable private key: %s", loose.p.logs())
	}
	if err := os.Chmod(keyOf1, 0o600); err != nil {
		t.Fatal(err)
	}

	// Configuration the code refuses to run with: a manager on a public address without TLS.
	cmd := exec.Command(filepath.Join(binDir, "dockfleet-manager"))
	cmd.Env = append(baseEnv(), "DOCKFLEET_API_TOKEN="+apiToken, "DOCKFLEET_STORE=memory", "DOCKFLEET_AGENT_TOKEN=agent-token-0123456789-xyz",
		"DOCKFLEET_API_ADDR=0.0.0.0:21279", "DOCKFLEET_GRPC_ADDR=127.0.0.1:21279")
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "not a loopback address") {
		t.Errorf("a manager on 0.0.0.0 without TLS: %v\n%s", err, out)
	}

	// No credential is ever logged: not the admin token, not the database password.
	logs, _ := filepath.Glob(filepath.Join(c.dir, "*.log"))
	if len(logs) < 3 {
		t.Fatalf("only %d log files to search", len(logs))
	}
	for _, f := range logs {
		raw := readFile(t, f)
		for name, secret := range map[string]string{"the admin token": apiToken, "the database password": dbCanary} {
			if strings.Contains(string(raw), secret) {
				t.Errorf("%s appears in %s", name, filepath.Base(f))
			}
		}
		if strings.Contains(string(raw), "PRIVATE KEY") {
			t.Errorf("key material appears in %s", filepath.Base(f))
		}
	}
}
