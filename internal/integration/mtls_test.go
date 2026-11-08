package integration

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dockfleet/dockfleet/internal/pki"
	"github.com/dockfleet/dockfleet/internal/protocol"
	dockfleetv1 "github.com/dockfleet/dockfleet/internal/protocol/dockfleet/v1"
	"github.com/dockfleet/dockfleet/internal/scheduler"
	"github.com/dockfleet/dockfleet/internal/store"
)

// testPKI is a throwaway certificate authority with credentials on disk.
type testPKI struct {
	t      *testing.T
	ca     *pki.CA
	dir    string
	caFile string
	seq    int
}

func newTestPKI(t *testing.T) *testPKI {
	t.Helper()
	now := time.Now()
	ca, err := pki.NewCA("Integration CA", now.Add(-time.Hour), now.Add(240*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	p := &testPKI{t: t, ca: ca, dir: t.TempDir()}
	p.caFile = filepath.Join(p.dir, "ca.pem")
	if err := os.WriteFile(p.caFile, ca.CertPEM(), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func (p *testPKI) paths(name string) (certFile, keyFile string) {
	p.seq++
	return filepath.Join(p.dir, fmt.Sprintf("%s-%d.pem", name, p.seq)), filepath.Join(p.dir, fmt.Sprintf("%s-%d.key", name, p.seq))
}

// serverFiles issues a manager certificate valid for localhost.
func (p *testPKI) serverFiles() (certFile, keyFile string) {
	p.t.Helper()
	now := time.Now()
	issued, err := p.ca.IssueServer(pki.ServerRequest{
		Name:      "manager",
		DNSNames:  []string{"localhost"},
		NotBefore: now.Add(-time.Minute),
		NotAfter:  now.Add(24 * time.Hour),
	})
	if err != nil {
		p.t.Fatal(err)
	}
	certFile, keyFile = p.paths("manager")
	if err := issued.Save(certFile, keyFile, pki.WriteOptions{}); err != nil {
		p.t.Fatal(err)
	}
	return certFile, keyFile
}

// nodeFiles issues a node certificate with an explicit validity window.
func (p *testPKI) nodeFiles(nodeID string, notBefore, notAfter time.Time) (certFile, keyFile string) {
	p.t.Helper()
	issued, err := p.ca.IssueNode(pki.NodeRequest{NodeID: nodeID, NotBefore: notBefore, NotAfter: notAfter})
	if err != nil {
		p.t.Fatal(err)
	}
	certFile, keyFile = p.paths(nodeID)
	if err := issued.Save(certFile, keyFile, pki.WriteOptions{}); err != nil {
		p.t.Fatal(err)
	}
	return certFile, keyFile
}

// validNode issues a node certificate that is valid now.
func (p *testPKI) validNode(nodeID string) (certFile, keyFile string) {
	p.t.Helper()
	now := time.Now()
	return p.nodeFiles(nodeID, now.Add(-time.Minute), now.Add(24*time.Hour))
}

// serverTLS is the manager's mutual-TLS configuration, trusting this CA for clients.
func (p *testPKI) serverTLS() *tls.Config {
	p.t.Helper()
	certFile, keyFile := p.serverFiles()
	cfg, err := pki.ServerTLS(pki.ServerConfig{CertFile: certFile, KeyFile: keyFile, ClientCAFile: p.caFile})
	if err != nil {
		p.t.Fatal(err)
	}
	return cfg
}

// clientTLS builds an agent configuration from certificate files. Tests tweak
// it through mutate to model misconfigurations.
func (p *testPKI) clientTLS(certFile, keyFile string, mutate func(*pki.ClientConfig)) *tls.Config {
	p.t.Helper()
	cfg := pki.ClientConfig{CAFile: p.caFile, CertFile: certFile, KeyFile: keyFile, ServerName: "localhost"}
	if mutate != nil {
		mutate(&cfg)
	}
	tlsConfig, err := pki.ClientTLS(cfg)
	if err != nil {
		p.t.Fatal(err)
	}
	return tlsConfig
}

func withTLS(c *tls.Config) func(*protocol.ClientConfig) {
	return func(cfg *protocol.ClientConfig) {
		cfg.TLS = c
		cfg.Token = ""
	}
}

// newMutualTLSManager starts a manager whose gRPC listener requires client certificates.
func newMutualTLSManager(t *testing.T, p *testPKI) *manager {
	t.Helper()
	m := newManager(t, nil)
	m.serverTLS = p.serverTLS()
	m.logs = &logCounter{}
	m.start()
	return m
}

func callCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// requireRejectedAtTransport checks that a call failed before reaching any
// handler: the TLS handshake was refused, which gRPC reports as Unavailable.
func requireRejectedAtTransport(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("the connection must be rejected")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %v (%v), want Unavailable from a failed handshake", status.Code(err), err)
	}
	if protocol.ErrorCode(err) != dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_UNSPECIFIED {
		t.Fatalf("a refused handshake must not produce a protocol error: %v", err)
	}
}

func TestMutualTLSTrustedNodeRegistersAndReceivesWork(t *testing.T) {
	p := newTestPKI(t)
	m := newMutualTLSManager(t, p)
	certFile, keyFile := p.validNode("sim-1")
	sim := m.startAgent("sim-1", withTLS(p.clientTLS(certFile, keyFile, nil)))

	waitFor(t, "registration over mutual TLS", func() bool { return m.sessionID("sim-1") != "" })
	if _, err := m.store.CreateWorkload(context.Background(), store.Workload{
		ID: "w1", Name: "web", Image: "nginx", CPUMillis: 100, MemoryBytes: 64 << 20, NodeID: "sim-1", CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the workload to be reported back over mutual TLS", func() bool {
		w, err := m.store.GetWorkload(context.Background(), "w1")
		return err == nil && w.Phase() == store.WorkloadRunning && w.Observed.AckedGeneration == 1
	})
	n, ok := m.node("sim-1")
	if !ok || !scheduler.Ready(n, time.Now(), time.Minute) {
		t.Fatalf("node should be ready: %+v", n)
	}
	if err := sim.stop(); err != nil {
		t.Fatalf("Run returned %v after a requested shutdown", err)
	}
}

func TestMutualTLSBindsRequestsToTheCertificateIdentity(t *testing.T) {
	p := newTestPKI(t)
	m := newMutualTLSManager(t, p)
	certFile, keyFile := p.validNode("sim-1")
	client := m.dial(withTLS(p.clientTLS(certFile, keyFile, nil)))

	// sim-2 is enrolled, but this certificate speaks for sim-1 only.
	_, err := client.Register(callCtx(t), nodeInfo("sim-2"))
	if !protocol.IsIdentityMismatch(err) || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Register as another node: error = %v, want PERMISSION_DENIED / IDENTITY_MISMATCH", err)
	}
	if protocol.NeedsRegistration(err) || !protocol.IsPermanent(err) {
		t.Errorf("an identity mismatch is a permanent failure: %v", err)
	}

	session, err := client.Register(callCtx(t), nodeInfo("sim-1"))
	if err != nil {
		t.Fatalf("Register as itself: %v", err)
	}
	if _, err := client.Heartbeat(callCtx(t), "sim-2", session.ID, nil); !protocol.IsIdentityMismatch(err) {
		t.Errorf("Heartbeat for another node: error = %v, want IDENTITY_MISMATCH", err)
	}
	if err := client.Deregister(callCtx(t), "sim-2", session.ID); !protocol.IsIdentityMismatch(err) {
		t.Errorf("Deregister for another node: error = %v, want IDENTITY_MISMATCH", err)
	}
	if _, ok := m.node("sim-2"); ok {
		t.Error("a node must not be stored on behalf of another identity")
	}
	if m.sessionID("sim-2") != "" {
		t.Error("a node must not get a session on behalf of another identity")
	}
	if m.logs.count("request names a different node than the client certificate") < 3 {
		t.Errorf("mismatches must be logged, got %d", m.logs.count("request names a different node than the client certificate"))
	}
}

func TestMutualTLSAgentWithTheWrongIdentityStops(t *testing.T) {
	p := newTestPKI(t)
	m := newMutualTLSManager(t, p)
	certFile, keyFile := p.validNode("sim-1")
	// The agent is configured as sim-2 but holds sim-1's certificate.
	sim := m.startAgent("sim-2", withTLS(p.clientTLS(certFile, keyFile, nil)))
	if err := sim.wait(); err == nil || !protocol.IsIdentityMismatch(err) {
		t.Fatalf("Run returned %v, want an IDENTITY_MISMATCH error", err)
	}
	if len(m.sessions()) != 0 {
		t.Errorf("no session may exist: %+v", m.sessions())
	}
}

func TestMutualTLSOnlyEnrolledNodesMayRegister(t *testing.T) {
	p := newTestPKI(t)
	m := newMutualTLSManager(t, p)
	certFile, keyFile := p.validNode("stranger")
	tlsConfig := p.clientTLS(certFile, keyFile, nil)

	_, err := m.dial(withTLS(tlsConfig)).Register(callCtx(t), nodeInfo("stranger"))
	if !protocol.IsNotEnrolled(err) || status.Code(err) != codes.PermissionDenied || protocol.IsPermanent(err) {
		t.Fatalf("Register: error = %v, want a retryable PERMISSION_DENIED / NOT_ENROLLED", err)
	}
	if _, ok := m.node("stranger"); ok {
		t.Fatal("an unenrolled node must not be stored")
	}

	// A valid certificate is not enough: the agent keeps retrying until an administrator enrolls it.
	sim := m.startAgent("stranger", withTLS(tlsConfig))
	waitFor(t, "repeated rejections", func() bool { return m.logs.count("rejected a node that is not enrolled") >= 3 })
	if !sim.running() || len(m.sessions()) != 0 {
		t.Fatalf("the agent must keep retrying without a session (running=%v)", sim.running())
	}

	m.enroll("stranger")
	waitFor(t, "registration after enrollment", func() bool { return m.sessionID("stranger") != "" })
}

func TestMutualTLSRevocationEndsTheSession(t *testing.T) {
	p := newTestPKI(t)
	m := newMutualTLSManager(t, p)
	certFile, keyFile := p.validNode("sim-1")
	sim := m.startAgent("sim-1", withTLS(p.clientTLS(certFile, keyFile, nil)))
	waitFor(t, "registration", func() bool { return m.sessionID("sim-1") != "" })

	if _, err := m.store.RevokeEnrollment(context.Background(), "sim-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the session to end at the next heartbeat", func() bool { return m.sessionID("sim-1") == "" })
	waitFor(t, "the revoked agent to be turned away", func() bool { return m.logs.count("rejected a node that is not enrolled") >= 2 })
	if !sim.running() {
		t.Fatalf("a revoked agent keeps retrying instead of exiting: %v", sim.result)
	}
	if n, _ := m.node("sim-1"); scheduler.Ready(n, time.Now().Add(time.Second), 500*time.Millisecond) {
		t.Errorf("a revoked node must stop refreshing its liveness: %+v", n)
	}

	if _, err := m.store.CreateEnrollment(context.Background(), store.Enrollment{NodeID: "sim-1", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "registration after re-enrollment", func() bool { return m.sessionID("sim-1") != "" })
}

func TestMutualTLSRejectsCertificatesFromAnUntrustedCA(t *testing.T) {
	p, rogue := newTestPKI(t), newTestPKI(t)
	m := newMutualTLSManager(t, p)
	// A perfectly formed certificate for an enrolled node, signed by someone else.
	certFile, keyFile := rogue.validNode("sim-1")
	tlsConfig := p.clientTLS(certFile, keyFile, nil)

	_, err := m.dial(withTLS(tlsConfig)).Register(callCtx(t), nodeInfo("sim-1"))
	requireRejectedAtTransport(t, err)
	if _, ok := m.node("sim-1"); ok || len(m.sessions()) != 0 {
		t.Fatal("nothing may be registered over an untrusted certificate")
	}

	sim := m.startAgent("sim-1", withTLS(tlsConfig))
	waitFor(t, "several rejected attempts", func() bool { return m.dialsTry.Load() >= 3 })
	if !sim.running() || len(m.sessions()) != 0 {
		t.Fatalf("the agent keeps trying, unregistered (running=%v)", sim.running())
	}
}

func TestMutualTLSClientRejectsAnUntrustedManager(t *testing.T) {
	p, rogue := newTestPKI(t), newTestPKI(t)
	m := newManager(t, nil)
	// The manager presents a certificate signed by a CA the agents do not trust.
	serverCert, serverKey := rogue.serverFiles()
	serverTLS, err := pki.ServerTLS(pki.ServerConfig{CertFile: serverCert, KeyFile: serverKey, ClientCAFile: p.caFile})
	if err != nil {
		t.Fatal(err)
	}
	m.serverTLS = serverTLS
	m.start()

	certFile, keyFile := p.validNode("sim-1")
	_, err = m.dial(withTLS(p.clientTLS(certFile, keyFile, nil))).Register(callCtx(t), nodeInfo("sim-1"))
	requireRejectedAtTransport(t, err)
	if !strings.Contains(strings.ToLower(err.Error()), "certificate") {
		t.Errorf("the failure should name the certificate problem: %v", err)
	}
	if _, ok := m.node("sim-1"); ok {
		t.Error("an agent must not talk to a manager it cannot verify")
	}
}

func TestMutualTLSClientChecksTheManagerName(t *testing.T) {
	p := newTestPKI(t)
	m := newMutualTLSManager(t, p)
	certFile, keyFile := p.validNode("sim-1")
	tlsConfig := p.clientTLS(certFile, keyFile, func(c *pki.ClientConfig) { c.ServerName = "evil.example.com" })

	_, err := m.dial(withTLS(tlsConfig)).Register(callCtx(t), nodeInfo("sim-1"))
	requireRejectedAtTransport(t, err)
	if !strings.Contains(strings.ToLower(err.Error()), "certificate") {
		t.Errorf("the failure should name the certificate problem: %v", err)
	}
}

func TestMutualTLSRejectsExpiredAndNotYetValidCertificates(t *testing.T) {
	p := newTestPKI(t)
	m := newMutualTLSManager(t, p)
	now := time.Now()
	tests := map[string][2]time.Time{
		"expired":       {now.Add(-3 * time.Hour), now.Add(-time.Hour)},
		"not yet valid": {now.Add(time.Hour), now.Add(2 * time.Hour)},
	}
	for name, window := range tests {
		t.Run(name, func(t *testing.T) {
			certFile, keyFile := p.nodeFiles("sim-1", window[0], window[1])
			client := m.dial(withTLS(p.clientTLS(certFile, keyFile, nil)))
			_, err := client.Register(callCtx(t), nodeInfo("sim-1"))
			requireRejectedAtTransport(t, err)
			if len(m.sessions()) != 0 {
				t.Fatalf("no session may be created: %+v", m.sessions())
			}
		})
	}
}

func TestMutualTLSCertificateExpiringOnAnOpenConnectionIsRejected(t *testing.T) {
	p := newTestPKI(t)
	m := newMutualTLSManager(t, p)
	certFile, keyFile := p.validNode("sim-1")
	client := m.dial(withTLS(p.clientTLS(certFile, keyFile, nil)))

	session, err := client.Register(callCtx(t), nodeInfo("sim-1"))
	if err != nil {
		t.Fatal(err)
	}
	// The connection stays open while the server's clock passes the certificate's expiry.
	m.skew.Store(int64(48 * time.Hour))
	_, err = client.Heartbeat(callCtx(t), "sim-1", session.ID, nil)
	if !protocol.IsCertificateInvalid(err) || status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Heartbeat past expiry: error = %v, want UNAUTHENTICATED / CERTIFICATE_INVALID", err)
	}
	if protocol.IsPermanent(err) {
		t.Error("an expired certificate is fixed by renewing it, so the agent must keep running")
	}

	m.skew.Store(0)
	if _, err := client.Heartbeat(callCtx(t), "sim-1", session.ID, nil); err != nil {
		t.Fatalf("Heartbeat with a valid clock: %v", err)
	}
}

func TestMutualTLSRequiresAClientCertificate(t *testing.T) {
	p := newTestPKI(t)
	m := newMutualTLSManager(t, p)
	roots := x509.NewCertPool()
	roots.AddCert(p.ca.Cert)
	anonymous := &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    roots,
		ServerName: "localhost",
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return &tls.Certificate{}, nil
		},
	}
	_, err := m.dial(withTLS(anonymous)).Register(callCtx(t), nodeInfo("sim-1"))
	requireRejectedAtTransport(t, err)

	// A server certificate is not an acceptable client certificate either.
	serverCert, serverKey := p.serverFiles()
	_, err = m.dial(withTLS(p.clientTLS(serverCert, serverKey, nil))).Register(callCtx(t), nodeInfo("sim-1"))
	requireRejectedAtTransport(t, err)
	if len(m.sessions()) != 0 {
		t.Fatalf("no session may be created: %+v", m.sessions())
	}
}

func TestNoPlaintextFallbackBetweenMismatchedPeers(t *testing.T) {
	p := newTestPKI(t)

	t.Run("plaintext agent against a mutual TLS manager", func(t *testing.T) {
		m := newMutualTLSManager(t, p)
		_, err := m.dial(nil).Register(callCtx(t), nodeInfo("sim-1"))
		requireRejectedAtTransport(t, err)
		if _, ok := m.node("sim-1"); ok || len(m.sessions()) != 0 {
			t.Fatal("nothing may be registered over plaintext")
		}
	})

	t.Run("mutual TLS agent against a plaintext manager", func(t *testing.T) {
		m := newManager(t, nil)
		m.start()
		certFile, keyFile := p.validNode("sim-1")
		_, err := m.dial(withTLS(p.clientTLS(certFile, keyFile, nil))).Register(callCtx(t), nodeInfo("sim-1"))
		requireRejectedAtTransport(t, err)
		if _, ok := m.node("sim-1"); ok {
			t.Fatal("a TLS agent must not downgrade to plaintext")
		}
	})

	t.Run("remote plaintext is refused before dialing", func(t *testing.T) {
		_, err := protocol.Dial(protocol.ClientConfig{Addr: "203.0.113.7:21281", Token: agentToken})
		if err != protocol.ErrInsecureRemote {
			t.Fatalf("Dial error = %v, want ErrInsecureRemote", err)
		}
	})
}

func TestMutualTLSRenewedCertificateIsUsedOnReconnect(t *testing.T) {
	p := newTestPKI(t)
	m := newMutualTLSManager(t, p)
	certFile, keyFile := p.validNode("sim-1")
	client := m.dial(withTLS(p.clientTLS(certFile, keyFile, nil)))
	if _, err := client.Register(callCtx(t), nodeInfo("sim-1")); err != nil {
		t.Fatalf("Register with the original certificate: %v", err)
	}

	// Renew in place, as an administrator would: new certificate and key at the same paths.
	now := time.Now()
	renewed, err := p.ca.IssueNode(pki.NodeRequest{NodeID: "sim-2", NotBefore: now.Add(-time.Minute), NotAfter: now.Add(24 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if err := renewed.Save(certFile, keyFile, pki.WriteOptions{Overwrite: true}); err != nil {
		t.Fatal(err)
	}
	later := now.Add(time.Minute)
	for _, f := range []string{certFile, keyFile} {
		if err := os.Chtimes(f, later, later); err != nil {
			t.Fatal(err)
		}
	}

	// Restarting the manager drops the old connection; the next one handshakes with the renewed files.
	m.restart()
	waitFor(t, "the renewed certificate to be presented", func() bool {
		_, err := client.Register(callCtx(t), nodeInfo("sim-2"))
		return err == nil
	})
	if _, err := client.Register(callCtx(t), nodeInfo("sim-1")); !protocol.IsIdentityMismatch(err) {
		t.Fatalf("the old identity must no longer be presented, error = %v", err)
	}
}

func TestMutualTLSManagerRefusesConfigurationWithoutClientAuthentication(t *testing.T) {
	p := newTestPKI(t)
	serverCert, serverKey := p.serverFiles()
	serverOnly, err := pki.ServerTLS(pki.ServerConfig{CertFile: serverCert, KeyFile: serverKey})
	if err != nil {
		t.Fatal(err)
	}
	node := protocol.NewNodeServer(nil, time.Second, discardLogger())
	if _, err := protocol.NewGRPCServer(node, protocol.ServerOptions{TLS: serverOnly}); err != protocol.ErrClientAuthRequired {
		t.Fatalf("NewGRPCServer error = %v, want ErrClientAuthRequired", err)
	}
}
