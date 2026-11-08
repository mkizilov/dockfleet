package pki

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

type tlsFixture struct {
	ca       *CA
	dir      string
	caFile   string
	certFile string
	keyFile  string
}

// newTLSFixture writes a CA bundle plus a server certificate for localhost.
func newTLSFixture(t *testing.T) *tlsFixture {
	t.Helper()
	now := time.Now()
	ca, err := NewCA("Test CA", now.Add(-time.Hour), now.Add(48*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	f := &tlsFixture{ca: ca, dir: t.TempDir()}
	f.caFile = filepath.Join(f.dir, "ca.pem")
	if err := os.WriteFile(f.caFile, ca.CertPEM(), 0o644); err != nil {
		t.Fatal(err)
	}
	f.certFile, f.keyFile = filepath.Join(f.dir, "manager.pem"), filepath.Join(f.dir, "manager.key")
	f.issueServer(t, WriteOptions{})
	return f
}

func (f *tlsFixture) issueServer(t *testing.T, opts WriteOptions) *Issued {
	t.Helper()
	now := time.Now()
	issued, err := f.ca.IssueServer(ServerRequest{
		Name:      "manager",
		DNSNames:  []string{"localhost"},
		IPs:       []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore: now.Add(-time.Minute),
		NotAfter:  now.Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := issued.Save(f.certFile, f.keyFile, opts); err != nil {
		t.Fatal(err)
	}
	return issued
}

func TestServerTLSWithoutClientAuth(t *testing.T) {
	f := newTLSFixture(t)
	cfg, err := ServerTLS(ServerConfig{CertFile: f.certFile, KeyFile: f.keyFile})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinVersion != tls.VersionTLS13 || cfg.ClientAuth != tls.NoClientCert {
		t.Fatalf("unexpected config: min=%x clientAuth=%v", cfg.MinVersion, cfg.ClientAuth)
	}
	cert, err := cfg.GetCertificate(nil)
	if err != nil || cert.Leaf == nil || cert.Leaf.Subject.CommonName != "manager" {
		t.Fatalf("GetCertificate = %+v, %v", cert, err)
	}
}

func TestServerTLSMutualRequiresAndVerifiesClientCertificates(t *testing.T) {
	f := newTLSFixture(t)
	cfg, err := ServerTLS(ServerConfig{CertFile: f.certFile, KeyFile: f.keyFile, ClientCAFile: f.caFile})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClientAuth != tls.RequireAndVerifyClientCert || cfg.MinVersion != tls.VersionTLS13 {
		t.Fatalf("mutual TLS must require verified client certificates over TLS 1.3: %+v", cfg)
	}
	inner, err := cfg.GetConfigForClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	if inner.ClientAuth != tls.RequireAndVerifyClientCert || inner.ClientCAs == nil || len(inner.Certificates) != 1 || inner.MinVersion != tls.VersionTLS13 {
		t.Fatalf("per-connection config is not mutual TLS: %+v", inner)
	}
	if len(inner.NextProtos) != 1 || inner.NextProtos[0] != "h2" {
		t.Errorf("gRPC needs ALPN h2, got %v", inner.NextProtos)
	}
}

func TestServerTLSRejectsBadInputs(t *testing.T) {
	f := newTLSFixture(t)
	missing := filepath.Join(f.dir, "missing.pem")
	if _, err := ServerTLS(ServerConfig{CertFile: missing, KeyFile: f.keyFile}); err == nil {
		t.Error("a missing certificate must be rejected")
	}
	if _, err := ServerTLS(ServerConfig{CertFile: f.certFile, KeyFile: f.keyFile, ClientCAFile: missing}); err == nil {
		t.Error("a missing client CA bundle must be rejected")
	}
	empty := filepath.Join(f.dir, "empty.pem")
	if err := os.WriteFile(empty, []byte("not a certificate"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ServerTLS(ServerConfig{CertFile: f.certFile, KeyFile: f.keyFile, ClientCAFile: empty}); err == nil || !strings.Contains(err.Error(), "no certificates") {
		t.Errorf("a client CA bundle without certificates must be rejected, got %v", err)
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(f.keyFile, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := ServerTLS(ServerConfig{CertFile: f.certFile, KeyFile: f.keyFile}); err == nil || !strings.Contains(err.Error(), "chmod 600") {
			t.Errorf("a world-readable private key must be rejected, got %v", err)
		}
	}
}

func TestServerCertificateIsReloadedWhenRenewed(t *testing.T) {
	f := newTLSFixture(t)
	cfg, err := ServerTLS(ServerConfig{CertFile: f.certFile, KeyFile: f.keyFile})
	if err != nil {
		t.Fatal(err)
	}
	first, _ := cfg.GetCertificate(nil)
	again, _ := cfg.GetCertificate(nil)
	if first != again {
		t.Error("an unchanged certificate must be served from the cache")
	}

	renewed := f.issueServer(t, WriteOptions{Overwrite: true})
	// Coarse file system timestamps could make old and new files look identical.
	later := time.Now().Add(time.Minute)
	for _, name := range []string{f.certFile, f.keyFile} {
		if err := os.Chtimes(name, later, later); err != nil {
			t.Fatal(err)
		}
	}
	got, err := cfg.GetCertificate(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Leaf.SerialNumber.Cmp(renewed.Cert.SerialNumber) != 0 {
		t.Fatalf("renewed certificate was not picked up: serving serial %v, want %v", got.Leaf.SerialNumber, renewed.Cert.SerialNumber)
	}
}

func TestBrokenRenewalKeepsThePreviousCertificate(t *testing.T) {
	f := newTLSFixture(t)
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	cfg, err := ServerTLS(ServerConfig{CertFile: f.certFile, KeyFile: f.keyFile, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	before, _ := cfg.GetCertificate(nil)

	if err := os.WriteFile(f.certFile, []byte("half written"), 0o644); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		got, err := cfg.GetCertificate(nil)
		if err != nil || got != before {
			t.Fatalf("a failed reload must keep serving the previous certificate: %v, %v", got, err)
		}
	}
	if n := strings.Count(logs.String(), "reload failed"); n != 1 {
		t.Errorf("the failed reload was logged %d times, want once:\n%s", n, logs.String())
	}
}

func TestClientCARotationIsPickedUp(t *testing.T) {
	f := newTLSFixture(t)
	cfg, err := ServerTLS(ServerConfig{CertFile: f.certFile, KeyFile: f.keyFile, ClientCAFile: f.caFile})
	if err != nil {
		t.Fatal(err)
	}
	other := newTestCA(t)
	bundle := append(f.ca.CertPEM(), other.CertPEM()...)
	if err := WriteFile(f.caFile, bundle, 0o644, WriteOptions{Overwrite: true}); err != nil {
		t.Fatal(err)
	}
	inner, err := cfg.GetConfigForClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	want := x509.NewCertPool()
	want.AddCert(f.ca.Cert)
	want.AddCert(other.Cert)
	if !inner.ClientCAs.Equal(want) {
		t.Error("the rotated CA bundle was not picked up")
	}
}

func TestClientTLS(t *testing.T) {
	f := newTLSFixture(t)
	agent := f.ca.mustIssueNode(t, "worker-1")
	agentCert, agentKey := filepath.Join(f.dir, "agent.pem"), filepath.Join(f.dir, "agent.key")
	if err := agent.Save(agentCert, agentKey, WriteOptions{}); err != nil {
		t.Fatal(err)
	}

	cfg, err := ClientTLS(ClientConfig{CAFile: f.caFile, CertFile: agentCert, KeyFile: agentKey, ServerName: "localhost"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinVersion != tls.VersionTLS13 || cfg.RootCAs == nil || cfg.ServerName != "localhost" || cfg.InsecureSkipVerify {
		t.Fatalf("client must verify the manager over TLS 1.3: %+v", cfg)
	}
	cert, err := cfg.GetClientCertificate(nil)
	if err != nil {
		t.Fatal(err)
	}
	if id, err := NodeIDFromCert(cert.Leaf); err != nil || id != "worker-1" {
		t.Fatalf("presented identity = %q, %v", id, err)
	}

	if _, err := ClientTLS(ClientConfig{CAFile: filepath.Join(f.dir, "missing.pem"), CertFile: agentCert, KeyFile: agentKey}); err == nil {
		t.Error("a missing CA file must be rejected")
	}
	if _, err := ClientTLS(ClientConfig{CAFile: f.caFile, CertFile: agentCert, KeyFile: filepath.Join(f.dir, "missing.key")}); err == nil {
		t.Error("a missing client key must be rejected")
	}
}

// mustIssueNode issues a valid-now node certificate for tests that run on the real clock.
func (ca *CA) mustIssueNode(t *testing.T, id string) *Issued {
	t.Helper()
	now := time.Now()
	issued, err := ca.IssueNode(NodeRequest{NodeID: id, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(24 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	return issued
}

func TestInspect(t *testing.T) {
	cert := &x509.Certificate{NotBefore: epoch, NotAfter: epoch.Add(100 * time.Hour)}
	tests := []struct {
		name string
		now  time.Time
		want Health
	}{
		{"valid", epoch.Add(time.Hour), Valid},
		{"expiring soon", epoch.Add(95 * time.Hour), ExpiringSoon},
		{"expired", epoch.Add(101 * time.Hour), Expired},
		{"not yet valid", epoch.Add(-time.Hour), NotYetValid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, remaining := Inspect(cert, tt.now, 10*time.Hour)
			if got != tt.want {
				t.Fatalf("Inspect = %v, want %v", got, tt.want)
			}
			if remaining != cert.NotAfter.Sub(tt.now) {
				t.Errorf("remaining = %s", remaining)
			}
		})
	}
	if Expired.String() != "expired" || ExpiringSoon.String() != "expiring soon" {
		t.Error("unexpected Health strings")
	}
}

func TestWatchExpiryLogsByHealth(t *testing.T) {
	now := time.Now()
	ca, err := NewCA("Test CA", now.Add(-24*time.Hour), now.Add(100*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name      string
		notBefore time.Time
		notAfter  time.Time
		wantLog   string
	}{
		{"expiring", now.Add(-time.Hour), now.Add(10 * 24 * time.Hour), "expires soon"},
		{"expired", now.Add(-3 * time.Hour), now.Add(-time.Hour), "has expired"},
		{"fine", now.Add(-time.Hour), now.Add(40 * 24 * time.Hour), "certificate is valid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			issued, err := ca.IssueNode(NodeRequest{NodeID: "worker-1", NotBefore: tt.notBefore, NotAfter: tt.notAfter})
			if err != nil {
				t.Fatal(err)
			}
			certFile := filepath.Join(t.TempDir(), "agent.pem")
			if err := os.WriteFile(certFile, issued.CertPEM, 0o644); err != nil {
				t.Fatal(err)
			}

			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				WatchExpiry(ctx, logger, "agent", certFile, DefaultWarnWithin, time.Hour)
				close(done)
			}()
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("WatchExpiry did not stop after cancellation")
			}
			if !strings.Contains(logs.String(), tt.wantLog) {
				t.Fatalf("log output %q does not contain %q", logs.String(), tt.wantLog)
			}
		})
	}
}

func TestWatchExpiryReportsUnreadableCertificate(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		WatchExpiry(ctx, logger, "agent", filepath.Join(t.TempDir(), "missing.pem"), DefaultWarnWithin, time.Hour)
		close(done)
	}()
	cancel()
	<-done
	if !strings.Contains(logs.String(), "cannot read certificate") {
		t.Fatalf("unexpected log output %q", logs.String())
	}
}
