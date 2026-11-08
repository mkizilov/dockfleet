package pki

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/store"
)

var epoch = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

func newTestCA(t *testing.T) *CA {
	t.Helper()
	ca, err := NewCA("Test CA", epoch.Add(-time.Hour), epoch.Add(365*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

func verifyChain(ca *CA, leaf *x509.Certificate, at time.Time, usage x509.ExtKeyUsage, host string) error {
	roots := x509.NewCertPool()
	roots.AddCert(ca.Cert)
	_, err := leaf.Verify(x509.VerifyOptions{
		Roots:       roots,
		CurrentTime: at,
		KeyUsages:   []x509.ExtKeyUsage{usage},
		DNSName:     host,
	})
	return err
}

func issueNode(t *testing.T, ca *CA, id string) *Issued {
	t.Helper()
	issued, err := ca.IssueNode(NodeRequest{NodeID: id, NotBefore: epoch.Add(-time.Minute), NotAfter: epoch.Add(24 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	return issued
}

func TestNewCA(t *testing.T) {
	ca := newTestCA(t)
	if !ca.Cert.IsCA || !ca.Cert.MaxPathLenZero || ca.Cert.KeyUsage&x509.KeyUsageCertSign == 0 {
		t.Fatalf("not a leaf-signing-only CA: %+v", ca.Cert)
	}
	if err := ca.Cert.CheckSignatureFrom(ca.Cert); err != nil {
		t.Fatalf("CA must be self-signed: %v", err)
	}
	if _, err := NewCA("", epoch, epoch.Add(time.Hour)); err == nil {
		t.Error("empty CA name must be rejected")
	}
	if _, err := NewCA("x", epoch, epoch); err == nil {
		t.Error("empty CA validity must be rejected")
	}
}

func TestIssueNodeCertificateIsBoundToItsIdentity(t *testing.T) {
	ca := newTestCA(t)
	issued := issueNode(t, ca, "worker-1")

	id, err := NodeIDFromCert(issued.Cert)
	if err != nil || id != "worker-1" {
		t.Fatalf("NodeIDFromCert = %q, %v", id, err)
	}
	if issued.Cert.IsCA {
		t.Error("a node certificate must not be a CA")
	}
	if err := verifyChain(ca, issued.Cert, epoch, x509.ExtKeyUsageClientAuth, ""); err != nil {
		t.Fatalf("node certificate must verify for client auth: %v", err)
	}
	if err := verifyChain(ca, issued.Cert, epoch, x509.ExtKeyUsageServerAuth, ""); err == nil {
		t.Error("a node certificate must not verify for server auth")
	}
	if _, err := tls.X509KeyPair(issued.CertPEM, issued.KeyPEM); err != nil {
		t.Fatalf("certificate and key do not match: %v", err)
	}
	if issued.Cert.SerialNumber.Sign() <= 0 {
		t.Error("serial number must be positive")
	}
}

func TestIssueServerCertificate(t *testing.T) {
	ca := newTestCA(t)
	issued, err := ca.IssueServer(ServerRequest{
		Name:      "manager",
		DNSNames:  []string{"manager.example.com"},
		IPs:       []net.IP{net.ParseIP("10.1.2.3")},
		NotBefore: epoch.Add(-time.Minute),
		NotAfter:  epoch.Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, host := range []string{"manager.example.com", "10.1.2.3"} {
		if err := verifyChain(ca, issued.Cert, epoch, x509.ExtKeyUsageServerAuth, host); err != nil {
			t.Errorf("server certificate must verify for %s: %v", host, err)
		}
	}
	if err := verifyChain(ca, issued.Cert, epoch, x509.ExtKeyUsageServerAuth, "evil.example.com"); err == nil {
		t.Error("server certificate must not verify for an unlisted host")
	}
	if err := verifyChain(ca, issued.Cert, epoch, x509.ExtKeyUsageClientAuth, ""); err == nil {
		t.Error("a server certificate must not verify for client auth")
	}
	if _, err := NodeIDFromCert(issued.Cert); err == nil {
		t.Error("a server certificate must not carry a node identity")
	}
}

func TestIssueRejectsBadRequests(t *testing.T) {
	ca := newTestCA(t)
	valid := NodeRequest{NodeID: "n1", NotBefore: epoch, NotAfter: epoch.Add(time.Hour)}

	for _, id := range []string{"", "bad id", "../x", "-n"} {
		req := valid
		req.NodeID = id
		if _, err := ca.IssueNode(req); err == nil {
			t.Errorf("node ID %q must be rejected", id)
		}
	}
	outlives := valid
	outlives.NotAfter = ca.Cert.NotAfter.Add(time.Hour)
	if _, err := ca.IssueNode(outlives); err == nil || !strings.Contains(err.Error(), "outlive") {
		t.Errorf("a certificate outliving the CA must be rejected, got %v", err)
	}
	reversed := valid
	reversed.NotAfter = valid.NotBefore.Add(-time.Hour)
	if _, err := ca.IssueNode(reversed); err == nil {
		t.Error("reversed validity must be rejected")
	}
	if _, err := ca.IssueServer(ServerRequest{Name: "m", NotBefore: epoch, NotAfter: epoch.Add(time.Hour)}); err == nil {
		t.Error("a server certificate without names must be rejected")
	}
}

func TestExpiredAndNotYetValidCertificates(t *testing.T) {
	ca := newTestCA(t)
	expired, err := ca.IssueNode(NodeRequest{NodeID: "old", NotBefore: epoch.Add(-3 * time.Hour), NotAfter: epoch.Add(-time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	var invalid x509.CertificateInvalidError
	if err := verifyChain(ca, expired.Cert, epoch, x509.ExtKeyUsageClientAuth, ""); !errors.As(err, &invalid) || invalid.Reason != x509.Expired {
		t.Fatalf("expired certificate verification error = %v, want Expired", err)
	}
	if err := CheckValidAt(expired.Cert, epoch); !errors.Is(err, ErrExpired) {
		t.Errorf("CheckValidAt = %v, want ErrExpired", err)
	}

	future, err := ca.IssueNode(NodeRequest{NodeID: "new", NotBefore: epoch.Add(time.Hour), NotAfter: epoch.Add(2 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckValidAt(future.Cert, epoch); !errors.Is(err, ErrNotValidYet) {
		t.Errorf("CheckValidAt = %v, want ErrNotValidYet", err)
	}
	if err := CheckValidAt(future.Cert, epoch.Add(90*time.Minute)); err != nil {
		t.Errorf("CheckValidAt inside the window = %v", err)
	}
}

func TestCertificateFromAnotherCAIsNotTrusted(t *testing.T) {
	trusted, other := newTestCA(t), newTestCA(t)
	foreign := issueNode(t, other, "worker-1")
	if err := verifyChain(trusted, foreign.Cert, epoch, x509.ExtKeyUsageClientAuth, ""); err == nil {
		t.Fatal("a certificate from another CA must not verify")
	}
}

func TestNodeIDFromCert(t *testing.T) {
	uri := func(s string) *url.URL {
		u, err := url.Parse(s)
		if err != nil {
			t.Fatal(err)
		}
		return u
	}
	tests := []struct {
		name    string
		uris    []*url.URL
		want    string
		wantErr bool
	}{
		{"valid", []*url.URL{uri("spiffe://dockfleet/node/worker-1")}, "worker-1", false},
		{"other URIs are ignored", []*url.URL{uri("https://example.com/x"), uri("spiffe://dockfleet/node/w2")}, "w2", false},
		{"no URIs", nil, "", true},
		{"foreign trust domain", []*url.URL{uri("spiffe://other/node/worker-1")}, "", true},
		{"not a node path", []*url.URL{uri("spiffe://dockfleet/manager")}, "", true},
		{"two identities", []*url.URL{uri("spiffe://dockfleet/node/a"), uri("spiffe://dockfleet/node/b")}, "", true},
		{"invalid id", []*url.URL{uri("spiffe://dockfleet/node/-bad")}, "", true},
		{"empty id", []*url.URL{uri("spiffe://dockfleet/node/")}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NodeIDFromCert(&x509.Certificate{URIs: tt.uris})
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Fatalf("NodeIDFromCert = %q, %v; want %q, error=%v", got, err, tt.want, tt.wantErr)
			}
		})
	}
}

func TestValidNodeIDMatchesStoreRule(t *testing.T) {
	for _, s := range []string{"", "a", "node-1", "web.api_2", "-a", ".a", "a b", "a/b", "ünï", strings.Repeat("a", 63), strings.Repeat("a", 64)} {
		if ValidNodeID(s) != store.ValidName(s) {
			t.Errorf("ValidNodeID(%q) = %v disagrees with store.ValidName", s, ValidNodeID(s))
		}
	}
}

func TestCSRFlowKeepsThePrivateKeyOffTheCA(t *testing.T) {
	ca := newTestCA(t)
	csrPEM, keyPEM, err := NewCSR("worker-1")
	if err != nil {
		t.Fatal(err)
	}
	issued, err := ca.SignNodeCSR(csrPEM, NodeRequest{NodeID: "worker-1", NotBefore: epoch.Add(-time.Minute), NotAfter: epoch.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if len(issued.KeyPEM) != 0 {
		t.Fatal("the CA must not produce a private key for a CSR")
	}
	if _, err := tls.X509KeyPair(issued.CertPEM, keyPEM); err != nil {
		t.Fatalf("signed certificate does not match the worker's key: %v", err)
	}
	if id, err := NodeIDFromCert(issued.Cert); err != nil || id != "worker-1" {
		t.Fatalf("identity = %q, %v", id, err)
	}
	if err := verifyChain(ca, issued.Cert, epoch, x509.ExtKeyUsageClientAuth, ""); err != nil {
		t.Fatalf("signed certificate must verify: %v", err)
	}
}

func TestSignNodeCSRRejectsBadInput(t *testing.T) {
	ca := newTestCA(t)
	req := NodeRequest{NodeID: "worker-1", NotBefore: epoch, NotAfter: epoch.Add(time.Hour)}
	csrPEM, _, err := NewCSR("worker-1")
	if err != nil {
		t.Fatal(err)
	}

	other := req
	other.NodeID = "worker-2"
	if _, err := ca.SignNodeCSR(csrPEM, other); err == nil || !strings.Contains(err.Error(), "worker-1") {
		t.Errorf("a CSR for another node must be rejected, got %v", err)
	}
	if _, err := ca.SignNodeCSR([]byte("not pem"), req); err == nil {
		t.Error("garbage input must be rejected")
	}
	block, _ := pem.Decode(csrPEM)
	block.Bytes[len(block.Bytes)-1] ^= 0xff
	if _, err := ca.SignNodeCSR(pem.EncodeToMemory(block), req); err == nil {
		t.Error("a CSR with a broken signature must be rejected")
	}
}

func TestCASaveAndLoad(t *testing.T) {
	ca := newTestCA(t)
	dir := filepath.Join(t.TempDir(), "pki")
	certFile, keyFile := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca.key")
	if err := ca.Save(certFile, keyFile); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if info, _ := os.Stat(keyFile); info.Mode().Perm() != 0o600 {
			t.Errorf("CA key mode = %o, want 600", info.Mode().Perm())
		}
		if info, _ := os.Stat(dir); info.Mode().Perm() != 0o700 {
			t.Errorf("PKI directory mode = %o, want 700", info.Mode().Perm())
		}
	}
	if err := ca.Save(certFile, keyFile); err == nil || !strings.Contains(err.Error(), "refusing to overwrite") {
		t.Fatalf("saving over an existing CA must fail, got %v", err)
	}

	loaded, err := LoadCA(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	issued := issueNode(t, loaded, "worker-1")
	if err := verifyChain(ca, issued.Cert, epoch, x509.ExtKeyUsageClientAuth, ""); err != nil {
		t.Fatalf("a loaded CA must sign certificates the original CA root trusts: %v", err)
	}
}

func TestLoadCARejectsUnsafeOrMismatchedFiles(t *testing.T) {
	dir := t.TempDir()
	ca, other := newTestCA(t), newTestCA(t)
	certFile, keyFile := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca.key")
	if err := ca.Save(certFile, keyFile); err != nil {
		t.Fatal(err)
	}
	otherCert, otherKey := filepath.Join(dir, "other.pem"), filepath.Join(dir, "other.key")
	if err := other.Save(otherCert, otherKey); err != nil {
		t.Fatal(err)
	}

	if _, err := LoadCA(certFile, otherKey); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Errorf("mismatched key must be rejected, got %v", err)
	}
	leaf := issueNode(t, ca, "n1")
	leafFile := filepath.Join(dir, "leaf.pem")
	if err := os.WriteFile(leafFile, leaf.CertPEM, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCA(leafFile, keyFile); err == nil || !strings.Contains(err.Error(), "not a CA") {
		t.Errorf("a leaf certificate must not load as a CA, got %v", err)
	}

	if runtime.GOOS != "windows" {
		if err := os.Chmod(keyFile, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadCA(certFile, keyFile); err == nil || !strings.Contains(err.Error(), "chmod 600") {
			t.Errorf("a world-readable key must be rejected, got %v", err)
		}
	}
}

func TestIssuedSaveOverwrite(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "agent.pem"), filepath.Join(dir, "agent.key")
	first := issueNode(t, ca, "worker-1")
	if err := first.Save(certFile, keyFile, WriteOptions{}); err != nil {
		t.Fatal(err)
	}
	second := issueNode(t, ca, "worker-1")
	if err := second.Save(certFile, keyFile, WriteOptions{}); err == nil {
		t.Fatal("renewal without Overwrite must refuse to replace existing files")
	}
	if err := second.Save(certFile, keyFile, WriteOptions{Overwrite: true}); err != nil {
		t.Fatal(err)
	}
	cert, err := ReadCertificate(certFile)
	if err != nil || cert.SerialNumber.Cmp(second.Cert.SerialNumber) != 0 {
		t.Fatalf("certificate was not replaced: %v", err)
	}
	if runtime.GOOS != "windows" {
		if info, _ := os.Stat(keyFile); info.Mode().Perm() != 0o600 {
			t.Errorf("replaced key mode = %o, want 600", info.Mode().Perm())
		}
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 2 {
		t.Errorf("temporary files were left behind: %v", entries)
	}
}
