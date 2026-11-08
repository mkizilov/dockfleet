package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/pki"
)

// runCLIAt runs a command with the given clock and returns what it printed.
func runCLIAt(now func() time.Time, args ...string) (string, error) {
	var out bytes.Buffer
	err := run(args, &out, now)
	return out.String(), err
}

func runCLI(args ...string) (string, error) {
	return runCLIAt(time.Now, args...)
}

// mustRun runs a command that must succeed and must never print key material.
func mustRun(t *testing.T, args ...string) string {
	t.Helper()
	out, err := runCLI(args...)
	if err != nil {
		t.Fatalf("dockfleet-pki %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	if strings.Contains(out, "BEGIN") || strings.Contains(out, "PRIVATE KEY") {
		t.Fatalf("output must never contain key material:\n%s", out)
	}
	return out
}

func readCert(t *testing.T, path string) *x509.Certificate {
	t.Helper()
	cert, err := pki.ReadCertificate(path)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func assertPrivate(t *testing.T, path string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		return
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Errorf("%s has mode %o, want owner-only", path, info.Mode().Perm())
	}
}

func TestDevelopmentWorkflow(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pki")
	mustRun(t, "init-ca", "-dir", dir)
	mustRun(t, "issue-manager", "-dir", dir, "-dns", "manager.example.com", "-ip", "10.1.2.3")
	out := mustRun(t, "issue-agent", "-dir", dir, "-node-id", "worker-1")
	if !strings.Contains(out, "/v1/enrollments") {
		t.Errorf("issuing a node certificate must remind the admin to enroll the node:\n%s", out)
	}

	agentDir := filepath.Join(dir, "agents", "worker-1")
	assertPrivate(t, filepath.Join(dir, "ca.key"))
	assertPrivate(t, filepath.Join(dir, "manager.key"))
	assertPrivate(t, filepath.Join(agentDir, "agent.key"))

	ca := readCert(t, filepath.Join(dir, "ca.pem"))
	roots := x509.NewCertPool()
	roots.AddCert(ca)

	manager := readCert(t, filepath.Join(dir, "manager.pem"))
	for _, host := range []string{"manager.example.com", "10.1.2.3"} {
		if _, err := manager.Verify(x509.VerifyOptions{Roots: roots, DNSName: host, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
			t.Errorf("manager certificate must verify for %s: %v", host, err)
		}
	}

	agent := readCert(t, filepath.Join(agentDir, "agent.pem"))
	if id, err := pki.NodeIDFromCert(agent); err != nil || id != "worker-1" {
		t.Fatalf("agent identity = %q, %v", id, err)
	}
	if _, err := agent.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		t.Errorf("agent certificate must verify for client auth: %v", err)
	}
	if _, err := tls.LoadX509KeyPair(filepath.Join(agentDir, "agent.pem"), filepath.Join(agentDir, "agent.key")); err != nil {
		t.Errorf("agent certificate and key must match: %v", err)
	}
	if bundled := readCert(t, filepath.Join(agentDir, "ca.pem")); !bundled.Equal(ca) {
		t.Error("the directory handed to a worker must contain the CA certificate")
	}
	if _, err := os.Stat(filepath.Join(agentDir, "ca.key")); err == nil {
		t.Error("the CA private key must never be copied next to a node's files")
	}
}

func TestManagerCertificateDefaultsToLoopbackNames(t *testing.T) {
	dir := t.TempDir()
	mustRun(t, "init-ca", "-dir", dir)
	mustRun(t, "issue-manager", "-dir", dir)
	manager := readCert(t, filepath.Join(dir, "manager.pem"))
	if len(manager.DNSNames) != 1 || manager.DNSNames[0] != "localhost" || len(manager.IPAddresses) != 2 {
		t.Fatalf("default names = %v %v, want localhost, 127.0.0.1 and ::1", manager.DNSNames, manager.IPAddresses)
	}
}

func TestInitCARefusesToOverwriteTheCA(t *testing.T) {
	dir := t.TempDir()
	mustRun(t, "init-ca", "-dir", dir)
	before, err := os.ReadFile(filepath.Join(dir, "ca.key"))
	if err != nil {
		t.Fatal(err)
	}

	for _, args := range [][]string{{"init-ca", "-dir", dir}, {"init-ca", "-dir", dir, "-force"}} {
		out, err := runCLI(args...)
		if err == nil {
			t.Fatalf("%v must fail:\n%s", args, out)
		}
	}
	after, _ := os.ReadFile(filepath.Join(dir, "ca.key"))
	if !bytes.Equal(before, after) {
		t.Fatal("the CA key was modified")
	}
}

func TestIssuingNeedsACA(t *testing.T) {
	dir := t.TempDir()
	for _, args := range [][]string{
		{"issue-manager", "-dir", dir},
		{"issue-agent", "-dir", dir, "-node-id", "worker-1"},
	} {
		_, err := runCLI(args...)
		if err == nil || !strings.Contains(err.Error(), "init-ca") {
			t.Errorf("%v: error = %v, want a hint to run init-ca", args, err)
		}
	}
}

func TestIssueAgentValidationAndRenewal(t *testing.T) {
	dir := t.TempDir()
	mustRun(t, "init-ca", "-dir", dir)

	if _, err := runCLI("issue-agent", "-dir", dir); err == nil {
		t.Error("-node-id must be required")
	}
	for _, id := range []string{"bad id", "../x", "-n"} {
		if _, err := runCLI("issue-agent", "-dir", dir, "-node-id", id); err == nil {
			t.Errorf("node ID %q must be rejected", id)
		}
	}
	if _, err := runCLI("issue-agent", "-dir", dir, "-node-id", "worker-1", "-days", "36500"); err == nil {
		t.Error("a certificate that outlives the CA must be rejected")
	}

	mustRun(t, "issue-agent", "-dir", dir, "-node-id", "worker-1")
	certFile := filepath.Join(dir, "agents", "worker-1", "agent.pem")
	first := readCert(t, certFile)

	if _, err := runCLI("issue-agent", "-dir", dir, "-node-id", "worker-1"); err == nil || !strings.Contains(err.Error(), "refusing to overwrite") {
		t.Fatalf("a second issue without -force must refuse, got %v", err)
	}
	mustRun(t, "issue-agent", "-dir", dir, "-node-id", "worker-1", "-force")
	if renewed := readCert(t, certFile); renewed.SerialNumber.Cmp(first.SerialNumber) == 0 {
		t.Error("-force must replace the certificate")
	}
}

func TestCSRWorkflowKeepsTheKeyOnTheWorker(t *testing.T) {
	caDir, workerDir := t.TempDir(), t.TempDir()
	mustRun(t, "init-ca", "-dir", caDir)

	mustRun(t, "new-csr", "-node-id", "worker-2", "-out", workerDir)
	assertPrivate(t, filepath.Join(workerDir, "agent.key"))
	csr := filepath.Join(workerDir, "agent.csr")

	if _, err := runCLI("sign-agent", "-dir", caDir, "-node-id", "worker-3", "-csr", csr); err == nil {
		t.Error("signing a request for another node must fail")
	}
	mustRun(t, "sign-agent", "-dir", caDir, "-node-id", "worker-2", "-csr", csr)

	signedDir := filepath.Join(caDir, "agents", "worker-2")
	if _, err := os.Stat(filepath.Join(signedDir, "agent.key")); err == nil {
		t.Fatal("the CA host must never hold the worker's private key")
	}
	// The worker combines the signed certificate with the key it generated.
	signed, err := os.ReadFile(filepath.Join(signedDir, "agent.pem"))
	if err != nil {
		t.Fatal(err)
	}
	key, err := os.ReadFile(filepath.Join(workerDir, "agent.key"))
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(signed, key)
	if err != nil {
		t.Fatalf("signed certificate does not match the worker's key: %v", err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if id, err := pki.NodeIDFromCert(leaf); err != nil || id != "worker-2" {
		t.Fatalf("identity = %q, %v", id, err)
	}
}

func TestInspectReportsIdentityAndExpiry(t *testing.T) {
	dir := t.TempDir()
	mustRun(t, "init-ca", "-dir", dir)
	mustRun(t, "issue-agent", "-dir", dir, "-node-id", "worker-1", "-days", "10")
	cert := filepath.Join(dir, "agents", "worker-1", "agent.pem")

	out := mustRun(t, "inspect", cert)
	for _, want := range []string{"worker-1", "client auth", "fingerprint:", "valid", "expiring soon"} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output lacks %q:\n%s", want, out)
		}
	}
	out = mustRun(t, "inspect", "-warn", "24h", cert)
	if strings.Contains(out, "expiring soon") {
		t.Errorf("a certificate with 10 days left is not expiring within 24h:\n%s", out)
	}

	later, err := runCLIAt(func() time.Time { return time.Now().Add(30 * 24 * time.Hour) }, "inspect", cert)
	if err != nil || !strings.Contains(later, "expired") {
		t.Errorf("inspect a month later = %v\n%s", err, later)
	}
	if _, err := runCLI("inspect"); err == nil {
		t.Error("inspect without files must fail")
	}
	if _, err := runCLI("inspect", filepath.Join(dir, "ca.key")); err == nil {
		t.Error("inspect must refuse a private key file")
	}
}

func TestUsageAndUnknownCommands(t *testing.T) {
	out, err := runCLI("help")
	if err != nil || !strings.Contains(out, "init-ca") {
		t.Fatalf("help = %v\n%s", err, out)
	}
	if _, err := runCLI(); err == nil {
		t.Error("no command must be an error")
	}
	if _, err := runCLI("frobnicate"); err == nil || !strings.Contains(err.Error(), "frobnicate") {
		t.Errorf("unknown command error = %v", err)
	}
	if _, err := runCLI("init-ca", "-dir", t.TempDir(), "stray"); err == nil {
		t.Error("stray arguments must be rejected")
	}
	if _, err := runCLI("init-ca", "-no-such-flag"); err == nil {
		t.Error("unknown flags must be rejected")
	}
}
