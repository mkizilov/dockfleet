package healthcheck

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/pki"
	"github.com/dockfleet/dockfleet/internal/testnet"
)

func TestURLFor(t *testing.T) {
	for _, tt := range []struct {
		addr   string
		tls    bool
		path   string
		want   string
		failed bool
	}{
		{"127.0.0.1:21280", false, "/readyz", "http://127.0.0.1:21280/readyz", false},
		{"0.0.0.0:21280", false, "/readyz", "http://127.0.0.1:21280/readyz", false},
		{":21280", true, "/readyz", "https://127.0.0.1:21280/readyz", false},
		{"[::]:21280", false, "/healthz", "http://[::1]:21280/healthz", false},
		{"10.1.2.3:8443", true, "/readyz", "https://10.1.2.3:8443/readyz", false},
		{"[::1]:21282", false, "/readyz", "http://[::1]:21282/readyz", false},
		{"21280", false, "/readyz", "", true},
		{"", false, "/readyz", "", true},
	} {
		got, err := URLFor(tt.addr, tt.tls, tt.path)
		if (err != nil) != tt.failed || got != tt.want {
			t.Errorf("URLFor(%q, %v) = %q, %v; want %q (failed %v)", tt.addr, tt.tls, got, err, tt.want, tt.failed)
		}
	}
}

func handler(status int, body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			http.Error(w, "the probe must not send credentials", http.StatusTeapot)
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
}

func TestPlainProbe(t *testing.T) {
	ok := testnet.NewServer(handler(http.StatusOK, `{"status":"ready"}`), false)
	defer ok.Close()
	unready := testnet.NewServer(handler(http.StatusServiceUnavailable, `{"error":"store unavailable"}`), false)
	defer unready.Close()
	redirect := testnet.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://127.0.0.1:1/elsewhere", http.StatusFound)
	}), false)
	defer redirect.Close()
	slow := testnet.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
	}), false)
	defer slow.Close()
	big := testnet.NewServer(handler(http.StatusInternalServerError, strings.Repeat("x", 1<<20)), false)
	defer big.Close()

	ctx := context.Background()
	if err := (Probe{URL: ok.URL + "/readyz"}).Run(ctx); err != nil {
		t.Fatalf("a ready server: %v", err)
	}
	if err := (Probe{URL: unready.URL + "/readyz"}).Run(ctx); err == nil || !strings.Contains(err.Error(), "503") || !strings.Contains(err.Error(), "store unavailable") {
		t.Fatalf("an unready server = %v, want the status and the reason it gave", err)
	}
	if err := (Probe{URL: redirect.URL + "/readyz"}).Run(ctx); err == nil || !strings.Contains(err.Error(), "302") {
		t.Fatalf("a redirect = %v; it must count as a failure, not be followed", err)
	}
	began := time.Now()
	if err := (Probe{URL: slow.URL, Timeout: 200 * time.Millisecond}).Run(ctx); err == nil || time.Since(began) > 5*time.Second {
		t.Fatalf("a server that does not answer = %v after %v", err, time.Since(began))
	}
	if err := (Probe{URL: big.URL}).Run(ctx); err == nil || len(err.Error()) > 8<<10 {
		t.Fatalf("a huge error body must be cut: %v", err)
	}

	// Nothing listens there.
	ln, err := testnet.Listen()
	if err != nil {
		t.Fatal(err)
	}
	dead := "http://" + ln.Addr().String()
	_ = ln.Close()
	if err := (Probe{URL: dead}).Run(ctx); err == nil {
		t.Fatal("a closed port is healthy")
	}
	for _, bad := range []string{"", "127.0.0.1:21280", "ftp://127.0.0.1/", "http://", "file:///etc/passwd"} {
		if err := (Probe{URL: bad}).Run(ctx); err == nil || !strings.Contains(err.Error(), "must look like") {
			t.Errorf("URL %q: %v", bad, err)
		}
	}
}

// serveTLS serves a handler over TLS with the given certificate on a port of the range.
func serveTLS(t *testing.T, cert tls.Certificate) string {
	t.Helper()
	ln, err := testnet.Listen()
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: handler(http.StatusOK, "ok"), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		_ = srv.Serve(tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}))
	}()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String()
}

func issue(t *testing.T, ca *pki.CA, dns []string, ips []net.IP, notBefore, notAfter time.Time) (tls.Certificate, string) {
	t.Helper()
	issued, err := ca.IssueServer(pki.ServerRequest{Name: "manager", DNSNames: dns, IPs: ips, NotBefore: notBefore, NotAfter: notAfter})
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(issued.CertPEM, issued.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "server.pem")
	if err := os.WriteFile(file, issued.CertPEM, 0o644); err != nil {
		t.Fatal(err)
	}
	return pair, file
}

func TestTLSProbeTrustsTheServersOwnCertificateOnly(t *testing.T) {
	now := time.Now()
	ca, err := pki.NewCA("test CA", now.Add(-24*time.Hour), now.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	loopback := []net.IP{net.ParseIP("127.0.0.1")}

	named, namedFile := issue(t, ca, []string{"manager.example.test"}, loopback, now.Add(-time.Minute), now.Add(time.Hour))
	byIP, byIPFile := issue(t, ca, nil, loopback, now.Add(-time.Minute), now.Add(time.Hour))
	noAddress, noAddressFile := issue(t, ca, []string{"manager.example.test"}, nil, now.Add(-time.Minute), now.Add(time.Hour))
	expired, expiredFile := issue(t, ca, nil, loopback, now.Add(-2*time.Hour), now.Add(-time.Hour))
	other, err := pki.NewCA("another CA", now.Add(-24*time.Hour), now.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	_, otherFile := issue(t, other, []string{"manager.example.test"}, loopback, now.Add(-time.Minute), now.Add(time.Hour))

	ctx := context.Background()
	check := func(name string, cert tls.Certificate, probe Probe, wantErr string) {
		t.Helper()
		addr := serveTLS(t, cert)
		probe.URL = "https://" + addr + "/readyz"
		err := probe.Run(ctx)
		switch {
		case wantErr == "" && err != nil:
			t.Errorf("%s: %v", name, err)
		case wantErr != "" && (err == nil || !strings.Contains(err.Error(), wantErr)):
			t.Errorf("%s: error = %v, want one mentioning %q", name, err, wantErr)
		}
	}

	check("the server's own certificate, by its DNS name", named, Probe{CertFile: namedFile}, "")
	check("a certificate with only an IP address", byIP, Probe{CertFile: byIPFile}, "")
	check("a name given explicitly", named, Probe{CertFile: namedFile, ServerName: "manager.example.test"}, "")
	check("an explicit name the certificate does not carry", named, Probe{CertFile: namedFile, ServerName: "other.example.test"}, "certificate")
	check("a certificate for a name but not for the address, with no name given and none to take", noAddress, Probe{CertFile: noAddressFile, ServerName: "manager.example.test"}, "")
	check("an expired certificate", expired, Probe{CertFile: expiredFile}, "expired")
	check("a server that presents another certificate than the file", named, Probe{CertFile: otherFile}, "certificate")
	check("a different server's certificate file", byIP, Probe{CertFile: namedFile, ServerName: "manager.example.test"}, "certificate")

	// Without the certificate to trust there is no probe: it never skips verification.
	addr := serveTLS(t, named)
	for name, p := range map[string]Probe{
		"no certificate file":      {URL: "https://" + addr},
		"a missing file":           {URL: "https://" + addr, CertFile: filepath.Join(t.TempDir(), "nope.pem")},
		"a file without a cert":    {URL: "https://" + addr, CertFile: writeFile(t, "not a certificate")},
		"a file that is no cert":   {URL: "https://" + addr, CertFile: writeFile(t, "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n")},
		"a file that is too large": {URL: "https://" + addr, CertFile: writeFile(t, strings.Repeat("x", maxCertFile+1))},
	} {
		if err := p.Run(ctx); err == nil {
			t.Errorf("%s: the probe succeeded", name)
		}
	}
	// And plain http to a TLS port fails rather than falling back.
	if err := (Probe{URL: "http://" + addr}).Run(ctx); err == nil {
		t.Error("plain http to a TLS port succeeded")
	}
}

func writeFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}
