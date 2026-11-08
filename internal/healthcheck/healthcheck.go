// Package healthcheck is what the `healthcheck` subcommand of each DockFleet
// binary runs. A container image without a shell or curl (the images are built
// from a distroless base) cannot say "curl /readyz" in its HEALTHCHECK, so the
// binary asks itself: it requests the health endpoint of its own process and
// exits 0 or 1.
//
// The probe never sends a credential, never follows a redirect and never skips
// certificate verification. Against an https endpoint it trusts exactly one
// thing: the certificate the process itself serves (its own certificate file),
// so it needs no CA bundle and cannot be pointed at a different server.
package healthcheck

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	defaultTimeout = 3 * time.Second
	maxBodyBytes   = 4 << 10
	maxCertFile    = 64 << 10
)

// Probe describes one request to a health endpoint.
type Probe struct {
	// URL is the endpoint, for example http://127.0.0.1:21280/readyz.
	URL string
	// CertFile is required for an https URL: the PEM certificate (or chain) the
	// server presents. It is the only root the probe trusts.
	CertFile string
	// ServerName is the name the certificate must carry. Empty means the first
	// DNS name in the certificate, or, if it has none, the host of the URL (which
	// is then checked against the certificate's IP addresses).
	ServerName string
	// Timeout bounds the whole request; zero means 3 seconds.
	Timeout time.Duration
}

// URLFor turns the address a process listens on into the URL a probe on the same
// machine uses: a wildcard address is probed on loopback.
func URLFor(listenAddr string, useTLS bool, path string) (string, error) {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return "", fmt.Errorf("listen address %q: %w", listenAddr, err)
	}
	switch host {
	case "", "0.0.0.0":
		host = "127.0.0.1"
	case "::", "[::]":
		host = "::1"
	}
	scheme := "http"
	if useTLS {
		scheme = "https"
	}
	return (&url.URL{Scheme: scheme, Host: net.JoinHostPort(host, port), Path: path}).String(), nil
}

// Run makes the request. It returns nil only for a 2xx answer.
func (p Probe) Run(ctx context.Context) error {
	u, err := url.Parse(p.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("health check URL %q must look like http://host:port/path", p.URL)
	}
	timeout := p.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	client := &http.Client{
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	if u.Scheme == "https" {
		tlsConfig, err := p.tlsConfig(u.Hostname())
		if err != nil {
			return err
		}
		client.Transport = &http.Transport{TLSClientConfig: tlsConfig, DisableKeepAlives: true}
	} else {
		client.Transport = &http.Transport{DisableKeepAlives: true}
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", u.Redacted(), err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("GET %s: %s: %s", u.Redacted(), resp.Status, strings.TrimSpace(string(body)))
	}
	return nil
}

func (p Probe) tlsConfig(urlHost string) (*tls.Config, error) {
	if p.CertFile == "" {
		return nil, errors.New("an https health check needs the server's own certificate file to trust (DOCKFLEET_TLS_CERT_FILE)")
	}
	f, err := os.Open(p.CertFile)
	if err != nil {
		return nil, fmt.Errorf("server certificate: %w", err)
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, maxCertFile+1))
	if err != nil {
		return nil, fmt.Errorf("server certificate: %w", err)
	}
	if len(raw) > maxCertFile {
		return nil, fmt.Errorf("server certificate %s is larger than %d bytes", p.CertFile, maxCertFile)
	}
	pool := x509.NewCertPool()
	var leaf *x509.Certificate
	for {
		var block *pem.Block
		block, raw = pem.Decode(raw)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("server certificate %s: %w", p.CertFile, err)
		}
		if leaf == nil {
			leaf = cert
		}
		pool.AddCert(cert)
	}
	if leaf == nil {
		return nil, fmt.Errorf("server certificate %s holds no certificate", p.CertFile)
	}
	name := p.ServerName
	if name == "" && len(leaf.DNSNames) > 0 {
		name = leaf.DNSNames[0]
	}
	if name == "" {
		name = urlHost
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pool, ServerName: name}, nil
}
