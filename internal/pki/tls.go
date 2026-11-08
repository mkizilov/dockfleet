package pki

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

// fileCache holds a value built from files and rebuilds it when any file's
// modification time or size changes. If a rebuild fails (for example a
// renewal that is only half written) the previous value stays in use, so a
// bad rotation cannot take a running process down; the failure is logged once.
type fileCache[T any] struct {
	what   string
	files  []string
	load   func() (T, error)
	logger *slog.Logger

	mu        sync.Mutex
	val       T
	loaded    bool
	sig       string
	failedSig string
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newFileCache[T any](what string, logger *slog.Logger, files []string, load func() (T, error)) (*fileCache[T], error) {
	if logger == nil {
		logger = discardLogger()
	}
	c := &fileCache[T]{what: what, files: files, load: load, logger: logger}
	if _, err := c.get(); err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	return c, nil
}

func signature(files []string) (string, error) {
	var b strings.Builder
	for _, f := range files {
		info, err := os.Stat(f)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "%s|%d|%d;", f, info.ModTime().UnixNano(), info.Size())
	}
	return b.String(), nil
}

func (c *fileCache[T]) get() (T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	sig, err := signature(c.files)
	if err == nil && c.loaded && sig == c.sig {
		return c.val, nil
	}
	var val T
	if err == nil {
		val, err = c.load()
	}
	if err != nil {
		if c.loaded {
			if sig != c.failedSig {
				c.failedSig = sig
				c.logger.Error("reload failed; continuing with the previous version", "what", c.what, "files", c.files, "error", err)
			}
			return c.val, nil
		}
		var zero T
		return zero, err
	}
	c.val, c.sig, c.loaded, c.failedSig = val, sig, true, ""
	return val, nil
}

func loadKeyPair(certFile, keyFile string) (*tls.Certificate, error) {
	if err := CheckKeyFile(keyFile); err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load key pair: %w", err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("parse certificate: %w", err)
	}
	cert.Leaf = leaf
	return &cert, nil
}

// LoadPool reads every certificate in a PEM bundle into a pool.
func LoadPool(file string) (*x509.CertPool, error) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("read CA bundle: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(raw) {
		return nil, fmt.Errorf("CA bundle %s contains no certificates", file)
	}
	return pool, nil
}

// ServerConfig configures the manager's TLS listeners.
type ServerConfig struct {
	CertFile string
	KeyFile  string
	// ClientCAFile turns on mutual TLS: every client must present a
	// certificate signed by a CA in this bundle, or the handshake fails.
	// Leave it empty for server-only TLS (the admin HTTP API).
	ClientCAFile string
	Logger       *slog.Logger
	// Now overrides the clock used to validate certificates (tests).
	Now func() time.Time
}

// ServerTLS builds a TLS 1.3 server configuration. The certificate, and with
// mutual TLS the client CA bundle, are re-read when their files change, so
// renewals apply to new connections without a restart.
//
// The mutual TLS configuration is meant for gRPC and only offers ALPN "h2".
func ServerTLS(c ServerConfig) (*tls.Config, error) {
	pairs, err := newFileCache("server certificate", c.Logger, []string{c.CertFile, c.KeyFile}, func() (*tls.Certificate, error) {
		return loadKeyPair(c.CertFile, c.KeyFile)
	})
	if err != nil {
		return nil, err
	}
	if c.ClientCAFile == "" {
		return &tls.Config{
			MinVersion: tls.VersionTLS13,
			Time:       c.Now,
			GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
				return pairs.get()
			},
		}, nil
	}

	cas, err := newFileCache("client CA bundle", c.Logger, []string{c.ClientCAFile}, func() (*x509.CertPool, error) {
		return LoadPool(c.ClientCAFile)
	})
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		ClientAuth: tls.RequireAndVerifyClientCert,
		Time:       c.Now,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			cert, err := pairs.get()
			if err != nil {
				return nil, err
			}
			pool, err := cas.get()
			if err != nil {
				return nil, err
			}
			return &tls.Config{
				MinVersion:   tls.VersionTLS13,
				ClientAuth:   tls.RequireAndVerifyClientCert,
				Certificates: []tls.Certificate{*cert},
				ClientCAs:    pool,
				NextProtos:   []string{"h2"},
				Time:         c.Now,
			}, nil
		},
	}, nil
}

// ClientConfig configures an agent's TLS connection to the manager.
type ClientConfig struct {
	// CAFile is the bundle that must have signed the manager's certificate.
	CAFile string
	// CertFile and KeyFile are the agent's own certificate; they are re-read
	// when the files change so a renewed certificate is used on reconnect.
	CertFile string
	KeyFile  string
	// ServerName is the name the manager's certificate must carry; empty
	// means the host part of the dialed address.
	ServerName string
	Logger     *slog.Logger
	// Now overrides the clock used to validate certificates (tests).
	Now func() time.Time
}

// ClientTLS builds a TLS 1.3 client configuration that verifies the manager
// against CAFile and presents the agent certificate.
func ClientTLS(c ClientConfig) (*tls.Config, error) {
	pool, err := LoadPool(c.CAFile)
	if err != nil {
		return nil, err
	}
	pairs, err := newFileCache("agent certificate", c.Logger, []string{c.CertFile, c.KeyFile}, func() (*tls.Certificate, error) {
		return loadKeyPair(c.CertFile, c.KeyFile)
	})
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    pool,
		ServerName: c.ServerName,
		Time:       c.Now,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return pairs.get()
		},
	}, nil
}

// ErrNotValidYet and ErrExpired describe a certificate outside its validity window.
var (
	ErrNotValidYet = errors.New("certificate is not valid yet")
	ErrExpired     = errors.New("certificate has expired")
)

// CheckValidAt returns ErrNotValidYet or ErrExpired when now is outside the
// certificate's validity window.
func CheckValidAt(cert *x509.Certificate, now time.Time) error {
	switch {
	case now.Before(cert.NotBefore):
		return fmt.Errorf("%w: valid from %s", ErrNotValidYet, cert.NotBefore.UTC().Format(time.RFC3339))
	case now.After(cert.NotAfter):
		return fmt.Errorf("%w: expired %s", ErrExpired, cert.NotAfter.UTC().Format(time.RFC3339))
	}
	return nil
}
