package protocol

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeSelfSignedCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		DNSNames:              []string{"localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

func TestServerTLSConfig(t *testing.T) {
	certFile, keyFile := writeSelfSignedCert(t)
	cfg, err := ServerTLSConfig(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Certificates) != 1 || cfg.MinVersion != tls.VersionTLS13 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if _, err := ServerTLSConfig(certFile, filepath.Join(t.TempDir(), "missing.pem")); err == nil {
		t.Error("expected error for missing key")
	}
}

func TestClientTLSConfig(t *testing.T) {
	certFile, _ := writeSelfSignedCert(t)
	cfg, err := ClientTLSConfig(certFile)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RootCAs == nil || cfg.MinVersion != tls.VersionTLS13 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if _, err := ClientTLSConfig(filepath.Join(t.TempDir(), "missing.pem")); err == nil {
		t.Error("expected error for missing CA file")
	}
	empty := filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(empty, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ClientTLSConfig(empty); err == nil {
		t.Error("expected error for CA file without certificates")
	}
}
