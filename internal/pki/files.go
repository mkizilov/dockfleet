package pki

import (
	"crypto"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
)

const (
	publicFileMode  os.FileMode = 0o644
	privateFileMode os.FileMode = 0o600
	privateDirMode  os.FileMode = 0o700
)

// WriteOptions controls how credential files are written.
type WriteOptions struct {
	// Overwrite replaces existing files (atomically, via a temporary file).
	// Without it an existing file is an error, so a CA key is never clobbered.
	Overwrite bool
}

// WriteFile writes data to path with the given mode, creating parent
// directories with owner-only permissions.
func WriteFile(path string, data []byte, mode os.FileMode, opts WriteOptions) error {
	if err := os.MkdirAll(filepath.Dir(path), privateDirMode); err != nil {
		return fmt.Errorf("create directory for %s: %w", path, err)
	}
	if !opts.Overwrite {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
		if err != nil {
			if errors.Is(err, fs.ErrExist) {
				return fmt.Errorf("%s already exists; refusing to overwrite", path)
			}
			return err
		}
		if _, err := f.Write(data); err != nil {
			_ = f.Close()
			_ = os.Remove(path)
			return fmt.Errorf("write %s: %w", path, err)
		}
		if err := f.Close(); err != nil {
			_ = os.Remove(path)
			return fmt.Errorf("write %s: %w", path, err)
		}
		return nil
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), ".pki-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return fmt.Errorf("replace %s: %w", path, err)
	}
	return nil
}

// Save writes the CA certificate (public) and its private key (owner-only).
// It never overwrites existing files.
func (ca *CA) Save(certFile, keyFile string) error {
	keyPEM, err := encodeKey(ca.key)
	if err != nil {
		return err
	}
	if err := WriteFile(keyFile, keyPEM, privateFileMode, WriteOptions{}); err != nil {
		return err
	}
	if err := WriteFile(certFile, ca.CertPEM(), publicFileMode, WriteOptions{}); err != nil {
		_ = os.Remove(keyFile)
		return err
	}
	return nil
}

// Save writes the certificate and, when present, the private key (owner-only).
// The key is written first so a certificate never exists without its key.
func (i *Issued) Save(certFile, keyFile string, opts WriteOptions) error {
	if len(i.KeyPEM) > 0 {
		if err := WriteFile(keyFile, i.KeyPEM, privateFileMode, opts); err != nil {
			return err
		}
	}
	return WriteFile(certFile, i.CertPEM, publicFileMode, opts)
}

// LoadCA reads a CA certificate and private key and checks that they match.
func LoadCA(certFile, keyFile string) (*CA, error) {
	if err := CheckKeyFile(keyFile); err != nil {
		return nil, err
	}
	cert, err := ReadCertificate(certFile)
	if err != nil {
		return nil, err
	}
	if !cert.IsCA {
		return nil, fmt.Errorf("%s is not a CA certificate", certFile)
	}
	raw, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("read CA key: %w", err)
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("%s contains no PEM data", keyFile)
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse CA key: %w", err)
	}
	signer, ok := parsed.(crypto.Signer)
	if !ok {
		return nil, fmt.Errorf("%s does not hold a signing key", keyFile)
	}
	pub, ok := signer.Public().(interface{ Equal(crypto.PublicKey) bool })
	if !ok || !pub.Equal(cert.PublicKey) {
		return nil, fmt.Errorf("CA key %s does not match certificate %s", keyFile, certFile)
	}
	return &CA{Cert: cert, key: signer}, nil
}

// ReadCertificate parses the first certificate in a PEM file.
func ReadCertificate(file string) (*x509.Certificate, error) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("read certificate: %w", err)
	}
	for {
		var block *pem.Block
		block, raw = pem.Decode(raw)
		if block == nil {
			return nil, fmt.Errorf("%s contains no certificate", file)
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse certificate in %s: %w", file, err)
		}
		return cert, nil
	}
}

// CheckKeyFile refuses private keys that other users can access. Group access
// is tolerated so services can share a key through a group; world access is not.
func CheckKeyFile(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat private key: %w", err)
	}
	if info.Mode().Perm()&0o007 != 0 {
		return fmt.Errorf("private key %s is accessible by all users (mode %04o); run: chmod 600 %s", path, info.Mode().Perm(), path)
	}
	return nil
}
