// Package pki implements DockFleet's certificate workflow: a development
// certificate authority, manager and node certificates, node identity
// encoding, certificate validation helpers, and TLS configuration that picks
// up renewed certificates without a restart.
//
// A node's identity is a URI SAN of the form spiffe://dockfleet/node/<node-id>
// on a client certificate. Issuing a certificate is the CA's statement that its
// holder may speak for that node ID.
package pki

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	// TrustDomain is the host part of every node identity URI.
	TrustDomain = "dockfleet"

	DefaultCAValidity   = 5 * 365 * 24 * time.Hour
	DefaultLeafValidity = 90 * 24 * time.Hour

	nodePathPrefix = "/node/"
	minRSABits     = 2048
)

// The same rule as store.ValidName; pki must not depend on the store.
var nodeIDPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$`)

// ValidNodeID reports whether s can be used as a node identity.
func ValidNodeID(s string) bool {
	return nodeIDPattern.MatchString(s)
}

// NodeURI is the URI SAN that carries a node's identity.
func NodeURI(nodeID string) *url.URL {
	return &url.URL{Scheme: "spiffe", Host: TrustDomain, Path: nodePathPrefix + nodeID}
}

// NodeIDFromCert returns the node identity encoded in cert. It fails unless
// the certificate carries exactly one DockFleet node URI SAN.
func NodeIDFromCert(cert *x509.Certificate) (string, error) {
	var id string
	for _, u := range cert.URIs {
		if u.Scheme != "spiffe" || u.Host != TrustDomain || !strings.HasPrefix(u.Path, nodePathPrefix) {
			continue
		}
		if id != "" {
			return "", errors.New("certificate carries more than one node identity")
		}
		id = strings.TrimPrefix(u.Path, nodePathPrefix)
	}
	if id == "" {
		return "", errors.New("certificate has no node identity (expected URI SAN spiffe://" + TrustDomain + nodePathPrefix + "<node-id>)")
	}
	if !ValidNodeID(id) {
		return "", fmt.Errorf("certificate node identity %q is not a valid node ID", id)
	}
	return id, nil
}

// Fingerprint is the hex SHA-256 of the certificate's DER encoding.
func Fingerprint(cert *x509.Certificate) string {
	sum := sha256.Sum256(cert.Raw)
	return hex.EncodeToString(sum[:])
}

// CA is a certificate authority able to sign leaf certificates.
type CA struct {
	Cert *x509.Certificate
	key  crypto.Signer
}

// Issued is a signed certificate. KeyPEM is empty when the key pair was
// generated elsewhere (CSR flow) so the private key never reached the CA.
type Issued struct {
	Cert    *x509.Certificate
	CertPEM []byte
	KeyPEM  []byte
}

// ServerRequest asks for a manager (server) certificate.
type ServerRequest struct {
	Name      string
	DNSNames  []string
	IPs       []net.IP
	NotBefore time.Time
	NotAfter  time.Time
}

// NodeRequest asks for a node (client) certificate bound to NodeID.
type NodeRequest struct {
	NodeID    string
	NotBefore time.Time
	NotAfter  time.Time
}

func newKey() (*ecdsa.PrivateKey, error) {
	return ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
}

func newSerial() (*big.Int, error) {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return nil, fmt.Errorf("create serial number: %w", err)
	}
	return n.Add(n, big.NewInt(1)), nil
}

func encodeCert(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func encodeKey(key crypto.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("encode private key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

// NewCA creates a self-signed ECDSA P-256 certificate authority that can only
// sign leaf certificates.
func NewCA(commonName string, notBefore, notAfter time.Time) (*CA, error) {
	if commonName == "" {
		return nil, errors.New("CA name must not be empty")
	}
	if !notAfter.After(notBefore) {
		return nil, errors.New("CA validity must end after it starts")
	}
	key, err := newKey()
	if err != nil {
		return nil, fmt.Errorf("generate CA key: %w", err)
	}
	serial, err := newSerial()
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: commonName, Organization: []string{"DockFleet"}},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            0,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		return nil, fmt.Errorf("sign CA certificate: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("parse CA certificate: %w", err)
	}
	return &CA{Cert: cert, key: key}, nil
}

// CertPEM returns the CA certificate, which is public and safe to distribute.
func (ca *CA) CertPEM() []byte {
	return encodeCert(ca.Cert.Raw)
}

func (ca *CA) sign(tmpl *x509.Certificate, pub crypto.PublicKey) (*Issued, error) {
	if !tmpl.NotAfter.After(tmpl.NotBefore) {
		return nil, errors.New("certificate validity must end after it starts")
	}
	if tmpl.NotAfter.After(ca.Cert.NotAfter) {
		return nil, fmt.Errorf("certificate would outlive the CA (CA expires %s)", ca.Cert.NotAfter.UTC().Format(time.RFC3339))
	}
	serial, err := newSerial()
	if err != nil {
		return nil, err
	}
	tmpl.SerialNumber = serial
	tmpl.BasicConstraintsValid = true
	tmpl.IsCA = false
	tmpl.KeyUsage = x509.KeyUsageDigitalSignature
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, pub, ca.key)
	if err != nil {
		return nil, fmt.Errorf("sign certificate: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("parse certificate: %w", err)
	}
	return &Issued{Cert: cert, CertPEM: encodeCert(der)}, nil
}

func serverTemplate(req ServerRequest) (*x509.Certificate, error) {
	if len(req.DNSNames) == 0 && len(req.IPs) == 0 {
		return nil, errors.New("a server certificate needs at least one DNS name or IP address")
	}
	if req.Name == "" {
		return nil, errors.New("server certificate name must not be empty")
	}
	return &x509.Certificate{
		Subject:     pkix.Name{CommonName: req.Name, Organization: []string{"DockFleet"}},
		DNSNames:    req.DNSNames,
		IPAddresses: req.IPs,
		NotBefore:   req.NotBefore,
		NotAfter:    req.NotAfter,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, nil
}

func nodeTemplate(req NodeRequest) (*x509.Certificate, error) {
	if !ValidNodeID(req.NodeID) {
		return nil, fmt.Errorf("node ID %q is invalid; use 1-63 characters of [A-Za-z0-9._-] starting with a letter or digit", req.NodeID)
	}
	return &x509.Certificate{
		Subject:     pkix.Name{CommonName: req.NodeID, Organization: []string{"DockFleet"}},
		URIs:        []*url.URL{NodeURI(req.NodeID)},
		NotBefore:   req.NotBefore,
		NotAfter:    req.NotAfter,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, nil
}

// IssueServer generates a key pair and signs a manager certificate.
func (ca *CA) IssueServer(req ServerRequest) (*Issued, error) {
	tmpl, err := serverTemplate(req)
	if err != nil {
		return nil, err
	}
	return ca.issueWithNewKey(tmpl)
}

// IssueNode generates a key pair and signs a certificate for a node.
func (ca *CA) IssueNode(req NodeRequest) (*Issued, error) {
	tmpl, err := nodeTemplate(req)
	if err != nil {
		return nil, err
	}
	return ca.issueWithNewKey(tmpl)
}

func (ca *CA) issueWithNewKey(tmpl *x509.Certificate) (*Issued, error) {
	key, err := newKey()
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}
	issued, err := ca.sign(tmpl, key.Public())
	if err != nil {
		return nil, err
	}
	issued.KeyPEM, err = encodeKey(key)
	if err != nil {
		return nil, err
	}
	return issued, nil
}

// NewCSR generates a key pair and a certificate signing request for a node.
// Run it on the worker so the private key never leaves the machine.
func NewCSR(nodeID string) (csrPEM, keyPEM []byte, err error) {
	if !ValidNodeID(nodeID) {
		return nil, nil, fmt.Errorf("node ID %q is invalid", nodeID)
	}
	key, err := newKey()
	if err != nil {
		return nil, nil, fmt.Errorf("generate key: %w", err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: nodeID, Organization: []string{"DockFleet"}},
	}, key)
	if err != nil {
		return nil, nil, fmt.Errorf("create certificate request: %w", err)
	}
	keyPEM, err = encodeKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}), keyPEM, nil
}

// SignNodeCSR signs a node certificate for the public key in a CSR. The
// requested names are ignored: the identity comes from req.NodeID, and the CSR
// must name the same node so a mix-up is caught.
func (ca *CA) SignNodeCSR(csrPEM []byte, req NodeRequest) (*Issued, error) {
	block, _ := pem.Decode(csrPEM)
	if block == nil || block.Type != "CERTIFICATE REQUEST" {
		return nil, errors.New("input is not a PEM certificate request")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse certificate request: %w", err)
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, fmt.Errorf("certificate request signature is invalid: %w", err)
	}
	if csr.Subject.CommonName != req.NodeID {
		return nil, fmt.Errorf("certificate request is for %q, not %q", csr.Subject.CommonName, req.NodeID)
	}
	switch pub := csr.PublicKey.(type) {
	case *ecdsa.PublicKey, ed25519.PublicKey:
	case *rsa.PublicKey:
		if pub.N.BitLen() < minRSABits {
			return nil, fmt.Errorf("RSA keys must be at least %d bits", minRSABits)
		}
	default:
		return nil, fmt.Errorf("unsupported public key type %T", csr.PublicKey)
	}
	tmpl, err := nodeTemplate(req)
	if err != nil {
		return nil, err
	}
	return ca.sign(tmpl, csr.PublicKey)
}
