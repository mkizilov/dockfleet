// Command dockfleet-pki manages DockFleet's development certificate
// authority and the certificates it signs for the manager and for nodes.
//
// Private keys are written with owner-only permissions into a directory that
// is git-ignored (.dev-pki by default); nothing ever prints a private key.
package main

import (
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dockfleet/dockfleet/internal/pki"
)

const (
	defaultDir = ".dev-pki"
	day        = 24 * time.Hour
)

const usage = `dockfleet-pki: certificates for DockFleet mutual TLS

Usage: dockfleet-pki <command> [flags]

Commands:
  init-ca         create the certificate authority (ca.pem, ca.key)
  issue-manager   issue the manager's server certificate (manager.pem, manager.key)
  issue-agent     issue a node certificate and key in one step (development)
  new-csr         on the worker: create a key and a signing request (agent.key, agent.csr)
  sign-agent      on the CA host: sign a worker's request; the key never leaves the worker
  inspect         print what certificate files contain and whether they are still valid

Every command takes -h for its flags. Files are never overwritten unless -force
is given (init-ca never accepts it, so a CA key cannot be replaced by accident).
`

func main() {
	if err := run(os.Args[1:], os.Stdout, time.Now); err != nil {
		fmt.Fprintln(os.Stderr, "dockfleet-pki:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer, now func() time.Time) error {
	if len(args) == 0 {
		fmt.Fprint(out, usage)
		return errors.New("no command given")
	}
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "init-ca":
		return initCA(rest, out, now)
	case "issue-manager":
		return issueManager(rest, out, now)
	case "issue-agent":
		return issueAgent(rest, out, now)
	case "new-csr":
		return newCSR(rest, out)
	case "sign-agent":
		return signAgent(rest, out, now)
	case "inspect":
		return inspect(rest, out, now)
	case "help", "-h", "-help", "--help":
		fmt.Fprint(out, usage)
		return nil
	default:
		return fmt.Errorf("unknown command %q (run 'dockfleet-pki help')", cmd)
	}
}

func newFlagSet(name string, out io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(out)
	return fs
}

// parse parses flags and rejects stray arguments. helped is true when -h was
// given, in which case the command should do nothing.
func parse(fs *flag.FlagSet, args []string, allowArgs bool) (helped bool, err error) {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return true, nil
		}
		return false, err
	}
	if !allowArgs && fs.NArg() > 0 {
		return false, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	return false, nil
}

func caPaths(dir string) (certFile, keyFile string) {
	return filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca.key")
}

func loadCA(dir string) (*pki.CA, error) {
	certFile, keyFile := caPaths(dir)
	ca, err := pki.LoadCA(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("%w (create the CA first: dockfleet-pki init-ca -dir %s)", err, dir)
	}
	return ca, nil
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func initCA(args []string, out io.Writer, now func() time.Time) error {
	fs := newFlagSet("init-ca", out)
	dir := fs.String("dir", defaultDir, "directory for the CA files")
	name := fs.String("name", "DockFleet Dev CA", "CA common name")
	days := fs.Int("days", int(pki.DefaultCAValidity/day), "CA validity in days")
	if helped, err := parse(fs, args, false); helped || err != nil {
		return err
	}

	t := now()
	ca, err := pki.NewCA(*name, t.Add(-time.Minute), t.Add(time.Duration(*days)*day))
	if err != nil {
		return err
	}
	certFile, keyFile := caPaths(*dir)
	if err := ca.Save(certFile, keyFile); err != nil {
		return err
	}
	fmt.Fprintf(out, "CA certificate: %s\n", certFile)
	fmt.Fprintf(out, "CA private key: %s (owner-only; keep it on the CA host and never commit or copy it to workers)\n", keyFile)
	fmt.Fprintf(out, "expires:        %s\n", ca.Cert.NotAfter.UTC().Format(time.RFC3339))
	fmt.Fprintf(out, "fingerprint:    %s\n", pki.Fingerprint(ca.Cert))
	return nil
}

func issueManager(args []string, out io.Writer, now func() time.Time) error {
	fs := newFlagSet("issue-manager", out)
	dir := fs.String("dir", defaultDir, "directory holding the CA; the certificate is written here too")
	name := fs.String("name", "dockfleet-manager", "certificate common name")
	dns := fs.String("dns", "", "comma-separated DNS names agents will use to reach the manager")
	ips := fs.String("ip", "", "comma-separated IP addresses agents will use to reach the manager")
	days := fs.Int("days", int(pki.DefaultLeafValidity/day), "validity in days")
	force := fs.Bool("force", false, "replace existing manager.pem and manager.key (renewal)")
	if helped, err := parse(fs, args, false); helped || err != nil {
		return err
	}

	dnsNames := splitList(*dns)
	var addrs []net.IP
	for _, s := range splitList(*ips) {
		ip := net.ParseIP(s)
		if ip == nil {
			return fmt.Errorf("%q is not an IP address", s)
		}
		addrs = append(addrs, ip)
	}
	if len(dnsNames) == 0 && len(addrs) == 0 {
		dnsNames = []string{"localhost"}
		addrs = []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
	}

	ca, err := loadCA(*dir)
	if err != nil {
		return err
	}
	t := now()
	issued, err := ca.IssueServer(pki.ServerRequest{
		Name:      *name,
		DNSNames:  dnsNames,
		IPs:       addrs,
		NotBefore: t.Add(-time.Minute),
		NotAfter:  t.Add(time.Duration(*days) * day),
	})
	if err != nil {
		return err
	}
	certFile, keyFile := filepath.Join(*dir, "manager.pem"), filepath.Join(*dir, "manager.key")
	if err := issued.Save(certFile, keyFile, pki.WriteOptions{Overwrite: *force}); err != nil {
		return err
	}
	fmt.Fprintf(out, "manager certificate: %s\n", certFile)
	fmt.Fprintf(out, "manager private key: %s\n", keyFile)
	fmt.Fprintf(out, "names: %s\n", strings.Join(append(dnsNames, ipStrings(addrs)...), ", "))
	fmt.Fprintf(out, "expires: %s\n", issued.Cert.NotAfter.UTC().Format(time.RFC3339))
	fmt.Fprintf(out, "Agents must reach the manager by one of these names (set DOCKFLEET_TLS_SERVER_NAME if they use another).\n")
	return nil
}

func ipStrings(ips []net.IP) []string {
	out := make([]string, 0, len(ips))
	for _, ip := range ips {
		out = append(out, ip.String())
	}
	return out
}

func agentDir(caDir, nodeID, out string) string {
	if out != "" {
		return out
	}
	return filepath.Join(caDir, "agents", nodeID)
}

// writeCABundle copies the (public) CA certificate next to a node's files so
// the directory holds everything a worker needs.
func writeCABundle(ca *pki.CA, dir string) (string, error) {
	path := filepath.Join(dir, "ca.pem")
	return path, pki.WriteFile(path, ca.CertPEM(), 0o644, pki.WriteOptions{Overwrite: true})
}

func reportNode(out io.Writer, dir string, cert string, issued *pki.Issued, withKey bool) {
	fmt.Fprintf(out, "agent certificate: %s\n", cert)
	if withKey {
		fmt.Fprintf(out, "agent private key: %s\n", filepath.Join(dir, "agent.key"))
	}
	fmt.Fprintf(out, "CA certificate:    %s\n", filepath.Join(dir, "ca.pem"))
	fmt.Fprintf(out, "node ID:           %s\n", mustNodeID(issued))
	fmt.Fprintf(out, "expires:           %s\n", issued.Cert.NotAfter.UTC().Format(time.RFC3339))
	fmt.Fprintf(out, "fingerprint:       %s\n", pki.Fingerprint(issued.Cert))
	fmt.Fprintf(out, "Enroll the node before it starts: POST /v1/enrollments {\"node_id\":%q}\n", mustNodeID(issued))
}

func mustNodeID(issued *pki.Issued) string {
	id, err := pki.NodeIDFromCert(issued.Cert)
	if err != nil {
		return "?"
	}
	return id
}

func issueAgent(args []string, out io.Writer, now func() time.Time) error {
	fs := newFlagSet("issue-agent", out)
	dir := fs.String("dir", defaultDir, "directory holding the CA")
	nodeID := fs.String("node-id", "", "node ID the certificate is bound to (required)")
	outDir := fs.String("out", "", "output directory (default <dir>/agents/<node-id>)")
	days := fs.Int("days", int(pki.DefaultLeafValidity/day), "validity in days")
	force := fs.Bool("force", false, "replace existing files (renewal)")
	if helped, err := parse(fs, args, false); helped || err != nil {
		return err
	}
	if *nodeID == "" {
		return errors.New("-node-id is required")
	}

	ca, err := loadCA(*dir)
	if err != nil {
		return err
	}
	t := now()
	issued, err := ca.IssueNode(pki.NodeRequest{NodeID: *nodeID, NotBefore: t.Add(-time.Minute), NotAfter: t.Add(time.Duration(*days) * day)})
	if err != nil {
		return err
	}
	target := agentDir(*dir, *nodeID, *outDir)
	certFile, keyFile := filepath.Join(target, "agent.pem"), filepath.Join(target, "agent.key")
	if err := issued.Save(certFile, keyFile, pki.WriteOptions{Overwrite: *force}); err != nil {
		return err
	}
	if _, err := writeCABundle(ca, target); err != nil {
		return err
	}
	reportNode(out, target, certFile, issued, true)
	fmt.Fprintf(out, "This command created the private key on the CA host. For production, prefer 'new-csr' on the worker and 'sign-agent' here.\n")
	return nil
}

func newCSR(args []string, out io.Writer) error {
	fs := newFlagSet("new-csr", out)
	nodeID := fs.String("node-id", "", "node ID the certificate will be bound to (required)")
	outDir := fs.String("out", ".", "directory for agent.key and agent.csr")
	force := fs.Bool("force", false, "replace existing agent.key and agent.csr")
	if helped, err := parse(fs, args, false); helped || err != nil {
		return err
	}
	if *nodeID == "" {
		return errors.New("-node-id is required")
	}
	csrPEM, keyPEM, err := pki.NewCSR(*nodeID)
	if err != nil {
		return err
	}
	opts := pki.WriteOptions{Overwrite: *force}
	keyFile, csrFile := filepath.Join(*outDir, "agent.key"), filepath.Join(*outDir, "agent.csr")
	if err := pki.WriteFile(keyFile, keyPEM, 0o600, opts); err != nil {
		return err
	}
	if err := pki.WriteFile(csrFile, csrPEM, 0o644, opts); err != nil {
		return err
	}
	fmt.Fprintf(out, "private key:     %s (stays on this machine)\n", keyFile)
	fmt.Fprintf(out, "signing request: %s (send this file to the CA host)\n", csrFile)
	return nil
}

func signAgent(args []string, out io.Writer, now func() time.Time) error {
	fs := newFlagSet("sign-agent", out)
	dir := fs.String("dir", defaultDir, "directory holding the CA")
	nodeID := fs.String("node-id", "", "node ID to certify (required); the request must name the same node")
	csrFile := fs.String("csr", "", "certificate signing request from 'new-csr' (required)")
	outDir := fs.String("out", "", "output directory (default <dir>/agents/<node-id>)")
	days := fs.Int("days", int(pki.DefaultLeafValidity/day), "validity in days")
	force := fs.Bool("force", false, "replace an existing agent.pem (renewal)")
	if helped, err := parse(fs, args, false); helped || err != nil {
		return err
	}
	if *nodeID == "" || *csrFile == "" {
		return errors.New("-node-id and -csr are required")
	}

	csrPEM, err := os.ReadFile(*csrFile)
	if err != nil {
		return fmt.Errorf("read request: %w", err)
	}
	ca, err := loadCA(*dir)
	if err != nil {
		return err
	}
	t := now()
	issued, err := ca.SignNodeCSR(csrPEM, pki.NodeRequest{NodeID: *nodeID, NotBefore: t.Add(-time.Minute), NotAfter: t.Add(time.Duration(*days) * day)})
	if err != nil {
		return err
	}
	target := agentDir(*dir, *nodeID, *outDir)
	certFile := filepath.Join(target, "agent.pem")
	if err := issued.Save(certFile, "", pki.WriteOptions{Overwrite: *force}); err != nil {
		return err
	}
	if _, err := writeCABundle(ca, target); err != nil {
		return err
	}
	reportNode(out, target, certFile, issued, false)
	fmt.Fprintf(out, "Send agent.pem and ca.pem back to the worker, next to the agent.key that never left it.\n")
	return nil
}

func inspect(args []string, out io.Writer, now func() time.Time) error {
	fs := newFlagSet("inspect", out)
	warn := fs.Duration("warn", pki.DefaultWarnWithin, "report certificates expiring within this time")
	if helped, err := parse(fs, args, true); helped || err != nil {
		return err
	}
	if fs.NArg() == 0 {
		return errors.New("name at least one certificate file")
	}
	for _, file := range fs.Args() {
		cert, err := pki.ReadCertificate(file)
		if err != nil {
			return err
		}
		health, remaining := pki.Inspect(cert, now(), *warn)
		nodeID, idErr := pki.NodeIDFromCert(cert)
		if idErr != nil {
			nodeID = "-"
		}
		fmt.Fprintf(out, "%s\n", file)
		fmt.Fprintf(out, "  subject:     %s\n", cert.Subject.CommonName)
		fmt.Fprintf(out, "  issuer:      %s\n", cert.Issuer.CommonName)
		fmt.Fprintf(out, "  node ID:     %s\n", nodeID)
		fmt.Fprintf(out, "  names:       %s\n", strings.Join(append(append([]string{}, cert.DNSNames...), ipStrings(cert.IPAddresses)...), ", "))
		fmt.Fprintf(out, "  usage:       %s\n", describeUsage(cert))
		fmt.Fprintf(out, "  valid:       %s to %s\n", cert.NotBefore.UTC().Format(time.RFC3339), cert.NotAfter.UTC().Format(time.RFC3339))
		fmt.Fprintf(out, "  status:      %s (%s remaining)\n", health, remaining.Round(time.Hour))
		fmt.Fprintf(out, "  fingerprint: %s\n", pki.Fingerprint(cert))
	}
	return nil
}

func describeUsage(cert *x509.Certificate) string {
	var parts []string
	if cert.IsCA {
		parts = append(parts, "certificate authority")
	}
	for _, u := range cert.ExtKeyUsage {
		switch u {
		case x509.ExtKeyUsageServerAuth:
			parts = append(parts, "server auth")
		case x509.ExtKeyUsageClientAuth:
			parts = append(parts, "client auth")
		}
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, ", ")
}
