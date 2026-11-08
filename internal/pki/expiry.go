package pki

import (
	"context"
	"crypto/x509"
	"log/slog"
	"time"
)

// DefaultWarnWithin is how long before expiry certificates start being reported as expiring.
const DefaultWarnWithin = 30 * 24 * time.Hour

// Health classifies a certificate's validity window.
type Health int

const (
	Valid Health = iota
	ExpiringSoon
	Expired
	NotYetValid
)

func (h Health) String() string {
	switch h {
	case Valid:
		return "valid"
	case ExpiringSoon:
		return "expiring soon"
	case Expired:
		return "expired"
	case NotYetValid:
		return "not yet valid"
	}
	return "unknown"
}

// Inspect classifies cert at now. remaining is the time until expiry and is
// negative once the certificate has expired.
func Inspect(cert *x509.Certificate, now time.Time, warnWithin time.Duration) (health Health, remaining time.Duration) {
	remaining = cert.NotAfter.Sub(now)
	switch {
	case now.Before(cert.NotBefore):
		return NotYetValid, remaining
	case remaining < 0:
		return Expired, remaining
	case remaining <= warnWithin:
		return ExpiringSoon, remaining
	}
	return Valid, remaining
}

// WatchExpiry re-reads certFile every interval until ctx is canceled and logs
// its health: an error once it is expired or not yet valid, a warning inside
// the warnWithin window. It checks once immediately. It does not stop the
// process: an expired certificate is rejected by peers, and a renewal is
// picked up from the same file.
func WatchExpiry(ctx context.Context, logger *slog.Logger, label, certFile string, warnWithin, interval time.Duration) {
	check := func() {
		cert, err := ReadCertificate(certFile)
		if err != nil {
			logger.Error("cannot read certificate to check its expiry", "certificate", label, "file", certFile, "error", err)
			return
		}
		health, remaining := Inspect(cert, time.Now(), warnWithin)
		attrs := []any{
			"certificate", label,
			"file", certFile,
			"subject", cert.Subject.CommonName,
			"not_after", cert.NotAfter.UTC().Format(time.RFC3339),
			"remaining", remaining.Round(time.Minute).String(),
		}
		switch health {
		case Expired:
			logger.Error("certificate has expired; peers will reject it until it is renewed", attrs...)
		case NotYetValid:
			logger.Error("certificate is not valid yet; check the system clock", attrs...)
		case ExpiringSoon:
			logger.Warn("certificate expires soon; renew it", attrs...)
		default:
			logger.Debug("certificate is valid", attrs...)
		}
	}

	check()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			check()
		}
	}
}
