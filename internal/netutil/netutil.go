package netutil

import (
	"fmt"
	"net"
)

// IsLoopbackAddr reports whether a host:port address names a loopback host.
// An empty host (":8080", all interfaces) is not loopback.
func IsLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// RequireLoopback fails when ln is bound to a non-loopback address. Plaintext
// servers must call it: remote access has to wait for TLS.
func RequireLoopback(name string, ln net.Listener) error {
	if !IsLoopbackAddr(ln.Addr().String()) {
		return fmt.Errorf("%s listener %s is not loopback; TLS is required for non-loopback addresses", name, ln.Addr())
	}
	return nil
}
