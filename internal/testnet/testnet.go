// Package testnet starts the HTTP and TLS servers that tests need on the
// project's loopback port range, instead of the ephemeral ports httptest picks.
// It is for tests only.
package testnet

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
)

// The ports kept for the servers tests start with NewServer. (The project's
// range is 21200-21299; the other tests that open sockets keep to their own
// parts of it, see AGENTS.md.)
const (
	FirstPort = 21240
	LastPort  = 21259
)

// Listen returns a loopback listener on a free port of the range. Of several
// tests that ask at once, each gets a port of its own, because a port is taken
// by listening on it.
func Listen() (net.Listener, error) {
	for port := FirstPort; port <= LastPort; port++ {
		if ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port)); err == nil {
			return ln, nil
		}
	}
	return nil, errors.New("testnet: no free port in 21240-21259")
}

// NewServer starts an httptest server on a port of the range; useTLS says
// whether it speaks TLS (with httptest's throwaway certificate). It panics if no
// port is free, which only a broken test run can cause.
func NewServer(h http.Handler, useTLS bool) *httptest.Server {
	ln, err := Listen()
	if err != nil {
		panic(err)
	}
	ts := httptest.NewUnstartedServer(h)
	_ = ts.Listener.Close()
	ts.Listener = ln
	if useTLS {
		ts.StartTLS()
	} else {
		ts.Start()
	}
	return ts
}
