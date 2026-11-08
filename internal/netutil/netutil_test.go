package netutil

import (
	"net"
	"testing"
)

func TestIsLoopbackAddr(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1:1": true, "localhost:1": true, "[::1]:1": true,
		"0.0.0.0:1": false, ":1": false, "10.0.0.1:1": false, "nonsense": false,
	} {
		if got := IsLoopbackAddr(addr); got != want {
			t.Errorf("IsLoopbackAddr(%q) = %v, want %v", addr, got, want)
		}
	}
}

type fakeListener struct {
	net.Listener
	addr net.Addr
}

func (f fakeListener) Addr() net.Addr { return f.addr }

func TestRequireLoopback(t *testing.T) {
	remote := fakeListener{addr: &net.TCPAddr{IP: net.ParseIP("10.1.2.3"), Port: 9000}}
	if err := RequireLoopback("grpc", remote); err == nil {
		t.Error("expected error for a non-loopback listener")
	}
	local := fakeListener{addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9000}}
	if err := RequireLoopback("grpc", local); err != nil {
		t.Errorf("loopback listener rejected: %v", err)
	}
	all := fakeListener{addr: &net.TCPAddr{IP: net.IPv4zero, Port: 9000}}
	if err := RequireLoopback("grpc", all); err == nil {
		t.Error("expected error for a wildcard listener")
	}
}
