package protocol

import (
	"context"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dockfleet/dockfleet/internal/registry"
	"github.com/dockfleet/dockfleet/internal/store"
)

// The observer sees every unary call with the code it ended with, those the
// handler refused and those the authentication refused before any handler ran.
func TestObserveSeesEveryUnaryCall(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	var slowest time.Duration

	node := NewNodeServer(registry.New(store.NewMemory(), discardLogger()), time.Second, discardLogger())
	lis := bufconn.Listen(1 << 20)
	srv, err := NewGRPCServer(node, ServerOptions{
		Token: serverToken,
		Observe: func(method string, code codes.Code, d time.Duration) {
			mu.Lock()
			defer mu.Unlock()
			seen = append(seen, method+" "+code.String())
			slowest = max(slowest, d)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	dial := func(token string) *Client {
		c, err := Dial(ClientConfig{
			Addr: "passthrough:///bufnet", Token: token,
			Dialer: func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) },
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	// Nothing is enrolled, so a correct token still gets PermissionDenied from the handler.
	if _, err := dial(serverToken).Register(ctxWithTimeout(t), testNode()); !IsNotEnrolled(err) {
		t.Fatalf("Register with the token: %v, want NOT_ENROLLED", err)
	}
	if _, err := dial("not-the-agent-token-0123").Register(ctxWithTimeout(t), testNode()); err == nil {
		t.Fatal("Register with a wrong token succeeded")
	}

	mu.Lock()
	defer mu.Unlock()
	want := []string{
		"/dockfleet.v1.NodeService/RegisterNode PermissionDenied",
		"/dockfleet.v1.NodeService/RegisterNode Unauthenticated",
	}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("observed %v, want %v", seen, want)
	}
	if slowest <= 0 {
		t.Fatalf("durations were not measured: %v", slowest)
	}
}
