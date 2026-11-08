package etcd_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/election"
	"github.com/dockfleet/dockfleet/internal/etcd"
	"github.com/dockfleet/dockfleet/internal/etcdfake"
)

// backendEnv is a backend under test and what the contract needs to make time pass.
type backendEnv struct {
	backend election.Backend
	key     string
	// advance makes d of lease time pass; nil when time cannot be moved (a real
	// etcd), in which case the tests that need it wait for real or are skipped.
	advance func(d time.Duration)
	// waitGone waits until the key is gone after a lease ran out.
	expire func(t *testing.T, l election.Lease)
}

// contract is what election.Backend must do, whatever is behind it. The fake
// runs it always and a real etcd runs it under the etcdintegration tag, which is
// how the fake is kept honest.
func contract(t *testing.T, newEnv func(t *testing.T) backendEnv) {
	ctx := context.Background()
	t.Run("a lease can be renewed and revoked, and revoking twice is fine", func(t *testing.T) {
		e := newEnv(t)
		l, err := e.backend.Grant(ctx, 3*time.Second)
		if err != nil || l.ID == "" || l.TTL < 3*time.Second {
			t.Fatalf("Grant = %+v, %v", l, err)
		}
		left, err := e.backend.KeepAlive(ctx, l)
		if err != nil || left <= 0 || left > l.TTL+time.Second {
			t.Fatalf("KeepAlive = %v, %v", left, err)
		}
		if err := e.backend.Revoke(ctx, l); err != nil {
			t.Fatalf("Revoke: %v", err)
		}
		if _, err := e.backend.KeepAlive(ctx, l); !errors.Is(err, election.ErrLeaseExpired) {
			t.Fatalf("KeepAlive of a revoked lease = %v, want ErrLeaseExpired", err)
		}
		if err := e.backend.Revoke(ctx, l); err != nil {
			t.Fatalf("revoking a lease that is gone must not fail: %v", err)
		}
		if _, err := e.backend.KeepAlive(ctx, election.Lease{ID: "123456789", TTL: time.Second}); !errors.Is(err, election.ErrLeaseExpired) {
			t.Fatalf("KeepAlive of a lease that never existed = %v, want ErrLeaseExpired", err)
		}
	})

	t.Run("exactly one claim wins and the others see who", func(t *testing.T) {
		e := newEnv(t)
		l1, _ := e.backend.Grant(ctx, 5*time.Second)
		l2, _ := e.backend.Grant(ctx, 5*time.Second)
		won, err := e.backend.Claim(ctx, e.key, "manager-a", l1)
		if err != nil || !won.Won || won.Holder.ID != "manager-a" || won.Holder.Revision <= 0 {
			t.Fatalf("first claim = %+v, %v", won, err)
		}
		lost, err := e.backend.Claim(ctx, e.key, "manager-b", l2)
		if err != nil || lost.Won || lost.Holder.ID != "manager-a" || lost.Holder.Revision != won.Holder.Revision {
			t.Fatalf("second claim = %+v, %v; want a loss that names manager-a at revision %d", lost, err, won.Holder.Revision)
		}
		h, ok, err := e.backend.Observe(ctx, e.key)
		if err != nil || !ok || h.ID != "manager-a" || h.Revision != won.Holder.Revision {
			t.Fatalf("Observe = %+v, %v, %v", h, ok, err)
		}
		// Claiming again with the lease that holds the key is a loss too: the key exists.
		again, err := e.backend.Claim(ctx, e.key, "manager-a", l1)
		if err != nil || again.Won {
			t.Fatalf("claiming a key one already holds = %+v, %v", again, err)
		}
		if _, err := e.backend.Claim(ctx, e.key+"-other", "manager-c", election.Lease{ID: "123456789", TTL: time.Second}); !errors.Is(err, election.ErrLeaseExpired) {
			t.Fatalf("claiming with a lease that does not exist = %v, want ErrLeaseExpired", err)
		}
	})

	t.Run("an empty key is observed as nobody", func(t *testing.T) {
		e := newEnv(t)
		if h, ok, err := e.backend.Observe(ctx, e.key); err != nil || ok || h.ID != "" {
			t.Fatalf("Observe = %+v, %v, %v", h, ok, err)
		}
	})

	t.Run("revoking the lease frees the key for the next revision", func(t *testing.T) {
		e := newEnv(t)
		l1, _ := e.backend.Grant(ctx, 5*time.Second)
		l2, _ := e.backend.Grant(ctx, 5*time.Second)
		first, _ := e.backend.Claim(ctx, e.key, "a", l1)
		if err := e.backend.Revoke(ctx, l1); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := e.backend.Observe(ctx, e.key); ok {
			t.Fatal("the key survived the revocation of its lease")
		}
		second, err := e.backend.Claim(ctx, e.key, "b", l2)
		if err != nil || !second.Won || second.Holder.Revision <= first.Holder.Revision {
			t.Fatalf("claim after the revocation = %+v, %v; its revision must be later than %d", second, err, first.Holder.Revision)
		}
	})

	t.Run("a lease that runs out frees the key", func(t *testing.T) {
		e := newEnv(t)
		l, err := e.backend.Grant(ctx, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if won, err := e.backend.Claim(ctx, e.key, "a", l); err != nil || !won.Won {
			t.Fatalf("claim = %+v, %v", won, err)
		}
		e.expire(t, l)
		if _, ok, err := e.backend.Observe(ctx, e.key); err != nil || ok {
			t.Fatalf("the key outlived its lease: %v, %v", ok, err)
		}
		if _, err := e.backend.KeepAlive(ctx, l); !errors.Is(err, election.ErrLeaseExpired) {
			t.Fatalf("KeepAlive after expiry = %v, want ErrLeaseExpired", err)
		}
	})

	t.Run("renewing keeps the key past its first expiry", func(t *testing.T) {
		e := newEnv(t)
		if e.advance == nil {
			t.Skip("time cannot be moved on this backend")
		}
		l, _ := e.backend.Grant(ctx, 10*time.Second)
		e.backend.Claim(ctx, e.key, "a", l)
		for i := 0; i < 4; i++ {
			e.advance(6 * time.Second)
			if _, err := e.backend.KeepAlive(ctx, l); err != nil {
				t.Fatalf("renewal %d: %v", i, err)
			}
		}
		if h, ok, _ := e.backend.Observe(ctx, e.key); !ok || h.ID != "a" {
			t.Fatalf("the key is %+v (held: %v) after 24s of 10s leases that were renewed", h, ok)
		}
	})

	t.Run("many claims at once have one winner", func(t *testing.T) {
		e := newEnv(t)
		const n = 8
		var wg sync.WaitGroup
		wins := make(chan string, n)
		for i := 0; i < n; i++ {
			l, err := e.backend.Grant(ctx, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			wg.Add(1)
			go func(id string, l election.Lease) {
				defer wg.Done()
				if c, err := e.backend.Claim(ctx, e.key, id, l); err == nil && c.Won {
					wins <- id
				}
			}(fmt.Sprintf("m%d", i), l)
		}
		wg.Wait()
		close(wins)
		var winners []string
		for id := range wins {
			winners = append(winners, id)
		}
		if len(winners) != 1 {
			t.Fatalf("winners = %v, want exactly one", winners)
		}
		if h, ok, _ := e.backend.Observe(ctx, e.key); !ok || h.ID != winners[0] {
			t.Fatalf("the key says %+v, the winner was %s", h, winners[0])
		}
	})
}

// manualClock is the fake's notion of time.
type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *manualClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newFake(t *testing.T) (*etcdfake.Server, *manualClock) {
	t.Helper()
	clock := &manualClock{t: time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)}
	srv := etcdfake.New(clock.now)
	t.Cleanup(srv.Close)
	return srv, clock
}

func TestFakeEtcdMeetsTheBackendContract(t *testing.T) {
	contract(t, func(t *testing.T) backendEnv {
		srv, clock := newFake(t)
		c, err := etcd.New(etcd.Config{Endpoints: []string{srv.URL()}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Close)
		return backendEnv{
			backend: c, key: "/dockfleet/test/leader", advance: clock.advance,
			expire: func(t *testing.T, l election.Lease) { clock.advance(l.TTL + time.Second) },
		}
	})
}

func TestClientFailsOverToAnotherMemberAndRemembersIt(t *testing.T) {
	good, _ := newFake(t)
	dead := etcdfake.NewServer(http.NotFoundHandler(), false)
	deadURL := dead.URL
	dead.Close() // nothing listens there any more
	failing := etcdfake.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"etcdserver: no leader","code":14,"message":"etcdserver: no leader"}`, http.StatusServiceUnavailable)
	}), false)
	t.Cleanup(failing.Close)

	c, err := etcd.New(etcd.Config{Endpoints: []string{deadURL, failing.URL, good.URL()}})
	if err != nil {
		t.Fatal(err)
	}
	if l, err := c.Grant(context.Background(), 5*time.Second); err != nil || l.ID == "" {
		t.Fatalf("Grant with two members down = %+v, %v", l, err)
	}
	// The member that answered is tried first from now on.
	before := good.Requests("", "/v3/lease/grant")
	if _, err := c.Grant(context.Background(), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if good.Requests("", "/v3/lease/grant") != before+1 {
		t.Error("the second call did not go to the member that worked")
	}

	allDown, err := etcd.New(etcd.Config{Endpoints: []string{deadURL, failing.URL}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := allDown.Grant(context.Background(), time.Second); err == nil {
		t.Fatal("Grant succeeded with every member down")
	}
}

func TestClientDoesNotShopAroundForAnswersEtcdMeant(t *testing.T) {
	first, _ := newFake(t)
	second, _ := newFake(t)
	c, err := etcd.New(etcd.Config{Endpoints: []string{first.URL(), second.URL()}})
	if err != nil {
		t.Fatal(err)
	}
	// "lease not found" is an answer, not an outage: the other member must not be asked.
	_, err = c.Claim(context.Background(), "/k", "a", election.Lease{ID: "424242", TTL: time.Second})
	if !errors.Is(err, election.ErrLeaseExpired) {
		t.Fatalf("Claim = %v, want ErrLeaseExpired", err)
	}
	if n := second.Requests("", "/v3/kv/txn"); n != 0 {
		t.Errorf("the second member was asked %d times about a lease the first said does not exist", n)
	}
}

func TestClientGivesUpOnASilentMemberAfterItsTimeout(t *testing.T) {
	srv, _ := newFake(t)
	srv.Stall("slow")
	c, err := etcd.New(etcd.Config{
		Endpoints:  []string{srv.URL()},
		HTTPClient: etcdfake.HTTPClient("slow"),
		Timeout:    100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	_, err = c.Grant(context.Background(), time.Second)
	if err == nil || time.Since(began) > 3*time.Second {
		t.Fatalf("Grant = %v after %v; a stalled member must end in an error at the timeout", err, time.Since(began))
	}
	srv.Release("slow")
}

func TestPartitionedClientGetsAnErrorAndOthersDoNot(t *testing.T) {
	srv, _ := newFake(t)
	mk := func(name string) *etcd.Client {
		c, err := etcd.New(etcd.Config{Endpoints: []string{srv.URL()}, HTTPClient: etcdfake.HTTPClient(name)})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	a, b := mk("a"), mk("b")
	srv.Partition("a", true)
	if _, err := a.Grant(context.Background(), time.Second); err == nil {
		t.Fatal("a partitioned client was served")
	}
	var ee *etcd.Error
	if _, err := a.Grant(context.Background(), time.Second); !errors.As(err, &ee) || ee.Status != http.StatusServiceUnavailable {
		t.Fatalf("error = %v, want etcd's 503", err)
	}
	if _, err := b.Grant(context.Background(), time.Second); err != nil {
		t.Fatalf("another client was affected: %v", err)
	}
	srv.Partition("a", false)
	if _, err := a.Grant(context.Background(), time.Second); err != nil {
		t.Fatalf("after the partition healed: %v", err)
	}
}

func TestEndpointsAreChecked(t *testing.T) {
	for _, tt := range []struct {
		name      string
		endpoints []string
		tls       *tls.Config
		want      string
	}{
		{"none", nil, nil, "at least one endpoint"},
		{"no scheme", []string{"127.0.0.1:2379"}, nil, "must look like"},
		{"a path", []string{"http://127.0.0.1:2379/v3"}, nil, "must look like"},
		{"credentials in the URL", []string{"http://user:pw@127.0.0.1:2379"}, nil, "must look like"},
		{"another scheme", []string{"ftp://127.0.0.1:2379"}, nil, "must look like"},
		{"plaintext on another host", []string{"http://etcd.example.com:2379"}, nil, "plaintext"},
		{"plaintext on a private address", []string{"http://10.1.2.3:2379"}, nil, "plaintext"},
		{"https without a TLS configuration", []string{"https://etcd.example.com:2379"}, nil, "no TLS configuration"},
		{"a second endpoint that is bad", []string{"http://127.0.0.1:2379", "http://etcd.example.com:2379"}, nil, "plaintext"},
	} {
		if _, err := etcd.New(etcd.Config{Endpoints: tt.endpoints, TLS: tt.tls}); err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%s: error = %v, want one mentioning %q", tt.name, err, tt.want)
		}
	}
	for _, ok := range [][]string{{"http://127.0.0.1:2379"}, {"http://localhost:2379"}, {"http://[::1]:2379"}, {"https://etcd.example.com:2379/"}} {
		cfg := etcd.Config{Endpoints: ok, TLS: &tls.Config{MinVersion: tls.VersionTLS12}}
		if _, err := etcd.New(cfg); err != nil {
			t.Errorf("endpoint %v was refused: %v", ok, err)
		}
	}
	if !errors.Is(mustFail(etcd.New(etcd.Config{Endpoints: []string{"http://etcd.example.com:2379"}})), etcd.ErrInsecureRemote) {
		t.Error("the plaintext refusal must be etcd.ErrInsecureRemote")
	}
}

func mustFail(_ *etcd.Client, err error) error { return err }

func TestClientVerifiesTheServerOverTLS(t *testing.T) {
	srv := etcdfake.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"header":{},"ID":"77","TTL":"5"}`))
	}), true)
	t.Cleanup(srv.Close)

	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	trusting, err := etcd.New(etcd.Config{Endpoints: []string{srv.URL}, TLS: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}})
	if err != nil {
		t.Fatal(err)
	}
	if l, err := trusting.Grant(context.Background(), 5*time.Second); err != nil || l.ID != "77" || l.TTL != 5*time.Second {
		t.Fatalf("Grant over TLS = %+v, %v", l, err)
	}

	// A client that does not trust the server's certificate does not talk to it.
	stranger, err := etcd.New(etcd.Config{Endpoints: []string{srv.URL}, TLS: &tls.Config{RootCAs: x509.NewCertPool(), MinVersion: tls.VersionTLS12}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stranger.Grant(context.Background(), time.Second); err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("Grant to a server with an untrusted certificate = %v, want a certificate error", err)
	}
}

func TestClientRejectsWhatIsNotTheExpectedReply(t *testing.T) {
	serve := func(body string, status int) *etcd.Client {
		srv := etcdfake.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		}), false)
		t.Cleanup(srv.Close)
		c, err := etcd.New(etcd.Config{Endpoints: []string{srv.URL}})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	ctx := context.Background()
	for name, c := range map[string]*etcd.Client{
		"not JSON":               serve("<html>proxy error</html>", 200),
		"a grant without an ID":  serve(`{"TTL":"5"}`, 200),
		"a grant of zero":        serve(`{"ID":"7","TTL":"0"}`, 200),
		"an empty body":          serve(``, 200),
		"an error with no shape": serve(`gateway timeout`, 504),
	} {
		if l, err := c.Grant(ctx, time.Second); err == nil {
			t.Errorf("%s: Grant returned %+v and no error", name, l)
		}
	}
	// A reply that is far too big is refused rather than read.
	huge := serve(`{"ID":"7","TTL":"5","pad":"`+strings.Repeat("x", 2<<20)+`"}`, 200)
	if _, err := huge.Grant(ctx, time.Second); err == nil || !strings.Contains(err.Error(), "larger than") {
		t.Errorf("a 2 MiB reply: %v", err)
	}
	// A claim that is lost without saying to whom is an error, not a guess.
	odd := serve(`{"header":{"revision":"9"},"responses":[{"response_range":{}}]}`, 200)
	if _, err := odd.Claim(ctx, "/k", "a", election.Lease{ID: "7", TTL: time.Second}); err == nil {
		t.Error("a lost claim with no holder was accepted")
	}
	// A holder whose value is not base64 is an error too.
	bad := serve(`{"responses":[{"response_range":{"kvs":[{"key":"a","value":"***","create_revision":"2"}]}}]}`, 200)
	if _, err := bad.Claim(ctx, "/k", "a", election.Lease{ID: "7", TTL: time.Second}); err == nil {
		t.Error("a holder that is not base64 was accepted")
	}
}

func TestCanceledContextEndsACall(t *testing.T) {
	srv, _ := newFake(t)
	srv.Stall("held")
	c, err := etcd.New(etcd.Config{Endpoints: []string{srv.URL()}, HTTPClient: etcdfake.HTTPClient("held"), Timeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := c.Grant(ctx, time.Second); done <- err }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a canceled call did not return")
	}
	srv.Release("held")
}
