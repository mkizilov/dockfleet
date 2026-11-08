// Package etcdfake is an in-memory stand-in for the part of etcd's JSON gateway
// that internal/etcd uses, for tests that need several managers to elect a
// leader without an etcd server. It is for tests only, like internal/dockerfake.
//
// It answers with the shapes a real etcd 3.5 answers with (the client's tests run
// against both), counts lease time on a clock the test controls, and can cut off
// or stall individual clients to model a partition or a paused process.
package etcdfake

import (
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"time"

	"github.com/dockfleet/dockfleet/internal/testnet"
)

// ClientHeader is the request header that names the caller to Partition and Stall.
const ClientHeader = "X-Test-Client"

// Listen returns a loopback listener on a free port of the project's range for
// test servers (see internal/testnet).
func Listen() (net.Listener, error) { return testnet.Listen() }

// NewServer starts an httptest server on a port of that range; useTLS says
// whether it speaks TLS.
func NewServer(h http.Handler, useTLS bool) *httptest.Server { return testnet.NewServer(h, useTLS) }

// tagged names the caller of every request to the fake.
type tagged struct {
	name string
	next http.RoundTripper
}

func (t tagged) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set(ClientHeader, t.name)
	return t.next.RoundTrip(r)
}

// HTTPClient is an HTTP client whose requests name the caller as name, so that
// Partition and Stall can single it out.
func HTTPClient(name string) *http.Client {
	return &http.Client{Transport: tagged{name, http.DefaultTransport}}
}

type lease struct {
	ttl    time.Duration
	expiry time.Time
}

type entry struct {
	value     string // base64, as it is stored on the wire
	createRev int64
	modRev    int64
	lease     int64
}

// Server is a fake etcd.
type Server struct {
	mu       sync.Mutex
	now      func() time.Time
	revision int64
	nextID   int64
	leases   map[int64]*lease
	keys     map[string]*entry // by base64 key
	cut      map[string]bool
	stall    map[string]chan struct{}
	requests map[string]int

	ts *httptest.Server
}

// New starts a fake. now is the clock lease expiry is counted on; nil means
// time.Now.
func New(now func() time.Time) *Server {
	if now == nil {
		now = time.Now
	}
	s := &Server{
		now: now, revision: 1, nextID: 1000,
		leases: map[int64]*lease{}, keys: map[string]*entry{},
		cut: map[string]bool{}, stall: map[string]chan struct{}{}, requests: map[string]int{},
	}
	s.ts = NewServer(http.HandlerFunc(s.serve), false)
	return s
}

// URL is the base URL of the fake.
func (s *Server) URL() string { return s.ts.URL }

// Close shuts the fake down.
func (s *Server) Close() {
	s.Release("")
	s.ts.Close()
}

// Partition cuts a client off (true) or lets it back (false): its requests are
// answered with etcd's "unavailable" error, as a member without quorum would.
func (s *Server) Partition(client string, cut bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cut[client] = cut
}

// Stall makes the client's requests hang until Release, or until the caller
// gives up, as when the network or the client's own process stops.
func (s *Server) Stall(client string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stall[client] == nil {
		s.stall[client] = make(chan struct{})
	}
}

// Release ends the stall of a client; an empty name releases every client.
func (s *Server) Release(client string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, ch := range s.stall {
		if client == "" || client == name {
			close(ch)
			delete(s.stall, name)
		}
	}
}

// Requests is how many requests a client made to a path such as "/v3/kv/txn".
func (s *Server) Requests(client, path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[client+" "+path]
}

// Holder is the value of a key, decoded, and whether it exists.
func (s *Server) Holder(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expire()
	e, ok := s.keys[b64(key)]
	if !ok {
		return "", false
	}
	v, _ := base64.StdEncoding.DecodeString(e.value)
	return string(v), true
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// expire removes the leases that have run out, with their keys. s.mu is held.
func (s *Server) expire() {
	now := s.now()
	bumped := false
	for id, l := range s.leases {
		if now.Before(l.expiry) {
			continue
		}
		delete(s.leases, id)
		for k, e := range s.keys {
			if e.lease == id {
				delete(s.keys, k)
				bumped = true
			}
		}
	}
	if bumped {
		s.revision++
	}
}

func (s *Server) header() map[string]string {
	return map[string]string{"cluster_id": "1", "member_id": "1", "revision": strconv.FormatInt(s.revision, 10), "raft_term": "2"}
}

func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, status, code int, msg string) {
	reply(w, status, map[string]any{"error": msg, "code": code, "message": msg})
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	client := r.Header.Get(ClientHeader)
	s.mu.Lock()
	s.requests[client+" "+r.URL.Path]++
	cut, stall := s.cut[client], s.stall[client]
	s.mu.Unlock()

	if stall != nil {
		select {
		case <-stall:
		case <-r.Context().Done():
			return
		}
	}
	if cut {
		fail(w, http.StatusServiceUnavailable, 14, "etcdserver: request timed out")
		return
	}
	if r.URL.Path == "/version" {
		reply(w, http.StatusOK, map[string]string{"etcdserver": "3.5.0-fake", "etcdcluster": "3.5.0"})
		return
	}
	if r.Method != http.MethodPost {
		fail(w, http.StatusMethodNotAllowed, 12, "method not allowed")
		return
	}
	var req struct {
		TTL     string      `json:"TTL"`
		ID      string      `json:"ID"`
		Key     string      `json:"key"`
		Compare []compareOp `json:"compare"`
		Success []txnOp     `json:"success"`
		Failure []txnOp     `json:"failure"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, 3, "invalid request: "+err.Error())
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.expire()
	switch r.URL.Path {
	case "/v3/lease/grant":
		ttl, _ := strconv.ParseInt(req.TTL, 10, 64)
		if ttl <= 0 {
			fail(w, http.StatusBadRequest, 3, "etcdserver: lease TTL must be positive")
			return
		}
		s.nextID++
		s.leases[s.nextID] = &lease{ttl: time.Duration(ttl) * time.Second, expiry: s.now().Add(time.Duration(ttl) * time.Second)}
		reply(w, http.StatusOK, map[string]any{"header": s.header(), "ID": strconv.FormatInt(s.nextID, 10), "TTL": strconv.FormatInt(ttl, 10)})
	case "/v3/lease/keepalive":
		id, _ := strconv.ParseInt(req.ID, 10, 64)
		l, ok := s.leases[id]
		if !ok {
			// A lease that is gone is answered with no ttl, as etcd does.
			reply(w, http.StatusOK, map[string]any{"result": map[string]any{"header": s.header(), "ID": req.ID}})
			return
		}
		l.expiry = s.now().Add(l.ttl)
		reply(w, http.StatusOK, map[string]any{"result": map[string]any{"header": s.header(), "ID": req.ID, "TTL": strconv.FormatInt(int64(l.ttl/time.Second), 10)}})
	case "/v3/lease/revoke":
		id, _ := strconv.ParseInt(req.ID, 10, 64)
		if _, ok := s.leases[id]; !ok {
			fail(w, http.StatusNotFound, 5, "etcdserver: requested lease not found")
			return
		}
		delete(s.leases, id)
		for k, e := range s.keys {
			if e.lease == id {
				delete(s.keys, k)
			}
		}
		s.revision++
		reply(w, http.StatusOK, map[string]any{"header": s.header()})
	case "/v3/kv/range":
		out := map[string]any{"header": s.header()}
		if e, ok := s.keys[req.Key]; ok {
			out["kvs"] = []any{s.kv(req.Key, e)}
			out["count"] = "1"
		}
		reply(w, http.StatusOK, out)
	case "/v3/kv/txn":
		s.txn(w, req.Compare, req.Success, req.Failure)
	default:
		fail(w, http.StatusNotFound, 12, "unknown method "+r.URL.Path)
	}
}

type compareOp struct {
	Target         string `json:"target"`
	Key            string `json:"key"`
	Result         string `json:"result"`
	CreateRevision string `json:"create_revision"`
}

type txnOp struct {
	Put *struct {
		Key   string `json:"key"`
		Value string `json:"value"`
		Lease string `json:"lease"`
	} `json:"request_put"`
	Range *struct {
		Key string `json:"key"`
	} `json:"request_range"`
}

func (s *Server) kv(key string, e *entry) map[string]string {
	return map[string]string{
		"key": key, "value": e.value, "create_revision": strconv.FormatInt(e.createRev, 10),
		"mod_revision": strconv.FormatInt(e.modRev, 10), "version": "1", "lease": strconv.FormatInt(e.lease, 10),
	}
}

// txn runs the one kind of transaction the election makes: "if the key does not
// exist, put it with a lease, else read it". s.mu is held.
func (s *Server) txn(w http.ResponseWriter, compare []compareOp, success, failure []txnOp) {
	holds := true
	for _, c := range compare {
		if c.Target != "CREATE" || c.Result != "EQUAL" {
			fail(w, http.StatusBadRequest, 12, "etcdserver: the fake only compares CREATE revisions for equality")
			return
		}
		rev, _ := strconv.ParseInt(c.CreateRevision, 10, 64)
		e, exists := s.keys[c.Key]
		if (exists && e.createRev != rev) || (!exists && rev != 0) {
			holds = false
		}
	}
	ops := failure
	if holds {
		ops = success
	}
	var responses []any
	for _, op := range ops {
		switch {
		case op.Put != nil:
			id, _ := strconv.ParseInt(op.Put.Lease, 10, 64)
			if id != 0 {
				if _, ok := s.leases[id]; !ok {
					fail(w, http.StatusNotFound, 5, "etcdserver: requested lease not found")
					return
				}
			}
			s.revision++
			created := s.revision
			if old, ok := s.keys[op.Put.Key]; ok {
				created = old.createRev
			}
			s.keys[op.Put.Key] = &entry{value: op.Put.Value, createRev: created, modRev: s.revision, lease: id}
			responses = append(responses, map[string]any{"response_put": map[string]any{"header": map[string]string{"revision": strconv.FormatInt(s.revision, 10)}}})
		case op.Range != nil:
			rng := map[string]any{"header": map[string]string{"revision": strconv.FormatInt(s.revision, 10)}}
			if e, ok := s.keys[op.Range.Key]; ok {
				rng["kvs"] = []any{s.kv(op.Range.Key, e)}
				rng["count"] = "1"
			}
			responses = append(responses, map[string]any{"response_range": rng})
		}
	}
	out := map[string]any{"header": s.header(), "responses": responses}
	if holds {
		out["succeeded"] = true
	}
	reply(w, http.StatusOK, out)
}
