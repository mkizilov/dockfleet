package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dockfleet/dockfleet/internal/dockerfake"
	"github.com/dockfleet/dockfleet/internal/logstream"
	"github.com/dockfleet/dockfleet/internal/protocol"
	"github.com/dockfleet/dockfleet/internal/scheduler"
	"github.com/dockfleet/dockfleet/internal/store"
)

// These tests read container logs end to end: real agents on in-memory Docker
// engines, a real manager reached over gRPC (plain and mutually authenticated),
// the control loops that deploy services, and the administrative API's own
// handler. Nothing is stubbed between a container writing a line and the line
// appearing in the HTTP response.

// managerLogs gives the API the manager's hub as it is now: a restarted
// manager has a new one, and the one before it is gone.
type managerLogs struct{ m *manager }

func (l managerLogs) Open(ctx context.Context, q logstream.Query) (*logstream.Stream, error) {
	l.m.mu.Lock()
	hub := l.m.hub
	l.m.mu.Unlock()
	if hub == nil {
		return nil, logstream.ErrClosed
	}
	return hub.Open(ctx, q)
}

func (m *manager) logsConnected(node string) bool {
	m.mu.Lock()
	hub := m.hub
	m.mu.Unlock()
	return hub != nil && hub.Connected(node)
}

func (m *manager) logStreams() int {
	m.mu.Lock()
	hub := m.hub
	m.mu.Unlock()
	if hub == nil {
		return 0
	}
	return hub.Stats().Streams
}

// logResponse is the writer the API streams into. A write waits until the test
// reads it, as a real connection would.
type logResponse struct {
	t      *testing.T
	pr     *io.PipeReader
	pw     *io.PipeWriter
	header http.Header
	sent   chan struct{}
	once   sync.Once
	mu     sync.Mutex
	code   int
	body   *bufio.Reader
	cancel context.CancelFunc
	done   chan struct{}
}

func (w *logResponse) Header() http.Header { return w.header }

func (w *logResponse) WriteHeader(code int) {
	w.mu.Lock()
	if w.code == 0 {
		w.code = code
	}
	w.mu.Unlock()
	w.once.Do(func() { close(w.sent) })
}

func (w *logResponse) Write(p []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	return w.pw.Write(p)
}

func (w *logResponse) Flush() {}

func (w *logResponse) status() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.code
}

// logRec is one JSON record of a log response.
type logRec map[string]any

func (r logRec) str(k string) string { s, _ := r[k].(string); return s }

func (r logRec) num(k string) int { f, _ := r[k].(float64); return int(f) }

func (a *scaleAdmin) streamLogsWith(bearer, path string) *logResponse {
	a.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	req := httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	pr, pw := io.Pipe()
	w := &logResponse{t: a.t, pr: pr, pw: pw, header: http.Header{}, sent: make(chan struct{}), body: bufio.NewReader(pr), cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(w.done)
		a.h.ServeHTTP(w, req)
		_ = pw.Close()
	}()
	a.t.Cleanup(func() {
		cancel()
		_ = pr.CloseWithError(io.ErrClosedPipe)
		<-w.done
	})
	select {
	case <-w.sent:
	case <-time.After(5 * time.Second):
		a.t.Fatalf("GET %s: no response", path)
	}
	return w
}

func (a *scaleAdmin) streamLogs(path string) *logResponse { return a.streamLogsWith(adminToken, path) }

func (w *logResponse) next() logRec {
	w.t.Helper()
	line, err := w.body.ReadString('\n')
	if err != nil {
		w.t.Fatalf("the response ended: %v (read %q)", err, line)
	}
	var rec logRec
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		w.t.Fatalf("%q: %v", line, err)
	}
	return rec
}

// records reads up to and including the end record.
func (w *logResponse) records() []logRec {
	w.t.Helper()
	var out []logRec
	for {
		rec := w.next()
		out = append(out, rec)
		if rec.str("type") == "end" {
			return out
		}
	}
}

func lineRecords(recs []logRec) []logRec {
	var out []logRec
	for _, r := range recs {
		if r.str("type") == "line" {
			out = append(out, r)
		}
	}
	return out
}

func recTexts(recs []logRec) []string {
	var out []string
	for _, r := range lineRecords(recs) {
		out = append(out, r.str("text"))
	}
	return out
}

// logFleet is a manager with real agents on in-memory engines.
type logFleet struct {
	t      *testing.T
	m      *manager
	admin  *scaleAdmin
	agents map[string]*simAgent
}

func newLogFleet(t *testing.T, nodes ...string) *logFleet {
	t.Helper()
	m := newManager(t, nil)
	m.start()
	f := &logFleet{t: t, m: m, agents: map[string]*simAgent{}}
	for _, id := range nodes {
		f.agents[id] = m.startAgent(id, nil)
	}
	for _, id := range nodes {
		id := id
		waitFor(t, id+" to register and open its log stream", func() bool { return m.sessionID(id) != "" && m.logsConnected(id) })
	}
	m.controlLoops()
	f.admin = newScaleAdmin(t, m, scheduler.Timing{}, 0)
	return f
}

func (f *logFleet) engines() []*dockerfake.Fake {
	var out []*dockerfake.Fake
	for _, a := range f.agents {
		out = append(out, a.engine)
	}
	return out
}

// replica is one running replica of a deployed service.
type replica struct {
	index  int
	id     string
	node   string
	engine *dockerfake.Fake
}

// deploy creates a service and waits until all its replicas run.
func (f *logFleet) deploy(name string, n int) map[int]replica {
	f.t.Helper()
	f.admin.create(name, n, "")
	waitFor(f.t, "the replicas of "+name, f.admin.converged(name, n, f.engines()...))
	out := map[int]replica{}
	for _, w := range f.admin.workloads(name) {
		if w.DesiredState == store.DesiredRunning {
			out[w.ReplicaIndex] = replica{index: w.ReplicaIndex, id: w.ID, node: w.NodeID, engine: f.agents[w.NodeID].engine}
		}
	}
	return out
}

// waitForFollowers waits until the agents read the output of every replica, which is
// when a request that follows has started.
func (f *logFleet) waitForFollowers(reps map[int]replica) {
	f.t.Helper()
	perNode := map[string]int{}
	for _, r := range reps {
		perNode[r.node]++
	}
	waitFor(f.t, "a follow reader for every replica", func() bool {
		for node, n := range perNode {
			if f.agents[node].engine.OpenLogReaders() != n {
				return false
			}
		}
		return true
	})
}

func (f *logFleet) waitForNoReaders() {
	f.t.Helper()
	waitFor(f.t, "every log reader to be released", func() bool {
		for _, e := range f.engines() {
			if e.OpenLogReaders() != 0 {
				return false
			}
		}
		return f.m.logStreams() == 0
	})
}

func TestLogsOfSeveralContainersOnSeveralNodesAreMergedAndFollowed(t *testing.T) {
	f := newLogFleet(t, "sim-1", "sim-2")
	reps := f.deploy("web", 4)
	nodes := map[string]bool{}
	for _, r := range reps {
		nodes[r.node] = true
	}
	if len(reps) != 4 || len(nodes) != 2 {
		t.Fatalf("replicas = %+v; want four spread over two nodes", reps)
	}

	// Each container wrote three lines, interleaved in time with the others.
	base := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	type written struct {
		ms   int
		text string
		r    replica
	}
	var all []written
	for _, r := range reps {
		for j := 1; j <= 3; j++ {
			w := written{ms: j*10 + r.index, text: fmt.Sprintf("replica %d line %d", r.index, j), r: r}
			r.engine.AppendLog(r.id, "stdout", w.text, base.Add(time.Duration(w.ms)*time.Millisecond))
			all = append(all, w)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ms < all[j].ms })

	t.Run("history is merged by time", func(t *testing.T) {
		recs := f.admin.streamLogs("/v1/services/web/logs?tail=all").records()
		lines := lineRecords(recs)
		if len(lines) != len(all) {
			t.Fatalf("%d lines, want %d", len(lines), len(all))
		}
		for i, w := range all {
			got := lines[i]
			if got.str("text") != w.text || got.str("service") != "web" || got.num("replica") != w.r.index || got.str("node") != w.r.node ||
				got.str("workload") != w.r.id || got.str("time") != base.Add(time.Duration(w.ms)*time.Millisecond).Format(time.RFC3339Nano) {
				t.Fatalf("line %d = %v, want %q of replica %d on %s at +%dms", i, got, w.text, w.r.index, w.r.node, w.ms)
			}
		}
		if end := recs[len(recs)-1]; end.str("reason") != "complete" || end.str("cursor") == "" {
			t.Fatalf("end = %v", end)
		}
	})

	t.Run("replicas and nodes can be picked", func(t *testing.T) {
		var wantTexts []string
		for _, w := range all {
			if w.r.index == 2 || w.r.index == 3 {
				wantTexts = append(wantTexts, w.text)
			}
		}
		got := recTexts(f.admin.streamLogs("/v1/services/web/logs?tail=all&replica=2,3").records())
		if strings.Join(got, "|") != strings.Join(wantTexts, "|") {
			t.Fatalf("replicas 2 and 3: %v, want %v", got, wantTexts)
		}
		onNode := 0
		for _, rec := range lineRecords(f.admin.streamLogs("/v1/services/web/logs?tail=2&node=sim-1").records()) {
			if rec.str("node") != "sim-1" {
				t.Fatalf("a line from %s with node=sim-1", rec.str("node"))
			}
			onNode++
		}
		wantOnNode := 0
		for _, r := range reps {
			if r.node == "sim-1" {
				wantOnNode += 2
			}
		}
		if onNode != wantOnNode {
			t.Fatalf("%d lines from sim-1, want %d (the last two of each of its replicas)", onNode, wantOnNode)
		}
	})

	t.Run("following shows new lines of every replica and stops when the client leaves", func(t *testing.T) {
		resp := f.admin.streamLogs("/v1/services/web/logs?tail=0&follow=true")
		f.waitForFollowers(reps)
		for j := 1; j <= 2; j++ {
			for _, r := range reps {
				if !r.engine.Log(r.id, fmt.Sprintf("live %d-%d", r.index, j)) {
					t.Fatalf("no container for replica %d", r.index)
				}
			}
		}
		got := map[int][]string{}
		for i := 0; i < len(reps)*2; i++ {
			rec := resp.next()
			if rec.str("type") != "line" {
				t.Fatalf("record = %v; the history was not asked for", rec)
			}
			r := reps[rec.num("replica")]
			if rec.str("node") != r.node || rec.str("workload") != r.id {
				t.Fatalf("record = %v, replica %d is %s on %s", rec, r.index, r.id, r.node)
			}
			got[r.index] = append(got[r.index], rec.str("text"))
		}
		for _, r := range reps {
			want := []string{fmt.Sprintf("live %d-1", r.index), fmt.Sprintf("live %d-2", r.index)}
			if strings.Join(got[r.index], "|") != strings.Join(want, "|") {
				t.Fatalf("replica %d: %v, want %v (its own lines, once each, in order)", r.index, got[r.index], want)
			}
		}

		resp.cancel()
		select {
		case <-resp.done:
		case <-time.After(5 * time.Second):
			t.Fatal("the handler kept running after the client left")
		}
		f.waitForNoReaders()
	})
}

func TestAFollowSurvivesTheRestartOfItsAgent(t *testing.T) {
	f := newLogFleet(t, "sim-1")
	rep := f.deploy("web", 1)[1]
	rep.engine.Log(rep.id, "before")

	resp := f.admin.streamLogs("/v1/services/web/logs?tail=all&follow=true")
	if rec := resp.next(); rec.str("text") != "before" {
		t.Fatalf("record = %v", rec)
	}
	waitFor(t, "the follow reader", func() bool { return rep.engine.OpenLogReaders() == 1 })

	// The agent goes away; its container keeps running and keeps writing.
	if err := f.agents["sim-1"].stop(); err != nil {
		t.Fatal(err)
	}
	if rec := resp.next(); rec.str("type") != "notice" || rec.str("kind") != "interrupted" || rec.str("node") != "sim-1" || rec.num("replica") != 1 {
		t.Fatalf("record = %v, want an interruption notice", rec)
	}
	rep.engine.Log(rep.id, "during the gap")
	waitFor(t, "the old reader to be released", func() bool { return rep.engine.OpenLogReaders() == 0 })

	f.agents["sim-1"] = f.m.startAgentOn(rep.engine, "sim-1", nil)
	if rec := resp.next(); rec.str("type") != "notice" || rec.str("kind") != "resumed" {
		t.Fatalf("record = %v, want a resume notice", rec)
	}
	if rec := resp.next(); rec.str("type") != "line" || rec.str("text") != "during the gap" {
		t.Fatalf("record = %v; the line written in the gap arrives once, and 'before' does not repeat", rec)
	}
	waitFor(t, "the new follow reader", func() bool { return rep.engine.OpenLogReaders() == 1 })
	rep.engine.Log(rep.id, "after")
	if rec := resp.next(); rec.str("text") != "after" {
		t.Fatalf("record = %v", rec)
	}
}

func TestAClientResumesFromItsCursorAfterTheManagerRestarts(t *testing.T) {
	f := newLogFleet(t, "sim-1")
	rep := f.deploy("web", 1)[1]

	resp := f.admin.streamLogs("/v1/services/web/logs?tail=0&follow=true")
	f.waitForFollowers(map[int]replica{1: rep})
	rep.engine.Log(rep.id, "one")
	rep.engine.Log(rep.id, "two")
	for _, want := range []string{"one", "two"} {
		if rec := resp.next(); rec.str("text") != want {
			t.Fatalf("record = %v, want %q", rec, want)
		}
	}

	// The manager goes away and comes back. The open response ends and says where it stopped.
	f.m.restart()
	recs := resp.records()
	end := recs[len(recs)-1]
	if len(recs) != 1 || end.str("reason") != "closed" || !strings.Contains(end.str("cursor"), rep.id+"=") {
		t.Fatalf("after the restart the response was %v; it must end with a cursor", recs)
	}
	rep.engine.Log(rep.id, "three")
	rep.engine.Log(rep.id, "four")
	waitFor(t, "the agent's log stream on the new manager", func() bool { return f.m.logsConnected("sim-1") })

	// Asking again from the cursor gives what came after "two", once.
	got := f.admin.streamLogs("/v1/services/web/logs?tail=all&after=" + url.QueryEscape(end.str("cursor"))).records()
	if texts := recTexts(got); strings.Join(texts, "|") != "three|four" {
		t.Fatalf("lines after the cursor = %v, want three and four", texts)
	}
	// And following from it continues without a gap.
	follow := f.admin.streamLogs("/v1/services/web/logs?tail=all&follow=true&after=" + url.QueryEscape(end.str("cursor")))
	if a, b := follow.next(), follow.next(); a.str("text") != "three" || b.str("text") != "four" {
		t.Fatalf("records = %v %v", a, b)
	}
	waitFor(t, "the follow reader", func() bool { return rep.engine.OpenLogReaders() == 1 })
	rep.engine.Log(rep.id, "five")
	if rec := follow.next(); rec.str("text") != "five" {
		t.Fatalf("record = %v", rec)
	}
}

func TestACanceledRequestReleasesEveryReader(t *testing.T) {
	f := newLogFleet(t, "sim-1", "sim-2")
	reps := f.deploy("web", 4)
	resp := f.admin.streamLogs("/v1/services/web/logs?tail=0&follow=true")
	f.waitForFollowers(reps)
	if f.m.logStreams() != 1 {
		t.Fatalf("%d streams open", f.m.logStreams())
	}
	resp.cancel()
	<-resp.done
	f.waitForNoReaders()
}

func TestLogAccessIsAuthenticatedAndLimitedToWhatTheManagerOwns(t *testing.T) {
	f := newLogFleet(t, "sim-1", "sim-2", "sim-3")
	reps := f.deploy("web", 3)
	spread := map[string]bool{}
	for _, r := range reps {
		spread[r.node] = true
		r.engine.Log(r.id, fmt.Sprintf("hello from %s", r.node))
	}
	if len(spread) != 3 {
		t.Fatalf("replicas = %+v; want one on each node", reps)
	}

	t.Run("the admin API needs the admin token", func(t *testing.T) {
		for _, tt := range []struct{ name, token string }{{"no token", ""}, {"the agents' token", agentToken}, {"a made-up token", "not-a-real-token-0123456789"}} {
			resp := f.admin.streamLogsWith(tt.token, "/v1/services/web/logs?tail=all")
			if resp.status() != http.StatusUnauthorized {
				t.Errorf("%s: status %d, want 401", tt.name, resp.status())
			}
			if body, _ := io.ReadAll(resp.body); strings.Contains(string(body), "hello from") {
				t.Errorf("%s: logs were served: %s", tt.name, body)
			}
		}
		if n := f.m.logStreams(); n != 0 {
			t.Errorf("%d streams were opened for requests without credentials", n)
		}
	})

	t.Run("the agents' stream does not take the admin token or a session that is not the node's", func(t *testing.T) {
		firstError := func(client *protocol.Client, node, session string) error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			conn, err := client.Logs(ctx, node, session)
			if err == nil {
				_, err = conn.Recv()
			}
			return err
		}
		withAdmin := f.m.dial(func(c *protocol.ClientConfig) { c.Token = adminToken })
		if err := firstError(withAdmin, "sim-1", "x"); status.Code(err) != codes.Unauthenticated {
			t.Errorf("admin token on the agent stream: %v, want Unauthenticated", err)
		}
		without := f.m.dial(func(c *protocol.ClientConfig) { c.Token = "" })
		if err := firstError(without, "sim-1", "x"); status.Code(err) != codes.Unauthenticated {
			t.Errorf("no token on the agent stream: %v, want Unauthenticated", err)
		}
		agentClient := f.m.dial(nil)
		if err := firstError(agentClient, "sim-1", "not-its-session"); !protocol.NeedsRegistration(err) {
			t.Errorf("a session that is not the node's: %v, want SESSION_SUPERSEDED", err)
		}
		if err := firstError(agentClient, "stranger", "x"); !protocol.NeedsRegistration(err) {
			t.Errorf("a node that never registered: %v, want NOT_REGISTERED", err)
		}
	})

	t.Run("a node whose enrollment is revoked loses its stream and its logs", func(t *testing.T) {
		var revoked replica
		for _, r := range reps {
			if r.node == "sim-3" {
				revoked = r
			}
		}
		if revoked.id == "" {
			t.Fatalf("no replica on sim-3: %+v", reps)
		}
		revoked.engine.Log(revoked.id, "secret of sim-3")
		if _, err := f.m.store.RevokeEnrollment(context.Background(), "sim-3", time.Now()); err != nil {
			t.Fatal(err)
		}
		waitFor(t, "the revoked node's log stream to end", func() bool { return !f.m.logsConnected("sim-3") })

		recs := f.admin.streamLogs("/v1/services/web/logs?tail=all").records()
		var notice logRec
		for _, r := range recs {
			if r.str("type") == "notice" {
				notice = r
			}
			if strings.Contains(r.str("text"), "secret") {
				t.Fatalf("a revoked node's log line was served: %v", r)
			}
		}
		if notice == nil || notice.str("kind") != "unavailable" || notice.str("node") != "sim-3" || notice.num("replica") != revoked.index {
			t.Fatalf("notice = %v, want one saying sim-3's replica cannot be read", notice)
		}
		if got := len(lineRecords(recs)); got != 2 {
			t.Fatalf("%d lines; the other two nodes still answer", got)
		}

		// Enrolling it again lets the agent register and open its stream again.
		if _, err := f.m.store.CreateEnrollment(context.Background(), store.Enrollment{NodeID: "sim-3", CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
		waitFor(t, "the node's stream after re-enrollment", func() bool { return f.m.logsConnected("sim-3") })
	})
}

func TestAContainerTheAgentDoesNotHaveIsReportedNotInvented(t *testing.T) {
	f := newLogFleet(t, "sim-1")
	// The manager knows a workload on sim-1 for which the node has no container, and
	// the node has a container that is not DockFleet's.
	if _, err := f.m.store.CreateWorkload(context.Background(), store.Workload{
		ID: "w-unborn", Name: "unborn", Image: "nginx", CPUMillis: 100, MemoryBytes: 64 << 20, NodeID: "sim-1", CreatedAt: time.Now(),
		DesiredState: store.DesiredStopped,
	}); err != nil {
		t.Fatal(err)
	}
	f.agents["sim-1"].engine.AddUnmanaged("bystander", "img", map[string]string{"app": "shop"})
	recs := f.admin.streamLogs("/v1/workloads/w-unborn/logs?tail=all").records()
	if len(recs) != 2 || recs[0].str("type") != "notice" || recs[0].str("kind") != "unavailable" || !strings.Contains(recs[0].str("message"), "no container") {
		t.Fatalf("records = %v", recs)
	}
	if rec := f.admin.streamLogs("/v1/workloads/bystander/logs?tail=all"); rec.status() != http.StatusNotFound {
		t.Fatalf("a container that is not a workload: status %d, want 404", rec.status())
	}
}

func TestMutualTLSBindsTheLogStreamToTheCertificateIdentity(t *testing.T) {
	p := newTestPKI(t)
	m := newMutualTLSManager(t, p)
	certFile, keyFile := p.validNode("sim-1")
	client := m.dial(withTLS(p.clientTLS(certFile, keyFile, nil)))
	session, err := client.Register(callCtx(t), nodeInfo("sim-1"))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// sim-1's certificate speaks for sim-1 only, whatever the session.
	conn, err := client.Logs(ctx, "sim-2", session.ID)
	if err == nil {
		_, err = conn.Recv()
	}
	if !protocol.IsIdentityMismatch(err) || status.Code(err) != codes.PermissionDenied || !protocol.IsPermanent(err) {
		t.Fatalf("a log stream for another node: %v, want a permanent PERMISSION_DENIED / IDENTITY_MISMATCH", err)
	}
	if m.logsConnected("sim-2") {
		t.Fatal("a stream was attached for a node the certificate does not name")
	}
	if _, err := client.Logs(ctx, "sim-1", session.ID); err != nil {
		t.Fatalf("a log stream for itself: %v", err)
	}
	waitFor(t, "the node's own stream", func() bool { return m.logsConnected("sim-1") })

	// A certificate from another CA never gets as far as a handler.
	rogue := newTestPKI(t)
	rogueCert, rogueKey := rogue.validNode("sim-1")
	_, err = m.dial(withTLS(p.clientTLS(rogueCert, rogueKey, nil))).Logs(ctx, "sim-1", session.ID)
	requireRejectedAtTransport(t, err)
}

func TestLogsFlowOverMutualTLS(t *testing.T) {
	p := newTestPKI(t)
	m := newMutualTLSManager(t, p)
	certFile, keyFile := p.validNode("sim-1")
	sim := m.startAgent("sim-1", withTLS(p.clientTLS(certFile, keyFile, nil)))
	waitFor(t, "the agent's registration and log stream", func() bool { return m.sessionID("sim-1") != "" && m.logsConnected("sim-1") })
	if _, err := m.store.CreateWorkload(context.Background(), store.Workload{
		ID: "w1", Name: "web", Image: "nginx", CPUMillis: 100, MemoryBytes: 64 << 20, NodeID: "sim-1", CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the container to exist and write", func() bool { return sim.engine.Log("w1", "over mutual TLS") })

	admin := newScaleAdmin(t, m, scheduler.Timing{}, 0)
	recs := admin.streamLogs("/v1/workloads/w1/logs?tail=10").records()
	lines := lineRecords(recs)
	if len(lines) != 1 || lines[0].str("text") != "over mutual TLS" || lines[0].str("node") != "sim-1" {
		t.Fatalf("records = %v", recs)
	}
}
