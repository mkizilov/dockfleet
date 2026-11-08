package api

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/logstream"
	"github.com/dockfleet/dockfleet/internal/store"
)

// These tests read container logs through the administrative API: a real hub
// behind the handlers, simulated agents on the other side of it, and a
// response writer that hands every write to the test and honors write
// deadlines, so a client that does not read is a client that does not read.

var logT0 = time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

// at is n milliseconds after logT0.
func at(n int) time.Time { return logT0.Add(time.Duration(n) * time.Millisecond) }

// stamp is how the API writes the time of a line.
func stamp(n int) string { return at(n).Format(time.RFC3339Nano) }

type simConn struct {
	down chan logstream.Down
	up   chan logstream.Up
	done chan struct{}
	once sync.Once
}

func (c *simConn) Send(d logstream.Down) error {
	select {
	case c.down <- d:
		return nil
	case <-c.done:
		return io.ErrClosedPipe
	}
}

func (c *simConn) Recv() (logstream.Up, error) {
	select {
	case u := <-c.up:
		return u, nil
	case <-c.done:
		return logstream.Up{}, io.EOF
	}
}

func (c *simConn) hangUp() { c.once.Do(func() { close(c.done) }) }

// simAgent plays an agent: it answers every request from the lines its
// containers "wrote", and sends live lines to the requests that follow.
type simAgent struct {
	conn *simConn

	mu       sync.Mutex
	lines    map[string][]logstream.Line
	opens    []logstream.Open
	live     map[string]string
	canceled []string
}

func attachSim(t *testing.T, hub *logstream.Hub, node string, preset map[string][]logstream.Line) *simAgent {
	t.Helper()
	a := &simAgent{
		conn:  &simConn{down: make(chan logstream.Down, 256), up: make(chan logstream.Up, 256), done: make(chan struct{})},
		lines: map[string][]logstream.Line{}, live: map[string]string{},
	}
	for w, ls := range preset {
		a.lines[w] = append([]logstream.Line(nil), ls...)
	}
	t.Cleanup(a.conn.hangUp)
	go a.respond()
	go func() { _ = hub.Attach(context.Background(), node, a.conn) }()
	deadline := time.Now().Add(5 * time.Second)
	for !hub.Connected(node) {
		if time.Now().After(deadline) {
			t.Fatalf("%s never attached", node)
		}
		time.Sleep(time.Millisecond)
	}
	return a
}

func (a *simAgent) respond() {
	for {
		select {
		case d := <-a.conn.down:
			a.handle(d)
		case <-a.conn.done:
			return
		}
	}
}

func (a *simAgent) handle(d logstream.Down) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch {
	case d.Open != nil:
		o := *d.Open
		a.opens = append(a.opens, o)
		var hist []logstream.Line
		for _, l := range a.lines[o.WorkloadID] {
			if o.After.IsZero() || l.Time.After(o.After) {
				hist = append(hist, l)
			}
		}
		if o.Tail >= 0 && len(hist) > o.Tail {
			hist = hist[len(hist)-o.Tail:]
		}
		a.conn.up <- logstream.Up{Batch: &logstream.Batch{RequestID: o.RequestID, Lines: hist, HistoryComplete: true}}
		if o.Follow {
			a.live[o.RequestID] = o.WorkloadID
		} else {
			a.conn.up <- logstream.Up{End: &logstream.End{RequestID: o.RequestID, Reason: logstream.EndComplete}}
		}
	case d.Cancel != nil:
		a.canceled = append(a.canceled, d.Cancel.RequestID)
		delete(a.live, d.Cancel.RequestID)
	}
}

// write makes a container write a line (stdout) at the time n ms after logT0.
func (a *simAgent) write(workload string, n int, text string) {
	a.writeTo(workload, n, logstream.StreamStdout, text)
}

func (a *simAgent) writeTo(workload string, n int, stream, text string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	line := logstream.Line{Time: at(n), Stream: stream, Text: text}
	a.lines[workload] = append(a.lines[workload], line)
	for req, w := range a.live {
		if w == workload {
			a.conn.up <- logstream.Up{Batch: &logstream.Batch{RequestID: req, Lines: []logstream.Line{line}}}
		}
	}
}

func (a *simAgent) openRequests() []logstream.Open {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]logstream.Open(nil), a.opens...)
}

func (a *simAgent) cancelCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.canceled)
}

// fakeLogResponse is the writer a handler streams into. A write waits until the
// test reads it, up to the write deadline the handler set: a test that does not
// read is a client that stopped reading.
type fakeLogResponse struct {
	ctx    context.Context
	header http.Header
	chunks chan []byte
	sent   chan struct{}
	once   sync.Once

	mu       sync.Mutex
	status   int
	deadline time.Time
	rest     []byte
}

func (w *fakeLogResponse) Header() http.Header { return w.header }

func (w *fakeLogResponse) WriteHeader(code int) {
	w.mu.Lock()
	if w.status == 0 {
		w.status = code
	}
	w.mu.Unlock()
	w.once.Do(func() { close(w.sent) })
}

func (w *fakeLogResponse) Write(p []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	w.mu.Lock()
	dl := w.deadline
	w.mu.Unlock()
	var expired <-chan time.Time
	if !dl.IsZero() {
		timer := time.NewTimer(time.Until(dl))
		defer timer.Stop()
		expired = timer.C
	}
	select {
	case w.chunks <- append([]byte(nil), p...):
		return len(p), nil
	case <-expired:
		return 0, os.ErrDeadlineExceeded
	case <-w.ctx.Done():
		return 0, w.ctx.Err()
	}
}

func (w *fakeLogResponse) Flush() {}

func (w *fakeLogResponse) SetWriteDeadline(t time.Time) error {
	w.mu.Lock()
	w.deadline = t
	w.mu.Unlock()
	return nil
}

func (w *fakeLogResponse) SetReadDeadline(time.Time) error { return nil }

func (w *fakeLogResponse) Read(p []byte) (int, error) {
	if len(w.rest) == 0 {
		select {
		case chunk, ok := <-w.chunks:
			if !ok {
				return 0, io.EOF
			}
			w.rest = chunk
		case <-time.After(10 * time.Second):
			return 0, errors.New("timed out waiting for the log response")
		}
	}
	n := copy(p, w.rest)
	w.rest = w.rest[n:]
	return n, nil
}

// logCall is a request to a log endpoint that is being answered.
type logCall struct {
	t      *testing.T
	w      *fakeLogResponse
	body   *bufio.Reader
	cancel context.CancelFunc
	done   chan struct{}
}

func (e *serviceEnv) getLogs(path string, headers ...string) *logCall {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	req := httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+token)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	w := &fakeLogResponse{ctx: ctx, header: http.Header{}, chunks: make(chan []byte), sent: make(chan struct{})}
	c := &logCall{t: e.t, w: w, body: bufio.NewReader(w), cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(c.done)
		e.h.ServeHTTP(w, req)
		close(w.chunks)
	}()
	e.t.Cleanup(func() {
		cancel()
		<-c.done
	})
	select {
	case <-w.sent:
	case <-time.After(5 * time.Second):
		e.t.Fatalf("GET %s: no response", path)
	}
	return c
}

func (c *logCall) status() int {
	c.w.mu.Lock()
	defer c.w.mu.Unlock()
	return c.w.status
}

// all reads the rest of the response.
func (c *logCall) all() string {
	c.t.Helper()
	b, err := io.ReadAll(c.body)
	if err != nil {
		c.t.Fatalf("reading the response: %v", err)
	}
	return string(b)
}

func (c *logCall) line() string {
	c.t.Helper()
	s, err := c.body.ReadString('\n')
	if err != nil {
		c.t.Fatalf("reading a line of the response (got %q): %v", s, err)
	}
	return strings.TrimSuffix(s, "\n")
}

// record reads the next JSON record.
func (c *logCall) record() logRecord {
	c.t.Helper()
	s := c.line()
	var rec logRecord
	if err := json.Unmarshal([]byte(s), &rec); err != nil {
		c.t.Fatalf("%q is not a record: %v", s, err)
	}
	return rec
}

// records reads records up to and including the end record.
func (c *logCall) records() []logRecord {
	c.t.Helper()
	var out []logRecord
	for {
		rec := c.record()
		out = append(out, rec)
		if rec.Type == "end" {
			return out
		}
	}
}

// texts are the texts of the lines among records.
func texts(recs []logRecord) []string {
	var out []string
	for _, r := range recs {
		if r.Type == "line" {
			out = append(out, r.Text)
		}
	}
	return out
}

func sameStrings(a, b []string) bool {
	return strings.Join(a, "\x00") == strings.Join(b, "\x00") && len(a) == len(b)
}

// withLogs puts a hub behind the handlers.
func (e *serviceEnv) withLogs(cfg logstream.HubConfig, writeTimeout time.Duration) *logstream.Hub {
	e.t.Helper()
	hub := logstream.NewHub(cfg)
	e.t.Cleanup(hub.Shutdown)
	e.h = New(Config{
		Store: e.st, Token: token, NodeTTL: 30 * time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		InstanceID: "test-manager", Now: func() time.Time { return e.clock }, Logs: hub, LogWriteTimeout: writeTimeout,
	})
	return hub
}

// logWorkloads creates the service "web" with replicas 1 and 2 of revision 1
// (on n1 and n2), replica 1 of revision 2 (on n1) and a replica that was
// stopped, and returns the workload IDs it made.
func (e *serviceEnv) logWorkloads() {
	e.t.Helper()
	e.addNode("n1", 4000, 8<<30)
	e.addNode("n2", 4000, 8<<30)
	svc := e.create(webService(2, 500))
	put(e, "web", webUpdate("nginx:1.28", 2, 500))
	ctx := context.Background()
	for _, w := range []struct {
		id, node          string
		revision, replica int
	}{
		{"w-web-1", "n1", 1, 1}, {"w-web-2", "n2", 1, 2}, {"w-web-r2-1", "n1", 2, 1}, {"w-web-gone", "n1", 1, 3},
	} {
		_, err := e.st.CreateWorkload(ctx, store.Workload{
			ID: w.id, Name: "web-" + w.id, Image: "nginx", CPUMillis: 1, MemoryBytes: 1, NodeID: w.node,
			ServiceID: svc.ID, ServiceRevision: w.revision, ReplicaIndex: w.replica, DesiredState: store.DesiredRunning, Generation: 1, CreatedAt: e.clock,
		})
		if err != nil {
			e.t.Fatal(err)
		}
	}
	if _, err := e.st.SetWorkloadDesiredState(ctx, "w-web-gone", store.DesiredStopped, e.clock); err != nil {
		e.t.Fatal(err)
	}
	if _, err := e.st.CreateWorkload(ctx, store.Workload{
		ID: "w-solo", Name: "solo", Image: "nginx", CPUMillis: 1, MemoryBytes: 1, NodeID: "n2", CreatedAt: e.clock,
	}); err != nil {
		e.t.Fatal(err)
	}
}

// logFleet creates the workloads and attaches an agent for each node, with the
// lines the containers wrote so far.
func (e *serviceEnv) logFleet(hub *logstream.Hub) (n1, n2 *simAgent) {
	e.t.Helper()
	e.logWorkloads()
	line := func(n int, text string) logstream.Line {
		return logstream.Line{Time: at(n), Stream: logstream.StreamStdout, Text: text}
	}
	n1 = attachSim(e.t, hub, "n1", map[string][]logstream.Line{
		"w-web-1":    {line(1, "a1"), line(4, "a2")},
		"w-web-r2-1": {line(6, "c1")},
	})
	n2 = attachSim(e.t, hub, "n2", map[string][]logstream.Line{
		"w-web-2": {line(2, "b1"), line(3, "b2"), {Time: at(5), Stream: logstream.StreamStderr, Text: "b-err"}},
		"w-solo":  {line(9, "solo says hi")},
	})
	return n1, n2
}

func testServiceLogsAreMergedByTimeAndFiltered(t *testing.T, e *serviceEnv) {
	hub := e.withLogs(logstream.HubConfig{}, 0)
	e.logFleet(hub)

	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{"every replica that is meant to run", "tail=10", []string{"a1", "b1", "b2", "a2", "b-err", "c1"}},
		{"one replica", "tail=10&replica=2", []string{"b1", "b2", "b-err"}},
		{"several replicas", "tail=10&replica=1,2&revision=1", []string{"a1", "b1", "b2", "a2", "b-err"}},
		{"a replica of a revision", "tail=10&replica=1&revision=2", []string{"c1"}},
		{"one node", "tail=10&node=n1", []string{"a1", "a2", "c1"}},
		{"one workload", "tail=10&workload=w-web-2", []string{"b1", "b2", "b-err"}},
		{"the last line of each replica", "tail=1", []string{"a2", "b-err", "c1"}},
		{"no history", "tail=0", nil},
		{"all of it", "tail=all", []string{"a1", "b1", "b2", "a2", "b-err", "c1"}},
		{"since a time", "tail=10&since=" + stamp(3), []string{"a2", "b-err", "c1"}},
		{"after a cursor", "tail=10&after=w-web-1=" + fmt.Sprint(at(1).UnixNano()) + ",w-web-2=" + fmt.Sprint(at(3).UnixNano()), []string{"a2", "b-err", "c1"}},
		{"a cursor and a time", "tail=10&since=" + stamp(2) + "&after=w-web-2=" + fmt.Sprint(at(3).UnixNano()), []string{"a2", "b-err", "c1"}},
	}
	for _, tt := range tests {
		e.run(t, tt.name, func(t *testing.T) {
			c := e.getLogs("/v1/services/web/logs?" + tt.query)
			if c.status() != http.StatusOK {
				t.Fatalf("status = %d: %s", c.status(), c.all())
			}
			recs := c.records()
			if got := texts(recs); !sameStrings(got, tt.want) {
				t.Fatalf("lines = %v, want %v", got, tt.want)
			}
			if end := recs[len(recs)-1]; end.Reason != "complete" || end.Cursor == "" && len(tt.want) > 0 {
				t.Fatalf("end record = %+v", end)
			}
		})
	}
}

func testServiceLogRecordsSayWhereALineCameFrom(t *testing.T, e *serviceEnv) {
	hub := e.withLogs(logstream.HubConfig{}, 0)
	e.logFleet(hub)
	c := e.getLogs("/v1/services/web/logs?tail=10&replica=2")
	if h := c.w.header; h.Get("Content-Type") != "application/x-ndjson" || h.Get("Cache-Control") != "no-store" || h.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("headers = %v", h)
	}
	first, second, third, end := c.record(), c.record(), c.record(), c.record()
	want := logRecord{Type: "line", Time: stamp(2), Service: "web", Replica: 2, Revision: 1, Workload: "w-web-2", Name: "web-w-web-2", Node: "n2", Stream: "stdout", Text: "b1"}
	if first != want {
		t.Fatalf("record = %+v\nwant     %+v", first, want)
	}
	if third.Stream != "stderr" || third.Text != "b-err" || second.Text != "b2" {
		t.Fatalf("records = %+v %+v", second, third)
	}
	if end.Type != "end" || end.Reason != "complete" || !strings.Contains(end.Cursor, "w-web-2="+fmt.Sprint(at(5).UnixNano())) {
		t.Fatalf("end = %+v; the cursor says where to resume", end)
	}

	// Without timestamps the lines carry none.
	c = e.getLogs("/v1/services/web/logs?tail=1&replica=2&timestamps=false")
	if rec := c.record(); rec.Time != "" || rec.Text != "b-err" {
		t.Fatalf("record = %+v", rec)
	}

	// A standalone workload has no service or replica.
	c = e.getLogs("/v1/workloads/w-solo/logs")
	solo := c.record()
	if solo.Type != "line" || solo.Service != "" || solo.Replica != 0 || solo.Name != "solo" || solo.Node != "n2" || solo.Text != "solo says hi" {
		t.Fatalf("record = %+v", solo)
	}
	// A replica addressed by its workload ID is still a replica of its service.
	c = e.getLogs("/v1/workloads/w-web-r2-1/logs?tail=5")
	if rec := c.record(); rec.Service != "web" || rec.Replica != 1 || rec.Revision != 2 || rec.Text != "c1" {
		t.Fatalf("record = %+v", rec)
	}
}

func testLogTextAndEventFormats(t *testing.T, e *serviceEnv) {
	hub := e.withLogs(logstream.HubConfig{}, 0)
	n1, _ := e.logFleet(hub)
	n1.writeTo("w-web-1", 7, logstream.StreamStderr, "col\x1b[31mored\x07 and\ttabbed")

	e.run(t, "text", func(t *testing.T) {
		c := e.getLogs("/v1/services/web/logs?tail=10&replica=1&revision=1&format=text")
		if got := c.w.header.Get("Content-Type"); got != "text/plain; charset=utf-8" {
			t.Fatalf("content type = %q", got)
		}
		want := stamp(1) + " web/1 stdout: a1\n" +
			stamp(4) + " web/1 stdout: a2\n" +
			stamp(7) + " web/1 stderr: col\uFFFD[31mored\uFFFD and\ttabbed\n" +
			"-- end (complete)\n"
		if got := c.all(); got != want {
			t.Fatalf("output =\n%q\nwant\n%q", got, want)
		}
	})
	e.run(t, "text without timestamps", func(t *testing.T) {
		c := e.getLogs("/v1/services/web/logs?tail=1&replica=1&revision=1&format=text&timestamps=false")
		if got, want := c.all(), "web/1 stderr: col\uFFFD[31mored\uFFFD and\ttabbed\n-- end (complete)\n"; got != want {
			t.Fatalf("output = %q, want %q", got, want)
		}
	})
	e.run(t, "ndjson escapes control characters", func(t *testing.T) {
		c := e.getLogs("/v1/services/web/logs?tail=1&replica=1&revision=1")
		raw := c.all()
		if strings.ContainsAny(raw, "\x1b\x07") || !strings.Contains(raw, `\u001b[31mored\u0007`) {
			t.Fatalf("output = %q; a terminal escape must not reach the output", raw)
		}
	})
	e.run(t, "events", func(t *testing.T) {
		c := e.getLogs("/v1/services/web/logs?tail=1&replica=2", "Accept", "text/event-stream")
		if got := c.w.header.Get("Content-Type"); got != "text/event-stream" {
			t.Fatalf("content type = %q", got)
		}
		out := c.all()
		blocks := strings.Split(strings.TrimSpace(out), "\n\n")
		if len(blocks) != 2 {
			t.Fatalf("events = %q", out)
		}
		lines := strings.Split(blocks[0], "\n")
		if len(lines) != 3 || lines[0] != "id: w-web-2="+fmt.Sprint(at(5).UnixNano()) || lines[1] != "event: line" || !strings.HasPrefix(lines[2], "data: {") ||
			!strings.Contains(lines[2], `"text":"b-err"`) {
			t.Fatalf("first event = %q", blocks[0])
		}
		if !strings.HasPrefix(blocks[1], "event: end\ndata: ") {
			t.Fatalf("last event = %q", blocks[1])
		}
	})
}

func testFollowingShowsNewLinesAndStopsWhenTheClientLeaves(t *testing.T, e *serviceEnv) {
	hub := e.withLogs(logstream.HubConfig{}, 0)
	n1, n2 := e.logFleet(hub)
	c := e.getLogs("/v1/services/web/logs?tail=1&follow=true&revision=1")
	if got := []string{c.record().Text, c.record().Text}; !sameStrings(got, []string{"a2", "b-err"}) {
		t.Fatalf("history = %v", got)
	}

	n1.write("w-web-1", 20, "live a")
	n2.write("w-web-2", 21, "live b")
	got := map[string]logRecord{}
	for i := 0; i < 2; i++ {
		rec := c.record()
		got[rec.Text] = rec
	}
	if got["live a"].Node != "n1" || got["live a"].Replica != 1 || got["live b"].Node != "n2" || got["live b"].Time != stamp(21) {
		t.Fatalf("live records = %+v", got)
	}

	// The client leaves: the handler returns and both agents are told to stop reading.
	c.cancel()
	select {
	case <-c.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler kept running after the client left")
	}
	deadline := time.Now().Add(5 * time.Second)
	for n1.cancelCount() == 0 || n2.cancelCount() == 0 || hub.Stats().Streams != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("cancels: %d and %d, open streams %d", n1.cancelCount(), n2.cancelCount(), hub.Stats().Streams)
		}
		time.Sleep(time.Millisecond)
	}
}

func testLogStreamResumesWhenAnAgentReconnects(t *testing.T, e *serviceEnv) {
	hub := e.withLogs(logstream.HubConfig{ReconnectWait: 10 * time.Second}, 0)
	n1, _ := e.logFleet(hub)
	c := e.getLogs("/v1/services/web/logs?tail=5&follow=true&revision=1&replica=1")
	if rec := c.record(); rec.Text != "a1" {
		t.Fatalf("record = %+v", rec)
	}
	c.record() // a2
	n1.conn.hangUp()
	rec := c.record()
	if rec.Type != "notice" || rec.Kind != "interrupted" || rec.Workload != "w-web-1" || rec.Node != "n1" || rec.Service != "web" {
		t.Fatalf("notice = %+v", rec)
	}

	// The agent comes back, having written another line meanwhile. Only that one is new.
	line := func(n int, text string) logstream.Line {
		return logstream.Line{Time: at(n), Stream: logstream.StreamStdout, Text: text}
	}
	attachSim(t, hub, "n1", map[string][]logstream.Line{"w-web-1": {line(1, "a1"), line(4, "a2"), line(30, "a3")}})
	if rec := c.record(); rec.Type != "notice" || rec.Kind != "resumed" {
		t.Fatalf("notice = %+v", rec)
	}
	if rec := c.record(); rec.Text != "a3" || rec.Time != stamp(30) {
		t.Fatalf("record = %+v; the lines before the interruption must not repeat", rec)
	}
}

func testLogStreamReportsNodesItCannotReach(t *testing.T, e *serviceEnv) {
	hub := e.withLogs(logstream.HubConfig{}, 0)
	e.logWorkloads()
	line := func(n int, text string) logstream.Line {
		return logstream.Line{Time: at(n), Stream: logstream.StreamStdout, Text: text}
	}
	// Only n1 is connected to this manager.
	attachSim(t, hub, "n1", map[string][]logstream.Line{"w-web-1": {line(1, "a1")}, "w-web-r2-1": {line(6, "c1")}})
	recs := e.getLogs("/v1/services/web/logs?tail=5").records()
	var notices []logRecord
	for _, r := range recs {
		if r.Type == "notice" {
			notices = append(notices, r)
		}
	}
	if len(notices) != 1 || notices[0].Kind != "unavailable" || notices[0].Node != "n2" || notices[0].Replica != 2 || !strings.Contains(notices[0].Message, "not connected") {
		t.Fatalf("notices = %+v", notices)
	}
	if got := texts(recs); !sameStrings(got, []string{"a1", "c1"}) {
		t.Fatalf("lines = %v; the replicas that can be read are still read", got)
	}
}

func testLogResumeUsesTheCursor(t *testing.T, e *serviceEnv) {
	hub := e.withLogs(logstream.HubConfig{}, 0)
	n1, n2 := e.logFleet(hub)
	cursor := "w-web-1=" + fmt.Sprint(at(1).UnixNano()) + ",w-web-2=" + fmt.Sprint(at(3).UnixNano())

	e.getLogs("/v1/services/web/logs?tail=10&revision=1&after=" + cursor).records()
	cursors := map[string]time.Time{}
	for _, o := range append(n1.openRequests(), n2.openRequests()...) {
		cursors[o.WorkloadID] = o.After
	}
	if !cursors["w-web-1"].Equal(at(1)) || !cursors["w-web-2"].Equal(at(3)) {
		t.Fatalf("cursors the agents were given = %v", cursors)
	}

	// An event stream is resumed with the header a browser sends on its own.
	c := e.getLogs("/v1/services/web/logs?tail=10&revision=1&replica=2", "Accept", "text/event-stream", "Last-Event-ID", "w-web-2="+fmt.Sprint(at(3).UnixNano()))
	if out := c.all(); !strings.Contains(out, `"text":"b-err"`) || strings.Contains(out, `"text":"b2"`) {
		t.Fatalf("events = %q; only what came after the last event may follow", out)
	}
}

func testBadLogRequestsAreRefused(t *testing.T, e *serviceEnv) {
	hub := e.withLogs(logstream.HubConfig{}, 0)
	e.logFleet(hub)
	bad := []struct {
		name, path, field string
	}{
		{"an unknown parameter", "/v1/services/web/logs?lines=5", "lines"},
		{"a filter the workload endpoint lacks", "/v1/workloads/w-solo/logs?replica=1", "replica"},
		{"a negative tail", "/v1/services/web/logs?tail=-1", "tail"},
		{"too long a tail", "/v1/services/web/logs?tail=999999", "tail"},
		{"a tail that is not a number", "/v1/services/web/logs?tail=some", "tail"},
		{"a parameter given twice", "/v1/services/web/logs?tail=1&tail=2", "tail"},
		{"follow that is not a boolean", "/v1/services/web/logs?follow=maybe", "follow"},
		{"timestamps that are not a boolean", "/v1/services/web/logs?timestamps=2x", "timestamps"},
		{"an unknown format", "/v1/services/web/logs?format=xml", "format"},
		{"a since nobody can read", "/v1/services/web/logs?since=yesterday", "since"},
		{"a negative since", "/v1/services/web/logs?since=-5m", "since"},
		{"a since too far back", "/v1/services/web/logs?since=9999h", "since"},
		{"a cursor that is not one", "/v1/services/web/logs?after=w-web-1", "after"},
		{"a cursor with a bad time", "/v1/services/web/logs?after=w-web-1=soon", "after"},
		{"replica zero", "/v1/services/web/logs?replica=0", "replica"},
		{"a replica that is not a number", "/v1/services/web/logs?replica=1,x", "replica"},
		{"a revision of zero", "/v1/services/web/logs?revision=0", "revision"},
		{"a node name with a slash", "/v1/services/web/logs?node=a/b", "node"},
		{"a workload name with spaces", "/v1/services/web/logs?workload=a%20b", "workload"},
	}
	for _, tt := range bad {
		e.run(t, tt.name, func(t *testing.T) {
			c := e.getLogs(tt.path)
			body := c.all()
			if c.status() != http.StatusBadRequest || !strings.Contains(body, `"field":"`+tt.field+`"`) {
				t.Fatalf("status %d: %s; want a 400 naming %s", c.status(), body, tt.field)
			}
		})
	}
	// Nothing was asked of any agent for a request that was refused.
	if n := hub.Stats().Streams; n != 0 {
		t.Fatalf("%d streams left open by refused requests", n)
	}
}

func testLogRequestsForThingsThatAreNotThere(t *testing.T, e *serviceEnv) {
	hub := e.withLogs(logstream.HubConfig{}, 0)
	e.logFleet(hub)
	for _, tt := range []struct{ name, path, message string }{
		{"an unknown service", "/v1/services/nobody/logs", "service not found"},
		{"an unknown workload", "/v1/workloads/nobody/logs", "workload not found"},
		{"a replica that does not exist", "/v1/services/web/logs?replica=9", "no replica of service web"},
		{"a revision that does not exist", "/v1/services/web/logs?revision=9", "no replica of service web"},
		{"a node that runs none", "/v1/services/web/logs?node=n9", "no replica of service web"},
		{"only a stopped replica is selected", "/v1/services/web/logs?workload=w-web-gone", "no replica of service web"},
	} {
		c := e.getLogs(tt.path)
		if body := c.all(); c.status() != http.StatusNotFound || !strings.Contains(body, tt.message) {
			t.Errorf("%s: status %d: %s", tt.name, c.status(), body)
		}
	}
	// A deleted service cannot be read by name any more.
	e.request(http.MethodDelete, "/v1/services/web", "")
	if c := e.getLogs("/v1/services/web/logs"); c.status() != http.StatusNotFound {
		t.Errorf("a deleted service: status %d", c.status())
	}
}

func testLogsNeedTheAdminTokenAndALogSource(t *testing.T, e *serviceEnv) {
	hub := e.withLogs(logstream.HubConfig{}, 0)
	e.logFleet(hub)
	for _, cred := range []struct{ name, auth string }{{"no credentials", ""}, {"a wrong token", "Bearer wrong-token-0123456789"}, {"no scheme", token}} {
		for _, path := range []string{"/v1/services/web/logs", "/v1/workloads/w-solo/logs"} {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			if cred.auth != "" {
				req.Header.Set("Authorization", cred.auth)
			}
			rec := httptest.NewRecorder()
			e.h.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized || strings.Contains(rec.Body.String(), "solo says hi") {
				t.Errorf("%s on %s: status %d", cred.name, path, rec.Code)
			}
		}
	}
	if n := hub.Stats().Streams; n != 0 {
		t.Fatalf("%d streams were opened for requests that carried no valid credentials", n)
	}
	// The endpoints only read.
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := e.request(method, "/v1/services/web/logs", "")
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s: status %d, want 405", method, rec.Code)
		}
	}

	// A manager without log streaming says so.
	plain := New(Config{Store: e.st, Token: token, NodeTTL: 30 * time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return e.clock }})
	req := httptest.NewRequest(http.MethodGet, "/v1/workloads/w-solo/logs", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	plain.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "not enabled") {
		t.Fatalf("without a log source: %d %s", rec.Code, rec.Body.String())
	}
}

func testTooManyLogStreamsAreRefused(t *testing.T, e *serviceEnv) {
	hub := e.withLogs(logstream.HubConfig{MaxStreams: 1}, 0)
	e.logFleet(hub)
	first := e.getLogs("/v1/services/web/logs?follow=true&replica=2&tail=1")
	first.record()

	second := e.getLogs("/v1/services/web/logs?follow=true&replica=1&revision=1&tail=1")
	if body := second.all(); second.status() != http.StatusTooManyRequests || second.w.header.Get("Retry-After") != "1" || !strings.Contains(body, "too many log streams") {
		t.Fatalf("second stream: %d %s", second.status(), body)
	}

	first.cancel()
	<-first.done
	deadline := time.Now().Add(5 * time.Second)
	for hub.Stats().Streams != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the first stream was not released")
		}
		time.Sleep(time.Millisecond)
	}
	third := e.getLogs("/v1/services/web/logs?replica=2&tail=1")
	if third.status() != http.StatusOK || third.record().Text != "b-err" {
		t.Fatalf("a stream after one was released: %d", third.status())
	}
}

// A client that does not read is dropped when its write deadline passes: the
// stream is closed, the agents are told to stop, and nothing keeps growing.
func testASlowLogClientIsDropped(t *testing.T, e *serviceEnv) {
	hub := e.withLogs(logstream.HubConfig{}, 50*time.Millisecond)
	n1, _ := e.logFleet(hub)
	c := e.getLogs("/v1/services/web/logs?follow=true&revision=1&replica=1&tail=0")
	// The client never reads. The container keeps writing.
	for i := 0; i < 200; i++ {
		n1.write("w-web-1", 100+i, strings.Repeat("x", 500))
	}
	select {
	case <-c.done:
	case <-time.After(5 * time.Second):
		t.Fatal("a client that does not read kept its stream open")
	}
	deadline := time.Now().Add(5 * time.Second)
	for n1.cancelCount() == 0 || hub.Stats().Streams != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the agent was told to stop %d times, %d streams open", n1.cancelCount(), hub.Stats().Streams)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestParseLogQuery(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	parse := func(rawQuery string, headers ...string) (logQuery, problems) {
		req := httptest.NewRequest(http.MethodGet, "/v1/services/web/logs?"+rawQuery, nil)
		for i := 0; i+1 < len(headers); i += 2 {
			req.Header.Set(headers[i], headers[i+1])
		}
		return parseLogQuery(req, serviceLogParams, now)
	}
	q, p := parse("")
	if len(p) != 0 || q.tail != 100 || q.follow || !q.timestamps || q.format != "ndjson" || !q.since.IsZero() || q.after != nil || q.replicas != nil {
		t.Fatalf("defaults = %+v, %v", q, p)
	}
	q, p = parse("since=10m&tail=all&follow=1&timestamps=0&format=text&replica=3&replica=1,2&revision=4&node=n1&workload=w1")
	if len(p) != 0 || !q.since.Equal(now.Add(-10*time.Minute)) || q.tail != logstream.MaxTail || !q.follow || q.timestamps || q.format != "text" ||
		len(q.replicas) != 3 || !q.replicas[1] || !q.replicas[2] || !q.replicas[3] || q.revision != 4 || q.node != "n1" || q.workload != "w1" {
		t.Fatalf("query = %+v, %v", q, p)
	}
	q, p = parse("since=2026-03-01T11:00:00Z&after=2026-03-01T11:30:00Z")
	if len(p) != 0 || !q.since.Equal(time.Date(2026, 3, 1, 11, 30, 0, 0, time.UTC)) {
		t.Fatalf("the later of since and a bare cursor applies: %+v, %v", q, p)
	}
	q, _ = parse("", "Accept", "text/event-stream")
	if q.format != "sse" {
		t.Fatalf("format = %q; a client that asks for events gets them", q.format)
	}
	q, _ = parse("format=text", "Accept", "text/event-stream")
	if q.format != "text" {
		t.Fatalf("format = %q; the parameter wins over the header", q.format)
	}
	// Every problem is reported, not only the first.
	_, p = parse("tail=x&follow=y&bogus=1")
	if len(p) != 3 {
		t.Fatalf("problems = %+v, want three", p)
	}
}

func TestPrintableReplacesControlCharacters(t *testing.T) {
	got := printable("a\x1b[2Jb\r\nc\td\u2028e\x7ff")
	want := "a\uFFFD[2Jb\uFFFD\uFFFDc\td\uFFFDe\uFFFDf"
	if got != want {
		t.Fatalf("printable = %q, want %q", got, want)
	}
	if printable("héllo wörld ✓") != "héllo wörld ✓" {
		t.Fatal("ordinary text was changed")
	}
}
