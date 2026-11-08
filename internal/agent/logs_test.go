package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dockfleet/dockfleet/internal/docker"
	"github.com/dockfleet/dockfleet/internal/dockerfake"
	"github.com/dockfleet/dockfleet/internal/logstream"
	"github.com/dockfleet/dockfleet/internal/protocol"
	dockfleetv1 "github.com/dockfleet/dockfleet/internal/protocol/dockfleet/v1"
)

var logBase = time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

func logTime(n int) time.Time { return logBase.Add(time.Duration(n) * time.Millisecond) }

func logLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func awaitLogs(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// fakeLogConn is the agent's end of the log stream, driven by the test. It
// checks what the agent sends against the credit the test has granted.
type fakeLogConn struct {
	down   chan logstream.Down
	closed chan struct{}
	once   sync.Once

	mu      sync.Mutex
	ups     []logstream.Up
	granted map[string]int
	sent    map[string]int
	broken  []string
}

func newFakeLogConn() *fakeLogConn {
	return &fakeLogConn{down: make(chan logstream.Down, 64), closed: make(chan struct{}), granted: map[string]int{}, sent: map[string]int{}}
}

func (c *fakeLogConn) Recv() (logstream.Down, error) {
	select {
	case d := <-c.down:
		return d, nil
	case <-c.closed:
		return logstream.Down{}, io.EOF
	}
}

func (c *fakeLogConn) Send(u logstream.Up) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.closed:
		return io.ErrClosedPipe
	default:
	}
	c.ups = append(c.ups, u)
	if b := u.Batch; b != nil {
		c.sent[b.RequestID] += b.Bytes()
		switch {
		case c.sent[b.RequestID] > c.granted[b.RequestID]:
			c.broken = append(c.broken, fmt.Sprintf("request %s sent %d bytes with %d granted", b.RequestID, c.sent[b.RequestID], c.granted[b.RequestID]))
		case len(b.Lines) > logstream.MaxBatchLines || b.Bytes() > logstream.MaxBatchBytes:
			c.broken = append(c.broken, fmt.Sprintf("a batch of %d lines and %d bytes", len(b.Lines), b.Bytes()))
		}
	}
	return nil
}

func (c *fakeLogConn) close() { c.once.Do(func() { close(c.closed) }) }

func (c *fakeLogConn) open(o logstream.Open) {
	c.mu.Lock()
	c.granted[o.RequestID] = o.Credit
	c.mu.Unlock()
	c.down <- logstream.Down{Open: &o}
}

func (c *fakeLogConn) credit(id string, n int) {
	c.mu.Lock()
	c.granted[id] += n
	c.mu.Unlock()
	c.down <- logstream.Down{Credit: &logstream.Credit{RequestID: id, Bytes: n}}
}

func (c *fakeLogConn) cancel(id string) {
	c.down <- logstream.Down{Cancel: &logstream.Cancel{RequestID: id}}
}

// texts are the lines received for a request, in order.
func (c *fakeLogConn) texts(id string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, u := range c.ups {
		if u.Batch != nil && u.Batch.RequestID == id {
			for _, l := range u.Batch.Lines {
				out = append(out, l.Text)
			}
		}
	}
	return out
}

func (c *fakeLogConn) lines(id string) []logstream.Line {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []logstream.Line
	for _, u := range c.ups {
		if u.Batch != nil && u.Batch.RequestID == id {
			out = append(out, u.Batch.Lines...)
		}
	}
	return out
}

func (c *fakeLogConn) historyDone(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, u := range c.ups {
		if u.Batch != nil && u.Batch.RequestID == id && u.Batch.HistoryComplete {
			return true
		}
	}
	return false
}

func (c *fakeLogConn) ends(id string) []logstream.End {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []logstream.End
	for _, u := range c.ups {
		if u.End != nil && u.End.RequestID == id {
			out = append(out, *u.End)
		}
	}
	return out
}

func (c *fakeLogConn) problems() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.broken...)
}

type logHarness struct {
	t    *testing.T
	d    *dockerfake.Fake
	conn *fakeLogConn
	done chan error
}

// newLogHarness runs a log server that serves up to requests requests at once.
func newLogHarness(t *testing.T, requests int) *logHarness {
	t.Helper()
	d := dockerfake.New("node-1")
	d.AddImage("img:1")
	srv := newLogServer(d, logLogger(), requests, time.Millisecond, 4*time.Millisecond, nil)
	h := &logHarness{t: t, d: d, conn: newFakeLogConn(), done: make(chan error, 1)}
	go func() { h.done <- srv.serve(context.Background(), h.conn) }()
	t.Cleanup(func() {
		h.conn.close()
		select {
		case <-h.done:
		case <-time.After(5 * time.Second):
			t.Error("the log server did not stop when the stream ended")
		}
		if p := h.conn.problems(); len(p) > 0 {
			t.Errorf("the agent broke the protocol: %v", p)
		}
	})
	return h
}

func (h *logHarness) container(id string) {
	h.t.Helper()
	ctx := context.Background()
	if _, err := h.d.Create(ctx, docker.ContainerSpec{WorkloadID: id, WorkloadName: id, Image: "img:1", CPUMillis: 100, MemoryBytes: 64 << 20}); err != nil {
		h.t.Fatal(err)
	}
	if err := h.d.Start(ctx, id); err != nil {
		h.t.Fatal(err)
	}
}

// write makes the container write lines "<prefix>1".."<prefix>n" at times start+1ms, start+2ms, ...
func (h *logHarness) write(id, prefix string, start, n int) {
	h.t.Helper()
	for i := 1; i <= n; i++ {
		if !h.d.AppendLog(id, "stdout", fmt.Sprintf("%s%d", prefix, i), logTime(start+i)) {
			h.t.Fatalf("no container %s", id)
		}
	}
}

func (h *logHarness) ended(id string) logstream.End {
	h.t.Helper()
	awaitLogs(h.t, "the request "+id+" to end", func() bool { return len(h.conn.ends(id)) > 0 })
	ends := h.conn.ends(id)
	if len(ends) != 1 {
		h.t.Fatalf("request %s ended %d times: %+v", id, len(ends), ends)
	}
	return ends[0]
}

func names(prefix string, from, to int) []string {
	var out []string
	for i := from; i <= to; i++ {
		out = append(out, fmt.Sprintf("%s%d", prefix, i))
	}
	return out
}

func equal(a, b []string) bool { return strings.Join(a, "\n") == strings.Join(b, "\n") }

func TestLogRequestSendsTheTailInOrder(t *testing.T) {
	h := newLogHarness(t, 4)
	h.container("w1")
	h.write("w1", "l", 0, 5)
	h.d.AppendLog("w1", "stderr", "problem", logTime(6))

	h.conn.open(logstream.Open{RequestID: "r1", WorkloadID: "w1", Tail: 4, Credit: 1 << 18})
	if end := h.ended("r1"); end.Reason != logstream.EndComplete {
		t.Fatalf("end = %+v", end)
	}
	if got, want := h.conn.texts("r1"), []string{"l3", "l4", "l5", "problem"}; !equal(got, want) {
		t.Fatalf("lines = %v, want %v", got, want)
	}
	lines := h.conn.lines("r1")
	if !lines[0].Time.Equal(logTime(3)) || lines[0].Stream != logstream.StreamStdout || lines[3].Stream != logstream.StreamStderr || !lines[3].Time.Equal(logTime(6)) {
		t.Fatalf("lines = %+v; times and streams must survive", lines)
	}
	if !h.conn.historyDone("r1") {
		t.Fatal("the history was never marked complete")
	}
	awaitLogs(t, "the reader to be released", func() bool { return h.d.OpenLogReaders() == 0 })
}

func TestLogRequestStartsAfterTheCursor(t *testing.T) {
	h := newLogHarness(t, 4)
	h.container("w1")
	h.write("w1", "l", 0, 5)
	h.conn.open(logstream.Open{RequestID: "r1", WorkloadID: "w1", Tail: -1, After: logTime(2), Credit: 1 << 18})
	h.ended("r1")
	if got, want := h.conn.texts("r1"), names("l", 3, 5); !equal(got, want) {
		t.Fatalf("lines = %v, want %v: a cursor excludes the line it names", got, want)
	}
}

func TestLogRequestFollowsWithoutGapsOrRepeats(t *testing.T) {
	h := newLogHarness(t, 4)
	h.container("w1")
	h.write("w1", "old", 0, 3)
	h.conn.open(logstream.Open{RequestID: "r1", WorkloadID: "w1", Tail: 2, Follow: true, Credit: 1 << 18})
	awaitLogs(t, "the history", func() bool { return h.conn.historyDone("r1") })

	h.write("w1", "new", 10, 3)
	awaitLogs(t, "the live lines", func() bool { return len(h.conn.texts("r1")) == 5 })
	if got, want := h.conn.texts("r1"), []string{"old2", "old3", "new1", "new2", "new3"}; !equal(got, want) {
		t.Fatalf("lines = %v, want %v", got, want)
	}

	h.d.Crash("w1", 1)
	end := h.ended("r1")
	if end.Reason != logstream.EndComplete || !strings.Contains(end.Message, "stopped") {
		t.Fatalf("end = %+v; a follow ends with the container", end)
	}
	if got := h.conn.texts("r1"); len(got) != 5 {
		t.Fatalf("lines = %v; nothing may be repeated at the end", got)
	}
}

func TestLogRequestWithoutHistorySendsOnlyNewLines(t *testing.T) {
	h := newLogHarness(t, 4)
	h.container("w1")
	h.write("w1", "old", 0, 3)
	h.conn.open(logstream.Open{RequestID: "r1", WorkloadID: "w1", Tail: 0, Follow: true, Credit: 1 << 18})
	awaitLogs(t, "the empty history to be marked complete", func() bool { return h.conn.historyDone("r1") })
	h.d.Log("w1", "fresh")
	awaitLogs(t, "the new line", func() bool { return len(h.conn.texts("r1")) == 1 })
	if got := h.conn.texts("r1"); !equal(got, []string{"fresh"}) {
		t.Fatalf("lines = %v; the lines from before the request must stay unsent", got)
	}
}

// The agent sends only what the manager has granted credit for, however much
// the container wrote, and sends the rest as credit arrives.
func TestLogRequestSendsOnlyWhatTheCreditAllows(t *testing.T) {
	h := newLogHarness(t, 4)
	h.container("w1")
	const total = 100
	text := strings.Repeat("x", 1000)
	for i := 1; i <= total; i++ {
		h.d.AppendLog("w1", "stdout", fmt.Sprintf("%03d%s", i, text), logTime(i))
	}
	cost := 3 + len(text) + logstream.LineOverhead
	window := logstream.MinWindow
	h.conn.open(logstream.Open{RequestID: "r1", WorkloadID: "w1", Tail: -1, Credit: window})

	received := window / cost
	awaitLogs(t, "the first window", func() bool { return len(h.conn.texts("r1")) == received })
	for received < total {
		step := min(5, total-received)
		h.conn.credit("r1", step*cost)
		received += step
		awaitLogs(t, fmt.Sprintf("%d lines", received), func() bool { return len(h.conn.texts("r1")) == received })
	}
	if end := h.ended("r1"); end.Reason != logstream.EndComplete {
		t.Fatalf("end = %+v", end)
	}
	for i, text := range h.conn.texts("r1") {
		if !strings.HasPrefix(text, fmt.Sprintf("%03d", i+1)) {
			t.Fatalf("line %d is %q; order must be kept", i+1, text[:5])
		}
	}
	// h.conn checks on every send that no request sent more than it was granted.
}

func TestLogRequestCancelStopsReading(t *testing.T) {
	h := newLogHarness(t, 4)
	h.container("w1")
	h.conn.open(logstream.Open{RequestID: "r1", WorkloadID: "w1", Tail: 5, Follow: true, Credit: 1 << 18})
	awaitLogs(t, "the history", func() bool { return h.conn.historyDone("r1") })
	awaitLogs(t, "the follow to open its reader", func() bool { return h.d.OpenLogReaders() == 1 })

	h.conn.cancel("r1")
	awaitLogs(t, "the reader to be released", func() bool { return h.d.OpenLogReaders() == 0 })
	// The manager canceled it, so it does not need to be told how it ended.
	h.d.Log("w1", "after the cancel")
	h.conn.open(logstream.Open{RequestID: "r2", WorkloadID: "w1", Tail: 1, Credit: 1 << 18})
	h.ended("r2")
	if got := h.conn.texts("r1"); len(got) != 0 {
		t.Fatalf("lines for a canceled request: %v", got)
	}
	if ends := h.conn.ends("r1"); len(ends) != 0 {
		t.Fatalf("a canceled request reported %+v", ends)
	}
}

func TestLogRequestsForContainersThatAreNotOursAreRefused(t *testing.T) {
	h := newLogHarness(t, 4)
	h.d.AddUnmanaged("bystander", "img:1", map[string]string{"app": "shop"})
	for _, id := range []string{"nobody", "bystander", "../etc/passwd"} {
		h.conn.open(logstream.Open{RequestID: "r-" + id, WorkloadID: id, Tail: 5, Credit: 1 << 18})
		if end := h.ended("r-" + id); end.Reason != logstream.EndNotFound {
			t.Errorf("%s: end = %+v, want not_found", id, end)
		}
		if got := h.conn.texts("r-" + id); len(got) != 0 {
			t.Errorf("%s: lines %v", id, got)
		}
	}
}

func TestLogRequestsAreLimited(t *testing.T) {
	h := newLogHarness(t, 1)
	h.container("w1")
	h.conn.open(logstream.Open{RequestID: "a", WorkloadID: "w1", Tail: 1, Follow: true, Credit: 1 << 18})
	awaitLogs(t, "the first request to be served", func() bool { return h.conn.historyDone("a") })

	h.conn.open(logstream.Open{RequestID: "b", WorkloadID: "w1", Tail: 1, Credit: 1 << 18})
	if end := h.ended("b"); end.Reason != logstream.EndBusy {
		t.Fatalf("second request: %+v, want busy", end)
	}
	// Repeating the first one's ID neither starts a second copy nor is it refused as busy.
	h.conn.open(logstream.Open{RequestID: "a", WorkloadID: "w1", Tail: 1, Follow: true, Credit: 1 << 18})

	// Once the first is canceled its slot is free; the server notices a moment after its reader is released.
	h.conn.cancel("a")
	var last logstream.End
	attempt := 0
	awaitLogs(t, "a free slot", func() bool {
		attempt++
		id := fmt.Sprintf("c%d", attempt)
		h.conn.open(logstream.Open{RequestID: id, WorkloadID: "w1", Tail: 1, Credit: 1 << 18})
		last = h.ended(id)
		return last.Reason != logstream.EndBusy
	})
	if last.Reason != logstream.EndComplete {
		t.Fatalf("a request after a slot was freed: %+v", last)
	}
	if ends := h.conn.ends("a"); len(ends) != 0 {
		t.Fatalf("the first request reported %+v", ends)
	}
}

func TestLogRequestEndsWhenTheContainerIsRemoved(t *testing.T) {
	h := newLogHarness(t, 4)
	h.container("w1")
	h.conn.open(logstream.Open{RequestID: "r1", WorkloadID: "w1", Tail: 1, Follow: true, Credit: 1 << 18})
	awaitLogs(t, "the follow", func() bool { return h.conn.historyDone("r1") && h.d.OpenLogReaders() == 1 })
	ctx := context.Background()
	if err := h.d.Stop(ctx, "w1", 0); err != nil {
		t.Fatal(err)
	}
	if err := h.d.Remove(ctx, "w1", true); err != nil {
		t.Fatal(err)
	}
	if end := h.ended("r1"); end.Reason != logstream.EndComplete {
		t.Fatalf("end = %+v", end)
	}
}

func TestEndOfTheStreamStopsEveryRequest(t *testing.T) {
	h := newLogHarness(t, 4)
	h.container("w1")
	h.container("w2")
	for _, id := range []string{"w1", "w2"} {
		h.conn.open(logstream.Open{RequestID: "r-" + id, WorkloadID: id, Tail: 1, Follow: true, Credit: 1 << 18})
	}
	awaitLogs(t, "both follows", func() bool { return h.d.OpenLogReaders() == 2 })
	h.conn.close()
	select {
	case err := <-h.done:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("serve = %v, want io.EOF", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the log server did not stop")
	}
	if n := h.d.OpenLogReaders(); n != 0 {
		t.Fatalf("%d readers left open after the stream ended", n)
	}
	h.done <- nil // for the cleanup, which waits for it too
}

func TestLongLinesAreCutAndMarked(t *testing.T) {
	h := newLogHarness(t, 4)
	h.container("w1")
	h.d.Log("w1", strings.Repeat("é", 20000))
	h.conn.open(logstream.Open{RequestID: "r1", WorkloadID: "w1", Tail: 1, MaxLineBytes: 1000, Credit: 1 << 18})
	h.ended("r1")
	lines := h.conn.lines("r1")
	if len(lines) != 1 || !lines[0].Truncated || len(lines[0].Text) > 1000 || !strings.HasPrefix(lines[0].Text, "éé") {
		t.Fatalf("lines = %d, first %d bytes, truncated %v", len(lines), len(lines[0].Text), lines[0].Truncated)
	}
}

func TestLogRequestWithATinyWindowIsRefused(t *testing.T) {
	h := newLogHarness(t, 4)
	h.container("w1")
	h.d.Log("w1", "hello")
	h.conn.open(logstream.Open{RequestID: "r1", WorkloadID: "w1", Tail: 1, Credit: 100})
	end := h.ended("r1")
	if end.Reason != logstream.EndError || !strings.Contains(end.Message, "window") {
		t.Fatalf("end = %+v", end)
	}
	if got := h.conn.texts("r1"); len(got) != 0 {
		t.Fatalf("lines = %v", got)
	}
}

func TestDockerFailuresAreReported(t *testing.T) {
	h := newLogHarness(t, 4)
	h.container("w1")
	h.d.FailNext("logs", errors.New("the daemon\nsays no"), 1)
	h.conn.open(logstream.Open{RequestID: "r1", WorkloadID: "w1", Tail: 5, Credit: 1 << 18})
	end := h.ended("r1")
	if end.Reason != logstream.EndError || !strings.Contains(end.Message, "the daemon says no") || strings.ContainsAny(end.Message, "\n\r") {
		t.Fatalf("end = %+v", end)
	}
}

func TestLogLinesAreBatchedWithinTheLimits(t *testing.T) {
	h := newLogHarness(t, 4)
	h.container("w1")
	const total = 1000
	for i := 1; i <= total; i++ {
		h.d.AppendLog("w1", "stdout", fmt.Sprintf("line %d", i), logTime(i))
	}
	h.conn.open(logstream.Open{RequestID: "r1", WorkloadID: "w1", Tail: -1, Credit: 1 << 20})
	h.ended("r1")
	if got := len(h.conn.texts("r1")); got != total {
		t.Fatalf("%d lines, want %d", got, total)
	}
	batches := 0
	h.conn.mu.Lock()
	for _, u := range h.conn.ups {
		if u.Batch != nil {
			batches++
		}
	}
	h.conn.mu.Unlock()
	if batches < total/logstream.MaxBatchLines {
		t.Fatalf("%d batches for %d lines; batches are bounded to %d lines", batches, total, logstream.MaxBatchLines)
	}
}

// opener is a manager connection that answers each attempt from a script.
type opener struct {
	mu       sync.Mutex
	attempts int
	script   []func() (logstream.AgentConn, error)
}

func (o *opener) Logs(context.Context, string, string) (logstream.AgentConn, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attempts++
	if len(o.script) == 0 {
		return nil, status.Error(codes.Unavailable, "script exhausted")
	}
	f := o.script[0]
	o.script = o.script[1:]
	return f()
}

func (o *opener) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.attempts
}

func fails(err error) func() (logstream.AgentConn, error) {
	return func() (logstream.AgentConn, error) { return nil, err }
}

func TestLogStreamIsOpenedAgainAfterItDropsAndNotAfterAFinalAnswer(t *testing.T) {
	d := dockerfake.New("node-1")
	srv := newLogServer(d, logLogger(), 4, time.Millisecond, 2*time.Millisecond, nil)

	t.Run("a transient failure is retried and a dropped stream is reopened", func(t *testing.T) {
		first, second := newFakeLogConn(), newFakeLogConn()
		o := &opener{script: []func() (logstream.AgentConn, error){
			fails(status.Error(codes.Unavailable, "manager down")),
			func() (logstream.AgentConn, error) { return first, nil },
			func() (logstream.AgentConn, error) { return second, nil },
		}}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { srv.run(ctx, o, "node-1", "session"); close(done) }()
		awaitLogs(t, "the first stream to be used", func() bool { return o.count() >= 2 })
		first.close() // the connection drops
		awaitLogs(t, "the second stream to be opened", func() bool { return o.count() >= 3 })
		cancel()
		second.close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("run did not stop with its context")
		}
	})

	final := []struct {
		name string
		err  error
	}{
		{"a manager without logs", status.Error(codes.Unimplemented, "off")},
		{"a session that ended", &protocol.Error{Code: dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_REGISTERED, Message: "gone"}},
		{"a node that is not enrolled", &protocol.Error{Code: dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_ENROLLED, Message: "no"}},
		{"another node's certificate", &protocol.Error{Code: dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_IDENTITY_MISMATCH, Message: "no"}},
		{"bad credentials", status.Error(codes.Unauthenticated, "no")},
	}
	for _, tt := range final {
		t.Run(tt.name, func(t *testing.T) {
			o := &opener{script: []func() (logstream.AgentConn, error){fails(tt.err), fails(tt.err), fails(tt.err)}}
			done := make(chan struct{})
			go func() { srv.run(context.Background(), o, "node-1", "session"); close(done) }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("run kept retrying a failure that retrying cannot fix")
			}
			if o.count() != 1 {
				t.Fatalf("%d attempts, want 1", o.count())
			}
		})
	}
}

func TestAgentAsksForALogStreamOnceWhenTheManagerServesNone(t *testing.T) {
	m := &fakeManager{interval: 2 * time.Millisecond}
	d := dockerfake.New("node-1")
	start(t, newTestAgent(m, d))
	awaitLogs(t, "the agent to ask for logs", func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.logAttempts >= 1
	})
	awaitLogs(t, "several heartbeats", func() bool { return len(m.calls()) >= 8 })
	m.mu.Lock()
	attempts := m.logAttempts
	m.mu.Unlock()
	if attempts != 1 {
		t.Fatalf("%d attempts to open a log stream; a manager that answers Unimplemented is not asked again", attempts)
	}
}

func TestAgentWithLogsSwitchedOffNeverOpensAStream(t *testing.T) {
	m := &fakeManager{interval: 2 * time.Millisecond}
	a := New(Config{
		Node: protocol.NodeInfo{ID: "node-1"}, Manager: m, Docker: dockerfake.New("node-1"), Logger: logLogger(),
		RPCTimeout: time.Second, RetryMin: time.Millisecond, RetryMax: 4 * time.Millisecond, Resync: 5 * time.Millisecond,
		LogRequests: -1,
	})
	start(t, a)
	awaitLogs(t, "several heartbeats", func() bool { return len(m.calls()) >= 5 })
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.logAttempts != 0 {
		t.Fatalf("%d attempts to open a log stream with log serving switched off", m.logAttempts)
	}
}
