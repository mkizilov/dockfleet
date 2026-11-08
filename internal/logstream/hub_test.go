package logstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeConn is the manager's end of an agent's stream, driven by the test.
type fakeConn struct {
	down chan Down // what the hub sent
	up   chan Up   // what the test feeds to the hub
	done chan struct{}
	once sync.Once
}

func newFakeConn() *fakeConn {
	return &fakeConn{down: make(chan Down, 4096), up: make(chan Up, 4096), done: make(chan struct{})}
}

func (c *fakeConn) Send(d Down) error {
	select {
	case c.down <- d:
		return nil
	case <-c.done:
		return io.ErrClosedPipe
	}
}

// Recv hands over everything the test queued before it reports the end.
func (c *fakeConn) Recv() (Up, error) {
	select {
	case u := <-c.up:
		return u, nil
	default:
	}
	select {
	case u := <-c.up:
		return u, nil
	case <-c.done:
		return Up{}, io.EOF
	}
}

// stuckConn accepts nothing: Send blocks until the test hangs up.
type stuckConn struct{ *fakeConn }

func (c stuckConn) Send(Down) error {
	<-c.done
	return io.ErrClosedPipe
}

func (c *fakeConn) hangUp() { c.once.Do(func() { close(c.done) }) }

// agentSide is the test playing an agent.
type agentSide struct {
	t    *testing.T
	conn *fakeConn
	done chan error // what Attach returned
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func attach(t *testing.T, h *Hub, node string) *agentSide {
	t.Helper()
	prev, _ := h.node(node)
	c := newFakeConn()
	a := &agentSide{t: t, conn: c, done: make(chan error, 1)}
	go func() { a.done <- h.Attach(context.Background(), node, c) }()
	t.Cleanup(c.hangUp)
	waitFor(t, node+" to attach", func() bool {
		cur, _ := h.node(node)
		return cur != nil && cur != prev
	})
	return a
}

func (a *agentSide) next() Down {
	a.t.Helper()
	select {
	case d := <-a.conn.down:
		return d
	case <-time.After(5 * time.Second):
		a.t.Fatal("timed out waiting for the hub to send something")
		return Down{}
	}
}

func (a *agentSide) open() Open {
	a.t.Helper()
	d := a.next()
	if d.Open == nil {
		a.t.Fatalf("hub sent %+v, want an Open", d)
	}
	return *d.Open
}

func (a *agentSide) send(u Up) { a.conn.up <- u }

func (a *agentSide) batch(req string, complete bool, lines ...Line) {
	a.send(Up{Batch: &Batch{RequestID: req, Lines: lines, HistoryComplete: complete}})
}

func (a *agentSide) end(req string, reason EndReason, msg string) {
	a.send(Up{End: &End{RequestID: req, Reason: reason, Message: msg}})
}

var epoch = time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

// ts is n milliseconds after the epoch.
func ts(n int) time.Time { return epoch.Add(time.Duration(n) * time.Millisecond) }

func line(n int, text string) Line { return Line{Time: ts(n), Stream: StreamStdout, Text: text} }

func open(t *testing.T, h *Hub, q Query) *Stream {
	t.Helper()
	st, err := h.Open(context.Background(), q)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(st.Close)
	return st
}

func targets(pairs ...string) []Target {
	var out []Target
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, Target{NodeID: pairs[i], WorkloadID: pairs[i+1]})
	}
	return out
}

// queued is how many lines of a container wait for the reader.
func queued(st *Stream, workload string) int {
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, s := range st.sources {
		if s.target.WorkloadID == workload {
			n := 0
			for _, it := range s.queue[s.head:] {
				if it.notice == nil {
					n++
				}
			}
			return n
		}
	}
	return -1
}

func nextEvent(t *testing.T, st *Stream) (Event, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return st.Next(ctx)
}

// describe renders an event: the text of a line, or "notice:<kind>".
func describe(ev Event) string {
	if ev.Entry != nil {
		return ev.Entry.Text
	}
	return "notice:" + string(ev.Notice.Kind)
}

func readAll(t *testing.T, st *Stream) []string {
	t.Helper()
	var out []string
	for {
		ev, err := nextEvent(t, st)
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("Next after %v: %v", out, err)
		}
		out = append(out, describe(ev))
	}
}

func readN(t *testing.T, st *Stream, n int) []string {
	t.Helper()
	var out []string
	for len(out) < n {
		ev, err := nextEvent(t, st)
		if err != nil {
			t.Fatalf("Next after %v: %v", out, err)
		}
		out = append(out, describe(ev))
	}
	return out
}

func TestHistoryIsMergedByTime(t *testing.T) {
	h := NewHub(HubConfig{})
	a, b := attach(t, h, "n1"), attach(t, h, "n2")
	st := open(t, h, Query{Targets: targets("n1", "w1", "n2", "w2"), Tail: 10})
	oa, ob := a.open(), b.open()
	if oa.WorkloadID != "w1" || ob.WorkloadID != "w2" || oa.Tail != 10 || oa.Follow || oa.RequestID == ob.RequestID || oa.Credit <= 0 || oa.MaxLineBytes != DefaultLineBytes {
		t.Fatalf("requests = %+v, %+v", oa, ob)
	}

	// A answers first. Its lines cannot be returned before B's first line is known, or they might come out of order.
	a.batch(oa.RequestID, true, line(1, "a1"), line(3, "a3"), line(5, "a5"))
	a.end(oa.RequestID, EndComplete, "")
	waitFor(t, "A's lines to arrive", func() bool { return queued(st, "w1") == 3 })
	if ev, ok := st.TryNext(); ok {
		t.Fatalf("returned %s before the other container's history was known", describe(ev))
	}

	b.batch(ob.RequestID, true, line(2, "b2"), line(4, "b4"))
	b.end(ob.RequestID, EndComplete, "")
	if got, want := readAll(t, st), []string{"a1", "b2", "a3", "b4", "a5"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("lines = %v, want %v", got, want)
	}
	if got := st.Cursor(); !got["w1"].Equal(ts(5)) || !got["w2"].Equal(ts(4)) {
		t.Fatalf("cursor = %v", got)
	}
}

// One container whose agent says nothing must not hold the history of the others
// back for ever: after the wait they go on, and its lines still arrive when they do.
func TestASilentAgentDoesNotHoldBackTheMergedHistory(t *testing.T) {
	h := NewHub(HubConfig{HistoryWait: 50 * time.Millisecond})
	a, b := attach(t, h, "n1"), attach(t, h, "n2")
	st := open(t, h, Query{Targets: targets("n1", "w1", "n2", "w2"), Tail: 10, Follow: true})
	oa, ob := a.open(), b.open()
	a.batch(oa.RequestID, true, line(1, "a1"), line(3, "a3"))
	// B has not answered. Within the wait nothing is returned...
	waitFor(t, "A's lines to arrive", func() bool { return queued(st, "w1") == 2 })
	if ev, ok := st.TryNext(); ok {
		t.Fatalf("returned %s before the wait was over", describe(ev))
	}
	// ...after it, A's lines come out.
	if got, want := readN(t, st, 2), []string{"a1", "a3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("lines = %v, want %v", got, want)
	}
	// B answers late; its lines are delivered, in the order they arrive.
	b.batch(ob.RequestID, true, line(2, "b2"))
	if got, want := readN(t, st, 1), []string{"b2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("lines = %v, want %v", got, want)
	}
}

func TestTheHistoryWaitCanBeSwitchedOff(t *testing.T) {
	h := NewHub(HubConfig{HistoryWait: -1})
	a, b := attach(t, h, "n1"), attach(t, h, "n2")
	st := open(t, h, Query{Targets: targets("n1", "w1", "n2", "w2"), Tail: 10})
	oa, ob := a.open(), b.open()
	a.batch(oa.RequestID, true, line(5, "a5"))
	a.end(oa.RequestID, EndComplete, "")
	waitFor(t, "A's line to arrive", func() bool { return queued(st, "w1") == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if ev, err := st.Next(ctx); err == nil {
		t.Fatalf("returned %s although the other container's history is unknown", describe(ev))
	}
	b.batch(ob.RequestID, true, line(2, "b2"))
	b.end(ob.RequestID, EndComplete, "")
	if got, want := readAll(t, st), []string{"b2", "a5"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("lines = %v, want %v", got, want)
	}
}

func TestOnlyHistoryIsMergedThenLiveLinesTakeTurns(t *testing.T) {
	h := NewHub(HubConfig{})
	a, b := attach(t, h, "n1"), attach(t, h, "n2")
	st := open(t, h, Query{Targets: targets("n1", "w1", "n2", "w2"), Tail: 10, Follow: true})
	oa, ob := a.open(), b.open()
	a.batch(oa.RequestID, true, line(1, "a-old"))
	b.batch(ob.RequestID, true, line(2, "b-old"))
	if got, want := readN(t, st, 2), []string{"a-old", "b-old"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("history = %v, want %v", got, want)
	}

	// Live lines: three from each, queued together. They alternate, each container in its own order.
	a.batch(oa.RequestID, false, line(10, "a1"), line(11, "a2"), line(12, "a3"))
	b.batch(ob.RequestID, false, line(10, "b1"), line(11, "b2"), line(12, "b3"))
	waitFor(t, "live lines to arrive", func() bool { return queued(st, "w1") == 3 && queued(st, "w2") == 3 })
	got := readN(t, st, 6)
	var fromA, fromB []string
	for _, s := range got {
		if strings.HasPrefix(s, "a") {
			fromA = append(fromA, s)
		} else {
			fromB = append(fromB, s)
		}
	}
	if !reflect.DeepEqual(fromA, []string{"a1", "a2", "a3"}) || !reflect.DeepEqual(fromB, []string{"b1", "b2", "b3"}) {
		t.Fatalf("lines = %v; each container's lines must keep their order", got)
	}
	if got[0][0] == got[1][0] {
		t.Fatalf("lines = %v; containers must take turns", got)
	}
}

func TestCreditIsReturnedAsTheReaderTakesLines(t *testing.T) {
	h := NewHub(HubConfig{Window: 8 * MinWindow})
	a := attach(t, h, "n1")
	st := open(t, h, Query{Targets: targets("n1", "w1"), Tail: 100})
	o := a.open()
	if o.Credit != 8*MinWindow {
		t.Fatalf("initial credit = %d, want %d", o.Credit, 8*MinWindow)
	}

	text := strings.Repeat("x", 2000)
	cost := len(text) + LineOverhead
	lines := make([]Line, 100)
	for i := range lines {
		lines[i] = line(i+1, text)
	}
	a.batch(o.RequestID, true, lines...)
	waitFor(t, "the lines to arrive", func() bool { return queued(st, "w1") == 100 })

	// Nothing has been read, so nothing is granted.
	if len(a.conn.down) != 0 {
		t.Fatalf("credit was granted before the reader took a line")
	}
	// A quarter of a window is returned at a time.
	perGrant := (o.Credit/4 + cost - 1) / cost // lines that make a quarter window
	readN(t, st, perGrant-1)
	if len(a.conn.down) != 0 {
		t.Fatalf("credit was granted after %d lines, before a quarter of the window was taken", perGrant-1)
	}
	readN(t, st, 1)
	d := a.next()
	if d.Credit == nil || d.Credit.RequestID != o.RequestID || d.Credit.Bytes != perGrant*cost {
		t.Fatalf("hub sent %+v, want a credit of %d bytes", d, perGrant*cost)
	}
	// Every byte comes back in the end, the last of it as soon as the queue is empty.
	readN(t, st, 100-perGrant)
	got, want := 0, (100-perGrant)*cost
	for got < want {
		d = a.next()
		if d.Credit == nil || d.Credit.RequestID != o.RequestID || d.Credit.Bytes <= 0 {
			t.Fatalf("hub sent %+v, want credit", d)
		}
		got += d.Credit.Bytes
	}
	if got != want {
		t.Fatalf("credit returned = %d bytes, want %d", got, want)
	}
}

// A reader that does not read stops the agent after one window: the manager's
// memory per request is bounded by the window, whatever the agent has to send.
func TestASlowReaderStopsTheAgentAtOneWindow(t *testing.T) {
	h := NewHub(HubConfig{Window: 4 * MinWindow})
	a := attach(t, h, "n1")
	st := open(t, h, Query{Targets: targets("n1", "w1"), Tail: 10, Follow: true})
	o := a.open()

	const total = 600
	text := strings.Repeat("x", 1000)
	cost := 4 + len(text) + LineOverhead // a line is four digits and the text
	var sent atomic.Int64
	blocked := make(chan struct{}, 1)
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go func() {
		credit := o.Credit
		for i := 1; i <= total; i++ {
			for credit < cost {
				select {
				case blocked <- struct{}{}:
				default:
				}
				select {
				case d := <-a.conn.down:
					if d.Credit != nil {
						credit += d.Credit.Bytes
					}
				case <-stop:
					return
				}
			}
			credit -= cost
			a.conn.up <- Up{Batch: &Batch{RequestID: o.RequestID, Lines: []Line{line(i, fmt.Sprintf("%04d%s", i, text))}, HistoryComplete: i == 1}}
			sent.Add(1)
		}
		a.conn.up <- Up{End: &End{RequestID: o.RequestID, Reason: EndComplete}}
	}()

	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("the agent never ran out of credit")
	}
	want := int64(o.Credit / cost)
	if got := sent.Load(); got != want {
		t.Fatalf("the agent sent %d lines before it had to wait, want %d (a window of %d bytes)", got, want, o.Credit)
	}
	waitFor(t, "the lines to arrive", func() bool { return int64(queued(st, "w1")) == want })

	// Reading releases the agent, and every line arrives once, in order.
	for i := 1; i <= total; i++ {
		ev, err := nextEvent(t, st)
		if err != nil || ev.Entry == nil || !strings.HasPrefix(ev.Entry.Text, fmt.Sprintf("%04d", i)) {
			t.Fatalf("event %d = %+v, %v", i, ev, err)
		}
	}
	if ev, err := nextEvent(t, st); err != nil || ev.Notice == nil || ev.Notice.Kind != NoticeEnded {
		t.Fatalf("after the last line: %+v, %v; want the container to have ended", ev, err)
	}
	if _, err := nextEvent(t, st); !errors.Is(err, io.EOF) {
		t.Fatalf("error = %v, want io.EOF", err)
	}
}

func TestAnAgentThatIgnoresItsWindowIsEnded(t *testing.T) {
	h := NewHub(HubConfig{Window: MinWindow})
	a := attach(t, h, "n1")
	st := open(t, h, Query{Targets: targets("n1", "w1"), Tail: 10})
	o := a.open()

	text := strings.Repeat("x", 4000)
	var lines []Line
	for i := 1; i <= 20; i++ { // 20 * 4064 bytes is more than the window
		lines = append(lines, line(i, text))
	}
	a.batch(o.RequestID, true, lines...)

	d := a.next()
	if d.Cancel == nil || d.Cancel.RequestID != o.RequestID {
		t.Fatalf("hub sent %+v, want the request canceled", d)
	}
	if got := readAll(t, st); !reflect.DeepEqual(got, []string{"notice:error"}) {
		t.Fatalf("events = %v; the lines that broke the window must not be kept", got)
	}
}

func TestMessagesForOtherNodesRequestsAreIgnored(t *testing.T) {
	h := NewHub(HubConfig{})
	a, b := attach(t, h, "n1"), attach(t, h, "n2")
	st := open(t, h, Query{Targets: targets("n1", "w1"), Tail: 10})
	o := a.open()

	// Node 2 knows the request ID, but the request was opened on node 1's stream.
	b.batch(o.RequestID, true, line(1, "forged"))
	b.end(o.RequestID, EndComplete, "")
	// Node 1 answers, and its answer is the only one that counts.
	a.batch(o.RequestID, true, line(2, "genuine"))
	a.end(o.RequestID, EndComplete, "")

	if got, want := readAll(t, st), []string{"genuine"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("lines = %v, want %v", got, want)
	}
}

func TestAnUnconnectedNodeIsReportedAndTheOthersStillAnswer(t *testing.T) {
	h := NewHub(HubConfig{})
	a := attach(t, h, "n1")
	st := open(t, h, Query{Targets: targets("n1", "w1", "gone", "w2"), Tail: 10})
	o := a.open()
	a.batch(o.RequestID, true, line(1, "alive"))
	a.end(o.RequestID, EndComplete, "")

	ev, err := nextEvent(t, st)
	if err != nil || ev.Notice == nil || ev.Notice.Kind != NoticeUnavailable || ev.Notice.NodeID != "gone" || ev.Notice.WorkloadID != "w2" ||
		!strings.Contains(ev.Notice.Message, "not connected") {
		t.Fatalf("first event = %+v, %v", ev, err)
	}
	if got, want := readAll(t, st), []string{"alive"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("lines = %v, want %v", got, want)
	}
}

func TestEndsAreExplained(t *testing.T) {
	tests := []struct {
		name   string
		follow bool
		reason EndReason
		msg    string
		want   NoticeKind // empty: no notice
		text   string
	}{
		{"history of a request that does not follow", false, EndComplete, "", "", ""},
		{"the container stopped", true, EndComplete, "container stopped", NoticeEnded, "container stopped"},
		{"a default reason", true, EndComplete, "", NoticeEnded, "the container stopped"},
		{"no container", false, EndNotFound, "", NoticeUnavailable, "no container"},
		{"a busy node", false, EndBusy, "", NoticeUnavailable, "as many log requests as it allows"},
		{"a failure", false, EndError, "docker said no\nand more", NoticeError, "docker said no and more"},
		{"canceled by the manager", false, EndCanceled, "", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHub(HubConfig{})
			a := attach(t, h, "n1")
			st := open(t, h, Query{Targets: targets("n1", "w1"), Tail: 1, Follow: tt.follow})
			o := a.open()
			a.end(o.RequestID, tt.reason, tt.msg)
			if tt.want == "" {
				if got := readAll(t, st); len(got) != 0 {
					t.Fatalf("events = %v, want none", got)
				}
				return
			}
			ev, err := nextEvent(t, st)
			if err != nil || ev.Notice == nil || ev.Notice.Kind != tt.want || !strings.Contains(ev.Notice.Message, tt.text) {
				t.Fatalf("event = %+v, %v; want a %s notice containing %q", ev, err, tt.want, tt.text)
			}
			if _, err := nextEvent(t, st); !errors.Is(err, io.EOF) {
				t.Fatalf("error = %v, want io.EOF", err)
			}
		})
	}
}

func TestFollowResumesWhereItStoppedWhenTheAgentReconnects(t *testing.T) {
	h := NewHub(HubConfig{ReconnectWait: 5 * time.Second})
	a1 := attach(t, h, "n1")
	st := open(t, h, Query{Targets: targets("n1", "w1"), Tail: 5, Follow: true})
	o1 := a1.open()
	a1.batch(o1.RequestID, true, line(1, "one"), line(2, "two"))
	if got, want := readN(t, st, 2), []string{"one", "two"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("lines = %v, want %v", got, want)
	}

	a1.conn.hangUp()
	if ev, err := nextEvent(t, st); err != nil || ev.Notice == nil || ev.Notice.Kind != NoticeInterrupted {
		t.Fatalf("after the connection dropped: %+v, %v", ev, err)
	}

	a2 := attach(t, h, "n1")
	o2 := a2.open()
	if !o2.Follow || !o2.After.Equal(ts(2)) || o2.Tail >= 0 || o2.RequestID == o1.RequestID || o2.WorkloadID != "w1" {
		t.Fatalf("resumed request = %+v; it must follow, start after the last line and ask for all that was missed", o2)
	}
	a2.batch(o2.RequestID, true, line(3, "three"))
	if got, want := readN(t, st, 2), []string{"notice:resumed", "three"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	// Late messages of the dead request change nothing.
	a2.batch(o1.RequestID, false, line(9, "ghost"))
	a2.batch(o2.RequestID, false, line(4, "four"))
	if got, want := readN(t, st, 1), []string{"four"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("lines = %v, want %v", got, want)
	}
}

func TestFollowGivesUpOnANodeThatDoesNotReturn(t *testing.T) {
	h := NewHub(HubConfig{ReconnectWait: 30 * time.Millisecond})
	a := attach(t, h, "n1")
	st := open(t, h, Query{Targets: targets("n1", "w1"), Tail: 5, Follow: true})
	a.open()
	a.conn.hangUp()
	if got, want := readAll(t, st), []string{"notice:interrupted", "notice:error"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestAQueryThatDoesNotFollowEndsWhenTheConnectionDrops(t *testing.T) {
	h := NewHub(HubConfig{ReconnectWait: time.Minute})
	a := attach(t, h, "n1")
	st := open(t, h, Query{Targets: targets("n1", "w1"), Tail: 5})
	o := a.open()
	a.batch(o.RequestID, false, line(1, "partial"))
	a.conn.hangUp()
	if got, want := readAll(t, st), []string{"partial", "notice:error"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestANewStreamOfTheSameNodeTakesOver(t *testing.T) {
	h := NewHub(HubConfig{ReconnectWait: 5 * time.Second})
	a1 := attach(t, h, "n1")
	st := open(t, h, Query{Targets: targets("n1", "w1"), Tail: 0, Follow: true})
	o1 := a1.open()
	a1.batch(o1.RequestID, true, line(1, "one"))
	readN(t, st, 1)

	a2 := attach(t, h, "n1")
	select {
	case <-a1.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the replaced stream was not ended")
	}
	o2 := a2.open()
	if !o2.After.Equal(ts(1)) || o2.WorkloadID != "w1" {
		t.Fatalf("request on the new stream = %+v", o2)
	}
	a2.batch(o2.RequestID, true, line(2, "two"))
	if got, want := readN(t, st, 3), []string{"notice:interrupted", "notice:resumed", "two"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestCloseCancelsTheRequestsAndWakesTheReader(t *testing.T) {
	h := NewHub(HubConfig{})
	a := attach(t, h, "n1")
	st := open(t, h, Query{Targets: targets("n1", "w1"), Tail: 5, Follow: true})
	o := a.open()

	result := make(chan error, 1)
	go func() {
		_, err := st.Next(context.Background())
		result <- err
	}()
	st.Close()
	st.Close() // twice is fine
	select {
	case err := <-result:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Next error = %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a blocked reader was not woken by Close")
	}
	if d := a.next(); d.Cancel == nil || d.Cancel.RequestID != o.RequestID {
		t.Fatalf("hub sent %+v, want the request canceled", d)
	}
	// What the agent still sends for the canceled request is dropped.
	a.batch(o.RequestID, false, line(1, "late"))
	if h.Stats().Streams != 0 {
		t.Fatalf("stats = %+v; a closed query must not be counted", h.Stats())
	}
}

func TestACanceledContextClosesTheQuery(t *testing.T) {
	h := NewHub(HubConfig{})
	a := attach(t, h, "n1")
	ctx, cancel := context.WithCancel(context.Background())
	st, err := h.Open(ctx, Query{Targets: targets("n1", "w1"), Tail: 5, Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	o := a.open()
	cancel()
	if d := a.next(); d.Cancel == nil || d.Cancel.RequestID != o.RequestID {
		t.Fatalf("hub sent %+v, want the request canceled", d)
	}
	if _, err := nextEvent(t, st); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next error = %v, want ErrClosed", err)
	}
}

func TestNextCanBeInterruptedWithoutLosingAnything(t *testing.T) {
	h := NewHub(HubConfig{})
	a := attach(t, h, "n1")
	st := open(t, h, Query{Targets: targets("n1", "w1"), Tail: 5, Follow: true})
	o := a.open()
	a.batch(o.RequestID, true)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := st.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Next error = %v, want the deadline", err)
	}
	a.batch(o.RequestID, false, line(1, "after the timeout"))
	if got := readN(t, st, 1); got[0] != "after the timeout" {
		t.Fatalf("lines = %v", got)
	}
}

func TestTheNumberOfOpenQueriesIsLimited(t *testing.T) {
	h := NewHub(HubConfig{MaxStreams: 2})
	attach(t, h, "n1")
	first := open(t, h, Query{Targets: targets("n1", "w1"), Follow: true})
	open(t, h, Query{Targets: targets("n1", "w2"), Follow: true})
	if _, err := h.Open(context.Background(), Query{Targets: targets("n1", "w3")}); !errors.Is(err, ErrTooManyStreams) {
		t.Fatalf("third query error = %v, want ErrTooManyStreams", err)
	}
	first.Close()
	st, err := h.Open(context.Background(), Query{Targets: targets("n1", "w3")})
	if err != nil {
		t.Fatalf("a query after one was closed: %v", err)
	}
	st.Close()
}

func TestQueriesAreValidated(t *testing.T) {
	h := NewHub(HubConfig{})
	var many []Target
	for i := 0; i <= MaxTargets; i++ {
		many = append(many, Target{NodeID: "n1", WorkloadID: fmt.Sprintf("w%d", i)})
	}
	long := strings.Repeat("x", MaxIDBytes+1)
	tests := map[string]Query{
		"no containers":        {},
		"too many containers":  {Targets: many},
		"no node":              {Targets: targets("", "w1")},
		"no workload":          {Targets: targets("n1", "")},
		"a node name too long": {Targets: targets(long, "w1")},
		"a workload too long":  {Targets: targets("n1", long)},
		"the same one twice":   {Targets: targets("n1", "w1", "n2", "w1")},
	}
	for name, q := range tests {
		if _, err := h.Open(context.Background(), q); !errors.Is(err, ErrBadQuery) {
			t.Errorf("%s: error = %v, want ErrBadQuery", name, err)
		}
	}
	if h.Stats().Streams != 0 {
		t.Errorf("refused queries are still counted: %+v", h.Stats())
	}
}

func TestTheHistoryAskedForIsBounded(t *testing.T) {
	h := NewHub(HubConfig{})
	a := attach(t, h, "n1")
	for _, tail := range []int{-5, 0, 7, MaxTail + 1000} {
		st := open(t, h, Query{Targets: targets("n1", "w1"), Tail: tail})
		o := a.open()
		want := min(max(tail, 0), MaxTail)
		if o.Tail != want {
			t.Errorf("tail %d was passed on as %d, want %d", tail, o.Tail, want)
		}
		st.Close()
		a.next() // the cancel
	}
}

func TestEachContainerGetsAShareOfTheWindow(t *testing.T) {
	h := NewHub(HubConfig{Window: 10 * MinWindow})
	a := attach(t, h, "n1")
	var pairs []string
	for i := 0; i < 4; i++ {
		pairs = append(pairs, "n1", fmt.Sprintf("w%d", i))
	}
	open(t, h, Query{Targets: targets(pairs...), Tail: 1})
	for i := 0; i < 4; i++ {
		if o := a.open(); o.Credit != 10*MinWindow/4 {
			t.Errorf("request %d got a window of %d, want %d", i, o.Credit, 10*MinWindow/4)
		}
	}
	// However small the window, a container's share holds at least two of the longest lines.
	small := NewHub(HubConfig{Window: 1})
	b := attach(t, small, "n1")
	open(t, small, Query{Targets: targets("n1", "w1"), Tail: 1})
	if o := b.open(); o.Credit != MinWindow || o.MaxLineBytes > o.Credit/2-LineOverhead {
		t.Errorf("tiny window: %+v; the credit must hold at least two of the longest lines", o)
	}
}

func TestLongLinesAndOddStreamsAreNormalized(t *testing.T) {
	h := NewHub(HubConfig{LineBytes: 1000})
	a := attach(t, h, "n1")
	st := open(t, h, Query{Targets: targets("n1", "w1"), Tail: 5})
	o := a.open()
	if o.MaxLineBytes != 1000 {
		t.Fatalf("MaxLineBytes = %d, want 1000", o.MaxLineBytes)
	}
	a.batch(o.RequestID, true,
		Line{Time: ts(1), Stream: StreamStderr, Text: strings.Repeat("é", 800)}, // 1600 bytes
		Line{Time: ts(2), Stream: "weird", Text: "fine"},
	)
	a.end(o.RequestID, EndComplete, "")
	long, err := nextEvent(t, st)
	if err != nil || !long.Entry.Truncated || len(long.Entry.Text) > 1000 || long.Entry.Stream != StreamStderr || !strings.HasPrefix(long.Entry.Text, "éé") {
		t.Fatalf("long line = %+v, %v", long.Entry, err)
	}
	odd, err := nextEvent(t, st)
	if err != nil || odd.Entry.Stream != StreamStdout || odd.Entry.Truncated {
		t.Fatalf("odd stream = %+v, %v", odd.Entry, err)
	}
}

func TestAnAgentThatStopsReadingIsDropped(t *testing.T) {
	h := NewHub(HubConfig{})
	c := stuckConn{newFakeConn()}
	done := make(chan error, 1)
	go func() { done <- h.Attach(context.Background(), "n1", c) }()
	t.Cleanup(c.hangUp)
	waitFor(t, "the agent to attach", func() bool { return h.Connected("n1") })
	n, _ := h.node("n1")
	// The writer takes one message and blocks in Send; the rest pile up in the outbox until it is full.
	for i := 0; i < outboxSize+2; i++ {
		n.enqueue(Down{Cancel: &Cancel{RequestID: "x"}})
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("an agent that does not read its stream was not dropped")
	}
	if h.Connected("n1") {
		t.Fatal("the dropped stream is still attached")
	}
}

func TestShutdownEndsEverything(t *testing.T) {
	h := NewHub(HubConfig{})
	a := attach(t, h, "n1")
	st := open(t, h, Query{Targets: targets("n1", "w1"), Follow: true})
	a.open()
	h.Shutdown()
	select {
	case <-a.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the agent stream was not ended")
	}
	if _, err := nextEvent(t, st); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next error = %v, want ErrClosed", err)
	}
	if _, err := h.Open(context.Background(), Query{Targets: targets("n1", "w1")}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Open after shutdown: %v", err)
	}
	if err := h.Attach(context.Background(), "n2", newFakeConn()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Attach after shutdown: %v", err)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	c := map[string]time.Time{"w2": ts(7), "w1": ts(3)}
	enc := EncodeCursor(c)
	if want := fmt.Sprintf("w1=%d,w2=%d", ts(3).UnixNano(), ts(7).UnixNano()); enc != want {
		t.Fatalf("EncodeCursor = %q, want %q", enc, want)
	}
	per, all, err := ParseCursor(enc)
	if err != nil || !all.IsZero() || len(per) != 2 || !per["w1"].Equal(ts(3)) || !per["w2"].Equal(ts(7)) {
		t.Fatalf("ParseCursor = %v, %v, %v", per, all, err)
	}
}

func TestParseCursor(t *testing.T) {
	type result struct {
		per map[string]time.Time
		all time.Time
	}
	tests := []struct {
		in   string
		want result
		bad  bool
	}{
		{in: "", want: result{}},
		{in: "  ", want: result{}},
		{in: "2026-03-01T09:00:00.005Z", want: result{all: ts(5)}},
		{in: "2026-03-01T10:00:00+01:00", want: result{all: ts(0)}},
		{in: fmt.Sprint(ts(5).UnixNano()), want: result{all: ts(5)}},
		{in: "w1=" + fmt.Sprint(ts(5).UnixNano()), want: result{per: map[string]time.Time{"w1": ts(5)}}},
		{in: "w1=2026-03-01T09:00:00Z,w2=0", want: result{per: map[string]time.Time{"w1": ts(0), "w2": time.Unix(0, 0).UTC()}}},
		{in: "yesterday", bad: true},
		{in: "-5", bad: true},
		{in: "w1", bad: true},
		{in: "=5", bad: true},
		{in: "w1=nope", bad: true},
		{in: "w1=5,,w2=6", bad: true},
		{in: strings.Repeat("x", MaxIDBytes+1) + "=5", bad: true},
	}
	for _, tt := range tests {
		per, all, err := ParseCursor(tt.in)
		if tt.bad {
			if err == nil {
				t.Errorf("ParseCursor(%q) = %v, %v; want an error", tt.in, per, all)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseCursor(%q): %v", tt.in, err)
			continue
		}
		if !all.Equal(tt.want.all) || len(per) != len(tt.want.per) {
			t.Errorf("ParseCursor(%q) = %v, %v; want %v, %v", tt.in, per, all, tt.want.per, tt.want.all)
			continue
		}
		for id, want := range tt.want.per {
			if !per[id].Equal(want) {
				t.Errorf("ParseCursor(%q)[%s] = %v, want %v", tt.in, id, per[id], want)
			}
		}
	}
}

func TestACursorIsPassedToTheAgent(t *testing.T) {
	h := NewHub(HubConfig{})
	a := attach(t, h, "n1")
	open(t, h, Query{
		Targets: targets("n1", "w1", "n1", "w2", "n1", "w3"), Tail: 5,
		After: map[string]time.Time{"w1": ts(10)}, Default: ts(4),
	})
	got := map[string]time.Time{}
	for i := 0; i < 3; i++ {
		o := a.open()
		got[o.WorkloadID] = o.After
	}
	if !got["w1"].Equal(ts(10)) || !got["w2"].Equal(ts(4)) || !got["w3"].Equal(ts(4)) {
		t.Fatalf("cursors passed to the agent = %v", got)
	}
}
