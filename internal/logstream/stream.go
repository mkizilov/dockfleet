package logstream

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

// Stream is the answer to a query: the log lines of the containers it names,
// and notices about them, in the order Next returns them.
//
// Lines come out in two phases. First the history: the lines each container
// had when the query began, merged by time, so a service's replicas read as one
// log. Then, if the query follows, the new lines in the order they arrive. A
// container's lines retain their input order; stdout/stderr timestamps can interleave.
//
// A Stream never holds more than one credit window per container. The window
// is granted to the agent up front and returned only as Next takes lines out,
// so a reader that stops calling Next stops the agents.
//
// Next and TryNext may be called from one goroutine at a time; Close from any.
type Stream struct {
	hub       *Hub
	q         Query
	window    int
	lineBytes int

	ctx      context.Context // stops the goroutines that resume containers
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	wake     chan struct{}
	closedCh chan struct{}

	mu        sync.Mutex
	sources   []*source
	rr        int
	closed    bool
	stopAfter func() bool
}

// item is what a container's queue holds: a line, or a notice that belongs
// at this place among its lines.
type item struct {
	line   Line
	gen    int // the request the line came from
	notice *Notice
}

// source is one container of a query. Everything below the line is guarded
// by the stream's mutex.
type source struct {
	st     *Stream
	target Target
	tail0  int       // the history asked for in the first request
	after0 time.Time // the cursor the query started from
	began  time.Time // when the query asked for it

	reqID string
	node  *nodeConn
	gen   int // counts the requests opened for this container

	queue []item
	head  int

	pushed  int // lines queued so far
	popped  int // lines handed to the reader
	histEnd int // lines pushed when the history ended; -1 while it goes on

	// Flow control of the current request, in bytes: what it may send in all,
	// what it has sent, and what the reader has taken out since credit was last returned.
	granted, received, consumed int

	done       bool
	lastPushed time.Time // the newest line queued
	emitted    time.Time // the newest line handed to the reader
}

func (s *source) peek() *item {
	if s.head < len(s.queue) {
		return &s.queue[s.head]
	}
	return nil
}

func (s *source) queueLen() int { return len(s.queue) - s.head }

// inHistory is whether lines of the history are still to come or still queued.
func (s *source) inHistory() bool {
	if s.histEnd < 0 {
		return !s.done
	}
	return s.popped < s.histEnd
}

func (st *Stream) signal() {
	select {
	case st.wake <- struct{}{}:
	default:
	}
}

// clip makes text from an agent safe to show: no control characters, bounded.
func clip(s string) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)
	if len(s) > 200 {
		s = strings.ToValidUTF8(s[:200], "")
	}
	return s
}

// begin opens a request for the container on node. st.mu is held.
func (st *Stream) begin(s *source, node *nodeConn, tail int, after time.Time) bool {
	s.gen++
	s.reqID = st.hub.nextID()
	s.node = node
	s.granted, s.received, s.consumed = st.window, 0, 0
	if !node.add(s) {
		s.node = nil
		return false
	}
	open := &Open{
		RequestID: s.reqID, WorkloadID: s.target.WorkloadID, Tail: tail, After: after,
		Follow: st.q.Follow, MaxLineBytes: st.lineBytes, Credit: st.window,
	}
	if !node.enqueue(Down{Open: open}) {
		node.remove(s.reqID)
		s.node = nil
		return false
	}
	return true
}

// finishLocked ends a container's part of the stream: no more lines will
// arrive, and a notice, if any, comes after the lines already queued.
func (st *Stream) finishLocked(s *source, n *Notice) {
	if s.done {
		return
	}
	s.done = true
	if s.histEnd < 0 {
		s.histEnd = s.pushed
	}
	if s.node != nil {
		s.node.remove(s.reqID)
	}
	if n != nil {
		n.WorkloadID, n.NodeID = s.target.WorkloadID, s.target.NodeID
		s.queue = append(s.queue, item{notice: n})
	}
	st.signal()
}

// abortLocked cancels the request on the agent and ends the container's part.
func (st *Stream) abortLocked(s *source, n *Notice) {
	if !s.done && s.node != nil {
		s.node.enqueue(Down{Cancel: &Cancel{RequestID: s.reqID}})
	}
	st.finishLocked(s, n)
}

// deliver queues the lines an agent sent for a request. An agent that sends
// more than it was granted has its request ended: the credit is the bound on
// the manager's memory, and an agent does not get to ignore it.
func (st *Stream) deliver(s *source, b Batch) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed || s.done || s.reqID != b.RequestID {
		return
	}
	s.received += b.Bytes()
	if s.received > s.granted {
		st.hub.log.Warn("agent sent more log lines than its window allows; ending the request", "node_id", s.target.NodeID, "workload_id", s.target.WorkloadID)
		st.abortLocked(s, &Notice{Kind: NoticeError, Message: "the agent sent more than its flow-control window allows"})
		return
	}
	for _, l := range b.Lines {
		if len(l.Text) > st.lineBytes {
			l.Text, l.Truncated = strings.ToValidUTF8(l.Text[:st.lineBytes], ""), true
		}
		if l.Stream != StreamStderr {
			l.Stream = StreamStdout
		}
		s.queue = append(s.queue, item{line: l, gen: s.gen})
		s.pushed++
		if l.Time.After(s.lastPushed) {
			s.lastPushed = l.Time
		}
	}
	if b.HistoryComplete && s.histEnd < 0 {
		s.histEnd = s.pushed
	}
	st.signal()
}

func (st *Stream) deliverEnd(s *source, e End) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed || s.done || s.reqID != e.RequestID {
		return
	}
	var n *Notice
	switch e.Reason {
	case EndComplete:
		if st.q.Follow {
			msg := clip(e.Message)
			if msg == "" {
				msg = "the container stopped"
			}
			n = &Notice{Kind: NoticeEnded, Message: msg}
		}
	case EndNotFound:
		n = &Notice{Kind: NoticeUnavailable, Message: fmt.Sprintf("node %s has no container for the workload (it may not have started yet, or it was removed)", s.target.NodeID)}
	case EndBusy:
		n = &Notice{Kind: NoticeUnavailable, Message: fmt.Sprintf("node %s is serving as many log requests as it allows", s.target.NodeID)}
	case EndError:
		n = &Notice{Kind: NoticeError, Message: clip(e.Message)}
	}
	st.finishLocked(s, n)
}

// nodeLost is called when the stream a request was running on has ended.
func (st *Stream) nodeLost(s *source, n *nodeConn) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed || s.done || s.node != n {
		return
	}
	s.node = nil
	wait := st.hub.cfg.ReconnectWait
	if !st.q.Follow || wait <= 0 {
		st.finishLocked(s, &Notice{Kind: NoticeError, Message: fmt.Sprintf("the connection to node %s was lost", n.id)})
		return
	}
	// What is left of the history is read as it comes, not waited for.
	if s.histEnd < 0 {
		s.histEnd = s.pushed
	}
	s.queue = append(s.queue, item{notice: &Notice{
		WorkloadID: s.target.WorkloadID, NodeID: n.id, Kind: NoticeInterrupted,
		Message: fmt.Sprintf("the connection to node %s was lost; waiting up to %s for it to come back", n.id, wait),
	}})
	st.signal()
	st.wg.Add(1)
	go st.resume(s)
}

// resume waits for the node of an interrupted container to attach again and
// opens a new request after the last line it queued, so nothing is read twice
// and nothing is missed.
func (st *Stream) resume(s *source) {
	defer st.wg.Done()
	timer := time.NewTimer(st.hub.cfg.ReconnectWait)
	defer timer.Stop()
	for {
		node, attached := st.hub.node(s.target.NodeID)
		if node != nil {
			st.mu.Lock()
			if st.closed || s.done {
				st.mu.Unlock()
				return
			}
			tail, after := -1, s.lastPushed
			if after.IsZero() {
				tail, after = s.tail0, s.after0
			}
			if st.begin(s, node, tail, after) {
				s.queue = append(s.queue, item{notice: &Notice{
					WorkloadID: s.target.WorkloadID, NodeID: s.target.NodeID, Kind: NoticeResumed,
					Message: fmt.Sprintf("node %s is back; following again", s.target.NodeID),
				}})
				st.signal()
				st.mu.Unlock()
				return
			}
			st.mu.Unlock()
		}
		select {
		case <-st.ctx.Done():
			return
		case <-timer.C:
			st.mu.Lock()
			if !st.closed && !s.done {
				st.finishLocked(s, &Notice{Kind: NoticeError, Message: fmt.Sprintf("node %s did not come back within %s", s.target.NodeID, st.hub.cfg.ReconnectWait)})
			}
			st.mu.Unlock()
			return
		case <-attached:
		}
	}
}

// Next returns the next event, waiting for one if there is none. It returns
// io.EOF when every container has ended and everything was read, ErrClosed
// after Close, and the context's error if ctx ends first; in that case nothing
// is lost and Next can be called again.
func (st *Stream) Next(ctx context.Context) (Event, error) {
	for {
		st.mu.Lock()
		if st.closed {
			st.mu.Unlock()
			return Event{}, ErrClosed
		}
		ev, ok, eof := st.pickLocked()
		var late <-chan time.Time
		var stopLate func() bool
		if !ok && !eof {
			if at, has := st.lateAtLocked(); has {
				timer := time.NewTimer(time.Until(at))
				late, stopLate = timer.C, timer.Stop
			}
		}
		st.mu.Unlock()
		switch {
		case ok:
			return ev, nil
		case eof:
			return Event{}, io.EOF
		}
		select {
		case <-ctx.Done():
			if stopLate != nil {
				stopLate()
			}
			return Event{}, ctx.Err()
		case <-st.closedCh:
			if stopLate != nil {
				stopLate()
			}
			return Event{}, ErrClosed
		case <-st.wake:
		case <-late:
		}
		if stopLate != nil {
			stopLate()
		}
	}
}

// late is whether a container has kept the merged history waiting for longer
// than the hub allows.
func (st *Stream) late(s *source, now time.Time) bool {
	wait := st.hub.cfg.HistoryWait
	return wait > 0 && now.Sub(s.began) >= wait
}

// lateAtLocked is when the earliest container that holds the merged history
// back becomes late, if there is one.
func (st *Stream) lateAtLocked() (time.Time, bool) {
	wait := st.hub.cfg.HistoryWait
	if wait <= 0 {
		return time.Time{}, false
	}
	now := time.Now()
	var at time.Time
	for _, s := range st.sources {
		if s.inHistory() && s.peek() == nil && !st.late(s, now) {
			if due := s.began.Add(wait); at.IsZero() || due.Before(at) {
				at = due
			}
		}
	}
	return at, !at.IsZero()
}

// TryNext returns the next event if one is ready.
func (st *Stream) TryNext() (Event, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return Event{}, false
	}
	ev, ok, _ := st.pickLocked()
	return ev, ok
}

// pickLocked chooses the next event. ok is false if there is none yet; eof is
// true if there never will be.
func (st *Stream) pickLocked() (ev Event, ok, eof bool) {
	// A notice is shown as soon as it reaches the front of its container's queue.
	for _, s := range st.sources {
		if it := s.peek(); it != nil && it.notice != nil {
			return st.popLocked(s), true, false
		}
	}

	// While any container is still delivering its history, only history is
	// returned, oldest first: that needs the next line of every one of them.
	var oldest *source
	waiting := false
	now := time.Now()
	for _, s := range st.sources {
		if !s.inHistory() {
			continue
		}
		it := s.peek()
		if it == nil {
			// A container that is late no longer holds the others back.
			waiting = waiting || !st.late(s, now)
			continue
		}
		if oldest == nil || it.line.Time.Before(oldest.peek().line.Time) {
			oldest = s
		}
	}
	if waiting {
		return Event{}, false, false
	}
	if oldest != nil {
		return st.popLocked(oldest), true, false
	}

	// After the history, lines come in the order they arrive, taking turns
	// between containers so a busy one cannot starve the others.
	n := len(st.sources)
	for i := 0; i < n; i++ {
		s := st.sources[(st.rr+i)%n]
		if s.peek() != nil {
			st.rr = (st.rr + i + 1) % n
			return st.popLocked(s), true, false
		}
	}
	for _, s := range st.sources {
		if !s.done {
			return Event{}, false, false
		}
	}
	return Event{}, false, true
}

func (st *Stream) popLocked(s *source) Event {
	it := s.queue[s.head]
	s.queue[s.head] = item{}
	s.head++
	if s.head >= 64 && s.head*2 >= len(s.queue) {
		n := copy(s.queue, s.queue[s.head:])
		clear(s.queue[n:])
		s.queue, s.head = s.queue[:n], 0
	}
	if it.notice != nil {
		n := *it.notice
		return Event{Notice: &n}
	}
	s.popped++
	if it.line.Time.After(s.emitted) {
		s.emitted = it.line.Time
	}
	if it.gen == s.gen {
		s.consumed += it.line.Size()
		st.creditLocked(s)
	}
	return Event{Entry: &Entry{WorkloadID: s.target.WorkloadID, NodeID: s.target.NodeID, Line: it.line}}
}

// creditLocked gives the agent back the credit of the lines the reader has
// taken, a quarter of a window at a time, or at once when the queue is empty.
func (st *Stream) creditLocked(s *source) {
	if s.consumed == 0 || (s.consumed < st.window/4 && s.queueLen() > 0) {
		return
	}
	if s.done || s.node == nil {
		s.consumed = 0
		return
	}
	if s.node.enqueue(Down{Credit: &Credit{RequestID: s.reqID, Bytes: s.consumed}}) {
		s.granted += s.consumed
		s.consumed = 0
	}
}

// Cursor is where a client should resume this stream: for each container, the
// time of the newest line the reader has been given (or the cursor the query
// started from, if it was given none).
func (st *Stream) Cursor() map[string]time.Time {
	st.mu.Lock()
	defer st.mu.Unlock()
	out := map[string]time.Time{}
	for _, s := range st.sources {
		switch {
		case !s.emitted.IsZero():
			out[s.target.WorkloadID] = s.emitted
		case !s.after0.IsZero():
			out[s.target.WorkloadID] = s.after0
		}
	}
	return out
}

// Close ends the query, cancels its requests on the agents and releases its
// resources. It is safe to call more than once.
func (st *Stream) Close() {
	st.mu.Lock()
	if st.closed {
		st.mu.Unlock()
		return
	}
	st.closed = true
	close(st.closedCh)
	for _, s := range st.sources {
		if !s.done && s.node != nil {
			s.node.enqueue(Down{Cancel: &Cancel{RequestID: s.reqID}})
			s.node.remove(s.reqID)
		}
		s.done = true
		s.queue, s.head = nil, 0
	}
	stop := st.stopAfter
	st.mu.Unlock()

	if stop != nil {
		stop()
	}
	st.cancel()
	st.wg.Wait()
	st.hub.forget(st)
}

// EncodeCursor writes a cursor as "workload=unixnano,workload=unixnano", sorted.
func EncodeCursor(c map[string]time.Time) string {
	ids := make([]string, 0, len(c))
	for id := range c {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, id+"="+strconv.FormatInt(c[id].UnixNano(), 10))
	}
	return strings.Join(parts, ",")
}

// ParseTime reads a time written as RFC 3339 or as Unix nanoseconds.
func ParseTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		if n < 0 {
			return time.Time{}, fmt.Errorf("%q is before 1970", s)
		}
		return time.Unix(0, n).UTC(), nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("%q is neither RFC 3339 nor Unix nanoseconds", s)
	}
	return t.UTC(), nil
}

// ParseCursor reads what EncodeCursor wrote, or a single time that applies to
// every container.
func ParseCursor(s string) (per map[string]time.Time, all time.Time, err error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, time.Time{}, nil
	}
	if !strings.Contains(s, "=") {
		all, err = ParseTime(s)
		return nil, all, err
	}
	per = map[string]time.Time{}
	for _, pair := range strings.Split(s, ",") {
		id, raw, ok := strings.Cut(pair, "=")
		id = strings.TrimSpace(id)
		if !ok || id == "" || len(id) > MaxIDBytes {
			return nil, time.Time{}, fmt.Errorf("%q is not workload=time", pair)
		}
		t, err := ParseTime(raw)
		if err != nil {
			return nil, time.Time{}, err
		}
		per[id] = t
	}
	return per, time.Time{}, nil
}
