package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/dockfleet/dockfleet/internal/docker"
	"github.com/dockfleet/dockfleet/internal/logstream"
	"github.com/dockfleet/dockfleet/internal/protocol"
)

const (
	// defaultLogRequests is how many log requests the agent serves at once.
	defaultLogRequests = 16
	// logFlushAfter is how long a line may wait in a batch for more to join it.
	logFlushAfter = 25 * time.Millisecond
	// maxLogWindow bounds the flow-control credit one request may hold, whatever
	// the manager grants: it is the most the agent will send without being asked.
	maxLogWindow = 16 << 20
)

// logOpener is what the log server needs of the manager connection;
// *protocol.Client satisfies it through Manager.
type logOpener interface {
	Logs(ctx context.Context, nodeID, sessionID string) (logstream.AgentConn, error)
}

// logServer answers the manager's requests for container logs. The manager
// never connects to an agent, so the agent opens one stream for its session
// and the requests arrive on it. Each request reads one container's output
// from Docker and sends it back as batches of lines.
//
// What the agent holds in memory is bounded whatever the container writes and
// however slowly the manager reads: a request sends only as many bytes as the
// manager has granted credit for, and reads the container's output only as it
// sends, so what is not sent stays in Docker. A request holds one batch (at
// most logstream.MaxBatchBytes) and the reader's buffers; there are at most
// `requests` of them.
type logServer struct {
	rt       docker.Runtime
	log      *slog.Logger
	requests int
	retryMin time.Duration
	retryMax time.Duration
	now      func() time.Time
}

func newLogServer(rt docker.Runtime, log *slog.Logger, requests int, retryMin, retryMax time.Duration, now func() time.Time) *logServer {
	if requests == 0 {
		requests = defaultLogRequests
	}
	if now == nil {
		now = time.Now
	}
	return &logServer{rt: rt, log: log, requests: requests, retryMin: retryMin, retryMax: retryMax, now: now}
}

// enabled is whether the agent serves logs at all.
func (s *logServer) enabled() bool { return s.requests > 0 }

// run keeps a log stream open for the session until ctx ends or the manager
// says retrying is pointless: it serves no logs, or the session is over (the
// agent's heartbeat registers again and starts a new run).
func (s *logServer) run(ctx context.Context, mgr logOpener, nodeID, sessionID string) {
	delay := s.retryMin
	for ctx.Err() == nil {
		conn, err := mgr.Logs(ctx, nodeID, sessionID)
		if err == nil {
			delay = s.retryMin
			s.log.Debug("log stream open", "node_id", nodeID)
			err = s.serve(ctx, conn)
		}
		switch {
		case ctx.Err() != nil:
			return
		case protocol.IsUnimplemented(err):
			s.log.Info("the manager does not serve container logs")
			return
		case protocol.NeedsRegistration(err):
			s.log.Debug("the log stream ended with the session", "error", err)
			return
		case protocol.IsPermanent(err):
			s.log.Error("the manager rejected the log stream; not retrying", "error", err)
			return
		case err == nil, errors.Is(err, io.EOF):
			s.log.Debug("log stream closed by the manager; opening it again")
		default:
			s.log.Warn("log stream failed; opening it again", "error", err, "retry_in", delay)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		delay = min(delay*2, s.retryMax)
	}
}

// serve reads the manager's messages from one stream and runs the requests
// they open, until the stream ends. Every request is stopped when it does.
func (s *logServer) serve(ctx context.Context, conn logstream.AgentConn) error {
	ctx, cancel := context.WithCancel(ctx)
	active := map[string]*logRequest{}
	var sendMu, mu sync.Mutex
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()
	send := func(u logstream.Up) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return conn.Send(u)
	}

	for {
		d, err := conn.Recv()
		if err != nil {
			return err
		}
		switch {
		case d.Open != nil:
			o := *d.Open
			mu.Lock()
			_, running := active[o.RequestID]
			full := len(active) >= s.requests
			if !running && !full {
				reqCtx, reqCancel := context.WithCancel(ctx)
				req := newLogRequest(reqCancel, o.Credit)
				active[o.RequestID] = req
				wg.Add(1)
				go func() {
					defer wg.Done()
					s.request(reqCtx, send, req, o)
					reqCancel()
					mu.Lock()
					delete(active, o.RequestID)
					mu.Unlock()
				}()
			}
			mu.Unlock()
			if full && !running {
				s.log.Warn("refusing a log request: the agent serves as many as it allows", "limit", s.requests)
				_ = send(logstream.Up{End: &logstream.End{RequestID: o.RequestID, Reason: logstream.EndBusy}})
			}
		case d.Cancel != nil:
			mu.Lock()
			req := active[d.Cancel.RequestID]
			mu.Unlock()
			if req != nil {
				req.cancel()
			}
		case d.Credit != nil:
			mu.Lock()
			req := active[d.Credit.RequestID]
			mu.Unlock()
			if req != nil {
				req.addCredit(d.Credit.Bytes)
			}
		}
	}
}

// logRequest is the flow control of one request: the bytes the manager has
// allowed it to send and it has not sent yet.
type logRequest struct {
	cancel context.CancelFunc
	window int

	mu     sync.Mutex
	credit int
	wake   chan struct{}
}

func newLogRequest(cancel context.CancelFunc, credit int) *logRequest {
	credit = min(max(credit, 0), maxLogWindow)
	return &logRequest{cancel: cancel, window: credit, credit: credit, wake: make(chan struct{}, 1)}
}

func (r *logRequest) addCredit(n int) {
	r.mu.Lock()
	r.credit = min(r.credit+max(n, 0), maxLogWindow)
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *logRequest) tryTake(n int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.credit < n {
		return false
	}
	r.credit -= n
	return true
}

// take waits until n bytes of credit are available and spends them.
func (r *logRequest) take(ctx context.Context, n int) error {
	for !r.tryTake(n) {
		select {
		case <-r.wake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// logBatcher collects the lines of one request into batches. A batch goes out
// when it is full, when the request has to wait for credit (the manager can
// only return credit for lines it has seen), at the end of the history, and
// shortly after its first line, so a live line is not held back.
type logBatcher struct {
	mu    sync.Mutex
	send  func(logstream.Up) error
	id    string
	req   *logRequest
	lines []logstream.Line
	size  int
	timer *time.Timer
	err   error
	// newest is the time of the newest line taken so far.
	newest time.Time
}

func (b *logBatcher) flushLocked(historyComplete bool) {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	if len(b.lines) == 0 && !historyComplete {
		return
	}
	batch := logstream.Batch{RequestID: b.id, Lines: b.lines, HistoryComplete: historyComplete}
	b.lines, b.size = nil, 0
	if b.err == nil {
		b.err = b.send(logstream.Up{Batch: &batch})
	}
}

// drop discards what is queued and stops the timer, for a request that was
// canceled: the manager has stopped listening for it.
func (b *logBatcher) drop(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	b.lines, b.size = nil, 0
	if b.err == nil {
		b.err = err
	}
}

func (b *logBatcher) flush(historyComplete bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked(historyComplete)
	return b.err
}

// add queues a line, waiting for credit for it if need be.
func (b *logBatcher) add(ctx context.Context, l docker.LogLine) error {
	cost := len(l.Text) + logstream.LineOverhead
	b.mu.Lock()
	if b.err == nil && (len(b.lines) >= logstream.MaxBatchLines || b.size+cost > logstream.MaxBatchBytes) {
		b.flushLocked(false)
	}
	if b.err == nil && !b.req.tryTake(cost) {
		b.flushLocked(false)
		b.mu.Unlock()
		err := b.req.take(ctx, cost)
		b.mu.Lock()
		if err != nil {
			b.mu.Unlock()
			return err
		}
	}
	if b.err != nil {
		err := b.err
		b.mu.Unlock()
		return err
	}
	stream := logstream.StreamStdout
	if l.Stream == "stderr" {
		stream = logstream.StreamStderr
	}
	b.lines = append(b.lines, logstream.Line{Time: l.Time, Stream: stream, Text: l.Text, Truncated: l.Truncated})
	b.size += cost
	if l.Time.After(b.newest) {
		b.newest = l.Time
	}
	if b.timer == nil {
		b.timer = time.AfterFunc(logFlushAfter, func() { _ = b.flush(false) })
	}
	b.mu.Unlock()
	return nil
}

// request serves one log request: the history first, then, if asked, the lines
// the container writes from then on. It returns when the request is over, and
// tells the manager why unless the manager ended it.
func (s *logServer) request(ctx context.Context, send func(logstream.Up) error, r *logRequest, o logstream.Open) {
	end := func(reason logstream.EndReason, msg string) {
		if ctx.Err() == nil {
			_ = send(logstream.Up{End: &logstream.End{RequestID: o.RequestID, Reason: reason, Message: msg}})
		}
	}
	maxLine := o.MaxLineBytes
	if maxLine <= 0 {
		maxLine = logstream.DefaultLineBytes
	}
	maxLine = min(maxLine, logstream.MaxLineBytes, r.window/2-logstream.LineOverhead)
	if maxLine < 1 {
		end(logstream.EndError, "the flow-control window is too small for a log line")
		return
	}
	tail := min(o.Tail, logstream.MaxTail)
	b := &logBatcher{send: send, id: o.RequestID, req: r}
	defer func() {
		if ctx.Err() != nil {
			b.drop(ctx.Err())
		}
	}()
	started := s.now().UTC()
	s.log.Debug("serving a log request", "workload_id", o.WorkloadID, "request", o.RequestID, "follow", o.Follow)

	fail := func(err error) {
		_ = b.flush(false)
		switch {
		case errors.Is(err, docker.ErrNotFound), errors.Is(err, docker.ErrInvalid):
			end(logstream.EndNotFound, "")
		default:
			end(logstream.EndError, clipMessage(err.Error()))
		}
	}

	// The history: what the container wrote already, in one read that ends.
	last := o.After
	if tail != 0 {
		rd, err := s.rt.StreamLogs(ctx, o.WorkloadID, docker.LogStreamOptions{Tail: tail, Since: o.After, MaxLineBytes: maxLine})
		if err != nil {
			fail(err)
			return
		}
		err = s.pump(ctx, rd, b, o.After)
		_ = rd.Close()
		if err != nil {
			if ctx.Err() == nil {
				fail(err)
			}
			return
		}
		last = latest(last, b)
	}
	if err := b.flush(true); err != nil || ctx.Err() != nil {
		return
	}
	if !o.Follow {
		end(logstream.EndComplete, "")
		return
	}

	// The follow starts where the history left off. The lines written between
	// the two reads are in the second, and the ones both reads see are dropped.
	from := last
	if from.IsZero() {
		from = started
	}
	rd, err := s.rt.StreamLogs(ctx, o.WorkloadID, docker.LogStreamOptions{Tail: -1, Since: from, Follow: true, MaxLineBytes: maxLine})
	if err != nil {
		fail(err)
		return
	}
	err = s.pump(ctx, rd, b, from)
	_ = rd.Close()
	_ = b.flush(false)
	switch {
	case ctx.Err() != nil:
	case err != nil:
		end(logstream.EndError, clipMessage(err.Error()))
	default:
		end(logstream.EndComplete, "the container stopped")
	}
}

// latest is the newest time of the lines a batcher has taken so far.
func latest(from time.Time, b *logBatcher) time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.newest.After(from) {
		return b.newest
	}
	return from
}

// pump reads lines until the reader ends and adds those after `after` to the
// batcher. It returns nil at the end of the output.
func (s *logServer) pump(ctx context.Context, rd docker.LogReader, b *logBatcher, after time.Time) error {
	for {
		l, err := rd.Next()
		switch {
		case errors.Is(err, io.EOF):
			return nil
		case err != nil:
			return err
		case !after.IsZero() && !l.Time.After(after):
			continue
		}
		if err := b.add(ctx, l); err != nil {
			return err
		}
	}
}

// clipMessage makes an error fit in the end of a request.
func clipMessage(s string) string {
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
