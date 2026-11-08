package logstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Defaults for HubConfig.
const (
	DefaultMaxStreams    = 32
	DefaultWindow        = 256 << 10
	DefaultReconnectWait = 30 * time.Second
	DefaultHistoryWait   = 10 * time.Second

	// outboxSize bounds the messages queued for one agent. The manager sends an
	// agent little (a request, a cancel, some credit), so a full outbox means the
	// agent has stopped reading and its stream is dropped.
	outboxSize = 256
)

var (
	// ErrTooManyStreams: the Hub already serves as many streams as it allows.
	ErrTooManyStreams = errors.New("too many log streams are open")
	// ErrBadQuery: the query is malformed.
	ErrBadQuery = errors.New("invalid log query")
)

// HubConfig configures NewHub.
type HubConfig struct {
	// MaxStreams is how many queries may be open at once; zero means DefaultMaxStreams.
	MaxStreams int
	// Window is the flow-control credit, in bytes, one query shares among its
	// containers: the most that is buffered for it in the manager. Zero means
	// DefaultWindow. A container never gets less than MinWindow.
	Window int
	// ReconnectWait is how long a following query waits for a node whose
	// connection was lost to come back before it gives up on that container.
	// Zero means DefaultReconnectWait; negative means not at all.
	ReconnectWait time.Duration
	// HistoryWait is how long the merged history of a query waits for a
	// container whose agent has not answered yet; after that the other
	// containers' lines go on without it, and its lines are delivered as they
	// arrive. Zero means DefaultHistoryWait; negative means no limit.
	HistoryWait time.Duration
	// LineBytes is the longest line sent to a query; longer lines are cut. Zero
	// means DefaultLineBytes.
	LineBytes int
	Logger    *slog.Logger
}

// Hub holds the log streams of the agents connected to this manager and serves
// queries from them. With several managers each Hub knows the agents connected
// to its own manager only.
type Hub struct {
	cfg HubConfig
	log *slog.Logger
	seq atomic.Uint64

	mu       sync.Mutex
	nodes    map[string]*nodeConn
	streams  map[*Stream]struct{}
	closed   bool
	attached chan struct{} // closed and replaced whenever an agent attaches
}

// NewHub returns a Hub with the defaults filled in.
func NewHub(cfg HubConfig) *Hub {
	if cfg.MaxStreams <= 0 {
		cfg.MaxStreams = DefaultMaxStreams
	}
	if cfg.Window <= 0 {
		cfg.Window = DefaultWindow
	}
	if cfg.ReconnectWait == 0 {
		cfg.ReconnectWait = DefaultReconnectWait
	}
	if cfg.HistoryWait == 0 {
		cfg.HistoryWait = DefaultHistoryWait
	}
	if cfg.LineBytes <= 0 || cfg.LineBytes > MaxLineBytes {
		cfg.LineBytes = DefaultLineBytes
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Hub{cfg: cfg, log: log, nodes: map[string]*nodeConn{}, streams: map[*Stream]struct{}{}, attached: make(chan struct{})}
}

// Stats is a snapshot of what the Hub serves.
type Stats struct {
	Nodes   []string
	Streams int
}

// Stats reports which nodes have a log stream attached and how many queries are open.
func (h *Hub) Stats() Stats {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := Stats{Streams: len(h.streams)}
	for id := range h.nodes {
		s.Nodes = append(s.Nodes, id)
	}
	return s
}

// Connected reports whether the node has a log stream attached.
func (h *Hub) Connected(nodeID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.nodes[nodeID] != nil
}

// Shutdown ends every agent stream and every query. The Hub accepts nothing afterwards.
func (h *Hub) Shutdown() {
	h.mu.Lock()
	h.closed = true
	nodes := make([]*nodeConn, 0, len(h.nodes))
	for _, n := range h.nodes {
		nodes = append(nodes, n)
	}
	streams := make([]*Stream, 0, len(h.streams))
	for st := range h.streams {
		streams = append(streams, st)
	}
	h.mu.Unlock()
	for _, st := range streams {
		st.Close()
	}
	for _, n := range nodes {
		n.cancel()
	}
}

func (h *Hub) nextID() string { return "r" + strconv.FormatUint(h.seq.Add(1), 36) }

// node returns the attached stream of a node, and a channel that is closed the
// next time any agent attaches.
func (h *Hub) node(id string) (*nodeConn, <-chan struct{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.nodes[id], h.attached
}

func (h *Hub) forget(st *Stream) {
	h.mu.Lock()
	delete(h.streams, st)
	h.mu.Unlock()
}

// nodeConn is one attached agent stream.
type nodeConn struct {
	id     string
	hub    *Hub
	conn   Conn
	outbox chan Down
	cancel context.CancelFunc

	mu      sync.Mutex
	sources map[string]*source // by request ID
	gone    bool
}

// enqueue queues a message for the agent without waiting. If the agent has
// stopped reading, its stream is dropped and the message is lost.
func (n *nodeConn) enqueue(d Down) bool {
	select {
	case n.outbox <- d:
		return true
	default:
		n.hub.log.Warn("agent is not reading its log stream; dropping it", "node_id", n.id)
		n.cancel()
		return false
	}
}

func (n *nodeConn) add(s *source) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.gone {
		return false
	}
	n.sources[s.reqID] = s
	return true
}

func (n *nodeConn) remove(id string) {
	n.mu.Lock()
	delete(n.sources, id)
	n.mu.Unlock()
}

func (n *nodeConn) lookup(id string) *source {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.sources[id]
}

// drain marks the stream gone and returns the requests that were running on it.
func (n *nodeConn) drain() []*source {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.gone = true
	out := make([]*source, 0, len(n.sources))
	for _, s := range n.sources {
		out = append(out, s)
	}
	n.sources = map[string]*source{}
	return out
}

// Attach serves one agent's log stream until it ends, the context is canceled,
// the agent attaches again (the newer stream wins) or the Hub shuts down. The
// caller has authenticated the agent and checked its session; Attach trusts
// nodeID. Requests that were running on the stream are resumed on the next one
// if their query follows and the node returns in time.
//
// Messages for requests the node was not asked, and lines beyond the credit a
// request granted, are ignored or end that request: an agent can only ever
// feed the requests that were opened on its own stream.
func (h *Hub) Attach(ctx context.Context, nodeID string, conn Conn) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	n := &nodeConn{
		id: nodeID, hub: h, conn: conn, outbox: make(chan Down, outboxSize), cancel: cancel,
		sources: map[string]*source{},
	}

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return ErrClosed
	}
	old := h.nodes[nodeID]
	h.nodes[nodeID] = n
	close(h.attached)
	h.attached = make(chan struct{})
	h.mu.Unlock()
	if old != nil {
		old.cancel()
	}
	h.log.Info("agent log stream attached", "node_id", nodeID)

	// The writer owns conn.Send. It stops when the context ends or a send fails;
	// a send blocked on a dead connection returns once the transport is torn
	// down, which happens when the caller returns.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case d := <-n.outbox:
				if err := conn.Send(d); err != nil {
					cancel()
					return
				}
			}
		}
	}()

	err := n.read(ctx)
	cancel()
	h.detach(n)
	if err != nil && ctx.Err() == nil {
		h.log.Info("agent log stream ended", "node_id", nodeID, "error", err)
	}
	return err
}

type recvResult struct {
	up  Up
	err error
}

func (n *nodeConn) read(ctx context.Context) error {
	results := make(chan recvResult)
	go func() {
		for {
			up, err := n.conn.Recv()
			select {
			case results <- recvResult{up, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r := <-results:
			if r.err != nil {
				if errors.Is(r.err, io.EOF) {
					return nil
				}
				return r.err
			}
			n.handle(r.up)
		}
	}
}

func (n *nodeConn) handle(up Up) {
	switch {
	case up.Batch != nil:
		if s := n.lookup(up.Batch.RequestID); s != nil {
			s.st.deliver(s, *up.Batch)
		}
	case up.End != nil:
		if s := n.lookup(up.End.RequestID); s != nil {
			s.st.deliverEnd(s, *up.End)
		}
	}
}

// detach forgets a finished stream and tells the queries that used it.
func (h *Hub) detach(n *nodeConn) {
	h.mu.Lock()
	if h.nodes[n.id] == n {
		delete(h.nodes, n.id)
	}
	h.mu.Unlock()
	for _, s := range n.drain() {
		s.st.nodeLost(s, n)
	}
}

// Open starts a query. It returns at once: lines arrive as the agents send
// them. Containers whose node is not connected are reported by a notice in
// the stream, not by an error. The caller must Close the stream; canceling ctx
// closes it too.
func (h *Hub) Open(ctx context.Context, q Query) (*Stream, error) {
	if len(q.Targets) == 0 || len(q.Targets) > MaxTargets {
		return nil, fmt.Errorf("%w: a query reads between 1 and %d containers", ErrBadQuery, MaxTargets)
	}
	seen := map[string]bool{}
	for _, t := range q.Targets {
		if t.NodeID == "" || t.WorkloadID == "" || len(t.NodeID) > MaxIDBytes || len(t.WorkloadID) > MaxIDBytes {
			return nil, fmt.Errorf("%w: a container needs a node and a workload of at most %d characters", ErrBadQuery, MaxIDBytes)
		}
		if seen[t.WorkloadID] {
			return nil, fmt.Errorf("%w: workload %s is listed twice", ErrBadQuery, t.WorkloadID)
		}
		seen[t.WorkloadID] = true
	}
	tail := min(max(q.Tail, 0), MaxTail)

	window := max(MinWindow, h.cfg.Window/len(q.Targets))
	lineBytes := min(h.cfg.LineBytes, window/2-LineOverhead)

	st := &Stream{hub: h, q: q, window: window, lineBytes: lineBytes, wake: make(chan struct{}, 1), closedCh: make(chan struct{})}
	st.ctx, st.cancel = context.WithCancel(context.Background())

	h.mu.Lock()
	switch {
	case h.closed:
		h.mu.Unlock()
		st.cancel()
		return nil, ErrClosed
	case len(h.streams) >= h.cfg.MaxStreams:
		h.mu.Unlock()
		st.cancel()
		return nil, ErrTooManyStreams
	}
	h.streams[st] = struct{}{}
	h.mu.Unlock()

	st.mu.Lock()
	for _, t := range q.Targets {
		after := q.After[t.WorkloadID]
		if after.IsZero() {
			after = q.Default
		}
		s := &source{st: st, target: t, histEnd: -1, tail0: tail, after0: after, began: time.Now()}
		st.sources = append(st.sources, s)
		node, _ := h.node(t.NodeID)
		switch {
		case node == nil:
			st.finishLocked(s, &Notice{Kind: NoticeUnavailable, Message: fmt.Sprintf("node %s is not connected to this manager, so its logs cannot be read", t.NodeID)})
		case !st.begin(s, node, tail, after):
			st.finishLocked(s, &Notice{Kind: NoticeUnavailable, Message: fmt.Sprintf("node %s cannot take the request now", t.NodeID)})
		}
	}
	st.mu.Unlock()

	stop := context.AfterFunc(ctx, st.Close)
	st.mu.Lock()
	st.stopAfter = stop
	closed := st.closed
	st.mu.Unlock()
	if closed {
		stop()
	}
	return st, nil
}
