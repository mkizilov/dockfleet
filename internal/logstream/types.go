// Package logstream carries container logs from the agents to the manager and
// on to whoever asked for them.
//
// An agent opens one long-lived stream to the manager for its session (the Logs
// RPC) and serves the log requests the manager sends down it. The manager side
// is a Hub: it holds the agents' streams, opens requests on them for the
// containers a query names, merges what comes back and hands it to the reader
// (the administrative API) as one Stream of events.
//
// Memory is bounded by credit, not by luck. A request starts with a window of
// credit; the agent may send lines only as long as it has credit, and the Hub
// grants more only as the reader takes lines out. A reader that stops reading
// therefore stops the agent, which stops reading the container's output: the
// lines stay in Docker's own log files, and neither side buffers more than one
// window per request. See docs/logs.md.
//
// The package has no I/O of its own and knows nothing of gRPC, Docker or HTTP:
// the Conn interfaces are implemented by internal/protocol and used by
// internal/agent and internal/api.
package logstream

import (
	"errors"
	"time"
)

// The two output streams of a container.
const (
	StreamStdout = "stdout"
	StreamStderr = "stderr"
)

// Limits that both sides apply.
const (
	// LineOverhead is what a line costs in flow-control credit on top of its
	// text, so that many empty lines are not free.
	LineOverhead = 64
	// MaxLineBytes is the longest line text any side accepts; a longer line is cut.
	MaxLineBytes = 64 << 10
	// DefaultLineBytes is the line length used when a request names none.
	DefaultLineBytes = 16 << 10
	// MaxBatchLines and MaxBatchBytes bound one message from an agent.
	MaxBatchLines = 256
	MaxBatchBytes = 256 << 10
	// MaxTail bounds how many lines of history a request may ask for from one container.
	MaxTail = 5000
	// MaxTargets bounds how many containers one query may read.
	MaxTargets = 32
	// MinWindow is the smallest credit window of a request: it must hold the
	// longest line the request allows.
	MinWindow = 2 * (DefaultLineBytes + LineOverhead)
	// MaxIDBytes bounds request and workload identifiers on the wire.
	MaxIDBytes = 64
)

// ErrClosed is returned by a Stream that was closed, and by a Hub that shut down.
var ErrClosed = errors.New("log stream closed")

// Line is one line of container output.
type Line struct {
	Time      time.Time
	Stream    string // StreamStdout or StreamStderr
	Text      string // without the line break; valid UTF-8
	Truncated bool   // the line was longer than the limit and was cut
}

// Size is what the line costs in flow-control credit.
func (l Line) Size() int { return len(l.Text) + LineOverhead }

// EndReason says why an agent stopped serving a request.
type EndReason int

const (
	// EndComplete: there is nothing more to send. The history of a request that
	// does not follow is all there is, or the container stopped or was removed.
	EndComplete EndReason = iota + 1
	// EndNotFound: the node has no container for the workload.
	EndNotFound
	// EndCanceled: the manager canceled the request.
	EndCanceled
	// EndBusy: the agent serves as many requests as it allows.
	EndBusy
	// EndError: reading the container's logs failed.
	EndError
)

func (r EndReason) String() string {
	switch r {
	case EndComplete:
		return "complete"
	case EndNotFound:
		return "not_found"
	case EndCanceled:
		return "canceled"
	case EndBusy:
		return "busy"
	case EndError:
		return "error"
	}
	return "unknown"
}

// Batch is lines of one request, oldest first. HistoryComplete marks the last
// batch of the history; it may be empty.
type Batch struct {
	RequestID       string
	Lines           []Line
	HistoryComplete bool
}

// Bytes is the credit the batch costs.
func (b Batch) Bytes() int {
	n := 0
	for _, l := range b.Lines {
		n += l.Size()
	}
	return n
}

// End closes a request.
type End struct {
	RequestID string
	Reason    EndReason
	Message   string
}

// Up is one message from an agent to the manager: exactly one field is set.
type Up struct {
	Batch *Batch
	End   *End
}

// Open asks an agent for the logs of one workload's container.
type Open struct {
	RequestID  string
	WorkloadID string
	// Tail is the number of lines of history after After; negative means all of
	// them, up to what the agent allows.
	Tail int
	// After excludes lines at or before this time; zero means no bound.
	After        time.Time
	Follow       bool
	MaxLineBytes int
	// Credit is the initial window in bytes.
	Credit int
}

// Cancel ends a request.
type Cancel struct{ RequestID string }

// Credit lets the agent send Bytes more of a request.
type Credit struct {
	RequestID string
	Bytes     int
}

// Down is one message from the manager to an agent: exactly one field is set.
type Down struct {
	Open   *Open
	Cancel *Cancel
	Credit *Credit
}

// Conn is the manager's end of one agent's log stream. Send must not be
// called from several goroutines at once; the Hub serializes it.
type Conn interface {
	Send(Down) error
	Recv() (Up, error)
}

// AgentConn is an agent's end of its log stream. Send must not be called from
// several goroutines at once; the agent serializes it.
type AgentConn interface {
	Recv() (Down, error)
	Send(Up) error
}

// Target names one container to read: a workload on the node it is placed on.
type Target struct {
	NodeID     string
	WorkloadID string
}

// Query is a request for logs, answered by a Stream.
type Query struct {
	Targets []Target
	// Tail is how many lines of history to start with, per container.
	Tail int
	// After is the cursor to resume from: for a container, only lines after its
	// time are sent. A container with no entry uses Default (zero: no bound).
	After   map[string]time.Time
	Default time.Time
	// Follow keeps the stream open for new lines until the containers stop.
	Follow bool
}

// NoticeKind classifies a Notice.
type NoticeKind string

const (
	// NoticeUnavailable: a container's logs cannot be read now (its node is not
	// connected, the node has no container for it, or the node is busy).
	NoticeUnavailable NoticeKind = "unavailable"
	// NoticeInterrupted: the connection to a node was lost; the Hub waits for it.
	NoticeInterrupted NoticeKind = "interrupted"
	// NoticeResumed: the node came back and the container is followed again.
	NoticeResumed NoticeKind = "resumed"
	// NoticeEnded: no more lines will come from a container.
	NoticeEnded NoticeKind = "ended"
	// NoticeError: reading a container's logs failed.
	NoticeError NoticeKind = "error"
)

// Notice says something about one container's part of a stream.
type Notice struct {
	WorkloadID string
	NodeID     string
	Kind       NoticeKind
	Message    string
}

// Entry is a line with the container it came from.
type Entry struct {
	WorkloadID string
	NodeID     string
	Line
}

// Event is one thing a Stream reports: an entry or a notice.
type Event struct {
	Entry  *Entry
	Notice *Notice
}
