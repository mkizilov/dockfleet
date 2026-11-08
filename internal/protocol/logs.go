package protocol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dockfleet/dockfleet/internal/logstream"
	dockfleetv1 "github.com/dockfleet/dockfleet/internal/protocol/dockfleet/v1"
	"github.com/dockfleet/dockfleet/internal/registry"
	"github.com/dockfleet/dockfleet/internal/store"
)

const (
	// defaultHelloWait is how long a new log stream may take to say who it is.
	defaultHelloWait = 10 * time.Second
	// defaultLogCheck is how often an open log stream is checked against its session.
	defaultLogCheck = 5 * time.Second
	// maxSessionID bounds the session token a hello may carry.
	maxSessionID = 128
)

// WithLogs makes the server accept the log streams of agents and hand them to
// hub. checkEvery is how often an open stream is checked against the session
// it was opened for (zero means 5 seconds): a stream whose session was
// replaced, ended or revoked is ended too.
func (s *NodeServer) WithLogs(hub *logstream.Hub, checkEvery time.Duration) *NodeServer {
	s.logs = hub
	if checkEvery <= 0 {
		checkEvery = defaultLogCheck
	}
	s.logCheck = checkEvery
	return s
}

// Logs is the agent's log stream. It is authenticated like every other call
// (by the interceptors) and bound to a session: the first message names the
// node and the session, the node must be the caller's identity, enrolled and
// holding that session, and the stream ends when the session does. Everything
// the agent sends is checked here, before the hub sees it.
func (s *NodeServer) Logs(stream dockfleetv1.NodeService_LogsServer) error {
	if s.logs == nil {
		return status.Error(codes.Unimplemented, "this manager does not serve container logs")
	}
	hello, err := s.readHello(stream)
	if err != nil {
		return err
	}
	ctx, nodeID, sessionID := stream.Context(), hello.GetNodeId(), hello.GetSessionId()
	if err := s.checkIdentity(ctx, nodeID); err != nil {
		return err
	}
	switch {
	case !store.ValidName(nodeID):
		return s.invalid("node_id must be 1-63 characters of [A-Za-z0-9._-] starting with a letter or digit")
	case sessionID == "" || len(sessionID) > maxSessionID:
		return s.invalid("session_id must be 1-128 characters")
	}
	if err := s.registry.CheckSession(ctx, nodeID, sessionID); err != nil {
		return s.sessionError(ctx, "open the log stream", nodeID, err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var ended atomic.Pointer[error]
	go s.watchSession(ctx, cancel, &ended, nodeID, sessionID)

	err = s.logs.Attach(ctx, nodeID, &serverLogs{stream: stream, invalid: s.invalid})
	switch {
	case ended.Load() != nil:
		return *ended.Load()
	case errors.Is(err, logstream.ErrClosed):
		return status.Error(codes.Unavailable, "the manager is shutting down")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "the log stream was replaced or canceled")
	}
	return err
}

// readHello reads the first message of a log stream, which must be the hello.
func (s *NodeServer) readHello(stream dockfleetv1.NodeService_LogsServer) (*dockfleetv1.LogsHello, error) {
	type result struct {
		msg *dockfleetv1.LogsRequest
		err error
	}
	first := make(chan result, 1)
	go func() {
		msg, err := stream.Recv()
		first <- result{msg, err}
	}()
	timer := time.NewTimer(s.helloWait)
	defer timer.Stop()
	select {
	case r := <-first:
		switch {
		case errors.Is(r.err, io.EOF):
			return nil, s.invalid("the log stream ended before its hello")
		case r.err != nil:
			return nil, r.err
		case r.msg.GetHello() == nil:
			return nil, s.invalid("the first message of a log stream must be the hello")
		}
		return r.msg.GetHello(), nil
	case <-timer.C:
		return nil, s.invalid("the log stream sent no hello in time")
	}
}

// watchSession ends the stream when its session is no longer the node's current
// one or the node is no longer enrolled. Errors of the check itself (the store
// being briefly unavailable) do not end it.
func (s *NodeServer) watchSession(ctx context.Context, cancel context.CancelFunc, ended *atomic.Pointer[error], nodeID, sessionID string) {
	tick := time.NewTicker(s.logCheck)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		err := s.registry.CheckSession(ctx, nodeID, sessionID)
		switch {
		case err == nil, ctx.Err() != nil:
		case errors.Is(err, registry.ErrNotEnrolled), errors.Is(err, registry.ErrNotRegistered), errors.Is(err, registry.ErrSessionSuperseded):
			reason := s.sessionError(ctx, "keep the log stream", nodeID, err)
			ended.Store(&reason)
			cancel()
			return
		default:
			s.logger.WarnContext(ctx, "checking the session of a log stream failed", "node_id", nodeID, "error", err)
		}
	}
}

// serverLogs is the manager's end of an agent's log stream as the hub sees it.
type serverLogs struct {
	stream  dockfleetv1.NodeService_LogsServer
	invalid func(string) error
}

func (c *serverLogs) Send(d logstream.Down) error {
	return c.stream.Send(downToProto(d))
}

func (c *serverLogs) Recv() (logstream.Up, error) {
	msg, err := c.stream.Recv()
	if err != nil {
		return logstream.Up{}, err
	}
	up, err := upFromProto(msg)
	if err != nil {
		return logstream.Up{}, c.invalid(err.Error())
	}
	return up, nil
}

// Logs opens the agent's log stream for its session and says who the agent is.
// The stream lives until ctx ends or either side closes it; the manager's
// requests arrive on it and the agent answers on it. A manager that refuses the
// stream (the session is not current, the node is not enrolled, or it does not
// serve logs) reports it in the error of Recv, or of this call.
func (c *Client) Logs(ctx context.Context, nodeID, sessionID string) (logstream.AgentConn, error) {
	stream, err := c.nodes.Logs(ctx)
	if err != nil {
		return nil, decodeError(err)
	}
	hello := &dockfleetv1.LogsRequest{Message: &dockfleetv1.LogsRequest_Hello{Hello: &dockfleetv1.LogsHello{NodeId: nodeID, SessionId: sessionID}}}
	if err := stream.Send(hello); err != nil {
		// A server that refused the stream says why on Recv; Send only says it is closed.
		if _, recvErr := stream.Recv(); recvErr != nil && !errors.Is(recvErr, io.EOF) {
			err = recvErr
		}
		return nil, decodeError(err)
	}
	return &agentLogs{stream: stream}, nil
}

type logsClientStream interface {
	Send(*dockfleetv1.LogsRequest) error
	Recv() (*dockfleetv1.LogsResponse, error)
}

// agentLogs is the agent's end of its log stream.
type agentLogs struct{ stream logsClientStream }

func (c *agentLogs) Send(u logstream.Up) error {
	msg, err := upToProto(u)
	if err != nil {
		return err
	}
	return decodeError(c.stream.Send(msg))
}

func (c *agentLogs) Recv() (logstream.Down, error) {
	msg, err := c.stream.Recv()
	if err != nil {
		return logstream.Down{}, decodeError(err)
	}
	return downFromProto(msg)
}

var endReasons = map[logstream.EndReason]dockfleetv1.LogEndReason{
	logstream.EndComplete: dockfleetv1.LogEndReason_LOG_END_REASON_COMPLETE,
	logstream.EndNotFound: dockfleetv1.LogEndReason_LOG_END_REASON_NOT_FOUND,
	logstream.EndCanceled: dockfleetv1.LogEndReason_LOG_END_REASON_CANCELED,
	logstream.EndBusy:     dockfleetv1.LogEndReason_LOG_END_REASON_BUSY,
	logstream.EndError:    dockfleetv1.LogEndReason_LOG_END_REASON_ERROR,
}

func reasonFromProto(r dockfleetv1.LogEndReason) (logstream.EndReason, bool) {
	for k, v := range endReasons {
		if v == r {
			return k, true
		}
	}
	return 0, false
}

func checkID(what, id string) error {
	if id == "" || len(id) > logstream.MaxIDBytes {
		return fmt.Errorf("%s must be 1-%d characters", what, logstream.MaxIDBytes)
	}
	return nil
}

// upFromProto checks and converts a message from an agent. Anything an honest
// agent would not send is an error, which ends the stream.
func upFromProto(msg *dockfleetv1.LogsRequest) (logstream.Up, error) {
	switch m := msg.GetMessage().(type) {
	case *dockfleetv1.LogsRequest_Batch:
		b := m.Batch
		if err := checkID("request_id", b.GetRequestId()); err != nil {
			return logstream.Up{}, err
		}
		if len(b.GetLines()) > logstream.MaxBatchLines {
			return logstream.Up{}, fmt.Errorf("a batch carries at most %d lines", logstream.MaxBatchLines)
		}
		out := logstream.Batch{RequestID: b.GetRequestId(), HistoryComplete: b.GetHistoryComplete(), Lines: make([]logstream.Line, 0, len(b.GetLines()))}
		size := 0
		for _, l := range b.GetLines() {
			var stream string
			switch l.GetStream() {
			case dockfleetv1.LogStream_LOG_STREAM_STDOUT:
				stream = logstream.StreamStdout
			case dockfleetv1.LogStream_LOG_STREAM_STDERR:
				stream = logstream.StreamStderr
			default:
				return logstream.Up{}, errors.New("a log line's stream must be stdout or stderr")
			}
			switch {
			case len(l.GetText()) > logstream.MaxLineBytes:
				return logstream.Up{}, fmt.Errorf("a log line carries at most %d bytes", logstream.MaxLineBytes)
			case l.GetTimeUnixNano() < 0:
				return logstream.Up{}, errors.New("a log line's time must not be before 1970")
			}
			size += len(l.GetText()) + logstream.LineOverhead
			out.Lines = append(out.Lines, logstream.Line{
				Time: time.Unix(0, l.GetTimeUnixNano()).UTC(), Stream: stream, Text: l.GetText(), Truncated: l.GetTruncated(),
			})
		}
		if size > logstream.MaxBatchBytes {
			return logstream.Up{}, fmt.Errorf("a batch carries at most %d bytes", logstream.MaxBatchBytes)
		}
		return logstream.Up{Batch: &out}, nil
	case *dockfleetv1.LogsRequest_End:
		e := m.End
		if err := checkID("request_id", e.GetRequestId()); err != nil {
			return logstream.Up{}, err
		}
		reason, ok := reasonFromProto(e.GetReason())
		if !ok {
			return logstream.Up{}, errors.New("an end needs a known reason")
		}
		return logstream.Up{End: &logstream.End{RequestID: e.GetRequestId(), Reason: reason, Message: e.GetMessage()}}, nil
	case *dockfleetv1.LogsRequest_Hello:
		return logstream.Up{}, errors.New("the hello may only be the first message")
	}
	return logstream.Up{}, errors.New("empty message on the log stream")
}

// upToProto converts what an agent sends. It refuses what the manager would
// refuse, so a bug in the agent shows as an error there and not as a dead stream.
func upToProto(u logstream.Up) (*dockfleetv1.LogsRequest, error) {
	switch {
	case u.Batch != nil:
		b := &dockfleetv1.LogBatch{RequestId: u.Batch.RequestID, HistoryComplete: u.Batch.HistoryComplete, Lines: make([]*dockfleetv1.LogLine, 0, len(u.Batch.Lines))}
		for _, l := range u.Batch.Lines {
			stream := dockfleetv1.LogStream_LOG_STREAM_STDOUT
			if l.Stream == logstream.StreamStderr {
				stream = dockfleetv1.LogStream_LOG_STREAM_STDERR
			}
			b.Lines = append(b.Lines, &dockfleetv1.LogLine{TimeUnixNano: l.Time.UnixNano(), Stream: stream, Text: l.Text, Truncated: l.Truncated})
		}
		return &dockfleetv1.LogsRequest{Message: &dockfleetv1.LogsRequest_Batch{Batch: b}}, nil
	case u.End != nil:
		reason, ok := endReasons[u.End.Reason]
		if !ok {
			return nil, errors.New("an end needs a known reason")
		}
		e := &dockfleetv1.LogEnd{RequestId: u.End.RequestID, Reason: reason, Message: u.End.Message}
		return &dockfleetv1.LogsRequest{Message: &dockfleetv1.LogsRequest_End{End: e}}, nil
	}
	return nil, errors.New("empty message for the log stream")
}

func clampUint32(n int) uint32 {
	switch {
	case n <= 0:
		return 0
	case int64(n) > math.MaxUint32:
		return math.MaxUint32
	}
	return uint32(n)
}

// downToProto converts what the manager sends an agent.
func downToProto(d logstream.Down) *dockfleetv1.LogsResponse {
	switch {
	case d.Open != nil:
		o := &dockfleetv1.LogOpen{
			RequestId: d.Open.RequestID, WorkloadId: d.Open.WorkloadID, Tail: int32(max(min(d.Open.Tail, math.MaxInt32), math.MinInt32)),
			Follow: d.Open.Follow, MaxLineBytes: clampUint32(d.Open.MaxLineBytes), CreditBytes: clampUint32(d.Open.Credit),
		}
		if !d.Open.After.IsZero() {
			o.AfterUnixNano = d.Open.After.UnixNano()
		}
		return &dockfleetv1.LogsResponse{Message: &dockfleetv1.LogsResponse_Open{Open: o}}
	case d.Cancel != nil:
		return &dockfleetv1.LogsResponse{Message: &dockfleetv1.LogsResponse_Cancel{Cancel: &dockfleetv1.LogCancel{RequestId: d.Cancel.RequestID}}}
	case d.Credit != nil:
		return &dockfleetv1.LogsResponse{Message: &dockfleetv1.LogsResponse_Credit{Credit: &dockfleetv1.LogCredit{RequestId: d.Credit.RequestID, Bytes: clampUint32(d.Credit.Bytes)}}}
	}
	return &dockfleetv1.LogsResponse{}
}

// downFromProto checks and converts a message from the manager.
func downFromProto(msg *dockfleetv1.LogsResponse) (logstream.Down, error) {
	switch m := msg.GetMessage().(type) {
	case *dockfleetv1.LogsResponse_Open:
		o := m.Open
		if err := checkID("request_id", o.GetRequestId()); err != nil {
			return logstream.Down{}, err
		}
		if err := checkID("workload_id", o.GetWorkloadId()); err != nil {
			return logstream.Down{}, err
		}
		open := logstream.Open{
			RequestID: o.GetRequestId(), WorkloadID: o.GetWorkloadId(), Tail: int(o.GetTail()), Follow: o.GetFollow(),
			MaxLineBytes: int(o.GetMaxLineBytes()), Credit: int(o.GetCreditBytes()),
		}
		if o.GetAfterUnixNano() > 0 {
			open.After = time.Unix(0, o.GetAfterUnixNano()).UTC()
		}
		return logstream.Down{Open: &open}, nil
	case *dockfleetv1.LogsResponse_Cancel:
		if err := checkID("request_id", m.Cancel.GetRequestId()); err != nil {
			return logstream.Down{}, err
		}
		return logstream.Down{Cancel: &logstream.Cancel{RequestID: m.Cancel.GetRequestId()}}, nil
	case *dockfleetv1.LogsResponse_Credit:
		if err := checkID("request_id", m.Credit.GetRequestId()); err != nil {
			return logstream.Down{}, err
		}
		return logstream.Down{Credit: &logstream.Credit{RequestID: m.Credit.GetRequestId(), Bytes: int(m.Credit.GetBytes())}}, nil
	}
	return logstream.Down{}, errors.New("empty message from the manager on the log stream")
}
