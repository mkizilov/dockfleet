package protocol

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/dockfleet/dockfleet/internal/logstream"
	"github.com/dockfleet/dockfleet/internal/pki"
	dockfleetv1 "github.com/dockfleet/dockfleet/internal/protocol/dockfleet/v1"
)

var logEpoch = time.Date(2026, 3, 1, 9, 0, 0, 123456789, time.UTC)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// testCerts issues a valid node certificate for worker-1, an expired one, and a
// server certificate that carries no node identity.
func testCerts(t *testing.T, now time.Time) (valid, expired, serverOnly *x509.Certificate) {
	t.Helper()
	ca, err := pki.NewCA("Test CA", now.Add(-time.Hour), now.Add(100*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	issue := func(notBefore, notAfter time.Time) *x509.Certificate {
		issued, err := ca.IssueNode(pki.NodeRequest{NodeID: "worker-1", NotBefore: notBefore, NotAfter: notAfter})
		if err != nil {
			t.Fatal(err)
		}
		return issued.Cert
	}
	server, err := ca.IssueServer(pki.ServerRequest{Name: "m", DNSNames: []string{"m"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	return issue(now.Add(-time.Minute), now.Add(time.Hour)), issue(now.Add(-3*time.Hour), now.Add(-time.Hour)), server.Cert
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// fakeLogsStream is the server's end of a log stream, driven by the test.
type fakeLogsStream struct {
	grpc.ServerStream
	ctx  context.Context
	in   chan *dockfleetv1.LogsRequest
	sent chan *dockfleetv1.LogsResponse
}

func newFakeLogsStream(ctx context.Context) *fakeLogsStream {
	return &fakeLogsStream{ctx: ctx, in: make(chan *dockfleetv1.LogsRequest, 16), sent: make(chan *dockfleetv1.LogsResponse, 16)}
}

func (f *fakeLogsStream) Context() context.Context { return f.ctx }

func (f *fakeLogsStream) Recv() (*dockfleetv1.LogsRequest, error) {
	select {
	case m, ok := <-f.in:
		if !ok {
			return nil, io.EOF
		}
		return m, nil
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	}
}

func (f *fakeLogsStream) Send(m *dockfleetv1.LogsResponse) error {
	f.sent <- m
	return nil
}

func hello(node, session string) *dockfleetv1.LogsRequest {
	return &dockfleetv1.LogsRequest{Message: &dockfleetv1.LogsRequest_Hello{Hello: &dockfleetv1.LogsHello{NodeId: node, SessionId: session}}}
}

func batchMsg(id string, lines ...*dockfleetv1.LogLine) *dockfleetv1.LogsRequest {
	return &dockfleetv1.LogsRequest{Message: &dockfleetv1.LogsRequest_Batch{Batch: &dockfleetv1.LogBatch{RequestId: id, Lines: lines}}}
}

func endMsg(id string, reason dockfleetv1.LogEndReason) *dockfleetv1.LogsRequest {
	return &dockfleetv1.LogsRequest{Message: &dockfleetv1.LogsRequest_End{End: &dockfleetv1.LogEnd{RequestId: id, Reason: reason}}}
}

func stdoutLine(text string) *dockfleetv1.LogLine {
	return &dockfleetv1.LogLine{TimeUnixNano: logEpoch.UnixNano(), Stream: dockfleetv1.LogStream_LOG_STREAM_STDOUT, Text: text}
}

func TestLogMessagesRoundTrip(t *testing.T) {
	up := logstream.Up{Batch: &logstream.Batch{RequestID: "r1", HistoryComplete: true, Lines: []logstream.Line{
		{Time: logEpoch, Stream: logstream.StreamStdout, Text: "out"},
		{Time: logEpoch.Add(time.Nanosecond), Stream: logstream.StreamStderr, Text: "err", Truncated: true},
	}}}
	msg, err := upToProto(up)
	if err != nil {
		t.Fatal(err)
	}
	got, err := upFromProto(msg)
	if err != nil {
		t.Fatal(err)
	}
	b := got.Batch
	if b == nil || b.RequestID != "r1" || !b.HistoryComplete || len(b.Lines) != 2 ||
		!b.Lines[0].Time.Equal(logEpoch) || b.Lines[0].Stream != "stdout" || b.Lines[0].Text != "out" || b.Lines[0].Truncated ||
		!b.Lines[1].Time.Equal(logEpoch.Add(time.Nanosecond)) || b.Lines[1].Stream != "stderr" || !b.Lines[1].Truncated {
		t.Fatalf("batch = %+v", b)
	}

	for _, reason := range []logstream.EndReason{logstream.EndComplete, logstream.EndNotFound, logstream.EndCanceled, logstream.EndBusy, logstream.EndError} {
		msg, err := upToProto(logstream.Up{End: &logstream.End{RequestID: "r2", Reason: reason, Message: "why"}})
		if err != nil {
			t.Fatal(err)
		}
		got, err := upFromProto(msg)
		if err != nil || got.End == nil || got.End.Reason != reason || got.End.RequestID != "r2" || got.End.Message != "why" {
			t.Fatalf("end %v = %+v, %v", reason, got.End, err)
		}
	}

	after := logEpoch.Add(-time.Hour)
	downs := []logstream.Down{
		{Open: &logstream.Open{RequestID: "r1", WorkloadID: "w1", Tail: -1, After: after, Follow: true, MaxLineBytes: 4096, Credit: 65536}},
		{Open: &logstream.Open{RequestID: "r2", WorkloadID: "w2", Tail: 50}},
		{Cancel: &logstream.Cancel{RequestID: "r1"}},
		{Credit: &logstream.Credit{RequestID: "r1", Bytes: 1024}},
	}
	for _, d := range downs {
		got, err := downFromProto(downToProto(d))
		if err != nil {
			t.Fatalf("%+v: %v", d, err)
		}
		switch {
		case d.Open != nil:
			if got.Open == nil || got.Open.RequestID != d.Open.RequestID || got.Open.WorkloadID != d.Open.WorkloadID || got.Open.Tail != d.Open.Tail ||
				!got.Open.After.Equal(d.Open.After) || got.Open.Follow != d.Open.Follow || got.Open.MaxLineBytes != d.Open.MaxLineBytes || got.Open.Credit != d.Open.Credit {
				t.Fatalf("open = %+v, want %+v", got.Open, d.Open)
			}
		case d.Cancel != nil:
			if got.Cancel == nil || got.Cancel.RequestID != "r1" {
				t.Fatalf("cancel = %+v", got.Cancel)
			}
		case d.Credit != nil:
			if got.Credit == nil || got.Credit.RequestID != "r1" || got.Credit.Bytes != 1024 {
				t.Fatalf("credit = %+v", got.Credit)
			}
		}
	}
	// A zero After is "no bound", not the year 1.
	got2, _ := downFromProto(downToProto(logstream.Down{Open: &logstream.Open{RequestID: "r", WorkloadID: "w"}}))
	if !got2.Open.After.IsZero() {
		t.Errorf("an open without a time came back with After = %v", got2.Open.After)
	}
}

func TestInvalidMessagesFromAnAgentAreRefused(t *testing.T) {
	long := strings.Repeat("x", logstream.MaxIDBytes+1)
	manyLines := make([]*dockfleetv1.LogLine, logstream.MaxBatchLines+1)
	for i := range manyLines {
		manyLines[i] = stdoutLine("x")
	}
	var bulky []*dockfleetv1.LogLine // under the line limit each, over the byte limit together
	for i := 0; i < logstream.MaxBatchLines; i++ {
		bulky = append(bulky, stdoutLine(strings.Repeat("y", 1100)))
	}
	tests := []struct {
		name string
		msg  *dockfleetv1.LogsRequest
	}{
		{"an empty message", &dockfleetv1.LogsRequest{}},
		{"a second hello", hello("n", "s")},
		{"a batch without an ID", batchMsg("", stdoutLine("x"))},
		{"a batch with a long ID", batchMsg(long, stdoutLine("x"))},
		{"too many lines", batchMsg("r", manyLines...)},
		{"too many bytes", batchMsg("r", bulky...)},
		{"a line that is too long", batchMsg("r", stdoutLine(strings.Repeat("z", logstream.MaxLineBytes+1)))},
		{"a line with no stream", batchMsg("r", &dockfleetv1.LogLine{TimeUnixNano: 1, Text: "x"})},
		{"a line from before 1970", batchMsg("r", &dockfleetv1.LogLine{TimeUnixNano: -1, Stream: dockfleetv1.LogStream_LOG_STREAM_STDOUT})},
		{"an end without a reason", endMsg("r", 0)},
		{"an end with an unknown reason", endMsg("r", 99)},
		{"an end without an ID", endMsg("", dockfleetv1.LogEndReason_LOG_END_REASON_COMPLETE)},
	}
	for _, tt := range tests {
		if _, err := upFromProto(tt.msg); err == nil {
			t.Errorf("%s was accepted", tt.name)
		}
	}
	if _, err := upFromProto(batchMsg("r")); err != nil {
		t.Errorf("an empty batch is how the history is marked complete: %v", err)
	}

	// And what the manager sends is checked by the agent.
	openMsg := func(id, workload string) *dockfleetv1.LogsResponse {
		return &dockfleetv1.LogsResponse{Message: &dockfleetv1.LogsResponse_Open{Open: &dockfleetv1.LogOpen{RequestId: id, WorkloadId: workload}}}
	}
	badDown := []struct {
		name string
		msg  *dockfleetv1.LogsResponse
	}{
		{"an empty message", &dockfleetv1.LogsResponse{}},
		{"an open without an ID", openMsg("", "w")},
		{"an open without a workload", openMsg("r", "")},
		{"an open with a long workload", openMsg("r", long)},
		{"a cancel without an ID", &dockfleetv1.LogsResponse{Message: &dockfleetv1.LogsResponse_Cancel{Cancel: &dockfleetv1.LogCancel{}}}},
		{"a credit without an ID", &dockfleetv1.LogsResponse{Message: &dockfleetv1.LogsResponse_Credit{Credit: &dockfleetv1.LogCredit{Bytes: 1}}}},
	}
	for _, tt := range badDown {
		if _, err := downFromProto(tt.msg); err == nil {
			t.Errorf("%s was accepted", tt.name)
		}
	}
	if _, err := upToProto(logstream.Up{}); err == nil {
		t.Error("an empty message was sent")
	}
	if _, err := upToProto(logstream.Up{End: &logstream.End{RequestID: "r"}}); err == nil {
		t.Error("an end without a reason was sent")
	}
}

type recordedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (r recordedStream) Context() context.Context { return r.ctx }

func TestStreamInterceptorsAuthenticateLikeUnaryOnes(t *testing.T) {
	token := StreamTokenInterceptor("secret-token-0123456789")
	called := 0
	handler := func(any, grpc.ServerStream) error { called++; return nil }
	withAuth := func(values ...string) context.Context {
		return metadata.NewIncomingContext(context.Background(), metadata.MD{"authorization": values})
	}
	for _, tt := range []struct {
		name string
		ctx  context.Context
	}{
		{"no credentials", context.Background()},
		{"the wrong token", withAuth("Bearer other-token-0123456789")},
		{"no bearer prefix", withAuth("secret-token-0123456789")},
		{"two credentials", withAuth("Bearer secret-token-0123456789", "Bearer secret-token-0123456789")},
		{"an empty password", withAuth("")},
	} {
		if err := token(nil, recordedStream{ctx: tt.ctx}, &grpc.StreamServerInfo{}, handler); status.Code(err) != codes.Unauthenticated {
			t.Errorf("%s: error = %v, want Unauthenticated", tt.name, err)
		}
	}
	if called != 0 {
		t.Fatalf("the handler ran %d times for unauthenticated streams", called)
	}
	if err := token(nil, recordedStream{ctx: withAuth("Bearer secret-token-0123456789")}, &grpc.StreamServerInfo{}, handler); err != nil || called != 1 {
		t.Fatalf("the right token: error = %v, handler ran %d times", err, called)
	}
}

func TestStreamIdentityInterceptorBindsTheCertificateIdentity(t *testing.T) {
	now := time.Now()
	valid, expired, serverOnly := testCerts(t, now)
	var seen string
	handler := func(_ any, ss grpc.ServerStream) error {
		seen, _ = identityFromContext(ss.Context())
		return nil
	}
	intercept := StreamIdentityInterceptor(func() time.Time { return now })
	call := func(ctx context.Context) error {
		seen = ""
		return intercept(nil, recordedStream{ctx: ctx}, &grpc.StreamServerInfo{}, handler)
	}
	if err := call(peerContext(valid, true)); err != nil || seen != "worker-1" {
		t.Fatalf("valid certificate: err=%v identity=%q", err, seen)
	}
	for _, tt := range []struct {
		name string
		ctx  context.Context
	}{
		{"an expired certificate", peerContext(expired, true)},
		{"no node identity", peerContext(serverOnly, true)},
		{"an unverified chain", peerContext(valid, false)},
		{"no peer information", context.Background()},
	} {
		if err := call(tt.ctx); status.Code(err) != codes.Unauthenticated || seen != "" {
			t.Errorf("%s: err=%v identity=%q; want Unauthenticated and no handler call", tt.name, err, seen)
		}
	}
}

func TestLogsRefusesAnotherNodesIdentity(t *testing.T) {
	logger := discardLogger()
	node := NewNodeServer(nil, time.Second, logger).WithLogs(logstream.NewHub(logstream.HubConfig{}), 0)
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), identityKey{}, "worker-1"))
	defer cancel()
	stream := newFakeLogsStream(ctx)
	stream.in <- hello("worker-2", "session")
	err := node.Logs(stream)
	if status.Code(err) != codes.PermissionDenied || !IsIdentityMismatch(decodeError(err)) {
		t.Fatalf("Logs error = %v, want PERMISSION_DENIED / IDENTITY_MISMATCH", err)
	}
}

func TestLogsNeedsAHelloFirst(t *testing.T) {
	node := NewNodeServer(nil, time.Second, discardLogger()).WithLogs(logstream.NewHub(logstream.HubConfig{}), 0)
	node.helloWait = 50 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tests := []struct {
		name string
		feed func(*fakeLogsStream)
	}{
		{"data instead of a hello", func(s *fakeLogsStream) { s.in <- batchMsg("r", stdoutLine("x")) }},
		{"nothing at all", func(*fakeLogsStream) {}},
		{"the stream closing", func(s *fakeLogsStream) { close(s.in) }},
		{"a bad node name", func(s *fakeLogsStream) { s.in <- hello("../etc", "session") }},
		{"no session", func(s *fakeLogsStream) { s.in <- hello("node-1", "") }},
	}
	for _, tt := range tests {
		stream := newFakeLogsStream(ctx)
		tt.feed(stream)
		err := node.Logs(stream)
		if status.Code(err) != codes.InvalidArgument || !IsPermanent(decodeError(err)) {
			t.Errorf("%s: error = %v, want a permanent INVALID_REQUEST", tt.name, err)
		}
	}
}

func TestLogsIsOffWithoutAHub(t *testing.T) {
	env := newEnv(t, nil)
	c := env.client(t, serverToken, nil)
	ctx := ctxWithTimeout(t)
	session, err := c.Register(ctx, testNode())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := c.Logs(ctx, "node-1", session.ID)
	if err == nil {
		_, err = conn.Recv()
	}
	if !IsUnimplemented(err) {
		t.Fatalf("error = %v, want Unimplemented", err)
	}
	if IsUnimplemented(errors.New("x")) || IsUnimplemented(nil) {
		t.Error("IsUnimplemented is true for what is not")
	}
}

// openLogs registers node-1 and opens its log stream on a server with a hub.
func openLogs(t *testing.T, check time.Duration) (*logstream.Hub, *testEnv, *Client, Session, logstream.AgentConn, context.CancelFunc) {
	t.Helper()
	hub := logstream.NewHub(logstream.HubConfig{})
	t.Cleanup(hub.Shutdown)
	env := newEnv(t, func(s *NodeServer) { s.WithLogs(hub, check) })
	c := env.client(t, serverToken, nil)
	session, err := c.Register(ctxWithTimeout(t), testNode())
	if err != nil {
		t.Fatal(err)
	}
	// The stream is bound to this context, which also keeps a stalled test from hanging.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	conn, err := c.Logs(ctx, "node-1", session.ID)
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	waitUntil(t, "the log stream to attach", func() bool { return hub.Connected("node-1") })
	return hub, env, c, session, conn, cancel
}

func TestLogsStreamCarriesRequestsAndLines(t *testing.T) {
	hub, _, _, _, conn, cancel := openLogs(t, 20*time.Millisecond)
	st, err := hub.Open(context.Background(), logstream.Query{
		Targets: []logstream.Target{{NodeID: "node-1", WorkloadID: "w1"}}, Tail: 7, Follow: true, Default: logEpoch.Add(-time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	down, err := conn.Recv()
	if err != nil || down.Open == nil {
		t.Fatalf("Recv = %+v, %v; want an open", down, err)
	}
	o := down.Open
	if o.WorkloadID != "w1" || o.Tail != 7 || !o.Follow || !o.After.Equal(logEpoch.Add(-time.Hour)) || o.Credit < logstream.MinWindow || o.MaxLineBytes <= 0 || o.RequestID == "" {
		t.Fatalf("open = %+v", o)
	}

	err = conn.Send(logstream.Up{Batch: &logstream.Batch{RequestID: o.RequestID, HistoryComplete: true, Lines: []logstream.Line{
		{Time: logEpoch, Stream: logstream.StreamStdout, Text: "first"},
		{Time: logEpoch.Add(time.Millisecond), Stream: logstream.StreamStderr, Text: "second", Truncated: true},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := ctxWithTimeout(t)
	for _, want := range []struct {
		text, stream string
		at           time.Time
		truncated    bool
	}{{"first", "stdout", logEpoch, false}, {"second", "stderr", logEpoch.Add(time.Millisecond), true}} {
		ev, err := st.Next(ctx)
		if err != nil || ev.Entry == nil || ev.Entry.Text != want.text || ev.Entry.Stream != want.stream || !ev.Entry.Time.Equal(want.at) ||
			ev.Entry.Truncated != want.truncated || ev.Entry.WorkloadID != "w1" || ev.Entry.NodeID != "node-1" {
			t.Fatalf("event = %+v, %v; want %+v", ev.Entry, err, want)
		}
	}

	// Taking the lines out returns credit; closing the query cancels the request.
	st.Close()
	var sawCancel bool
	for i := 0; i < 3 && !sawCancel; i++ {
		d, err := conn.Recv()
		if err != nil {
			t.Fatal(err)
		}
		sawCancel = d.Cancel != nil && d.Cancel.RequestID == o.RequestID
	}
	if !sawCancel {
		t.Fatal("closing the query did not cancel the request on the agent")
	}

	// The agent going away detaches it.
	cancel()
	waitUntil(t, "the stream to detach", func() bool { return !hub.Connected("node-1") })
}

func TestLogsRefusesWhatIsNotTheCurrentSession(t *testing.T) {
	hub := logstream.NewHub(logstream.HubConfig{})
	t.Cleanup(hub.Shutdown)
	env := newEnv(t, func(s *NodeServer) { s.WithLogs(hub, time.Hour) })
	c := env.client(t, serverToken, nil)
	session, err := c.Register(ctxWithTimeout(t), testNode())
	if err != nil {
		t.Fatal(err)
	}
	first := func(node, sess string) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, err := c.Logs(ctx, node, sess)
		if err == nil {
			_, err = conn.Recv()
		}
		return err
	}
	if err := first("node-1", "not-a-session"); ErrorCode(err) != dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_SESSION_SUPERSEDED {
		t.Errorf("a wrong session: error = %v, want SESSION_SUPERSEDED", err)
	}
	if err := first("node-2", session.ID); ErrorCode(err) != dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_REGISTERED {
		t.Errorf("a node that never registered: error = %v, want NOT_REGISTERED", err)
	}
	if err := first("not a name", session.ID); ErrorCode(err) != dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_INVALID_REQUEST {
		t.Errorf("a malformed node ID: error = %v, want INVALID_REQUEST", err)
	}
	// A newer registration makes the older session useless for logs too.
	newer, err := c.Register(ctxWithTimeout(t), testNode())
	if err != nil {
		t.Fatal(err)
	}
	if err := first("node-1", session.ID); ErrorCode(err) != dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_SESSION_SUPERSEDED {
		t.Errorf("a superseded session: error = %v, want SESSION_SUPERSEDED", err)
	}
	if hub.Connected("node-1") {
		t.Fatal("a refused stream was attached")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := c.Logs(ctx, "node-1", newer.ID); err != nil {
		t.Fatalf("the current session was refused: %v", err)
	}
	waitUntil(t, "the stream to attach", func() bool { return hub.Connected("node-1") })
}

func TestLogsRequiresTheAgentCredentials(t *testing.T) {
	hub := logstream.NewHub(logstream.HubConfig{})
	t.Cleanup(hub.Shutdown)
	env := newEnv(t, func(s *NodeServer) { s.WithLogs(hub, time.Hour) })
	for _, tt := range []struct{ name, token string }{{"no token", ""}, {"a wrong token", "not-the-agent-token-123"}} {
		name := tt.name
		c := env.client(t, tt.token, nil)
		ctx, cancel := context.WithCancel(context.Background())
		conn, err := c.Logs(ctx, "node-1", "session")
		if err == nil {
			_, err = conn.Recv()
		}
		cancel()
		if status.Code(err) != codes.Unauthenticated || !IsPermanent(err) {
			t.Errorf("%s: error = %v, want a permanent Unauthenticated", name, err)
		}
	}
	if hub.Connected("node-1") {
		t.Fatal("an unauthenticated stream was attached")
	}
}

func TestLogsEndsWhenTheSessionEnds(t *testing.T) {
	t.Run("deregistered", func(t *testing.T) {
		hub, _, c, session, conn, _ := openLogs(t, 10*time.Millisecond)
		if err := c.Deregister(ctxWithTimeout(t), "node-1", session.ID); err != nil {
			t.Fatal(err)
		}
		_, err := conn.Recv()
		if ErrorCode(err) != dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_REGISTERED {
			t.Fatalf("Recv = %v, want NOT_REGISTERED", err)
		}
		waitUntil(t, "the stream to detach", func() bool { return !hub.Connected("node-1") })
	})
	t.Run("superseded by a new registration", func(t *testing.T) {
		_, _, c, _, conn, _ := openLogs(t, 10*time.Millisecond)
		if _, err := c.Register(ctxWithTimeout(t), testNode()); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Recv(); ErrorCode(err) != dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_SESSION_SUPERSEDED {
			t.Fatalf("Recv = %v, want SESSION_SUPERSEDED", err)
		}
	})
	t.Run("enrollment revoked", func(t *testing.T) {
		hub, env, _, _, conn, _ := openLogs(t, 10*time.Millisecond)
		if _, err := env.store.RevokeEnrollment(context.Background(), "node-1", time.Now()); err != nil {
			t.Fatal(err)
		}
		_, err := conn.Recv()
		if !IsNotEnrolled(err) || status.Code(err) != codes.PermissionDenied {
			t.Fatalf("Recv = %v, want PERMISSION_DENIED / NOT_ENROLLED", err)
		}
		waitUntil(t, "the stream to detach", func() bool { return !hub.Connected("node-1") })
	})
}

func TestLogsEndsOnAnInvalidMessageFromTheAgent(t *testing.T) {
	_, _, c, session, _, _ := openLogs(t, time.Hour)
	// A second, raw stream (this one replaces the first on the hub) that sends nonsense.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	raw, err := c.nodes.Logs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Send(hello("node-1", session.ID)); err != nil {
		t.Fatal(err)
	}
	if err := raw.Send(batchMsg("r", &dockfleetv1.LogLine{Text: "no stream"})); err != nil {
		t.Fatal(err)
	}
	_, err = raw.Recv()
	if status.Code(err) != codes.InvalidArgument || !IsPermanent(decodeError(err)) {
		t.Fatalf("Recv = %v, want a permanent INVALID_REQUEST", err)
	}
}

func TestLogsNoHelloEndsTheStream(t *testing.T) {
	hub := logstream.NewHub(logstream.HubConfig{})
	t.Cleanup(hub.Shutdown)
	env := newEnv(t, func(s *NodeServer) { s.WithLogs(hub, time.Hour); s.helloWait = 50 * time.Millisecond })
	c := env.client(t, serverToken, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	raw, err := c.nodes.Logs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The client says nothing; the server gives up on its own.
	if _, err := raw.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Recv = %v, want InvalidArgument", err)
	}
}

func TestVersionAdvertisesLogs(t *testing.T) {
	if VersionMajor != 1 || VersionMinor < 4 {
		t.Fatalf("version = %d.%d; the log stream is protocol 1.4", VersionMajor, VersionMinor)
	}
}
