package docker

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

var streamBase = time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

// stamped is a line the way Docker writes it with timestamps on: the time, a
// space, the text, and the line break.
func stamped(offset time.Duration, text string) string {
	return streamBase.Add(offset).Format("2006-01-02T15:04:05.000000000Z07:00") + " " + text
}

func readerOver(frames ...[]byte) *logReader {
	return newLogReader(io.NopCloser(bytes.NewReader(bytes.Join(frames, nil))), func() {}, 1024)
}

func drain(t *testing.T, r LogReader) ([]LogLine, error) {
	t.Helper()
	var out []LogLine
	for {
		l, err := r.Next()
		if err != nil {
			return out, err
		}
		out = append(out, l)
		if len(out) > 10000 {
			t.Fatal("the reader does not end")
		}
	}
}

func TestLogReaderSplitsLinesAndParsesTimestamps(t *testing.T) {
	r := readerOver(
		frame(streamStdout, stamped(0, "one\n")),
		frame(streamStderr, stamped(time.Millisecond, "two\n")),
		frame(streamStdout, stamped(2*time.Millisecond, "three\n")+stamped(3*time.Millisecond, "four\n")),
		frame(streamStdout, stamped(4*time.Millisecond, "\n")),       // an empty line
		frame(streamStdout, stamped(5*time.Millisecond, "crlf\r\n")), // a Windows line break
	)
	got, err := drain(t, r)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("error = %v, want io.EOF", err)
	}
	want := []LogLine{
		{Time: streamBase, Stream: "stdout", Text: "one"},
		{Time: streamBase.Add(time.Millisecond), Stream: "stderr", Text: "two"},
		{Time: streamBase.Add(2 * time.Millisecond), Stream: "stdout", Text: "three"},
		{Time: streamBase.Add(3 * time.Millisecond), Stream: "stdout", Text: "four"},
		{Time: streamBase.Add(4 * time.Millisecond), Stream: "stdout", Text: ""},
		{Time: streamBase.Add(5 * time.Millisecond), Stream: "stdout", Text: "crlf"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("lines =\n%+v\nwant\n%+v", got, want)
	}
	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("Next after the end = %v, want io.EOF again", err)
	}
}

func TestLogReaderJoinsPartialLinesPerStream(t *testing.T) {
	// A line split over frames, with a line of the other stream written in between.
	r := readerOver(
		frame(streamStdout, stamped(0, "hel")),
		frame(streamStderr, stamped(time.Millisecond, "err\n")),
		frame(streamStdout, "lo wor"),
		frame(streamStdout, "ld\n"),
	)
	got, _ := drain(t, r)
	if len(got) != 2 || got[0].Text != "err" || got[0].Stream != "stderr" || got[1].Text != "hello world" || got[1].Stream != "stdout" || !got[1].Time.Equal(streamBase) {
		t.Fatalf("lines = %+v", got)
	}
}

func TestLogReaderFlushesWhatTheOutputLeftWithoutALineBreak(t *testing.T) {
	r := readerOver(frame(streamStdout, stamped(0, "complete\n")+stamped(time.Second, "no newline")), frame(streamStderr, stamped(2*time.Second, "also none")))
	got, err := drain(t, r)
	if !errors.Is(err, io.EOF) || len(got) != 3 || got[1].Text != "no newline" || got[1].Stream != "stdout" || got[2].Text != "also none" || got[2].Stream != "stderr" {
		t.Fatalf("lines = %+v, %v", got, err)
	}
}

func TestLogReaderCutsLongLinesWithoutBufferingThem(t *testing.T) {
	long := strings.Repeat("é", 4000) // 8000 bytes
	// The line arrives in a frame far larger than the line limit, then a short line follows.
	r := readerOver(frame(streamStdout, stamped(0, long+"\n")+stamped(time.Second, "short\n")))
	got, err := drain(t, r)
	if !errors.Is(err, io.EOF) || len(got) != 2 {
		t.Fatalf("lines = %d, %v", len(got), err)
	}
	if !got[0].Truncated || len(got[0].Text) > 1024 || !utf8.ValidString(got[0].Text) || !strings.HasPrefix(got[0].Text, "éé") || strings.ContainsRune(got[0].Text, '\uFFFD') {
		t.Fatalf("long line: truncated %v, %d bytes, valid %v", got[0].Truncated, len(got[0].Text), utf8.ValidString(got[0].Text))
	}
	if got[1].Text != "short" || got[1].Truncated || !got[1].Time.Equal(streamBase.Add(time.Second)) {
		t.Fatalf("the line after a cut line = %+v", got[1])
	}
	// What the reader keeps per output stream is bounded, whatever the line length.
	for s := range r.part {
		if cap(r.part[s].buf) > 4*(1024+stampSpace) {
			t.Fatalf("stream %d kept a buffer of %d bytes", s, cap(r.part[s].buf))
		}
	}
}

func TestLogReaderKeepsReadingAHugeFrameInChunks(t *testing.T) {
	// A frame of 40 MiB of one endless line followed by a short one: the reader
	// must neither allocate the frame nor take long over it.
	const size = 40 << 20
	header := make([]byte, frameHeaderLen)
	header[0] = streamStdout
	binary.BigEndian.PutUint32(header[4:], size)
	body := io.MultiReader(
		bytes.NewReader(header),
		strings.NewReader(stamped(0, "")),
		io.LimitReader(zeroReader{'x'}, size-int64(len(stamped(0, "")))-1),
		strings.NewReader("\n"),
		bytes.NewReader(frame(streamStdout, stamped(time.Second, "after\n"))),
	)
	r := newLogReader(io.NopCloser(body), func() {}, 1024)
	l, err := r.Next()
	if err != nil || !l.Truncated || len(l.Text) != 1024 {
		t.Fatalf("first line: %d bytes, truncated %v, %v", len(l.Text), l.Truncated, err)
	}
	if l, err = r.Next(); err != nil || l.Text != "after" {
		t.Fatalf("second line = %+v, %v", l, err)
	}
}

type zeroReader struct{ b byte }

func (z zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = z.b
	}
	return len(p), nil
}

func TestLogReaderToleratesLinesWithoutATimestampAndBadUTF8(t *testing.T) {
	r := readerOver(
		frame(streamStdout, stamped(time.Second, "first\n")),
		frame(streamStdout, "no stamp here\n"),
		frame(streamStdout, stamped(2*time.Second, "bad \xff\xfe bytes\n")),
	)
	got, _ := drain(t, r)
	if len(got) != 3 {
		t.Fatalf("lines = %+v", got)
	}
	// A line without a timestamp takes the previous line's time, so it keeps its place.
	if got[1].Text != "no stamp here" || !got[1].Time.Equal(streamBase.Add(time.Second)) {
		t.Errorf("line without a timestamp = %+v", got[1])
	}
	if !utf8.ValidString(got[2].Text) || !strings.Contains(got[2].Text, "bad ") {
		t.Errorf("invalid bytes must be replaced: %q", got[2].Text)
	}
}

func TestLogReaderErrors(t *testing.T) {
	tests := map[string][]byte{
		"a TTY stream":             append([]byte{0, 0, 0, 0, 0, 0, 0, 3}, "abc"...),
		"a header cut short":       {streamStdout, 0, 0},
		"a frame cut short":        append([]byte{streamStdout, 0, 0, 0, 0, 0, 0, 10}, "abc"...),
		"an unknown stream":        append([]byte{9, 0, 0, 0, 0, 0, 0, 1}, 'x'),
		"nothing after an overrun": append([]byte{streamStdout, 0, 0, 0, 0, 0, 0, 6}, "ab"...),
	}
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			r := readerOver(data)
			_, err := drain(t, r)
			if err == nil || errors.Is(err, io.EOF) {
				t.Fatalf("error = %v, want a failure that is not a clean end", err)
			}
		})
	}
}

func TestLogReaderCloseEndsABlockedRead(t *testing.T) {
	pr, pw := io.Pipe()
	canceled := false
	r := newLogReader(pr, func() { canceled = true }, 1024)
	result := make(chan error, 1)
	go func() {
		_, err := r.Next()
		result <- err
	}()
	if _, err := pw.Write(frame(streamStdout, stamped(0, "partial"))); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("Next after Close = %v, want io.ErrClosedPipe", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not end the blocked read")
	}
	if !canceled {
		t.Error("Close did not cancel the request to the daemon")
	}
	if err := r.Close(); err != nil {
		t.Errorf("second Close = %v", err)
	}
	if _, err := r.Next(); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("Next on a closed reader = %v", err)
	}
}

func TestLogReaderWaitsForOutputWhenFollowing(t *testing.T) {
	pr, pw := io.Pipe()
	r := newLogReader(pr, func() {}, 1024)
	defer r.Close()
	got := make(chan LogLine, 1)
	go func() {
		l, err := r.Next()
		if err == nil {
			got <- l
		}
	}()
	select {
	case <-got:
		t.Fatal("a line appeared out of nothing")
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := pw.Write(frame(streamStdout, stamped(0, "live\n"))); err != nil {
		t.Fatal(err)
	}
	select {
	case l := <-got:
		if l.Text != "live" {
			t.Fatalf("line = %+v", l)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the line written later did not arrive")
	}
	pw.Close()
	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("Next after the daemon ended the stream = %v, want io.EOF", err)
	}
}

func TestStreamLogsAsksTheDaemonForTimestampsAndPassesTheOptions(t *testing.T) {
	f := &fakeBackend{}
	f.add("aaa", "running", managedBy(testNode, "w-1"))
	f.logStream = frame(streamStdout, stamped(0, "hello\n"))
	a := newTestAdapter(t, f)

	since := streamBase.Add(-time.Hour)
	r, err := a.StreamLogs(context.Background(), "w-1", LogStreamOptions{Tail: -1, Since: since, Follow: true, MaxLineBytes: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := drain(t, r)
	if !errors.Is(err, io.EOF) || len(got) != 1 || got[0].Text != "hello" || !got[0].Time.Equal(streamBase) {
		t.Fatalf("lines = %+v, %v", got, err)
	}
	want := logRequest{Tail: -1, Since: since, Timestamps: true, Follow: true}
	if len(f.logReqs) != 1 || f.logReqs[0] != want {
		t.Fatalf("daemon request = %+v, want %+v", f.logReqs, want)
	}
	if r.(*logReader).maxLine != 100 {
		t.Errorf("line limit = %d, want 100", r.(*logReader).maxLine)
	}

	for in, want := range map[int]int{0: defaultStreamLine, -5: defaultStreamLine, 10: 10, 1 << 30: maxStreamLine} {
		if got := (LogStreamOptions{MaxLineBytes: in}).maxLine(); got != want {
			t.Errorf("maxLine(%d) = %d, want %d", in, got, want)
		}
	}
}

// Only the logs of containers DockFleet manages on this node can be read: a
// stranger's container, another node's and an unknown workload all look the same.
func TestStreamLogsRefusesContainersThatAreNotThisNodes(t *testing.T) {
	f := &fakeBackend{}
	seed(f)
	a := newTestAdapter(t, f)
	for _, id := range []string{"w-c", "w-d", "ccc", "nope"} {
		if _, err := a.StreamLogs(context.Background(), id, LogStreamOptions{}); !errors.Is(err, ErrNotFound) {
			t.Errorf("StreamLogs(%q) = %v, want ErrNotFound", id, err)
		}
	}
	if _, err := a.StreamLogs(context.Background(), "../etc", LogStreamOptions{}); !errors.Is(err, ErrInvalid) {
		t.Errorf("a malformed workload ID = %v, want ErrInvalid", err)
	}
	if len(f.logReqs) != 0 {
		t.Fatalf("the daemon was asked for logs of containers that are not ours: %+v", f.logReqs)
	}
	if _, err := a.StreamLogs(context.Background(), "w-b", LogStreamOptions{}); err != nil {
		t.Fatalf("StreamLogs of a managed container: %v", err)
	}
	if len(f.logReqs) != 1 {
		t.Fatalf("daemon requests = %+v", f.logReqs)
	}
}

func TestSDKStreamLogsSendsFollowAndTailAll(t *testing.T) {
	d := daemonWithContainers()
	a := newDaemonAdapter(t, d)
	r, err := a.StreamLogs(context.Background(), "w-1", LogStreamOptions{Tail: -1, Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, _ := drain(t, r)
	if len(got) != 2 || got[0].Text != "hello" || got[1].Stream != "stderr" {
		t.Fatalf("lines = %+v", got)
	}
	var seen bool
	for _, req := range d.sent() {
		if req.Method == http.MethodGet && req.Path == "/containers/c1/logs" {
			seen = true
			q := req.Query
			if q.Get("tail") != "all" || q.Get("follow") != "1" || q.Get("timestamps") != "1" || q.Get("stdout") != "1" || q.Get("stderr") != "1" {
				t.Errorf("logs query = %v", q)
			}
		}
	}
	if !seen {
		t.Fatal("no logs request was sent")
	}

	// No lines and no follow: tail=0 is "none", not "all".
	d2 := daemonWithContainers()
	r2, err := newDaemonAdapter(t, d2).StreamLogs(context.Background(), "w-1", LogStreamOptions{Tail: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	for _, req := range d2.sent() {
		if req.Path == "/containers/c1/logs" && (req.Query.Get("tail") != "0" || req.Query.Get("follow") == "1") {
			t.Errorf("logs query = %v", req.Query)
		}
	}
}
