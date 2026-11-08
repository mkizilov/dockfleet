package docker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

const (
	defaultLogTail  = 100
	maxLogTail      = 10000
	defaultLogBytes = 1 << 20
	maxLogBytes     = 8 << 20

	// Docker multiplexes stdout and stderr of containers without a TTY into one
	// stream of frames: 1 byte stream type, 3 zero bytes, 4 bytes big-endian
	// length, then the payload.
	frameHeaderLen = 8
	streamStdout   = 1
	streamStderr   = 2
)

func (o LogOptions) tail() int {
	switch {
	case o.Tail <= 0:
		return defaultLogTail
	case o.Tail > maxLogTail:
		return maxLogTail
	}
	return o.Tail
}

func (o LogOptions) maxBytes() int {
	switch {
	case o.MaxBytes <= 0:
		return defaultLogBytes
	case o.MaxBytes > maxLogBytes:
		return maxLogBytes
	}
	return o.MaxBytes
}

// demux reads Docker's multiplexed log stream and returns at most maxBytes of
// payload, in order. Longer output sets Truncated; the rest is not read, so a
// huge log cannot exhaust memory.
func demux(r io.Reader, maxBytes int) (Logs, error) {
	var out Logs
	remaining := maxBytes
	header := make([]byte, frameHeaderLen)
	for {
		if _, err := io.ReadFull(r, header); err != nil {
			if err == io.EOF {
				return out, nil
			}
			return out, fmt.Errorf("read log frame header: %w", err)
		}
		var stream string
		switch header[0] {
		case streamStdout:
			stream = "stdout"
		case streamStderr:
			stream = "stderr"
		default:
			return out, fmt.Errorf("unexpected log stream type %d (is the container using a TTY?)", header[0])
		}
		size := int64(binary.BigEndian.Uint32(header[4:]))
		take := size
		if take > int64(remaining) {
			take = int64(remaining)
		}
		payload := make([]byte, take)
		if _, err := io.ReadFull(r, payload); err != nil {
			return out, fmt.Errorf("read log frame: %w", err)
		}
		if len(payload) > 0 {
			out.Chunks = append(out.Chunks, LogChunk{Stream: stream, Text: string(payload)})
		}
		remaining -= int(take)
		if take < size || remaining == 0 {
			// Anything further is dropped; report it only if there was more.
			if take < size {
				out.Truncated = true
			} else if more := moreData(r); more {
				out.Truncated = true
			}
			return out, nil
		}
	}
}

// moreData reports whether the stream has another byte, consuming at most one.
func moreData(r io.Reader) bool {
	var b [1]byte
	n, _ := r.Read(b[:])
	return n > 0
}

const (
	defaultStreamLine = 16 << 10
	maxStreamLine     = 64 << 10

	// stampSpace is room for the timestamp Docker puts in front of every line
	// (up to 35 bytes, plus the space after it).
	stampSpace = 40
	// streamChunk is how much of a frame is read at a time.
	streamChunk = 32 << 10
)

func (o LogStreamOptions) maxLine() int {
	switch {
	case o.MaxLineBytes <= 0:
		return defaultStreamLine
	case o.MaxLineBytes > maxStreamLine:
		return maxStreamLine
	}
	return o.MaxLineBytes
}

// partial is the line one output stream is in the middle of.
type partial struct {
	buf  []byte
	over bool // bytes beyond the limit were dropped
}

func (p *partial) add(b []byte, limit int) {
	if room := limit - len(p.buf); len(b) > room {
		p.over = true
		b = b[:max(room, 0)]
	}
	p.buf = append(p.buf, b...)
}

// logReader turns Docker's multiplexed, time-stamped log stream into lines. It
// reads the stream a chunk at a time and keeps at most one partial line per
// output stream, each cut at the line limit, so its memory does not depend on
// how much the container writes or how long its lines are.
type logReader struct {
	rc      io.ReadCloser
	br      *bufio.Reader
	cancel  context.CancelFunc
	maxLine int
	limit   int // the most bytes kept of a line, its timestamp included

	closed atomic.Bool
	buf    []byte // payload chunks are read into it
	chunk  []byte // the part of the current chunk not scanned yet
	stream int    // the output stream (streamStdout or streamStderr) of the current frame
	left   int64  // payload bytes of the current frame not read yet
	part   [3]partial
	final  []LogLine // partial lines flushed when the output ends
	err    error
	last   time.Time // the timestamp of the previous line, for a line without one
}

var _ LogReader = (*logReader)(nil)

// newLogReader reads lines from rc, which must carry the timestamps Docker adds
// on request. cancel is called by Close, to stop a read that waits on the daemon.
func newLogReader(rc io.ReadCloser, cancel context.CancelFunc, maxLine int) *logReader {
	return &logReader{
		rc: rc, br: bufio.NewReaderSize(rc, 4<<10), cancel: cancel,
		maxLine: maxLine, limit: maxLine + stampSpace, buf: make([]byte, streamChunk),
	}
}

// Next returns the next line. It returns io.EOF when the output has ended and
// io.ErrClosedPipe after Close.
func (r *logReader) Next() (LogLine, error) {
	for {
		if r.closed.Load() {
			return LogLine{}, io.ErrClosedPipe
		}
		if len(r.chunk) > 0 {
			if l, ok := r.scan(); ok {
				return l, nil
			}
			continue
		}
		if len(r.final) > 0 {
			l := r.final[0]
			r.final = r.final[1:]
			return l, nil
		}
		if r.err != nil {
			return LogLine{}, r.err
		}
		r.fill()
	}
}

// Close ends the read, including one that waits in Next on another goroutine.
func (r *logReader) Close() error {
	if r.closed.Swap(true) {
		return nil
	}
	r.cancel()
	return r.rc.Close()
}

func (r *logReader) fail(err error) {
	if r.closed.Load() {
		r.err = io.ErrClosedPipe
		return
	}
	r.err = fmt.Errorf("read log stream: %w", err)
}

// fill reads the next chunk of payload, and the frame header before it when the
// previous frame is used up.
func (r *logReader) fill() {
	if r.left == 0 {
		var h [frameHeaderLen]byte
		if _, err := io.ReadFull(r.br, h[:]); err != nil {
			if errors.Is(err, io.EOF) && !r.closed.Load() {
				r.flushPartials()
				r.err = io.EOF
				return
			}
			r.fail(err)
			return
		}
		if h[0] != streamStdout && h[0] != streamStderr {
			r.err = fmt.Errorf("unexpected log stream type %d (is the container using a TTY?)", h[0])
			return
		}
		r.stream, r.left = int(h[0]), int64(binary.BigEndian.Uint32(h[4:]))
		if r.left == 0 {
			return
		}
	}
	n := int(min(r.left, int64(len(r.buf))))
	m, err := r.br.Read(r.buf[:n])
	r.chunk = r.buf[:m]
	r.left -= int64(m)
	if err != nil && m == 0 {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		r.fail(err)
	}
}

// scan takes the next line out of the current chunk, if it holds the end of one.
func (r *logReader) scan() (LogLine, bool) {
	p := &r.part[r.stream]
	i := bytes.IndexByte(r.chunk, '\n')
	if i < 0 {
		p.add(r.chunk, r.limit)
		r.chunk = nil
		return LogLine{}, false
	}
	p.add(r.chunk[:i], r.limit)
	r.chunk = r.chunk[i+1:]
	return r.emit(r.stream), true
}

// flushPartials turns what the output left without a line break into lines.
func (r *logReader) flushPartials() {
	for _, s := range []int{streamStdout, streamStderr} {
		if len(r.part[s].buf) > 0 {
			r.final = append(r.final, r.emit(s))
		}
	}
}

// emit makes a line of what an output stream has collected, and starts the next.
func (r *logReader) emit(stream int) LogLine {
	p := &r.part[stream]
	raw, over := p.buf, p.over
	defer func() { p.buf, p.over = p.buf[:0], false }()

	t, text := splitStamp(raw)
	if t.IsZero() {
		t = r.last
	} else {
		r.last = t
	}
	text = bytes.TrimSuffix(text, []byte("\r"))
	if len(text) > r.maxLine {
		text, over = cutUTF8(text, r.maxLine), true
	}
	line := LogLine{Time: t, Stream: "stdout", Text: strings.ToValidUTF8(string(text), "\uFFFD"), Truncated: over}
	if stream == streamStderr {
		line.Stream = "stderr"
	}
	return line
}

// splitStamp separates the RFC 3339 timestamp Docker puts in front of a line.
// A line without a timestamp is returned whole with the zero time.
func splitStamp(raw []byte) (time.Time, []byte) {
	stamp, rest := raw, []byte(nil)
	if i := bytes.IndexByte(raw, ' '); i >= 0 {
		stamp, rest = raw[:i], raw[i+1:]
	}
	if len(stamp) > stampSpace {
		return time.Time{}, raw
	}
	t, err := time.Parse(time.RFC3339Nano, string(stamp))
	if err != nil {
		return time.Time{}, raw
	}
	return t.UTC(), rest
}

// cutUTF8 keeps the first n bytes of b, less the rune that the cut would split.
func cutUTF8(b []byte, n int) []byte {
	if len(b) <= n {
		return b
	}
	for n > 0 && !utf8.RuneStart(b[n]) {
		n--
	}
	return b[:n]
}
