package dockerfake

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dockfleet/dockfleet/internal/docker"
)

// The fake keeps what each container writes and serves it the way the real
// adapter does: the last lines, those since a time, and, when asked to follow,
// the lines written later, until the container stops or is removed.

// AppendLog makes a managed container write a line to stdout ("stdout") or
// stderr ("stderr"), as of the time at. It reports false if the node has no
// such container. Times should not go backwards; the readers do not sort.
func (f *Fake) AppendLog(workloadID, stream, text string, at time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	e := f.managed(workloadID)
	if e == nil {
		return false
	}
	e.logs = append(e.logs, docker.LogLine{Time: at.UTC(), Stream: stream, Text: text})
	f.broadcast()
	return true
}

// Log makes a managed container write a line to stdout, stamped with the
// current time (or one nanosecond after its previous line, if that is later).
func (f *Fake) Log(workloadID, text string) bool {
	f.mu.Lock()
	e := f.managed(workloadID)
	at := time.Now().UTC()
	if e != nil && len(e.logs) > 0 {
		if last := e.logs[len(e.logs)-1].Time; !at.After(last) {
			at = last.Add(time.Nanosecond)
		}
	}
	f.mu.Unlock()
	return f.AppendLog(workloadID, "stdout", text, at)
}

// LogLines returns what a managed container has written so far.
func (f *Fake) LogLines(workloadID string) []docker.LogLine {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e := f.managed(workloadID); e != nil {
		return append([]docker.LogLine(nil), e.logs...)
	}
	return nil
}

// OpenLogReaders is how many log readers are open on the engine, so a test can
// tell that a canceled request really released its reader.
func (f *Fake) OpenLogReaders() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.readers
}

// StreamLogs implements docker.Runtime.
func (f *Fake) StreamLogs(ctx context.Context, workloadID string, opts docker.LogStreamOptions) (docker.LogReader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.take("logs"); err != nil {
		return nil, err
	}
	e := f.managed(workloadID)
	if e == nil {
		return nil, fmt.Errorf("%w: %s", docker.ErrNotFound, workloadID)
	}
	maxLine := opts.MaxLineBytes
	if maxLine <= 0 {
		maxLine = 16 << 10
	}
	maxLine = min(maxLine, 64<<10)

	// The lines to start with: those since the time, and of them the last Tail.
	start := len(e.logs)
	if opts.Tail != 0 {
		var visible []int
		for i, l := range e.logs {
			if opts.Since.IsZero() || !l.Time.Before(opts.Since) {
				visible = append(visible, i)
			}
		}
		switch {
		case len(visible) == 0:
		case opts.Tail < 0 || opts.Tail >= len(visible):
			start = visible[0]
		default:
			start = visible[len(visible)-opts.Tail]
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	f.readers++
	return &logReader{f: f, e: e, opts: opts, maxLine: maxLine, pos: start, ctx: ctx, cancel: cancel}, nil
}

type logReader struct {
	f       *Fake
	e       *entry
	opts    docker.LogStreamOptions
	maxLine int
	pos     int
	ctx     context.Context
	cancel  context.CancelFunc
	closed  bool
}

var _ docker.LogReader = (*logReader)(nil)

// Next returns the next line, waits for one while the container runs and the
// reader follows, and returns io.EOF otherwise.
func (r *logReader) Next() (docker.LogLine, error) {
	for {
		if r.ctx.Err() != nil {
			return docker.LogLine{}, io.ErrClosedPipe
		}
		r.f.mu.Lock()
		for r.pos < len(r.e.logs) {
			l := r.e.logs[r.pos]
			r.pos++
			if !r.opts.Since.IsZero() && l.Time.Before(r.opts.Since) {
				continue
			}
			r.f.mu.Unlock()
			return clip(l, r.maxLine), nil
		}
		wake := r.f.wake
		over := !r.opts.Follow || r.e.gone || r.e.State != docker.StateRunning
		r.f.mu.Unlock()
		if over {
			return docker.LogLine{}, io.EOF
		}
		select {
		case <-wake:
		case <-r.ctx.Done():
			return docker.LogLine{}, io.ErrClosedPipe
		}
	}
}

// Close releases the reader and ends a Next that waits on another goroutine.
func (r *logReader) Close() error {
	r.cancel()
	r.f.mu.Lock()
	defer r.f.mu.Unlock()
	if !r.closed {
		r.closed = true
		r.f.readers--
	}
	return nil
}

// clip cuts a line at the limit the way the adapter does.
func clip(l docker.LogLine, limit int) docker.LogLine {
	if len(l.Text) <= limit {
		return l
	}
	n := limit
	for n > 0 && !utf8.RuneStart(l.Text[n]) {
		n--
	}
	l.Text, l.Truncated = strings.ToValidUTF8(l.Text[:n], ""), true
	return l
}
