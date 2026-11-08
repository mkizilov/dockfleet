package api

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/dockfleet/dockfleet/internal/logstream"
	"github.com/dockfleet/dockfleet/internal/store"
)

// Container logs. The agents stream the output of their containers to the
// manager, and these endpoints stream it on to the administrator: what the
// containers wrote recently, and, on request, what they write from now on.
// Nothing here starts, stops or changes anything; it reads.

// LogSource opens streams of container logs; *logstream.Hub implements it.
type LogSource interface {
	Open(ctx context.Context, q logstream.Query) (*logstream.Stream, error)
}

const (
	defaultLogTail         = 100
	defaultLogWriteTimeout = 30 * time.Second
	// logKeepalive is how often an idle event stream gets a comment line, so
	// that proxies do not close it.
	logKeepalive = 15 * time.Second
	// logBufferBytes is the most a response holds before it is written out.
	logBufferBytes = 16 << 10
	// logBurst is how many ready events are written before the response is flushed.
	logBurst = 128
	// maxLogSince bounds a relative since=, such as 30m.
	maxLogSince = 30 * 24 * time.Hour
)

var (
	serviceLogParams  = fieldSet("tail", "follow", "timestamps", "since", "after", "format", "replica", "revision", "node", "workload")
	workloadLogParams = fieldSet("tail", "follow", "timestamps", "since", "after", "format")
)

// logQuery is what a request asks for.
type logQuery struct {
	tail       int
	follow     bool
	timestamps bool
	format     string
	after      map[string]time.Time
	since      time.Time
	replicas   map[int]bool
	revision   int
	node       string
	workload   string
}

// logTarget is a container to read and what to call it in the output.
type logTarget struct {
	nodeID     string
	workloadID string
	name       string // the workload's name
	service    string // empty for a workload of no service
	replica    int
	revision   int
}

// label is how a container is named in text output.
func (t logTarget) label() string {
	if t.service != "" {
		return t.service + "/" + strconv.Itoa(t.replica)
	}
	return t.name
}

// parseLogQuery reads and validates the query string. Every problem found is
// reported, and a parameter that is not supported is refused rather than
// ignored, so a typo cannot silently turn a filter into "everything".
func parseLogQuery(r *http.Request, allowed map[string]bool, now time.Time) (logQuery, problems) {
	var p problems
	q := logQuery{tail: defaultLogTail, timestamps: true}
	values := r.URL.Query()

	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		switch {
		case !allowed[k]:
			p.add(k, "is not a supported parameter")
		case len(values[k]) > 1 && k != "replica":
			p.add(k, "must be given once")
		}
	}
	one := func(k string) (string, bool) {
		v := values[k]
		if len(v) != 1 || !allowed[k] {
			return "", false
		}
		return strings.TrimSpace(v[0]), true
	}

	if v, ok := one("tail"); ok {
		switch n, err := strconv.Atoi(v); {
		case v == "all":
			q.tail = logstream.MaxTail
		case err != nil || n < 0 || n > logstream.MaxTail:
			p.add("tail", "must be a number from 0 to %d, or all", logstream.MaxTail)
		default:
			q.tail = n
		}
	}
	for _, k := range []string{"follow", "timestamps"} {
		if v, ok := one(k); ok {
			b, err := strconv.ParseBool(v)
			if err != nil {
				p.add(k, "must be true or false")
				continue
			}
			if k == "follow" {
				q.follow = b
			} else {
				q.timestamps = b
			}
		}
	}

	q.format = "ndjson"
	if strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		q.format = "sse"
	}
	if v, ok := one("format"); ok {
		switch v {
		case "ndjson", "text", "sse":
			q.format = v
		default:
			p.add("format", "must be ndjson, text or sse")
		}
	}

	if v, ok := one("since"); ok {
		if d, err := time.ParseDuration(v); err == nil {
			if d <= 0 || d > maxLogSince {
				p.add("since", "a duration must be between 0 and %s", maxLogSince)
			} else {
				q.since = now.Add(-d).UTC()
			}
		} else if t, err := logstream.ParseTime(v); err == nil {
			q.since = t
		} else {
			p.add("since", "must be a duration such as 10m, an RFC 3339 time, or Unix nanoseconds")
		}
	}
	cursor, _ := one("after")
	if cursor == "" && allowed["after"] {
		cursor = strings.TrimSpace(r.Header.Get("Last-Event-ID"))
	}
	if cursor != "" {
		per, all, err := logstream.ParseCursor(cursor)
		if err != nil {
			p.add("after", "%s", err.Error())
		}
		q.after = per
		if all.After(q.since) {
			q.since = all
		}
	}

	for _, v := range values["replica"] {
		if !allowed["replica"] {
			break
		}
		for _, part := range strings.Split(v, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil || n < 1 || n > maxReplicas {
				p.add("replica", "must be replica numbers from 1 to %d, separated by commas", maxReplicas)
				break
			}
			if q.replicas == nil {
				q.replicas = map[int]bool{}
			}
			q.replicas[n] = true
		}
	}
	if v, ok := one("revision"); ok {
		if n, err := strconv.Atoi(v); err != nil || n < 1 {
			p.add("revision", "must be a revision number, 1 or more")
		} else {
			q.revision = n
		}
	}
	for _, k := range []string{"node", "workload"} {
		if v, ok := one(k); ok {
			if !store.ValidName(v) {
				p.add(k, "is not a valid identifier")
				continue
			}
			if k == "node" {
				q.node = v
			} else {
				q.workload = v
			}
		}
	}
	return q, p
}

// readLogQuery parses the request and answers it if it is not valid.
func (s *server) readLogQuery(w http.ResponseWriter, r *http.Request, allowed map[string]bool) (logQuery, bool) {
	q, probs := parseLogQuery(r, allowed, s.now())
	if len(probs) > 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid log request", "details": probs})
		return q, false
	}
	return q, true
}

// serviceLogs streams the logs of a service's replicas, merged by time. By
// default these are the replicas that are meant to run, of every revision;
// replica, revision, node and workload narrow them down.
func (s *server) serviceLogs(w http.ResponseWriter, r *http.Request) {
	q, ok := s.readLogQuery(w, r, serviceLogParams)
	if !ok {
		return
	}
	svc, err := s.store.GetServiceByName(r.Context(), r.PathValue("name"))
	if err != nil {
		s.failService(w, r, err, false)
		return
	}
	workloads, err := s.store.ListWorkloadsForService(r.Context(), svc.ID)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	var targets []logTarget
	for _, wl := range workloads {
		switch {
		case wl.DesiredState != store.DesiredRunning,
			len(q.replicas) > 0 && !q.replicas[wl.ReplicaIndex],
			q.revision != 0 && wl.ServiceRevision != q.revision,
			q.node != "" && wl.NodeID != q.node,
			q.workload != "" && wl.ID != q.workload:
			continue
		}
		targets = append(targets, logTarget{nodeID: wl.NodeID, workloadID: wl.ID, name: wl.Name, service: svc.Name, replica: wl.ReplicaIndex, revision: wl.ServiceRevision})
	}
	switch {
	case len(targets) == 0:
		writeError(w, http.StatusNotFound, "no replica of service "+svc.Name+" that is meant to run matches")
		return
	case len(targets) > logstream.MaxTargets:
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "invalid log request",
			"details": problems{{Field: "replica", Message: strconv.Itoa(len(targets)) + " replicas match, and at most " +
				strconv.Itoa(logstream.MaxTargets) + " can be read at once; narrow them with replica, revision or node"}},
		})
		return
	}
	sort.Slice(targets, func(i, j int) bool {
		a, b := targets[i], targets[j]
		if a.revision != b.revision {
			return a.revision < b.revision
		}
		if a.replica != b.replica {
			return a.replica < b.replica
		}
		return a.workloadID < b.workloadID
	})
	s.streamLogs(w, r, q, targets)
}

// workloadLogs streams the logs of one workload, a replica of a service or a
// standalone one, addressed by its ID.
func (s *server) workloadLogs(w http.ResponseWriter, r *http.Request) {
	q, ok := s.readLogQuery(w, r, workloadLogParams)
	if !ok {
		return
	}
	wl, err := s.store.GetWorkload(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "workload not found")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	t := logTarget{nodeID: wl.NodeID, workloadID: wl.ID, name: wl.Name, replica: wl.ReplicaIndex, revision: wl.ServiceRevision}
	if wl.ServiceID != "" {
		if svc, err := s.store.GetService(r.Context(), wl.ServiceID); err == nil {
			t.service = svc.Name
		}
	}
	s.streamLogs(w, r, q, []logTarget{t})
}

// logRecord is one record of the JSON output (and the data of an event).
type logRecord struct {
	Type      string `json:"type"` // line, notice or end
	Time      string `json:"time,omitempty"`
	Service   string `json:"service,omitempty"`
	Replica   int    `json:"replica,omitempty"`
	Revision  int    `json:"revision,omitempty"`
	Workload  string `json:"workload,omitempty"`
	Name      string `json:"name,omitempty"`
	Node      string `json:"node,omitempty"`
	Stream    string `json:"stream,omitempty"`
	Text      string `json:"text"`
	Truncated bool   `json:"truncated,omitempty"`
	Kind      string `json:"kind,omitempty"`
	Message   string `json:"message,omitempty"`
	Reason    string `json:"reason,omitempty"`
	Cursor    string `json:"cursor,omitempty"`
}

// logWriter writes a response through a small buffer and gives every write a
// deadline: a client that does not read is dropped when the deadline passes
// instead of holding its stream open, so what is buffered for it, here and in
// the log stream behind, stays bounded.
type logWriter struct {
	bw      *bufio.Writer
	rc      *http.ResponseController
	timeout time.Duration
	err     error
}

func (o *logWriter) write(b []byte) {
	if o.err == nil {
		_ = o.rc.SetWriteDeadline(time.Now().Add(o.timeout))
		_, o.err = o.bw.Write(b)
	}
}

func (o *logWriter) flush() error {
	if o.err == nil {
		_ = o.rc.SetWriteDeadline(time.Now().Add(o.timeout))
		if o.err = o.bw.Flush(); o.err == nil {
			if err := o.rc.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
				o.err = err
			}
		}
	}
	return o.err
}

// printable makes text safe to show on a terminal: control characters, which
// could move the cursor or change what an administrator sees, become U+FFFD.
func printable(s string) string {
	return strings.Map(func(r rune) rune {
		if r != '\t' && (unicode.IsControl(r) || r == 0x2028 || r == 0x2029) {
			return '\uFFFD'
		}
		return r
	}, s)
}

// streamLogs opens the query on the hub and writes what comes back until the
// client leaves, the output ends, or the client stops reading.
func (s *server) streamLogs(w http.ResponseWriter, r *http.Request, q logQuery, targets []logTarget) {
	if s.logs == nil {
		writeError(w, http.StatusServiceUnavailable, "container logs are not enabled on this manager")
		return
	}
	query := logstream.Query{Tail: q.tail, Follow: q.follow, After: q.after, Default: q.since}
	byWorkload := make(map[string]logTarget, len(targets))
	for _, t := range targets {
		query.Targets = append(query.Targets, logstream.Target{NodeID: t.nodeID, WorkloadID: t.workloadID})
		byWorkload[t.workloadID] = t
	}
	st, err := s.logs.Open(r.Context(), query)
	switch {
	case errors.Is(err, logstream.ErrTooManyStreams):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, "too many log streams are open; try again shortly")
		return
	case errors.Is(err, logstream.ErrBadQuery):
		writeError(w, http.StatusBadRequest, err.Error())
		return
	case errors.Is(err, logstream.ErrClosed):
		writeError(w, http.StatusServiceUnavailable, "the manager is shutting down")
		return
	case err != nil:
		s.internalError(w, r, err)
		return
	}
	defer st.Close()

	rc := http.NewResponseController(w)
	// The server's read timeout would end a response that lives longer than it.
	_ = rc.SetReadDeadline(time.Time{})
	contentType := map[string]string{"ndjson": "application/x-ndjson", "text": "text/plain; charset=utf-8", "sse": "text/event-stream"}[q.format]
	h := w.Header()
	h.Set("Content-Type", contentType)
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	out := &logWriter{bw: bufio.NewWriterSize(w, logBufferBytes), rc: rc, timeout: s.logWriteTimeout}
	if out.flush() != nil {
		return
	}

	render := func(ev logstream.Event) {
		if ev.Entry != nil {
			t := byWorkload[ev.Entry.WorkloadID]
			rec := logRecord{
				Type: "line", Service: t.service, Replica: t.replica, Revision: t.revision, Workload: ev.Entry.WorkloadID, Name: t.name,
				Node: ev.Entry.NodeID, Stream: ev.Entry.Stream, Text: ev.Entry.Text, Truncated: ev.Entry.Truncated,
			}
			if q.timestamps && !ev.Entry.Time.IsZero() {
				rec.Time = ev.Entry.Time.UTC().Format(time.RFC3339Nano)
			}
			writeLogEvent(out, q.format, rec, t, st)
			return
		}
		n := ev.Notice
		t := byWorkload[n.WorkloadID]
		writeLogEvent(out, q.format, logRecord{
			Type: "notice", Service: t.service, Replica: t.replica, Revision: t.revision, Workload: n.WorkloadID, Name: t.name,
			Node: n.NodeID, Kind: string(n.Kind), Message: n.Message,
		}, t, st)
	}

	ctx := r.Context()
	for {
		ev, err := nextLogEvent(ctx, st, q)
		switch {
		case err == nil:
		case errors.Is(err, errLogIdle):
			out.write([]byte(": keepalive\n\n"))
			if out.flush() != nil {
				return
			}
			continue
		case errors.Is(err, io.EOF), errors.Is(err, logstream.ErrClosed) && ctx.Err() == nil:
			reason := "complete"
			if !errors.Is(err, io.EOF) {
				reason = "closed"
			}
			writeLogEvent(out, q.format, logRecord{Type: "end", Reason: reason, Cursor: logstream.EncodeCursor(st.Cursor())}, logTarget{}, st)
			_ = out.flush()
			return
		default:
			return // the client left
		}
		render(ev)
		for n := 0; n < logBurst; n++ {
			next, ok := st.TryNext()
			if !ok {
				break
			}
			render(next)
		}
		if out.flush() != nil {
			s.logger.InfoContext(ctx, "a log client stopped reading; its stream was closed", "remote", r.RemoteAddr)
			return
		}
	}
}

var errLogIdle = errors.New("no log event for a while")

// nextLogEvent waits for the next event. An event stream that has been idle
// for a while reports it, so that the caller can send a keepalive.
func nextLogEvent(ctx context.Context, st *logstream.Stream, q logQuery) (logstream.Event, error) {
	if q.format != "sse" || !q.follow {
		return st.Next(ctx)
	}
	idle, cancel := context.WithTimeout(ctx, logKeepalive)
	defer cancel()
	ev, err := st.Next(idle)
	if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		return logstream.Event{}, errLogIdle
	}
	return ev, err
}

// writeLogEvent writes one record in the requested format.
func writeLogEvent(out *logWriter, format string, rec logRecord, t logTarget, st *logstream.Stream) {
	switch format {
	case "text":
		var sb strings.Builder
		switch rec.Type {
		case "line":
			if rec.Time != "" {
				sb.WriteString(rec.Time + " ")
			}
			sb.WriteString(printable(t.label()) + " " + rec.Stream + ": " + printable(rec.Text))
			if rec.Truncated {
				sb.WriteString(" [cut]")
			}
		case "notice":
			sb.WriteString("-- " + rec.Kind + " " + printable(t.label()) + ": " + printable(rec.Message))
		default:
			sb.WriteString("-- end (" + rec.Reason + ")")
		}
		sb.WriteByte('\n')
		out.write([]byte(sb.String()))
	case "sse":
		data, err := json.Marshal(rec)
		if err != nil {
			return
		}
		var sb strings.Builder
		if rec.Type == "line" {
			sb.WriteString("id: " + logstream.EncodeCursor(st.Cursor()) + "\n")
		}
		sb.WriteString("event: " + rec.Type + "\ndata: ")
		sb.Write(data)
		sb.WriteString("\n\n")
		out.write([]byte(sb.String()))
	default:
		data, err := json.Marshal(rec)
		if err != nil {
			return
		}
		out.write(append(data, '\n'))
	}
}
