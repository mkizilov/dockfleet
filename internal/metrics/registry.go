// Package metrics is a small Prometheus-compatible metrics registry: counters
// and histograms with labels, collectors that read the state they report on
// every scrape, and the text exposition format (version 0.0.4).
//
// It imports only the standard library, so every layer can report through it.
// Memory is bounded: an instrument keeps at most MaxSeries label combinations,
// and an observation that does not fit is counted as dropped instead of kept,
// so a client that invents label values (a request path, a method) cannot grow
// the process.
package metrics

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// ContentType is the media type of the text exposition format.
const ContentType = "text/plain; version=0.0.4; charset=utf-8"

// MaxSeries bounds how many label combinations one instrument keeps.
const MaxSeries = 1000

// Type is the kind of a metric family.
type Type string

// The metric types the registry produces.
const (
	CounterType   Type = "counter"
	GaugeType     Type = "gauge"
	HistogramType Type = "histogram"
)

var (
	namePattern  = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
	labelPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

// Label is one name and value of a sample.
type Label struct {
	Name, Value string
}

// Sample is one line of the exposition: a full sample name (with _bucket, _sum
// or _count for histograms), its labels and its value.
type Sample struct {
	Name   string
	Labels []Label
	Value  float64
}

// Family is a metric with its samples.
type Family struct {
	Name    string
	Help    string
	Type    Type
	Samples []Sample
}

// NewFamily starts a family without samples.
func NewFamily(name, help string, t Type) *Family {
	return &Family{Name: name, Help: help, Type: t}
}

// Add appends a sample named like the family. labels are name, value pairs.
func (f *Family) Add(value float64, labels ...string) {
	s := Sample{Name: f.Name, Value: value}
	for i := 0; i+1 < len(labels); i += 2 {
		s.Labels = append(s.Labels, Label{Name: labels[i], Value: labels[i+1]})
	}
	f.Samples = append(f.Samples, s)
}

// Collector reads the state it reports when it is called, once per scrape. It
// is how values that live in the store (replica counts, node resources) are
// exported without a second copy that could drift.
type Collector func(ctx context.Context) ([]Family, error)

type namedCollector struct {
	name string
	fn   Collector
}

type instrument interface {
	family() Family
}

// Registry holds instruments and collectors and writes them out.
type Registry struct {
	mu          sync.Mutex
	names       map[string]bool
	instruments []instrument
	collectors  []namedCollector
	collectErrs map[string]uint64

	dropped atomic.Uint64
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{names: map[string]bool{}, collectErrs: map[string]uint64{}}
}

// AddCollector registers a collector under a name. A collector with the same
// name is replaced, so building a component twice is harmless.
func (r *Registry) AddCollector(name string, fn Collector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.collectors {
		if r.collectors[i].name == name {
			r.collectors[i].fn = fn
			return
		}
	}
	r.collectors = append(r.collectors, namedCollector{name: name, fn: fn})
}

// Dropped is how many observations were refused: too many label combinations,
// the wrong number of label values, or a value a counter cannot take.
func (r *Registry) Dropped() uint64 { return r.dropped.Load() }

func (r *Registry) register(name string, labels []string, extra string, in instrument) {
	if !namePattern.MatchString(name) {
		panic(fmt.Sprintf("metrics: invalid metric name %q", name))
	}
	for _, l := range labels {
		if !labelPattern.MatchString(l) || strings.HasPrefix(l, "__") || l == extra {
			panic(fmt.Sprintf("metrics: invalid label name %q on %s", l, name))
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.names[name] {
		panic(fmt.Sprintf("metrics: %s is registered twice", name))
	}
	r.names[name] = true
	r.instruments = append(r.instruments, in)
}

// Gather returns every family: the instruments', the collectors' and the
// registry's own, sorted by name. A collector that fails is skipped and
// counted; the rest of the scrape still succeeds.
func (r *Registry) Gather(ctx context.Context) []Family {
	r.mu.Lock()
	instruments := append([]instrument(nil), r.instruments...)
	collectors := append([]namedCollector(nil), r.collectors...)
	r.mu.Unlock()

	var out []Family
	seen := map[string]bool{}
	take := func(fams []Family) bool {
		for _, f := range fams {
			if seen[f.Name] || !namePattern.MatchString(f.Name) {
				return false
			}
		}
		for _, f := range fams {
			seen[f.Name] = true
			if f.Type != HistogramType {
				sortSamples(f.Samples)
			}
			out = append(out, f)
		}
		return true
	}
	for _, in := range instruments {
		take([]Family{in.family()})
	}
	for _, c := range collectors {
		fams, err := c.fn(ctx)
		if err == nil && !take(fams) {
			err = fmt.Errorf("collector %s returned a duplicate or invalid family", c.name)
		}
		if err != nil {
			r.mu.Lock()
			r.collectErrs[c.name]++
			r.mu.Unlock()
		}
	}

	dropped := NewFamily("dockfleet_metrics_dropped_observations_total",
		"Observations refused because an instrument already had too many label combinations or was given a bad one.", CounterType)
	dropped.Add(float64(r.dropped.Load()))
	errs := NewFamily("dockfleet_metrics_collect_errors_total", "Scrapes in which a collector failed, by collector.", CounterType)
	r.mu.Lock()
	for _, c := range collectors {
		errs.Add(float64(r.collectErrs[c.name]), "collector", c.name)
	}
	r.mu.Unlock()
	take([]Family{*dropped, *errs})

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Expose gathers the registry and writes it in the text exposition format.
func (r *Registry) Expose(ctx context.Context, w io.Writer) error {
	return WriteText(w, r.Gather(ctx))
}

func sortSamples(s []Sample) {
	sort.SliceStable(s, func(i, j int) bool { return labelKey(s[i].Labels) < labelKey(s[j].Labels) })
}

func labelKey(ls []Label) string {
	var sb strings.Builder
	for _, l := range ls {
		sb.WriteString(l.Name)
		sb.WriteByte('=')
		sb.WriteString(l.Value)
		sb.WriteByte(0)
	}
	return sb.String()
}

// WriteText writes families in the Prometheus text exposition format.
func WriteText(w io.Writer, fams []Family) error {
	bw := bufio.NewWriter(w)
	for _, f := range fams {
		bw.WriteString("# HELP " + f.Name + " " + escapeHelp(f.Help) + "\n")
		bw.WriteString("# TYPE " + f.Name + " " + string(f.Type) + "\n")
		for _, s := range f.Samples {
			bw.WriteString(s.Name)
			if len(s.Labels) > 0 {
				bw.WriteByte('{')
				for i, l := range s.Labels {
					if i > 0 {
						bw.WriteByte(',')
					}
					bw.WriteString(l.Name + `="` + escapeLabel(l.Value) + `"`)
				}
				bw.WriteByte('}')
			}
			bw.WriteByte(' ')
			bw.WriteString(formatFloat(s.Value))
			bw.WriteByte('\n')
		}
	}
	return bw.Flush()
}

func escapeHelp(s string) string {
	s = strings.ToValidUTF8(s, "\uFFFD")
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`).Replace(s)
}

func escapeLabel(s string) string {
	s = strings.ToValidUTF8(s, "\uFFFD")
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}

func formatFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// vec is the label handling the instruments share.
type vec struct {
	r      *Registry
	name   string
	help   string
	typ    Type
	labels []string

	mu     sync.Mutex
	series map[string]*series
}

type series struct {
	values  []string
	value   float64  // counters
	buckets []uint64 // histograms: observations per bucket, not cumulative
	sum     float64
	count   uint64
}

// setup sets a vec up in place: it holds a mutex, so it is never copied.
func (v *vec) setup(r *Registry, name, help string, typ Type, labels []string) {
	v.r, v.name, v.help, v.typ = r, name, help, typ
	v.labels = append([]string(nil), labels...)
	v.series = map[string]*series{}
}

// find returns the series for the label values, creating it. The caller holds
// v.mu. It returns nil, and counts a drop, if the values do not match the
// labels or the instrument is full.
func (v *vec) find(values []string) *series {
	if len(values) != len(v.labels) {
		v.r.dropped.Add(1)
		return nil
	}
	key := strings.Join(values, "\xff")
	if s, ok := v.series[key]; ok {
		return s
	}
	if len(v.series) >= MaxSeries {
		v.r.dropped.Add(1)
		return nil
	}
	s := &series{values: append([]string(nil), values...)}
	v.series[key] = s
	return s
}

// sorted returns the series ordered by their label values. The caller holds
// v.mu. An instrument without labels always has its one series, at zero, so
// that rate() has a start from the first scrape.
func (v *vec) sorted() []*series {
	if len(v.labels) == 0 && len(v.series) == 0 {
		return []*series{{}}
	}
	out := make([]*series, 0, len(v.series))
	for _, s := range v.series {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.Join(out[i].values, "\xff") < strings.Join(out[j].values, "\xff")
	})
	return out
}

func (v *vec) labelsOf(s *series, extra ...Label) []Label {
	out := make([]Label, 0, len(v.labels)+len(extra))
	for i, name := range v.labels {
		out = append(out, Label{Name: name, Value: s.values[i]})
	}
	return append(out, extra...)
}

// CounterVec is a counter per combination of label values.
type CounterVec struct {
	vec
}

// NewCounterVec registers a counter. It panics on an invalid or duplicate
// name, which is a programming error.
func (r *Registry) NewCounterVec(name, help string, labels ...string) *CounterVec {
	c := &CounterVec{}
	c.setup(r, name, help, CounterType, labels)
	r.register(name, labels, "", c)
	return c
}

// Add adds v to the counter for the label values. A negative or NaN value is
// refused: a counter only goes up.
func (c *CounterVec) Add(v float64, values ...string) {
	if v < 0 || math.IsNaN(v) {
		c.r.dropped.Add(1)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if s := c.find(values); s != nil {
		s.value += v
	}
}

// Inc adds one.
func (c *CounterVec) Inc(values ...string) { c.Add(1, values...) }

// Value is the current value for the label values, or 0 if there is none.
func (c *CounterVec) Value(values ...string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s, ok := c.series[strings.Join(values, "\xff")]; ok {
		return s.value
	}
	return 0
}

func (c *CounterVec) family() Family {
	c.mu.Lock()
	defer c.mu.Unlock()
	f := Family{Name: c.name, Help: c.help, Type: CounterType}
	for _, s := range c.sorted() {
		f.Samples = append(f.Samples, Sample{Name: c.name, Labels: c.labelsOf(s), Value: s.value})
	}
	return f
}

// HistogramVec is a histogram per combination of label values.
type HistogramVec struct {
	vec
	bounds []float64
}

// NewHistogramVec registers a histogram with the given upper bounds, which
// must be finite and strictly increasing. It panics on an invalid or duplicate
// name or bad bounds, which are programming errors.
func (r *Registry) NewHistogramVec(name, help string, bounds []float64, labels ...string) *HistogramVec {
	if len(bounds) == 0 {
		panic(fmt.Sprintf("metrics: histogram %s needs buckets", name))
	}
	for i, b := range bounds {
		if math.IsNaN(b) || math.IsInf(b, 0) || (i > 0 && b <= bounds[i-1]) {
			panic(fmt.Sprintf("metrics: histogram %s needs finite, increasing buckets", name))
		}
	}
	h := &HistogramVec{bounds: append([]float64(nil), bounds...)}
	h.setup(r, name, help, HistogramType, labels)
	r.register(name, labels, "le", h)
	return h
}

// Observe records one value for the label values.
func (h *HistogramVec) Observe(v float64, values ...string) {
	if math.IsNaN(v) {
		h.r.dropped.Add(1)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.find(values)
	if s == nil {
		return
	}
	if s.buckets == nil {
		s.buckets = make([]uint64, len(h.bounds))
	}
	// The first bucket whose upper bound is at least v; beyond the last one the
	// value only counts in +Inf, which is the total count.
	if i := sort.SearchFloat64s(h.bounds, v); i < len(h.bounds) {
		s.buckets[i]++
	}
	s.sum += v
	s.count++
}

// Count is how many values were observed for the label values.
func (h *HistogramVec) Count(values ...string) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s, ok := h.series[strings.Join(values, "\xff")]; ok {
		return s.count
	}
	return 0
}

func (h *HistogramVec) family() Family {
	h.mu.Lock()
	defer h.mu.Unlock()
	f := Family{Name: h.name, Help: h.help, Type: HistogramType}
	for _, s := range h.sorted() {
		var cumulative uint64
		for i, bound := range h.bounds {
			if s.buckets != nil {
				cumulative += s.buckets[i]
			}
			f.Samples = append(f.Samples, Sample{
				Name: h.name + "_bucket", Labels: h.labelsOf(s, Label{Name: "le", Value: formatFloat(bound)}), Value: float64(cumulative),
			})
		}
		f.Samples = append(f.Samples,
			Sample{Name: h.name + "_bucket", Labels: h.labelsOf(s, Label{Name: "le", Value: "+Inf"}), Value: float64(s.count)},
			Sample{Name: h.name + "_sum", Labels: h.labelsOf(s), Value: s.sum},
			Sample{Name: h.name + "_count", Labels: h.labelsOf(s), Value: float64(s.count)},
		)
	}
	return f
}
