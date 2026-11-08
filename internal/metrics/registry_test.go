package metrics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func scrape(t *testing.T, r *Registry) string {
	t.Helper()
	var buf bytes.Buffer
	if err := r.Expose(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

// wantLine fails unless the exposition contains the line exactly.
func wantLine(t *testing.T, out, line string) {
	t.Helper()
	for _, l := range strings.Split(out, "\n") {
		if l == line {
			return
		}
	}
	t.Fatalf("exposition has no line %q:\n%s", line, out)
}

func wantNoLine(t *testing.T, out, prefix string) {
	t.Helper()
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, prefix) {
			t.Fatalf("exposition has a line starting with %q: %q", prefix, l)
		}
	}
}

func TestExpositionFormat(t *testing.T) {
	r := NewRegistry()
	requests := r.NewCounterVec("test_requests_total", "Requests.", "method", "code")
	latency := r.NewHistogramVec("test_latency_seconds", "Latency.", []float64{0.1, 1})
	requests.Inc("GET", "200")
	requests.Inc("GET", "200")
	requests.Inc("POST", "500")
	latency.Observe(0.0625)
	latency.Observe(0.5)
	latency.Observe(4)

	want := `# HELP dockfleet_metrics_collect_errors_total Scrapes in which a collector failed, by collector.
# TYPE dockfleet_metrics_collect_errors_total counter
# HELP dockfleet_metrics_dropped_observations_total Observations refused because an instrument already had too many label combinations or was given a bad one.
# TYPE dockfleet_metrics_dropped_observations_total counter
dockfleet_metrics_dropped_observations_total 0
# HELP test_latency_seconds Latency.
# TYPE test_latency_seconds histogram
test_latency_seconds_bucket{le="0.1"} 1
test_latency_seconds_bucket{le="1"} 2
test_latency_seconds_bucket{le="+Inf"} 3
test_latency_seconds_sum 4.5625
test_latency_seconds_count 3
# HELP test_requests_total Requests.
# TYPE test_requests_total counter
test_requests_total{method="GET",code="200"} 2
test_requests_total{method="POST",code="500"} 1
`
	if got := scrape(t, r); got != want {
		t.Fatalf("exposition =\n%s\nwant\n%s", got, want)
	}
	if again := scrape(t, r); again != want {
		t.Fatal("two scrapes of the same state differ")
	}
}

func TestHistogramBucketsAreCumulativeAndInclusive(t *testing.T) {
	r := NewRegistry()
	h := r.NewHistogramVec("test_seconds", "H.", []float64{1, 2, 4}, "route")
	for _, v := range []float64{1, 1.5, 2, 3, 4, 4.5, 100} {
		h.Observe(v, "/x")
	}
	h.Observe(0.5, "/y")
	out := scrape(t, r)
	// A value equal to a bound counts in that bucket, and buckets are cumulative.
	wantLine(t, out, `test_seconds_bucket{route="/x",le="1"} 1`)
	wantLine(t, out, `test_seconds_bucket{route="/x",le="2"} 3`)
	wantLine(t, out, `test_seconds_bucket{route="/x",le="4"} 5`)
	wantLine(t, out, `test_seconds_bucket{route="/x",le="+Inf"} 7`)
	wantLine(t, out, `test_seconds_sum{route="/x"} 116`)
	wantLine(t, out, `test_seconds_count{route="/x"} 7`)
	wantLine(t, out, `test_seconds_bucket{route="/y",le="1"} 1`)
	wantLine(t, out, `test_seconds_count{route="/y"} 1`)
	if h.Count("/x") != 7 || h.Count("/nothing") != 0 {
		t.Fatalf("counts = %d, %d", h.Count("/x"), h.Count("/nothing"))
	}
}

func TestLabelValuesAndHelpAreEscaped(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("test_total", "Line one\nline two with a \\ backslash.", "v")
	c.Inc("a\"b\\c\nd")
	c.Inc("bad \xff utf8")
	out := scrape(t, r)
	wantLine(t, out, `# HELP test_total Line one\nline two with a \\ backslash.`)
	wantLine(t, out, `test_total{v="a\"b\\c\nd"} 1`)
	wantLine(t, out, "test_total{v=\"bad \uFFFD utf8\"} 1")
}

func TestCountersOnlyGoUpAndRefuseWhatDoesNotFit(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("test_total", "C.", "a")
	c.Add(2.5, "x")
	// Three refusals: a counter does not go down, and the label values must match the labels.
	c.Add(-1, "x")
	c.Add(1, "x", "too many")
	c.Add(1)
	c.Inc("x")
	if got := c.Value("x"); got != 3.5 {
		t.Fatalf("value = %v, want 3.5", got)
	}
	if r.Dropped() != 3 {
		t.Fatalf("dropped = %d, want 3", r.Dropped())
	}
	wantLine(t, scrape(t, r), "dockfleet_metrics_dropped_observations_total 3")
}

func TestAnInstrumentKeepsABoundedNumberOfSeries(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("test_total", "C.", "id")
	h := r.NewHistogramVec("test_seconds", "H.", []float64{1}, "id")
	for i := 0; i < MaxSeries+7; i++ {
		c.Inc(fmt.Sprint(i))
		h.Observe(0.5, fmt.Sprint(i))
	}
	c.Inc("0") // a series that exists is still updated
	if c.Value("0") != 2 || c.Value(fmt.Sprint(MaxSeries+3)) != 0 {
		t.Fatalf("value of series 0 = %v, of a refused one = %v", c.Value("0"), c.Value(fmt.Sprint(MaxSeries+3)))
	}
	if r.Dropped() != 14 {
		t.Fatalf("dropped = %d, want 14", r.Dropped())
	}
	out := scrape(t, r)
	if n := strings.Count(out, "\ntest_total{"); n != MaxSeries {
		t.Fatalf("%d counter samples, want %d", n, MaxSeries)
	}
}

func TestAFailingCollectorIsCountedAndTheRestOfTheScrapeSurvives(t *testing.T) {
	r := NewRegistry()
	r.NewCounterVec("test_total", "C.").Inc()
	good := func(context.Context) ([]Family, error) {
		f := NewFamily("test_gauge", "G.", GaugeType)
		f.Add(3, "b", "2")
		f.Add(1, "b", "1")
		return []Family{*f}, nil
	}
	r.AddCollector("good", good)
	r.AddCollector("bad", func(context.Context) ([]Family, error) { return nil, errors.New("the store is down") })
	r.AddCollector("clash", func(context.Context) ([]Family, error) {
		return []Family{*NewFamily("test_gauge", "Again.", GaugeType)}, nil
	})

	out := scrape(t, r)
	wantLine(t, out, "test_total 1")
	wantLine(t, out, `test_gauge{b="1"} 1`) // samples of a collector come out sorted
	wantLine(t, out, `test_gauge{b="2"} 3`)
	wantLine(t, out, `dockfleet_metrics_collect_errors_total{collector="bad"} 1`)
	wantLine(t, out, `dockfleet_metrics_collect_errors_total{collector="clash"} 1`)
	wantLine(t, out, `dockfleet_metrics_collect_errors_total{collector="good"} 0`)
	if strings.Count(out, "# TYPE test_gauge") != 1 {
		t.Fatalf("a family appeared twice:\n%s", out)
	}
	wantLine(t, scrape(t, r), `dockfleet_metrics_collect_errors_total{collector="bad"} 2`)
}

func TestACollectorWithTheSameNameIsReplaced(t *testing.T) {
	r := NewRegistry()
	for _, v := range []float64{1, 2} {
		v := v
		r.AddCollector("same", func(context.Context) ([]Family, error) {
			f := NewFamily("test_gauge", "G.", GaugeType)
			f.Add(v)
			return []Family{*f}, nil
		})
	}
	wantLine(t, scrape(t, r), "test_gauge 2")
}

func TestCollectorsReceiveTheScrapeContext(t *testing.T) {
	type key struct{}
	r := NewRegistry()
	r.AddCollector("ctx", func(ctx context.Context) ([]Family, error) {
		f := NewFamily("test_seen", "S.", GaugeType)
		if ctx.Value(key{}) == "yes" {
			f.Add(1)
		}
		return []Family{*f}, nil
	})
	var buf bytes.Buffer
	if err := r.Expose(context.WithValue(context.Background(), key{}, "yes"), &buf); err != nil {
		t.Fatal(err)
	}
	wantLine(t, buf.String(), "test_seen 1")
}

func TestRegistrationMistakesPanic(t *testing.T) {
	mustPanic := func(what string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s did not panic", what)
			}
		}()
		fn()
	}
	r := NewRegistry()
	r.NewCounterVec("test_total", "C.")
	mustPanic("a duplicate name", func() { r.NewCounterVec("test_total", "C.") })
	mustPanic("a bad metric name", func() { r.NewCounterVec("1bad", "C.") })
	mustPanic("a bad label name", func() { r.NewCounterVec("test_a_total", "C.", "bad-label") })
	mustPanic("a reserved label name", func() { r.NewCounterVec("test_b_total", "C.", "__x") })
	mustPanic("le on a histogram", func() { r.NewHistogramVec("test_c_seconds", "H.", []float64{1}, "le") })
	mustPanic("no buckets", func() { r.NewHistogramVec("test_d_seconds", "H.", nil) })
	mustPanic("unordered buckets", func() { r.NewHistogramVec("test_e_seconds", "H.", []float64{2, 1}) })
	mustPanic("repeated buckets", func() { r.NewHistogramVec("test_f_seconds", "H.", []float64{1, 1}) })
}

func TestInstrumentsWithoutLabelsStartAtZero(t *testing.T) {
	r := NewRegistry()
	r.NewCounterVec("test_errors_total", "E.")
	r.NewHistogramVec("test_seconds", "H.", []float64{1})
	out := scrape(t, r)
	wantLine(t, out, "test_errors_total 0")
	wantLine(t, out, `test_seconds_bucket{le="1"} 0`)
	wantLine(t, out, `test_seconds_bucket{le="+Inf"} 0`)
	wantLine(t, out, "test_seconds_count 0")
}

func TestConcurrentUse(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("test_total", "C.", "w")
	h := r.NewHistogramVec("test_seconds", "H.", []float64{1}, "w")
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				c.Inc(fmt.Sprint(w))
				h.Observe(0.1, fmt.Sprint(w))
				if i%100 == 0 {
					_ = r.Expose(context.Background(), &bytes.Buffer{})
				}
			}
		}()
	}
	wg.Wait()
	for w := 0; w < 8; w++ {
		if c.Value(fmt.Sprint(w)) != 500 || h.Count(fmt.Sprint(w)) != 500 {
			t.Fatalf("worker %d: counter %v, histogram %d", w, c.Value(fmt.Sprint(w)), h.Count(fmt.Sprint(w)))
		}
	}
}

func TestFormatFloat(t *testing.T) {
	for v, want := range map[float64]string{0: "0", 1: "1", 0.005: "0.005", 1.5e9: "1.5e+09", -2: "-2", 123456789: "1.23456789e+08"} {
		if got := formatFloat(v); got != want {
			t.Errorf("formatFloat(%v) = %q, want %q", v, got, want)
		}
	}
}
