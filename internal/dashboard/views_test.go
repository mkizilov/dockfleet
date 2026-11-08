package dashboard

import (
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestFormatBytes(t *testing.T) {
	for _, tt := range []struct {
		n    int64
		want string
	}{
		{0, "0 B"}, {1, "1 B"}, {1023, "1023 B"}, {1024, "1 KiB"}, {1500, "1.5 KiB"}, {1536, "1.5 KiB"}, {128 << 20, "128 MiB"},
		{384 << 20, "384 MiB"}, {8 << 30, "8 GiB"}, {24 << 30, "24 GiB"}, {1 << 40, "1 TiB"}, {5 << 40, "5 TiB"}, {1 << 50, "1024 TiB"}, {-5, "-5 B"},
	} {
		if got := formatBytes(tt.n); got != tt.want {
			t.Errorf("formatBytes(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}

func TestFormatCPU(t *testing.T) {
	for millis, want := range map[int64]string{0: "0 CPU", 1: "0.001 CPU", 100: "0.1 CPU", 250: "0.25 CPU", 500: "0.5 CPU", 1000: "1 CPU", 1500: "1.5 CPU", 12000: "12 CPU"} {
		if got := formatCPU(millis); got != want {
			t.Errorf("formatCPU(%d) = %q, want %q", millis, got, want)
		}
	}
}

func TestAgo(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	d := &Dashboard{cfg: Config{Now: func() time.Time { return now }}}
	for name, tt := range map[string]struct {
		at   time.Time
		want string
	}{
		"never":              {time.Time{}, "never"},
		"before the epoch":   {time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), "never"},
		"just now":           {now.Add(-2 * time.Second), "just now"},
		"in the future":      {now.Add(time.Minute), "just now"},
		"five seconds":       {now.Add(-5 * time.Second), "5s ago"},
		"under a minute":     {now.Add(-59 * time.Second), "59s ago"},
		"a minute":           {now.Add(-60 * time.Second), "1m ago"},
		"under an hour":      {now.Add(-59 * time.Minute), "59m ago"},
		"an hour":            {now.Add(-time.Hour), "1h ago"},
		"under two days":     {now.Add(-47 * time.Hour), "47h ago"},
		"two days":           {now.Add(-48 * time.Hour), "2d ago"},
		"a little over four": {now.Add(-100 * time.Hour), "4d ago"},
	} {
		if got := d.ago(tt.at); got != tt.want {
			t.Errorf("%s: ago = %q, want %q", name, got, tt.want)
		}
	}
}

func TestBadgeClass(t *testing.T) {
	for _, tt := range []struct{ state, want string }{
		{"healthy", "ok"}, {"running", "ok"}, {"complete", "ok"}, {"stable", "ok"}, {"ready", "ok"},
		{"suspect", "warn"}, {"starting", "warn"}, {"progressing", "warn"}, {"paused", "warn"}, {"scaling_up", "warn"}, {"updating", "warn"},
		{"unavailable", "bad"}, {"failed", "bad"}, {"unhealthy", "bad"}, {"blocked", "bad"}, {"degraded", "bad"}, {"unschedulable", "bad"},
		{"", ""}, {"something-new", ""},
	} {
		if got := badgeClass(tt.state); got != tt.want {
			t.Errorf("badgeClass(%q) = %q, want %q", tt.state, got, tt.want)
		}
	}
}

func TestKeyValuesAreSorted(t *testing.T) {
	if got := keyValues(map[string]string{"zone": "a", "tier": "gold", "arch": "arm64"}); !reflect.DeepEqual(got, []string{"arch=arm64", "tier=gold", "zone=a"}) {
		t.Errorf("keyValues = %v", got)
	}
	if got := keyValues(nil); len(got) != 0 {
		t.Errorf("keyValues(nil) = %v", got)
	}
}

func TestDerefReadsOptionalValues(t *testing.T) {
	when, exit := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC), 3
	var noTime *time.Time
	var noExit *int
	for name, tt := range map[string]struct{ in, want any }{
		"a time":         {&when, when},
		"a missing time": {noTime, time.Time{}},
		"a number":       {&exit, 3},
		"a missing one":  {noExit, 0},
		"anything else":  {"text", "text"},
	} {
		if got := deref(tt.in); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%s: deref = %#v, want %#v", name, got, tt.want)
		}
	}
}

func TestSummarizeCountsWhatNeedsAttention(t *testing.T) {
	node := func(health string, cpu, allocated int64) apiNode {
		n := apiNode{Health: health}
		n.Allocatable.CPUMillis, n.Allocatable.MemoryBytes = cpu, cpu<<20
		n.Allocated.CPUMillis, n.Allocated.MemoryBytes = allocated, allocated<<20
		return n
	}
	service := func(replicas, ready, failed, unhealthy, lost, waiting int, rollout string) apiService {
		var s apiService
		s.Replicas = replicas
		s.Status.Scaling.Ready = ready
		s.Status.FailedReplicas, s.Status.UnhealthyReplicas, s.Status.LostReplicas, s.Status.UnschedulableReplicas = failed, unhealthy, lost, waiting
		s.Status.Rollout.State = rollout
		return s
	}
	got := summarize(
		[]apiNode{node("healthy", 4000, 1000), node("healthy", 4000, 500), node("suspect", 2000, 0), node("unavailable", 2000, 300), node("", 1000, 0)},
		[]apiService{service(3, 2, 1, 0, 0, 0, "progressing"), service(4, 1, 0, 1, 1, 1, "failed"), service(0, 0, 0, 0, 0, 0, "complete")},
	)
	want := summary{
		NodesHealthy: 2, NodesSuspect: 1, NodesUnavailable: 1,
		CPUAllocatable: 13000, CPUAllocated: 1800, MemAllocatable: 13000 << 20, MemAllocated: 1800 << 20,
		Services: 3, ReplicasDesired: 7, ReplicasRunning: 3, ReplicasProblem: 1 + 1 + 1 + 1, RolloutsFailed: 1,
	}
	if got != want {
		t.Errorf("summary = %+v\nwant      %+v", got, want)
	}
	if got := summarize(nil, nil); got != (summary{}) {
		t.Errorf("an empty cluster = %+v", got)
	}
}

func TestSortNodesPutsTheOnesThatNeedAttentionFirstAndKeepsTheRestInOrder(t *testing.T) {
	nodes := []apiNode{{ID: "a", Health: "healthy"}, {ID: "b", Health: "suspect"}, {ID: "c", Health: "healthy"}, {ID: "d", Health: "unavailable"}, {ID: "e", Health: "suspect"}}
	sortNodes(nodes)
	var ids string
	for _, n := range nodes {
		ids += n.ID
	}
	if ids != "dbeac" {
		t.Errorf("order = %s, want dbeac (unavailable, then suspect, then healthy; stable within each)", ids)
	}
}

func TestSafeNext(t *testing.T) {
	for _, tt := range []struct{ next, want string }{
		{"/dashboard", "/dashboard"}, {"/dashboard/events?before=3", "/dashboard/events?before=3"}, {"/dashboard?x=1", "/dashboard?x=1"},
		{"", "/dashboard"}, {"/", "/dashboard"}, {"//evil.example", "/dashboard"}, {"http://evil.example", "/dashboard"},
		{"/dashboard/login", "/dashboard"}, {"/dashboard/login?next=/x", "/dashboard"}, {"/dashboardx", "/dashboard"},
		{"/dashboard\\..", "/dashboard"}, {"/dashboard/a\rb", "/dashboard"}, {"/dashboard/a\nb", "/dashboard"}, {"/dashboard/a\x00b", "/dashboard"},
	} {
		if got := safeNext(tt.next); got != tt.want {
			t.Errorf("safeNext(%q) = %q, want %q", tt.next, got, tt.want)
		}
	}
}

func TestSameOrigin(t *testing.T) {
	for name, tt := range map[string]struct {
		host    string
		headers map[string]string
		want    bool
	}{
		"no browser headers":        {"example.com", nil, true},
		"the same origin":           {"example.com", map[string]string{"Origin": "http://example.com"}, true},
		"the same origin with port": {"example.com:21280", map[string]string{"Origin": "http://example.com:21280"}, true},
		"another host":              {"example.com", map[string]string{"Origin": "https://evil.example"}, false},
		"another port":              {"example.com", map[string]string{"Origin": "http://example.com:8080"}, false},
		"an origin that is no URL":  {"example.com", map[string]string{"Origin": "://bad"}, false},
		"null origin":               {"example.com", map[string]string{"Origin": "null"}, false},
		"fetch same-origin":         {"example.com", map[string]string{"Sec-Fetch-Site": "same-origin"}, true},
		"fetch none":                {"example.com", map[string]string{"Sec-Fetch-Site": "none"}, true},
		"fetch cross-site":          {"example.com", map[string]string{"Sec-Fetch-Site": "cross-site"}, false},
		"fetch same-site":           {"example.com", map[string]string{"Sec-Fetch-Site": "same-site"}, false},
	} {
		r, err := http.NewRequest(http.MethodPost, "http://"+tt.host+"/dashboard/login", nil)
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range tt.headers {
			r.Header.Set(k, v)
		}
		if got := sameOrigin(r); got != tt.want {
			t.Errorf("%s: sameOrigin = %v, want %v", name, got, tt.want)
		}
	}
}
