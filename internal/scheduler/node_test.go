package scheduler

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/store"
)

var epoch = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func TestNodeHealthFollowsHowLongTheNodeHasBeenSilent(t *testing.T) {
	const suspect, gone = 30 * time.Second, 2 * time.Minute
	timing := Timing{SuspectAfter: suspect, UnavailableAfter: gone}
	registered := epoch.Add(-time.Hour)

	tests := []struct {
		name   string
		node   store.Node
		now    time.Time
		timing Timing
		want   string
	}{
		{"a heartbeat just now", store.Node{LastHeartbeat: epoch, RegisteredAt: registered}, epoch, timing, store.NodeHealthy},
		{"exactly at the suspect limit", store.Node{LastHeartbeat: epoch.Add(-suspect), RegisteredAt: registered}, epoch, timing, store.NodeHealthy},
		{"a moment past it", store.Node{LastHeartbeat: epoch.Add(-suspect - time.Nanosecond), RegisteredAt: registered}, epoch, timing, store.NodeSuspect},
		{"silent for a minute", store.Node{LastHeartbeat: epoch.Add(-time.Minute), RegisteredAt: registered}, epoch, timing, store.NodeSuspect},
		{"exactly at the unavailable limit", store.Node{LastHeartbeat: epoch.Add(-gone), RegisteredAt: registered}, epoch, timing, store.NodeSuspect},
		{"a moment past it", store.Node{LastHeartbeat: epoch.Add(-gone - time.Nanosecond), RegisteredAt: registered}, epoch, timing, store.NodeUnavailable},
		{"silent for a day", store.Node{LastHeartbeat: epoch.Add(-24 * time.Hour), RegisteredAt: registered}, epoch, timing, store.NodeUnavailable},
		{"a heartbeat from the future (clock skew)", store.Node{LastHeartbeat: epoch.Add(time.Hour), RegisteredAt: registered}, epoch, timing, store.NodeHealthy},
		{"never heard from, registered long ago", store.Node{RegisteredAt: registered}, epoch, timing, store.NodeUnavailable},
		// An agent that left cleanly is suspect at once and unavailable when it has been gone long enough.
		{"left a moment ago", store.Node{DeregisteredAt: epoch, RegisteredAt: registered}, epoch, timing, store.NodeSuspect},
		{"left a minute ago", store.Node{DeregisteredAt: epoch.Add(-time.Minute), RegisteredAt: registered}, epoch, timing, store.NodeSuspect},
		{"left three minutes ago", store.Node{DeregisteredAt: epoch.Add(-3 * time.Minute), RegisteredAt: registered}, epoch, timing, store.NodeUnavailable},
		// Nodes are never given up on without a limit.
		{"no unavailable limit", store.Node{LastHeartbeat: epoch.Add(-24 * time.Hour), RegisteredAt: registered}, epoch, Timing{SuspectAfter: suspect}, store.NodeSuspect},
		// A manager that began to watch a minute ago cannot call a node unavailable for being silent before that.
		{"silence from before the manager was watching", store.Node{LastHeartbeat: epoch.Add(-time.Hour), RegisteredAt: registered}, epoch,
			Timing{SuspectAfter: suspect, UnavailableAfter: gone, Since: epoch.Add(-time.Minute)}, store.NodeSuspect},
		{"and once it has watched long enough", store.Node{LastHeartbeat: epoch.Add(-time.Hour), RegisteredAt: registered}, epoch.Add(gone),
			Timing{SuspectAfter: suspect, UnavailableAfter: gone, Since: epoch.Add(-time.Minute)}, store.NodeUnavailable},
		{"a node that spoke after the manager began to watch", store.Node{LastHeartbeat: epoch.Add(-gone - time.Second), RegisteredAt: registered}, epoch,
			Timing{SuspectAfter: suspect, UnavailableAfter: gone, Since: epoch.Add(-gone - time.Minute)}, store.NodeUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.node.ID = "n"
			if got := tt.timing.Health(tt.node, tt.now); got != tt.want {
				t.Fatalf("Health = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLostNodesAreTheUnavailableOnes(t *testing.T) {
	timing := Timing{SuspectAfter: 30 * time.Second, UnavailableAfter: 2 * time.Minute}
	nodes := []store.Node{
		{ID: "alive", LastHeartbeat: epoch, RegisteredAt: epoch},
		{ID: "late", LastHeartbeat: epoch.Add(-time.Minute), RegisteredAt: epoch.Add(-time.Hour)},
		{ID: "gone", LastHeartbeat: epoch.Add(-time.Hour), RegisteredAt: epoch.Add(-2 * time.Hour)},
	}
	lost := timing.Lost(nodes, epoch)
	if len(lost) != 1 || !lost["gone"] {
		t.Fatalf("lost = %v, want only gone", lost)
	}
}

func TestHealthReasonsNameTheLimitsNotTheAge(t *testing.T) {
	timing := Timing{SuspectAfter: 30 * time.Second, UnavailableAfter: 2 * time.Minute}
	if got := timing.HealthReason(store.NodeHealthy); got != "" {
		t.Errorf("healthy: %q", got)
	}
	for state, want := range map[string]string{store.NodeSuspect: "30s", store.NodeUnavailable: "2m0s"} {
		if got := timing.HealthReason(state); !strings.Contains(got, want) {
			t.Errorf("%s: %q does not name %s", state, got, want)
		}
	}
}

func TestUnavailability(t *testing.T) {
	const ttl = 30 * time.Second
	tests := []struct {
		name      string
		heartbeat time.Time
		now       time.Time
		want      string
	}{
		{"a heartbeat just now", epoch, epoch, ""},
		{"a recent heartbeat", epoch.Add(-10 * time.Second), epoch, ""},
		{"exactly at the limit", epoch.Add(-ttl), epoch, ""},
		{"a moment past the limit", epoch.Add(-ttl - time.Nanosecond), epoch, "no heartbeat within the last 30s"},
		{"long silent", epoch.Add(-48 * time.Hour), epoch, "no heartbeat within the last 30s"},
		{"a heartbeat from the future (clock skew)", epoch.Add(time.Hour), epoch, ""},
		{"never heartbeated", time.Time{}, epoch, "the node has deregistered or has never sent a heartbeat"},
		{"deregistered", time.Time{}, epoch.Add(time.Hour), "the node has deregistered or has never sent a heartbeat"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := store.Node{ID: "n", LastHeartbeat: tt.heartbeat}
			if got := Unavailability(n, tt.now, ttl); got != tt.want {
				t.Fatalf("Unavailability = %q, want %q", got, tt.want)
			}
			if Ready(n, tt.now, ttl) != (tt.want == "") {
				t.Fatalf("Ready = %v for %q", Ready(n, tt.now, ttl), tt.want)
			}
		})
	}

	// The text is the same however long the node has been silent, so a stored
	// explanation does not change from pass to pass.
	n := store.Node{ID: "n", LastHeartbeat: epoch}
	if a, b := Unavailability(n, epoch.Add(time.Minute), ttl), Unavailability(n, epoch.Add(72*time.Hour), ttl); a != b || a == "" {
		t.Fatalf("explanations differ over time: %q vs %q", a, b)
	}
}

func TestNodeFor(t *testing.T) {
	stored := store.Node{ID: "n1", CPUMillis: 4000, MemoryBytes: 8 << 30, Labels: map[string]string{"zone": "a"}, LastHeartbeat: epoch}
	got := NodeFor(stored, epoch.Add(10*time.Second), 30*time.Second)
	want := Node{ID: "n1", CPUMillis: 4000, MemoryBytes: 8 << 30, Labels: map[string]string{"zone": "a"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("NodeFor = %+v, want %+v", got, want)
	}
	if got := NodeFor(stored, epoch.Add(time.Hour), 30*time.Second); got.Unavailable == "" {
		t.Fatalf("a silent node must be unavailable: %+v", got)
	}
}

func TestAssignmentFrom(t *testing.T) {
	ports := []store.ServicePort{{ContainerPort: 80, Protocol: "tcp", HostPort: 8080}}
	w := store.Workload{ID: "w1", Name: "web-1", NodeID: "n1", CPUMillis: 500, MemoryBytes: 1 << 20, ServiceID: "svc", Ports: ports}
	tests := []struct {
		name    string
		desired string
		wantOK  bool
	}{
		{"an unset desired state means running", "", true},
		{"running", store.DesiredRunning, true},
		{"stopped reserves nothing", store.DesiredStopped, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w.DesiredState = tt.desired
			a, ok := AssignmentFrom(w)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && (a.Name != "web-1" || a.NodeID != "n1" || a.Group != "svc" || a.CPUMillis != 500 || a.MemoryBytes != 1<<20 || !reflect.DeepEqual(a.Ports, ports)) {
				t.Fatalf("assignment = %+v", a)
			}
		})
	}

	// What the observed state says does not matter: a failed container is still meant to run.
	failed := w
	failed.DesiredState = store.DesiredRunning
	failed.Observed = &store.Observation{State: store.WorkloadFailed}
	if _, ok := AssignmentFrom(failed); !ok {
		t.Fatal("a failed workload that is meant to run still reserves its resources")
	}
}

func TestUsedByNode(t *testing.T) {
	workloads := []store.Workload{
		{NodeID: "a", CPUMillis: 100, MemoryBytes: 10},
		{NodeID: "a", CPUMillis: 200, MemoryBytes: 20, DesiredState: store.DesiredRunning},
		{NodeID: "a", CPUMillis: 5000, MemoryBytes: 5000, DesiredState: store.DesiredStopped},
		{NodeID: "b", CPUMillis: 7, MemoryBytes: 70},
	}
	got := UsedByNode(workloads)
	want := map[string]Used{"a": {CPUMillis: 300, MemoryBytes: 30, Workloads: 2}, "b": {CPUMillis: 7, MemoryBytes: 70, Workloads: 1}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("UsedByNode = %+v, want %+v", got, want)
	}
	if len(UsedByNode(nil)) != 0 {
		t.Fatal("no workloads, nothing used")
	}
}

// The scheduler decides; it does not act. Keeping its imports to the standard
// library's pure parts and the store's data types means a decision never needs
// a database, a socket or a Docker daemon, and the same snapshot always gives
// the same answer. Adding an import here is a design decision, so it is listed.
func TestSchedulerStaysPure(t *testing.T) {
	allowed := map[string]bool{
		"fmt": true, "math": true, "math/bits": true, "sort": true, "strings": true, "time": true,
		"github.com/dockfleet/dockfleet/internal/store": true,
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		checked++
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, spec := range parsed.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if !allowed[path] {
				t.Errorf("%s imports %s: the scheduler must stay free of I/O, Docker and the clock", file, path)
			}
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "time" && sel.Sel.Name == "Now" {
					t.Errorf("%s reads the clock with time.Now; pass the time in instead", file)
				}
			}
			return true
		})
	}
	if checked < 3 {
		t.Fatalf("checked only %d source files; the test is not looking at the package", checked)
	}
}
