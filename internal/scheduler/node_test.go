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
