package scheduler

import (
	"math"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/dockfleet/dockfleet/internal/store"
)

const (
	mib = int64(1) << 20
	gib = int64(1) << 30

	down = "no heartbeat within the last 30s"
)

// nd builds a node; labels are "key=value" strings.
func nd(id string, cpu, mem int64, labels ...string) Node {
	node := Node{ID: id, CPUMillis: cpu, MemoryBytes: mem}
	if len(labels) > 0 {
		node.Labels = map[string]string{}
		for _, l := range labels {
			k, v, _ := strings.Cut(l, "=")
			node.Labels[k] = v
		}
	}
	return node
}

func off(n Node) Node {
	n.Unavailable = down
	return n
}

// rp builds replica number index of a service; its group is the service.
func rp(service string, index int, cpu, mem int64) Replica {
	return Replica{Service: service, Group: service, Index: index, CPUMillis: cpu, MemoryBytes: mem}
}

func selecting(r Replica, labels ...string) Replica {
	r.NodeSelector = map[string]string{}
	for _, l := range labels {
		k, v, _ := strings.Cut(l, "=")
		r.NodeSelector[k] = v
	}
	return r
}

func named(r Replica, name string) Replica {
	r.Name = name
	return r
}

func publishing(r Replica, ports ...store.ServicePort) Replica {
	r.Ports = ports
	return r
}

// as builds an existing assignment that belongs to no service.
func as(name, node string, cpu, mem int64, ports ...store.ServicePort) Assignment {
	return Assignment{Name: name, NodeID: node, CPUMillis: cpu, MemoryBytes: mem, Ports: ports}
}

func hostPort(port int, protocol string) store.ServicePort {
	return store.ServicePort{ContainerPort: port, Protocol: protocol, HostPort: port}
}

// place schedules one replica.
func place(t *testing.T, in Input) Decision {
	t.Helper()
	got := Schedule(in)
	if len(got) != 1 {
		t.Fatalf("Schedule returned %d decisions for %d replicas", len(got), len(in.Replicas))
	}
	return got[0]
}

func TestPlacingOneReplica(t *testing.T) {
	tests := []struct {
		name       string
		nodes      []Node
		assigned   []Assignment
		replica    Replica
		overcommit Overcommit
		wantNode   string // empty: the replica cannot be placed
		wantCode   string
	}{
		// where it goes
		{name: "the only node has room", nodes: []Node{nd("a", 4000, 8*gib)}, replica: rp("web", 1, 500, 128*mib), wantNode: "a"},
		{name: "the node with the larger free share wins",
			nodes:    []Node{nd("a", 4000, 8*gib), nd("b", 4000, 8*gib)},
			assigned: []Assignment{as("x", "a", 3000, 1*gib)},
			replica:  rp("web", 1, 500, 128*mib), wantNode: "b"},
		{name: "equal nodes tie on ID, whatever order they come in",
			nodes:   []Node{nd("c", 4000, 8*gib), nd("a", 4000, 8*gib), nd("b", 4000, 8*gib)},
			replica: rp("web", 1, 500, 128*mib), wantNode: "a"},
		{name: "free share beats free amount",
			nodes:    []Node{nd("big", 64000, 64*gib), nd("small", 1000, 1*gib)},
			assigned: []Assignment{as("x", "big", 40000, 40*gib)},
			replica:  rp("web", 1, 500, 128*mib), wantNode: "small"},

		// node availability
		{name: "an offline node is skipped even when it is the emptiest",
			nodes:   []Node{off(nd("big", 64000, 64*gib)), nd("small", 1000, 1*gib)},
			replica: rp("web", 1, 500, 128*mib), wantNode: "small"},
		{name: "every node offline", nodes: []Node{off(nd("a", 4000, 8*gib)), off(nd("b", 4000, 8*gib))},
			replica: rp("web", 1, 500, 128*mib), wantCode: CodeNoAvailableNodes},
		{name: "no nodes at all", replica: rp("web", 1, 500, 128*mib), wantCode: CodeNoNodes},

		// resource exhaustion
		{name: "CPU exhausted", nodes: []Node{nd("a", 1000, 8*gib)}, assigned: []Assignment{as("x", "a", 800, 0)},
			replica: rp("web", 1, 500, 128*mib), wantCode: CodeInsufficientResources},
		{name: "memory exhausted", nodes: []Node{nd("a", 4000, 1*gib)}, assigned: []Assignment{as("x", "a", 0, 900*mib)},
			replica: rp("web", 1, 500, 200*mib), wantCode: CodeInsufficientResources},
		{name: "CPU and memory exhausted", nodes: []Node{nd("a", 1000, 1*gib)}, assigned: []Assignment{as("x", "a", 900, 1000*mib)},
			replica: rp("web", 1, 500, 200*mib), wantCode: CodeInsufficientResources},
		{name: "an exact fit is allowed", nodes: []Node{nd("a", 1000, 1*gib)}, assigned: []Assignment{as("x", "a", 500, 512*mib)},
			replica: rp("web", 1, 500, 512*mib), wantNode: "a"},
		{name: "one millicore over", nodes: []Node{nd("a", 1000, 1*gib)}, assigned: []Assignment{as("x", "a", 500, 0)},
			replica: rp("web", 1, 501, 1*mib), wantCode: CodeInsufficientResources},
		{name: "one byte over", nodes: []Node{nd("a", 1000, 1*gib)}, assigned: []Assignment{as("x", "a", 0, 512*mib)},
			replica: rp("web", 1, 100, 512*mib+1), wantCode: CodeInsufficientResources},
		{name: "several assignments add up", nodes: []Node{nd("a", 1000, 8*gib)},
			assigned: []Assignment{as("x", "a", 300, 0), as("y", "a", 300, 0), as("z", "a", 300, 0)},
			replica:  rp("web", 1, 200, 1*mib), wantCode: CodeInsufficientResources},
		{name: "larger than any node can ever give", nodes: []Node{nd("a", 4000, 8*gib), nd("b", 2000, 16*gib)},
			replica: rp("web", 1, 8000, 1*gib), wantCode: CodeExceedsCapacity},
		{name: "too big for now but not for an idle node", nodes: []Node{nd("a", 4000, 8*gib)}, assigned: []Assignment{as("x", "a", 3800, 0)},
			replica: rp("web", 1, 500, 128*mib), wantCode: CodeInsufficientResources},
		{name: "assignments on unknown nodes reserve nothing", nodes: []Node{nd("a", 1000, 1*gib)}, assigned: []Assignment{as("x", "ghost", 5000, 50*gib)},
			replica: rp("web", 1, 500, 128*mib), wantNode: "a"},

		// oversubscription only when configured
		{name: "CPU overcommit lets it fit", nodes: []Node{nd("a", 1000, 1*gib)}, assigned: []Assignment{as("x", "a", 1000, 0)},
			replica: rp("web", 1, 500, 128*mib), overcommit: Overcommit{CPU: 2}, wantNode: "a"},
		{name: "CPU overcommit does not stretch memory", nodes: []Node{nd("a", 1000, 1*gib)}, assigned: []Assignment{as("x", "a", 1000, 1*gib)},
			replica: rp("web", 1, 500, 128*mib), overcommit: Overcommit{CPU: 2}, wantCode: CodeInsufficientResources},
		{name: "memory overcommit lets it fit", nodes: []Node{nd("a", 4000, 1*gib)}, assigned: []Assignment{as("x", "a", 0, 1*gib)},
			replica: rp("web", 1, 500, 128*mib), overcommit: Overcommit{Memory: 1.5}, wantNode: "a"},
		{name: "overcommit stops at the factor", nodes: []Node{nd("a", 1000, 1*gib)}, assigned: []Assignment{as("x", "a", 1901, 0)},
			replica: rp("web", 1, 100, 1*mib), overcommit: Overcommit{CPU: 2}, wantCode: CodeInsufficientResources},
		{name: "overcommit reaches the factor exactly", nodes: []Node{nd("a", 1000, 1*gib)}, assigned: []Assignment{as("x", "a", 1900, 0)},
			replica: rp("web", 1, 100, 1*mib), overcommit: Overcommit{CPU: 2}, wantNode: "a"},
		{name: "a factor below one is not a reduction", nodes: []Node{nd("a", 1000, 1*gib)},
			replica: rp("web", 1, 800, 128*mib), overcommit: Overcommit{CPU: 0.5, Memory: 0.5}, wantNode: "a"},
		{name: "a NaN factor is ignored", nodes: []Node{nd("a", 1000, 1*gib)},
			replica: rp("web", 1, 800, 128*mib), overcommit: Overcommit{CPU: math.NaN()}, wantNode: "a"},
		{name: "overcommit still cannot beat the scaled capacity", nodes: []Node{nd("a", 1000, 1*gib)},
			replica: rp("web", 1, 2001, 1*mib), overcommit: Overcommit{CPU: 2}, wantCode: CodeExceedsCapacity},

		// placement constraints
		{name: "the matching label picks the node", nodes: []Node{nd("a", 4000, 8*gib, "zone=a"), nd("b", 1000, 1*gib, "zone=b")},
			replica: selecting(rp("web", 1, 500, 128*mib), "zone=b"), wantNode: "b"},
		{name: "every label must match", nodes: []Node{nd("a", 4000, 8*gib, "zone=a", "disk=hdd"), nd("b", 4000, 8*gib, "zone=a", "disk=ssd"), nd("c", 4000, 8*gib, "disk=ssd")},
			replica: selecting(rp("web", 1, 500, 128*mib), "zone=a", "disk=ssd"), wantNode: "b"},
		{name: "a selector no node satisfies", nodes: []Node{nd("a", 4000, 8*gib, "zone=a"), nd("b", 4000, 8*gib)},
			replica: selecting(rp("web", 1, 500, 128*mib), "zone=b"), wantCode: CodeSelectorUnsatisfied},
		{name: "the only matching node is offline", nodes: []Node{off(nd("a", 4000, 8*gib, "zone=b")), nd("b", 4000, 8*gib, "zone=a")},
			replica: selecting(rp("web", 1, 500, 128*mib), "zone=b"), wantCode: CodeSelectorUnsatisfied},
		{name: "the only matching node is full", nodes: []Node{nd("a", 1000, 8*gib, "zone=b"), nd("b", 4000, 8*gib, "zone=a")},
			assigned: []Assignment{as("x", "a", 900, 0)},
			replica:  selecting(rp("web", 1, 500, 128*mib), "zone=b"), wantCode: CodeInsufficientResources},
		{name: "a selector bigger than the matching node", nodes: []Node{nd("a", 1000, 8*gib, "zone=b"), nd("b", 64000, 64*gib, "zone=a")},
			replica: selecting(rp("web", 1, 4000, 128*mib), "zone=b"), wantCode: CodeExceedsCapacity},
		{name: "a selector on an unlabeled node", nodes: []Node{nd("a", 4000, 8*gib)},
			replica: selecting(rp("web", 1, 500, 128*mib), "zone=a"), wantCode: CodeSelectorUnsatisfied},

		// host ports
		{name: "a free host port", nodes: []Node{nd("a", 4000, 8*gib)},
			replica: publishing(rp("web", 1, 500, 128*mib), hostPort(8080, "tcp")), wantNode: "a"},
		{name: "the host port is taken on the only node", nodes: []Node{nd("a", 4000, 8*gib)}, assigned: []Assignment{as("x", "a", 100, 1*mib, hostPort(8080, "tcp"))},
			replica: publishing(rp("web", 1, 500, 128*mib), hostPort(8080, "tcp")), wantCode: CodeHostPortConflict},
		{name: "the host port is free on the other node", nodes: []Node{nd("a", 4000, 8*gib), nd("b", 1000, 1*gib)}, assigned: []Assignment{as("x", "a", 100, 1*mib, hostPort(8080, "tcp"))},
			replica: publishing(rp("web", 1, 500, 128*mib), hostPort(8080, "tcp")), wantNode: "b"},
		{name: "the same number on another protocol is free", nodes: []Node{nd("a", 4000, 8*gib)}, assigned: []Assignment{as("x", "a", 100, 1*mib, hostPort(8080, "udp"))},
			replica: publishing(rp("web", 1, 500, 128*mib), hostPort(8080, "tcp")), wantNode: "a"},
		{name: "udp conflicts with udp", nodes: []Node{nd("a", 4000, 8*gib)}, assigned: []Assignment{as("x", "a", 100, 1*mib, hostPort(53, "udp"))},
			replica: publishing(rp("dns", 1, 100, 1*mib), hostPort(53, "udp")), wantCode: CodeHostPortConflict},
		{name: "an empty protocol means tcp", nodes: []Node{nd("a", 4000, 8*gib)}, assigned: []Assignment{as("x", "a", 100, 1*mib, store.ServicePort{ContainerPort: 80, HostPort: 8080})},
			replica: publishing(rp("web", 1, 500, 128*mib), hostPort(8080, "tcp")), wantCode: CodeHostPortConflict},
		{name: "a port the node chooses never conflicts", nodes: []Node{nd("a", 4000, 8*gib)},
			assigned: []Assignment{as("x", "a", 100, 1*mib, store.ServicePort{ContainerPort: 80, Protocol: "tcp"})},
			replica:  publishing(rp("web", 1, 500, 128*mib), store.ServicePort{ContainerPort: 80, Protocol: "tcp"}), wantNode: "a"},
		{name: "one clashing port among several is enough", nodes: []Node{nd("a", 4000, 8*gib)}, assigned: []Assignment{as("x", "a", 100, 1*mib, hostPort(9090, "tcp"))},
			replica: publishing(rp("web", 1, 500, 128*mib), hostPort(8080, "tcp"), hostPort(9090, "tcp")), wantCode: CodeHostPortConflict},

		// spreading the replicas of one service
		{name: "a node already running the service is passed over",
			nodes:    []Node{nd("a", 4000, 8*gib), nd("b", 4000, 8*gib)},
			assigned: []Assignment{{Name: "web-1", NodeID: "a", Group: "web", CPUMillis: 100, MemoryBytes: 1 * mib}, as("db", "b", 2000, 2*gib)},
			replica:  rp("web", 2, 500, 128*mib), wantNode: "b"},
		{name: "spreading yields to feasibility",
			nodes:    []Node{nd("a", 4000, 8*gib), nd("b", 1000, 1*gib)},
			assigned: []Assignment{{Name: "web-1", NodeID: "a", Group: "web", CPUMillis: 100, MemoryBytes: 1 * mib}, as("db", "b", 900, 0)},
			replica:  rp("web", 2, 500, 128*mib), wantNode: "a"},
		{name: "another service on the node does not count",
			nodes:    []Node{nd("a", 4000, 8*gib), nd("b", 4000, 8*gib)},
			assigned: []Assignment{{Name: "api-1", NodeID: "b", Group: "api", CPUMillis: 3000, MemoryBytes: 1 * mib}},
			replica:  rp("web", 1, 500, 128*mib), wantNode: "a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := place(t, Input{Nodes: tt.nodes, Assignments: tt.assigned, Replicas: []Replica{tt.replica}, Overcommit: tt.overcommit})
			if tt.wantNode != "" {
				if d.NodeID != tt.wantNode || d.Unschedulable != nil || !d.Placed() {
					t.Fatalf("decision = %+v, want node %q", d, tt.wantNode)
				}
				return
			}
			if d.Placed() || d.Unschedulable == nil || d.Unschedulable.Code != tt.wantCode {
				t.Fatalf("decision = %+v, want unschedulable with code %q", d, tt.wantCode)
			}
			if d.Unschedulable.Message == "" || len(d.Unschedulable.Nodes) != len(tt.nodes) {
				t.Fatalf("an explanation needs a message and one reason per node: %+v", d.Unschedulable)
			}
			for _, nr := range d.Unschedulable.Nodes {
				if nr.NodeID == "" || nr.Code == "" || nr.Message == "" {
					t.Errorf("incomplete node reason %+v", nr)
				}
			}
		})
	}
}

func TestPlacingSeveralReplicas(t *testing.T) {
	two := []Node{nd("a", 4000, 8*gib), nd("b", 4000, 8*gib)}
	three := []Node{nd("a", 4000, 8*gib), nd("b", 4000, 8*gib), nd("c", 4000, 8*gib)}
	tests := []struct {
		name     string
		nodes    []Node
		assigned []Assignment
		replicas []Replica
		want     []string // the node of each replica in placement order; "" where it cannot be placed
	}{
		{name: "replicas spread over equal nodes", nodes: three,
			replicas: []Replica{rp("web", 1, 500, 128*mib), rp("web", 2, 500, 128*mib), rp("web", 3, 500, 128*mib)},
			want:     []string{"a", "b", "c"}},
		{name: "more replicas than nodes wrap around", nodes: three,
			replicas: []Replica{rp("web", 1, 500, 128*mib), rp("web", 2, 500, 128*mib), rp("web", 3, 500, 128*mib), rp("web", 4, 500, 128*mib)},
			want:     []string{"a", "b", "c", "a"}},
		{name: "a node fills up and the rest are left over", nodes: []Node{nd("a", 1000, 8*gib)},
			replicas: []Replica{rp("web", 1, 400, 1*mib), rp("web", 2, 400, 1*mib), rp("web", 3, 400, 1*mib)},
			want:     []string{"a", "a", ""}},
		{name: "a bigger node takes more of the replicas", nodes: []Node{nd("small", 1000, 8*gib), nd("big", 4000, 8*gib)},
			replicas: []Replica{rp("web", 1, 500, 1*mib), rp("web", 2, 500, 1*mib), rp("web", 3, 500, 1*mib), rp("web", 4, 500, 1*mib)},
			want:     []string{"big", "small", "big", "small"}},
		{name: "services are placed in name order and each is spread", nodes: two,
			replicas: []Replica{rp("web", 2, 500, 128*mib), rp("api", 1, 500, 128*mib), rp("web", 1, 500, 128*mib), rp("api", 2, 500, 128*mib)},
			want:     []string{"a", "b", "a", "b"}},
		{name: "a replica that does not fit does not hold up the next one", nodes: []Node{nd("a", 2000, 8*gib)},
			replicas: []Replica{rp("small", 1, 500, 1*mib), rp("big", 1, 3000, 1*mib)},
			want:     []string{"", "a"}},
		{name: "existing assignments count, and spreading beats emptiness", nodes: two,
			assigned: []Assignment{as("x", "a", 900, 0)},
			replicas: []Replica{rp("web", 1, 100, 1*mib), rp("web", 2, 100, 1*mib)},
			want:     []string{"b", "a"}},
		{name: "one fixed host port allows one replica per node", nodes: two,
			replicas: []Replica{
				publishing(rp("web", 1, 500, 128*mib), hostPort(8080, "tcp")),
				publishing(rp("web", 2, 500, 128*mib), hostPort(8080, "tcp")),
				publishing(rp("web", 3, 500, 128*mib), hostPort(8080, "tcp")),
			},
			want: []string{"a", "b", ""}},
		{name: "two services contend for the same host port", nodes: []Node{nd("a", 4000, 8*gib)},
			replicas: []Replica{publishing(rp("api", 1, 500, 128*mib), hostPort(80, "tcp")), publishing(rp("web", 1, 500, 128*mib), hostPort(80, "tcp"))},
			want:     []string{"a", ""}},
		{name: "replicas of different services with different selectors", nodes: []Node{nd("a", 4000, 8*gib, "role=db"), nd("b", 4000, 8*gib, "role=web")},
			replicas: []Replica{selecting(rp("db", 1, 500, 128*mib), "role=db"), selecting(rp("web", 1, 500, 128*mib), "role=web"), selecting(rp("web", 2, 500, 128*mib), "role=db")},
			want:     []string{"a", "b", "a"}},
		{name: "nothing to place", nodes: two, want: []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Schedule(Input{Nodes: tt.nodes, Assignments: tt.assigned, Replicas: tt.replicas})
			nodes := make([]string, 0, len(got))
			for _, d := range got {
				nodes = append(nodes, d.NodeID)
			}
			if !reflect.DeepEqual(nodes, tt.want) {
				t.Fatalf("placed on %q, want %q", nodes, tt.want)
			}
			for _, d := range got {
				if d.Placed() == (d.Unschedulable != nil) {
					t.Errorf("%s: exactly one of a node and an explanation is expected: %+v", d.Replica.Label(), d)
				}
			}
		})
	}
}

// Replicas placed together see each other, so a batch can never promise more
// than a node has.
func TestABatchCannotOversubscribeANode(t *testing.T) {
	nodes := []Node{nd("a", 1000, 1*gib), nd("b", 1000, 1*gib)}
	var replicas []Replica
	for i := 1; i <= 10; i++ {
		replicas = append(replicas, rp("web", i, 300, 100*mib))
	}
	cpu := map[string]int64{}
	placed := 0
	for _, d := range Schedule(Input{Nodes: nodes, Replicas: replicas}) {
		if d.Placed() {
			cpu[d.NodeID] += d.Replica.CPUMillis
			placed++
		}
	}
	if placed != 6 || cpu["a"] != 900 || cpu["b"] != 900 {
		t.Fatalf("placed %d replicas using %v millicores; each 1000m node fits three 300m replicas", placed, cpu)
	}
}

func TestUnschedulableMessages(t *testing.T) {
	tests := []struct {
		name         string
		in           Input
		wantCode     string
		wantMessage  string
		wantNodeText map[string]string // substrings of a node's own reason
	}{
		{name: "no nodes",
			in:       Input{Replicas: []Replica{rp("web", 1, 500, 128*mib)}},
			wantCode: CodeNoNodes, wantMessage: "web#1 cannot be scheduled: no nodes are registered"},
		{name: "every node is offline",
			in:       Input{Nodes: []Node{off(nd("a", 4000, 8*gib)), off(nd("b", 4000, 8*gib))}, Replicas: []Replica{rp("web", 1, 500, 128*mib)}},
			wantCode: CodeNoAvailableNodes, wantMessage: "0/2 nodes can run web#1: 2 node(s) not available",
			wantNodeText: map[string]string{"a": down, "b": down}},
		{name: "a selector nobody satisfies",
			in: Input{Nodes: []Node{off(nd("a", 4000, 8*gib, "zone=b")), nd("b", 4000, 8*gib, "zone=a")},
				Replicas: []Replica{selecting(rp("web", 1, 500, 128*mib), "zone=b")}},
			wantCode:    CodeSelectorUnsatisfied,
			wantMessage: "0/2 nodes can run web#1: 1 node(s) not available; 1 node(s) do not match the node selector {zone=b}",
			wantNodeText: map[string]string{"a": down, "b": `does not match the node selector: label zone is "a" (want "b")`}},
		{name: "a label that is not set",
			in: Input{Nodes: []Node{nd("a", 4000, 8*gib)},
				Replicas: []Replica{selecting(rp("web", 1, 500, 128*mib), "zone=b", "disk=ssd")}},
			wantCode:     CodeSelectorUnsatisfied,
			wantMessage:  "0/1 nodes can run web#1: 1 node(s) do not match the node selector {disk=ssd,zone=b}",
			wantNodeText: map[string]string{"a": `label disk is not set (want "ssd"); label zone is not set (want "b")`}},
		{name: "a host port in use",
			in: Input{Nodes: []Node{nd("a", 4000, 8*gib)}, Assignments: []Assignment{as("db-0", "a", 100, 1*mib, hostPort(5432, "tcp"))},
				Replicas: []Replica{publishing(rp("db", 1, 500, 128*mib), hostPort(5432, "tcp"))}},
			wantCode:     CodeHostPortConflict,
			wantMessage:  "0/1 nodes can run db#1: 1 node(s) already use host port 5432/tcp",
			wantNodeText: map[string]string{"a": "host port 5432/tcp is already used by db-0"}},
		{name: "a replica holding a port is named by the name its workload will have",
			in: Input{Nodes: []Node{nd("a", 4000, 8*gib)},
				Replicas: []Replica{named(publishing(rp("web", 1, 500, 128*mib), hostPort(80, "tcp")), "web-r1-1-abc123"), publishing(rp("web", 2, 500, 128*mib), hostPort(80, "tcp"))}},
			wantCode:     CodeHostPortConflict,
			wantMessage:  "0/1 nodes can run web#2: 1 node(s) already use host port 80/tcp",
			wantNodeText: map[string]string{"a": "host port 80/tcp is already used by web-r1-1-abc123"}},
		{name: "a host port held by a replica placed a moment earlier",
			in: Input{Nodes: []Node{nd("a", 4000, 8*gib)},
				Replicas: []Replica{publishing(rp("web", 1, 500, 128*mib), hostPort(80, "tcp")), publishing(rp("web", 2, 500, 128*mib), hostPort(80, "tcp"))}},
			wantCode:     CodeHostPortConflict,
			wantMessage:  "0/1 nodes can run web#2: 1 node(s) already use host port 80/tcp",
			wantNodeText: map[string]string{"a": "host port 80/tcp is already used by web#1"}},
		{name: "not enough CPU",
			in: Input{Nodes: []Node{nd("a", 1000, 1*gib)}, Assignments: []Assignment{as("x", "a", 800, 0)},
				Replicas: []Replica{rp("web", 1, 500, 128*mib)}},
			wantCode:     CodeInsufficientResources,
			wantMessage:  "0/1 nodes can run web#1: 1 node(s) lack CPU (needs 500m)",
			wantNodeText: map[string]string{"a": "insufficient resources (CPU: needs 500m, 200m free)"}},
		{name: "not enough memory",
			in: Input{Nodes: []Node{nd("a", 4000, 1*gib)}, Assignments: []Assignment{as("x", "a", 0, 960*mib)},
				Replicas: []Replica{rp("web", 1, 500, 128*mib)}},
			wantCode:     CodeInsufficientResources,
			wantMessage:  "0/1 nodes can run web#1: 1 node(s) lack memory (needs 128.0 MiB)",
			wantNodeText: map[string]string{"a": "insufficient resources (memory: needs 128.0 MiB, 64.0 MiB free)"}},
		{name: "not enough of either",
			in: Input{Nodes: []Node{nd("a", 1000, 1*gib)}, Assignments: []Assignment{as("x", "a", 900, 960*mib)},
				Replicas: []Replica{rp("web", 1, 500, 128*mib)}},
			wantCode:     CodeInsufficientResources,
			wantMessage:  "0/1 nodes can run web#1: 1 node(s) lack CPU and memory (needs 500m and 128.0 MiB)",
			wantNodeText: map[string]string{"a": "insufficient resources (CPU: needs 500m, 100m free; memory: needs 128.0 MiB, 64.0 MiB free)"}},
		{name: "a replica no node could ever hold",
			in: Input{Nodes: []Node{nd("a", 4000, 8*gib), nd("b", 2000, 16*gib)}, Replicas: []Replica{rp("web", 1, 8000, 1*gib)}},
			wantCode: CodeExceedsCapacity,
			wantMessage: "web#1 needs 8000m CPU and 1.0 GiB memory, more than any eligible node can offer even when idle " +
				"(the largest can give 4000m CPU and 16.0 GiB memory)"},
		{name: "a replica that is not one of several",
			in:       Input{Replicas: []Replica{{Service: "solo", CPUMillis: 1, MemoryBytes: 1}}},
			wantCode: CodeNoNodes, wantMessage: "solo cannot be scheduled: no nodes are registered"},
		{name: "every kind of rejection at once",
			in: Input{
				Nodes: []Node{
					off(nd("a", 4000, 8*gib, "zone=y")),
					nd("b", 4000, 8*gib, "zone=x"),
					nd("c", 4000, 8*gib, "zone=y"),
					nd("d", 4000, 8*gib, "zone=y"),
				},
				Assignments: []Assignment{as("db-0", "c", 100, 1*mib, hostPort(9000, "tcp")), as("busy", "d", 3000, 1*mib)},
				Replicas:    []Replica{publishing(selecting(rp("web", 1, 2000, 128*mib), "zone=y"), hostPort(9000, "tcp"))},
			},
			wantCode: CodeInsufficientResources,
			wantMessage: "0/4 nodes can run web#1: 1 node(s) not available; 1 node(s) do not match the node selector {zone=y}; " +
				"1 node(s) already use host port 9000/tcp; 1 node(s) lack CPU (needs 2000m)",
			wantNodeText: map[string]string{
				"a": down,
				"b": `label zone is "x" (want "y")`,
				"c": "host port 9000/tcp is already used by db-0",
				"d": "CPU: needs 2000m, 1000m free",
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The replica under test is the last one placed; any before it set the scene.
			decisions := Schedule(tt.in)
			d := decisions[len(decisions)-1]
			u := d.Unschedulable
			if d.Placed() || u == nil {
				t.Fatalf("the replica was placed: %+v", d)
			}
			if u.Code != tt.wantCode || u.Message != tt.wantMessage {
				t.Fatalf("code %q, message %q\nwant code %q, message %q", u.Code, u.Message, tt.wantCode, tt.wantMessage)
			}
			for id, want := range tt.wantNodeText {
				found := false
				for _, nr := range u.Nodes {
					if nr.NodeID == id {
						found = true
						if !strings.Contains(nr.Message, want) {
							t.Errorf("node %s: %q does not contain %q", id, nr.Message, want)
						}
					}
				}
				if !found {
					t.Errorf("no reason for node %s in %+v", id, u.Nodes)
				}
			}
		})
	}
}

func TestEachNodeGivesItsOwnReasonInNodeOrder(t *testing.T) {
	d := place(t, Input{
		Nodes: []Node{
			nd("d", 4000, 8*gib, "zone=y"),
			off(nd("a", 4000, 8*gib, "zone=y")),
			nd("c", 4000, 8*gib, "zone=y"),
			nd("b", 4000, 8*gib, "zone=x"),
		},
		Assignments: []Assignment{as("db-0", "c", 100, 1*mib, hostPort(9000, "tcp")), as("busy", "d", 3000, 1*mib)},
		Replicas:    []Replica{publishing(selecting(rp("web", 1, 2000, 128*mib), "zone=y"), hostPort(9000, "tcp"))},
	})
	var got [][2]string
	for _, nr := range d.Unschedulable.Nodes {
		got = append(got, [2]string{nr.NodeID, nr.Code})
	}
	want := [][2]string{{"a", NodeUnavailable}, {"b", NodeSelectorMismatch}, {"c", NodeHostPortInUse}, {"d", NodeInsufficientResources}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("node reasons = %v, want %v", got, want)
	}
}

// What the explanation says must not depend on how long a node has been down,
// or a record of it would change at every pass.
func TestExplanationsAreStable(t *testing.T) {
	in := Input{Nodes: []Node{off(nd("a", 4000, 8*gib))}, Replicas: []Replica{rp("web", 1, 500, 128*mib)}}
	first, second := place(t, in), place(t, in)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("two identical runs explained differently:\n%+v\n%+v", first, second)
	}
}

// fixture is a cluster busy enough that every rule has something to decide.
func fixture() Input {
	return Input{
		Nodes: []Node{
			nd("n1", 8000, 16*gib, "zone=a", "disk=ssd"),
			nd("n2", 8000, 16*gib, "zone=a", "disk=hdd"),
			nd("n3", 4000, 8*gib, "zone=b", "disk=ssd"),
			off(nd("n4", 16000, 32*gib, "zone=b")),
			nd("n5", 2000, 4*gib, "zone=c"),
		},
		Assignments: []Assignment{
			as("db-0", "n1", 2000, 4*gib, hostPort(5432, "tcp")),
			{Name: "web-old", NodeID: "n2", Group: "web", CPUMillis: 1000, MemoryBytes: 1 * gib},
			as("cache", "n3", 3000, 1*gib, hostPort(6379, "tcp")),
		},
		Replicas: []Replica{
			rp("web", 1, 500, 256*mib), rp("web", 2, 500, 256*mib), rp("web", 3, 500, 256*mib), rp("web", 4, 500, 256*mib),
			publishing(rp("proxy", 1, 250, 64*mib), hostPort(80, "tcp")), publishing(rp("proxy", 2, 250, 64*mib), hostPort(80, "tcp")),
			publishing(rp("proxy", 3, 250, 64*mib), hostPort(80, "tcp")), publishing(rp("proxy", 4, 250, 64*mib), hostPort(80, "tcp")),
			selecting(rp("batch", 1, 3000, 4*gib), "disk=ssd"), selecting(rp("batch", 2, 3000, 4*gib), "disk=ssd"),
			selecting(rp("batch", 3, 3000, 4*gib), "disk=ssd"), selecting(rp("gpu", 1, 100, 1*mib), "gpu=true"),
			publishing(rp("kv", 1, 100, 64*mib), hostPort(6379, "tcp")),
		},
		Overcommit: Overcommit{CPU: 1.25},
	}
}

// summarize reduces decisions to what must be reproducible.
func summarize(ds []Decision) []string {
	out := make([]string, 0, len(ds))
	for _, d := range ds {
		line := d.Replica.Label() + " -> " + d.NodeID
		if d.Unschedulable != nil {
			line += "[" + d.Unschedulable.Code + "] " + d.Unschedulable.Message
			for _, nr := range d.Unschedulable.Nodes {
				line += "\n    " + nr.NodeID + " " + nr.Code + ": " + nr.Message
			}
		}
		out = append(out, line)
	}
	return out
}

func TestDecisionsDoNotDependOnInputOrder(t *testing.T) {
	base := fixture()
	decisions := Schedule(base)
	want := summarize(decisions)
	if len(want) != len(base.Replicas) {
		t.Fatalf("%d decisions for %d replicas", len(want), len(base.Replicas))
	}
	placed, refused := 0, 0
	for _, d := range decisions {
		if d.Placed() {
			placed++
		} else {
			refused++
		}
	}
	if placed == 0 || refused == 0 {
		t.Fatalf("the fixture should both place and refuse replicas, got %d and %d:\n%s", placed, refused, strings.Join(want, "\n"))
	}

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 100; i++ {
		in := fixture()
		rng.Shuffle(len(in.Nodes), func(a, b int) { in.Nodes[a], in.Nodes[b] = in.Nodes[b], in.Nodes[a] })
		rng.Shuffle(len(in.Assignments), func(a, b int) { in.Assignments[a], in.Assignments[b] = in.Assignments[b], in.Assignments[a] })
		rng.Shuffle(len(in.Replicas), func(a, b int) { in.Replicas[a], in.Replicas[b] = in.Replicas[b], in.Replicas[a] })
		if got := summarize(Schedule(in)); !reflect.DeepEqual(got, want) {
			t.Fatalf("shuffle %d changed the decisions:\n%s\n--- want ---\n%s", i, strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
	}
	for i := 0; i < 20; i++ {
		if got := summarize(Schedule(fixture())); !reflect.DeepEqual(got, want) {
			t.Fatalf("run %d of the same input gave different decisions", i)
		}
	}
}

func TestScheduleLeavesItsInputAlone(t *testing.T) {
	in := fixture()
	before := fixture()
	Schedule(in)
	if !reflect.DeepEqual(in, before) {
		t.Fatal("Schedule modified its input")
	}
}

func TestPermille(t *testing.T) {
	tests := []struct {
		free, total, want int64
	}{
		{0, 100, 0}, {50, 100, 500}, {100, 100, 1000}, {150, 100, 1000}, {-5, 100, 0}, {5, 0, 0}, {1, 3, 333}, {2, 3, 666},
		{1 << 62, 1<<62 + 1<<61, 666},
		{math.MaxInt64 - 1, math.MaxInt64, 999},
		{math.MaxInt64, math.MaxInt64, 1000},
	}
	for _, tt := range tests {
		if got := permille(tt.free, tt.total); got != tt.want {
			t.Errorf("permille(%d, %d) = %d, want %d", tt.free, tt.total, got, tt.want)
		}
	}
}

func TestAllocatable(t *testing.T) {
	tests := []struct {
		name       string
		overcommit Overcommit
		cpu, mem   int64
		wantCPU    int64
		wantMem    int64
	}{
		{"no overcommit", Overcommit{}, 1000, 1 * gib, 1000, 1 * gib},
		{"a factor of one", Overcommit{CPU: 1, Memory: 1}, 1000, 1 * gib, 1000, 1 * gib},
		{"both raised", Overcommit{CPU: 2, Memory: 1.5}, 1000, 1 * gib, 2000, gib + gib/2},
		{"a fraction is rounded down", Overcommit{CPU: 1.5}, 1001, 1, 1501, 1},
		{"below one is ignored", Overcommit{CPU: 0.1, Memory: -3}, 1000, 1 * gib, 1000, 1 * gib},
		{"NaN is ignored", Overcommit{CPU: math.NaN(), Memory: math.NaN()}, 1000, 1 * gib, 1000, 1 * gib},
		{"nothing stays nothing", Overcommit{CPU: 8, Memory: 8}, 0, -5, 0, 0},
		{"huge values do not wrap", Overcommit{CPU: 8, Memory: math.Inf(1)}, math.MaxInt64, 1 * gib, math.MaxInt64, math.MaxInt64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cpu, mem := tt.overcommit.Allocatable(tt.cpu, tt.mem)
			if cpu != tt.wantCPU || mem != tt.wantMem {
				t.Fatalf("Allocatable = %d, %d; want %d, %d", cpu, mem, tt.wantCPU, tt.wantMem)
			}
		})
	}
}
