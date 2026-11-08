// Package scheduler decides which node runs each replica. It makes decisions
// only: it reads a snapshot it is given and returns where each replica should
// go, or why none can. It does no I/O, reads no clock and knows nothing of
// Docker, so a decision can be made, tested and explained without a daemon or
// a database. Saving decisions (placement) and carrying them out (the agent and
// its Docker adapter) are separate packages.
//
// The decision is deterministic. The same snapshot always gives the same
// answer, whatever order its parts arrive in: nodes are taken in ID order,
// replicas in (service, group, index) order, and every tie is broken by a fixed
// rule that ends at the node ID.
//
// A replica may go to a node that is available, matches the replica's node
// selector, has no conflicting fixed host port, and has room for the replica's
// CPU and memory. Room is the node's capacity (times the configured
// overcommit factor, 1 unless an operator raised it) minus what the workloads
// already assigned to it reserve. Among the nodes that qualify, replicas of
// one service are spread across nodes first, then load is spread by free
// capacity.
package scheduler

import (
	"fmt"
	"math"
	"math/bits"
	"sort"
	"strings"

	"github.com/dockfleet/dockfleet/internal/store"
)

// Why a replica cannot be placed (Unschedulable.Code). They follow the stages a
// node passes through, and the code names the stage where no node was left.
const (
	CodeNoNodes               = "no_nodes"
	CodeNoAvailableNodes      = "no_available_nodes"
	CodeSelectorUnsatisfied   = "selector_unsatisfied"
	CodeHostPortConflict      = "host_port_conflict"
	CodeInsufficientResources = "insufficient_resources"
	CodeExceedsCapacity       = "exceeds_capacity"
)

// Why one node was rejected (NodeReason.Code).
const (
	NodeUnavailable           = "unavailable"
	NodeSelectorMismatch      = "selector_mismatch"
	NodeHostPortInUse         = "host_port_in_use"
	NodeInsufficientResources = "insufficient_resources"
)

// Overcommit allows a node to be promised more than it has. A factor of 1 (the
// default, and what anything below 1 means) reserves at most the node's
// capacity; 1.5 lets the reserved CPU reach one and a half times the CPUs.
type Overcommit struct {
	CPU    float64
	Memory float64
}

func factor(f float64) float64 {
	if !(f >= 1) { // also catches NaN
		return 1
	}
	return f
}

// Allocatable is how much of a node's capacity may be reserved.
func (o Overcommit) Allocatable(cpuMillis, memoryBytes int64) (cpu, memory int64) {
	return scale(cpuMillis, factor(o.CPU)), scale(memoryBytes, factor(o.Memory))
}

func scale(v int64, f float64) int64 {
	if v <= 0 {
		return 0
	}
	scaled := float64(v) * f
	if scaled >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(scaled)
}

// Node is a worker as the scheduler sees it.
type Node struct {
	ID          string
	CPUMillis   int64
	MemoryBytes int64
	Labels      map[string]string

	// Unavailable says why the node cannot take new replicas, or is empty when
	// it can. The text should not change from one moment to the next while the
	// reason stays the same: it is stored and compared.
	Unavailable string
}

// Assignment is a workload already placed that still reserves its node's
// resources and host ports.
type Assignment struct {
	Name   string // for messages
	NodeID string
	Group  string // the service it belongs to, or empty

	CPUMillis   int64
	MemoryBytes int64
	Ports       []store.ServicePort
}

// Replica is one placement to decide.
type Replica struct {
	Service string // the service's name: orders replicas and names them in messages
	Group   string // replicas sharing a group are spread across nodes; empty means none
	Index   int    // 1-based; 0 for a replica that is not one of several

	// Name is what the replica's workload will be called. Messages name a
	// replica that holds a host port by it, the way they name the workloads
	// already placed, so an explanation reads the same before and after the
	// holder is saved. It defaults to Label.
	Name string

	// CPUMillis and MemoryBytes are reserved on the node, so the sum of the
	// limits on a node never exceeds what it may be promised.
	CPUMillis   int64
	MemoryBytes int64

	NodeSelector map[string]string
	Ports        []store.ServicePort
}

// Label names the replica in messages.
func (r Replica) Label() string {
	if r.Index == 0 {
		return r.Service
	}
	return fmt.Sprintf("%s#%d", r.Service, r.Index)
}

// holder is how messages name the replica once it holds resources on a node.
func (r Replica) holder() string {
	if r.Name != "" {
		return r.Name
	}
	return r.Label()
}

// Input is a snapshot of the cluster and what to place in it.
type Input struct {
	Nodes       []Node
	Assignments []Assignment
	Replicas    []Replica
	Overcommit  Overcommit
}

// Decision is the outcome for one replica.
type Decision struct {
	Replica Replica

	// NodeID is the chosen node, or empty when the replica cannot be placed.
	NodeID string

	// Unschedulable says why, when it cannot.
	Unschedulable *Unschedulable
}

// Placed reports whether the replica got a node.
func (d Decision) Placed() bool { return d.NodeID != "" }

// Unschedulable explains why no node can take a replica.
type Unschedulable struct {
	Code    string
	Message string

	// Nodes has one entry per node, in node ID order.
	Nodes []NodeReason
}

// NodeReason is why one node was rejected.
type NodeReason struct {
	NodeID  string
	Code    string
	Message string
}

// The stages every node goes through, in order. A node is rejected at the first
// one it fails.
const (
	stageAvailability = iota
	stageSelector
	stagePorts
	stageResources
	stages
)

type portKey struct {
	port     int
	protocol string
}

// hostKey identifies the host side of a port: the number and the protocol.
func hostKey(p store.ServicePort) portKey {
	proto := p.Protocol
	if proto == "" {
		proto = "tcp"
	}
	return portKey{p.HostPort, proto}
}

type nodeState struct {
	node       Node
	allocCPU   int64
	allocMem   int64
	usedCPU    int64
	usedMem    int64
	hostPorts  map[portKey]string // fixed host ports in use, and by whom
	groupCount map[string]int
}

func (s *nodeState) freeCPU() int64 { return s.allocCPU - s.usedCPU }
func (s *nodeState) freeMem() int64 { return s.allocMem - s.usedMem }

func (s *nodeState) reserve(name, group string, cpu, mem int64, ports []store.ServicePort) {
	s.usedCPU += cpu
	s.usedMem += mem
	for _, p := range ports {
		if p.HostPort != 0 {
			s.hostPorts[hostKey(p)] = name
		}
	}
	if group != "" {
		s.groupCount[group]++
	}
}

func newStates(in Input) []*nodeState {
	nodes := append([]Node(nil), in.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	states := make([]*nodeState, 0, len(nodes))
	byID := make(map[string]*nodeState, len(nodes))
	for _, n := range nodes {
		cpu, mem := in.Overcommit.Allocatable(n.CPUMillis, n.MemoryBytes)
		s := &nodeState{node: n, allocCPU: cpu, allocMem: mem, hostPorts: map[portKey]string{}, groupCount: map[string]int{}}
		states = append(states, s)
		byID[n.ID] = s
	}
	// Assignments are applied in a fixed order, so which workload is named as
	// holding a contested port does not depend on the order they were given.
	assignments := append([]Assignment(nil), in.Assignments...)
	sort.SliceStable(assignments, func(i, j int) bool {
		if assignments[i].NodeID != assignments[j].NodeID {
			return assignments[i].NodeID < assignments[j].NodeID
		}
		return assignments[i].Name < assignments[j].Name
	})
	for _, a := range assignments {
		if s := byID[a.NodeID]; s != nil {
			s.reserve(a.Name, a.Group, a.CPUMillis, a.MemoryBytes, a.Ports)
		}
	}
	return states
}

// Schedule decides where each replica of in.Replicas goes. Replicas are placed
// one after another, each seeing the resources and ports taken by the ones
// before it, so several replicas in one call cannot overcommit a node together.
// The decisions come back in placement order: by service, group, then index.
func Schedule(in Input) []Decision {
	states := newStates(in)
	replicas := append([]Replica(nil), in.Replicas...)
	sort.SliceStable(replicas, func(i, j int) bool {
		a, b := replicas[i], replicas[j]
		if a.Service != b.Service {
			return a.Service < b.Service
		}
		if a.Group != b.Group {
			return a.Group < b.Group
		}
		return a.Index < b.Index
	})

	decisions := make([]Decision, 0, len(replicas))
	for _, r := range replicas {
		d := Decision{Replica: r}
		verdicts := make([]verdict, len(states))
		var best *nodeState
		for i, s := range states {
			verdicts[i] = s.check(r)
			if verdicts[i].ok() && (best == nil || betterFor(r, s, best)) {
				best = s
			}
		}
		if best != nil {
			d.NodeID = best.node.ID
			best.reserve(r.holder(), r.Group, r.CPUMillis, r.MemoryBytes, r.Ports)
		} else {
			d.Unschedulable = explain(r, states, verdicts)
		}
		decisions = append(decisions, d)
	}
	return decisions
}

// verdict is a node's answer for one replica.
type verdict struct {
	stage  int // the stage the node failed at; meaningful when reason is set
	reason *NodeReason

	// cpuShort and memShort tell which resource fell short, at the resources stage.
	cpuShort, memShort bool
}

func (v verdict) ok() bool { return v.reason == nil }

func (s *nodeState) check(r Replica) verdict {
	reject := func(stage int, code, format string, args ...any) verdict {
		return verdict{stage: stage, reason: &NodeReason{NodeID: s.node.ID, Code: code, Message: fmt.Sprintf(format, args...)}}
	}
	if s.node.Unavailable != "" {
		return reject(stageAvailability, NodeUnavailable, "%s", s.node.Unavailable)
	}
	if mismatches := selectorMismatches(r.NodeSelector, s.node.Labels); len(mismatches) > 0 {
		return reject(stageSelector, NodeSelectorMismatch, "does not match the node selector: %s", strings.Join(mismatches, "; "))
	}
	if conflicts := s.portConflicts(r.Ports); len(conflicts) > 0 {
		return reject(stagePorts, NodeHostPortInUse, "%s", strings.Join(conflicts, "; "))
	}
	cpuShort, memShort := r.CPUMillis > s.freeCPU(), r.MemoryBytes > s.freeMem()
	if cpuShort || memShort {
		var parts []string
		if cpuShort {
			parts = append(parts, fmt.Sprintf("CPU: needs %s, %s free", formatCPU(r.CPUMillis), formatCPU(max(s.freeCPU(), 0))))
		}
		if memShort {
			parts = append(parts, fmt.Sprintf("memory: needs %s, %s free", formatBytes(r.MemoryBytes), formatBytes(max(s.freeMem(), 0))))
		}
		v := reject(stageResources, NodeInsufficientResources, "insufficient resources (%s)", strings.Join(parts, "; "))
		v.cpuShort, v.memShort = cpuShort, memShort
		return v
	}
	return verdict{}
}

// selectorMismatches lists, in label order, how the node's labels differ from
// what the selector asks for.
func selectorMismatches(selector, labels map[string]string) []string {
	keys := make([]string, 0, len(selector))
	for k := range selector {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []string
	for _, k := range keys {
		got, ok := labels[k]
		switch {
		case !ok:
			out = append(out, fmt.Sprintf("label %s is not set (want %q)", k, selector[k]))
		case got != selector[k]:
			out = append(out, fmt.Sprintf("label %s is %q (want %q)", k, got, selector[k]))
		}
	}
	return out
}

// portConflicts lists, in port order, the fixed host ports the replica wants
// that are already in use on the node.
func (s *nodeState) portConflicts(ports []store.ServicePort) []string {
	var wanted []portKey
	for _, p := range ports {
		if p.HostPort != 0 {
			wanted = append(wanted, hostKey(p))
		}
	}
	sort.Slice(wanted, func(i, j int) bool {
		if wanted[i].port != wanted[j].port {
			return wanted[i].port < wanted[j].port
		}
		return wanted[i].protocol < wanted[j].protocol
	})
	var out []string
	for _, k := range wanted {
		if holder, taken := s.hostPorts[k]; taken {
			out = append(out, fmt.Sprintf("host port %d/%s is already used by %s", k.port, k.protocol, holder))
		}
	}
	return out
}

// betterFor reports whether candidate a is a better home for r than b. Equal
// candidates are never better, so the first one in node ID order wins ties.
func betterFor(r Replica, a, b *nodeState) bool {
	if r.Group != "" && a.groupCount[r.Group] != b.groupCount[r.Group] {
		return a.groupCount[r.Group] < b.groupCount[r.Group]
	}
	if sa, sb := scoreAfter(a, r), scoreAfter(b, r); sa != sb {
		return sa > sb
	}
	if fa, fb := a.freeCPU()-r.CPUMillis, b.freeCPU()-r.CPUMillis; fa != fb {
		return fa > fb
	}
	if fa, fb := a.freeMem()-r.MemoryBytes, b.freeMem()-r.MemoryBytes; fa != fb {
		return fa > fb
	}
	return false
}

// scoreAfter is how empty the node would stay if r went there: the free share
// of its CPU plus the free share of its memory, in thousandths each.
func scoreAfter(s *nodeState, r Replica) int64 {
	return permille(s.freeCPU()-r.CPUMillis, s.allocCPU) + permille(s.freeMem()-r.MemoryBytes, s.allocMem)
}

// permille is free/total in thousandths, exact for any int64 and clamped to
// 0..1000.
func permille(free, total int64) int64 {
	switch {
	case total <= 0 || free <= 0:
		return 0
	case free >= total:
		return 1000
	}
	hi, lo := bits.Mul64(uint64(free), 1000)
	quotient, _ := bits.Div64(hi, lo, uint64(total)) // hi < total because free < total
	return int64(quotient)
}

func formatCPU(millis int64) string { return fmt.Sprintf("%dm", millis) }

func formatBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(b)/(1<<10))
	}
	return fmt.Sprintf("%d B", b)
}
