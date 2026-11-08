package scheduler

import (
	"fmt"
	"sort"
	"strings"
)

// stageCodes is the Unschedulable.Code for the stage where the last candidate
// node was rejected.
var stageCodes = [stages]string{
	stageAvailability: CodeNoAvailableNodes,
	stageSelector:     CodeSelectorUnsatisfied,
	stagePorts:        CodeHostPortConflict,
	stageResources:    CodeInsufficientResources,
}

// explain says why no node can take r. verdicts[i] is the verdict of states[i];
// none of them is a success, or the replica would have been placed.
func explain(r Replica, states []*nodeState, verdicts []verdict) *Unschedulable {
	u := &Unschedulable{Nodes: make([]NodeReason, 0, len(states))}
	if len(states) == 0 {
		u.Code = CodeNoNodes
		u.Message = fmt.Sprintf("%s cannot be scheduled: no nodes are registered", r.Label())
		return u
	}

	var failedAt [stages]int
	var cpuOnly, memOnly, both int
	for _, v := range verdicts {
		if v.reason == nil {
			continue
		}
		u.Nodes = append(u.Nodes, *v.reason)
		failedAt[v.stage]++
		if v.stage == stageResources {
			switch {
			case v.cpuShort && v.memShort:
				both++
			case v.cpuShort:
				cpuOnly++
			default:
				memOnly++
			}
		}
	}

	// The code names the stage at which no candidate was left.
	remaining := len(states)
	for stage, n := range failedAt {
		remaining -= n
		if remaining == 0 {
			u.Code = stageCodes[stage]
			break
		}
	}

	if u.Code == CodeInsufficientResources {
		if cpu, mem, never := neverFits(r, states, verdicts); never {
			u.Code = CodeExceedsCapacity
			u.Message = fmt.Sprintf("%s needs %s CPU and %s memory, more than any eligible node can offer even when idle (the largest can give %s CPU and %s memory)",
				r.Label(), formatCPU(r.CPUMillis), formatBytes(r.MemoryBytes), formatCPU(cpu), formatBytes(mem))
			return u
		}
	}

	var parts []string
	if n := failedAt[stageAvailability]; n > 0 {
		parts = append(parts, fmt.Sprintf("%d node(s) not available", n))
	}
	if n := failedAt[stageSelector]; n > 0 {
		parts = append(parts, fmt.Sprintf("%d node(s) do not match the node selector %s", n, formatSelector(r.NodeSelector)))
	}
	if n := failedAt[stagePorts]; n > 0 {
		parts = append(parts, fmt.Sprintf("%d node(s) already use host port %s", n, contestedPorts(r, states, verdicts)))
	}
	if cpuOnly > 0 {
		parts = append(parts, fmt.Sprintf("%d node(s) lack CPU (needs %s)", cpuOnly, formatCPU(r.CPUMillis)))
	}
	if memOnly > 0 {
		parts = append(parts, fmt.Sprintf("%d node(s) lack memory (needs %s)", memOnly, formatBytes(r.MemoryBytes)))
	}
	if both > 0 {
		parts = append(parts, fmt.Sprintf("%d node(s) lack CPU and memory (needs %s and %s)", both, formatCPU(r.CPUMillis), formatBytes(r.MemoryBytes)))
	}
	u.Message = fmt.Sprintf("0/%d nodes can run %s: %s", len(states), r.Label(), strings.Join(parts, "; "))
	return u
}

// neverFits reports whether r is larger than what every node that got as far
// as the resource check could ever give, however empty it was. It also returns
// the most CPU and memory any of those nodes can give.
func neverFits(r Replica, states []*nodeState, verdicts []verdict) (cpu, mem int64, never bool) {
	candidates := 0
	never = true
	for i, v := range verdicts {
		if v.reason == nil || v.stage != stageResources {
			continue
		}
		candidates++
		s := states[i]
		cpu, mem = max(cpu, s.allocCPU), max(mem, s.allocMem)
		if r.CPUMillis <= s.allocCPU && r.MemoryBytes <= s.allocMem {
			never = false
		}
	}
	return cpu, mem, never && candidates > 0
}

// contestedPorts names, in port order, the replica's fixed host ports that are
// taken on at least one node that got as far as the port check.
func contestedPorts(r Replica, states []*nodeState, verdicts []verdict) string {
	taken := map[portKey]bool{}
	for i, v := range verdicts {
		if v.reason == nil || v.stage != stagePorts {
			continue
		}
		for _, p := range r.Ports {
			if p.HostPort != 0 {
				if _, used := states[i].hostPorts[hostKey(p)]; used {
					taken[hostKey(p)] = true
				}
			}
		}
	}
	keys := make([]portKey, 0, len(taken))
	for k := range taken {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].port != keys[j].port {
			return keys[i].port < keys[j].port
		}
		return keys[i].protocol < keys[j].protocol
	})
	names := make([]string, 0, len(keys))
	for _, k := range keys {
		names = append(names, fmt.Sprintf("%d/%s", k.port, k.protocol))
	}
	return strings.Join(names, ", ")
}

func formatSelector(selector map[string]string) string {
	keys := make([]string, 0, len(selector))
	for k := range selector {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, k+"="+selector[k])
	}
	return "{" + strings.Join(pairs, ",") + "}"
}
