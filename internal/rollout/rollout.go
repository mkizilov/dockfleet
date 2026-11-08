// Package rollout decides how a new revision of a service replaces the old one.
//
// It is pure: it reads the store's data types and the time it is given, and
// does nothing else, so the scheduler pass that creates replicas and the
// reconciler that retires them (which run on different managers, at different
// moments) reach the same answer from the same snapshot.
//
// The rules are the two limits of the service's RolloutPolicy:
//
//   - MaxSurge: replicas of the new spec may be created only while the replicas
//     that are meant to run, old and new together, number at most the wanted
//     count plus MaxSurge.
//   - MaxUnavailable: a ready replica of the old spec may be stopped only while
//     the ready replicas, old and new together, stay at least the wanted count
//     minus MaxUnavailable. A replica that is not ready provides nothing, so it
//     can be stopped at any time.
//
// A rollout never goes beyond either limit. When it cannot move without doing
// so (no room for the surge replica, no new replica ready to take over) it
// waits, and says so: Facts.Describe names what it is waiting for, and the
// record kept by the reconciler turns a wait that goes on for longer than the
// service's progress deadline into a reported failure. Pausing a rollout sets
// the surge to zero and stops the retirement of ready replicas, so nothing
// moves until it is resumed.
package rollout

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dockfleet/dockfleet/internal/store"
)

// Events the reconciler records when a rollout changes state.
const (
	EventCompleted  = "rollout.completed"
	EventFailed     = "rollout.failed"
	EventRecovered  = "rollout.recovered"
	EventSuperseded = "rollout.superseded"
)

// maxNamed bounds how many replicas a message names.
const maxNamed = 3

// Ready is whether a replica provides its service: it is meant to run, its
// agent has received the latest instruction and reports the container running
// (or finished successfully), and if it has a health check, the check passes.
// A replica that has not reported yet is not ready.
func Ready(w store.Workload) bool {
	o := w.Observed
	if w.DesiredState != store.DesiredRunning || o == nil || o.AckedGeneration < w.Generation {
		return false
	}
	switch o.State {
	case store.WorkloadSucceeded:
		return true
	case store.WorkloadRunning:
		if w.HealthCheck != nil {
			return o.Health == store.HealthHealthy
		}
		return o.Health != store.HealthUnhealthy
	}
	return false
}

// Available is whether a replica can be counted on: it is ready and its node is
// not under suspicion. A replica on a node that has gone quiet may be running,
// but a rollout must not give up another replica on the strength of it.
func Available(w store.Workload, nodes map[string]string) bool {
	return Ready(w) && nodes[w.NodeID] != store.NodeSuspect && nodes[w.NodeID] != store.NodeUnavailable
}

// Census counts the replicas of one service that are meant to run.
type Census struct {
	// Want is the wanted replica count.
	Want int
	// Updated are the replicas whose spec is the current one, UpdatedReady the
	// ones among them that can be counted on.
	Updated      int
	UpdatedReady int
	// Old are the replicas of another spec, OldReady the ones among them that
	// can be counted on. Replicas on a node that has been given up on are in
	// neither: the failover is replacing them.
	Old      int
	OldReady int
}

// Available is how many replicas can be counted on.
func (c Census) Available() int { return c.UpdatedReady + c.OldReady }

// Count takes the census of a service's workloads. nodes maps a node ID to its
// health; a nil map treats every node as healthy.
func Count(def store.Definition, workloads []store.Workload, nodes map[string]string) Census {
	c := Census{Want: def.Service.Replicas}
	for _, w := range workloads {
		if w.ServiceID != def.Service.ID || w.DesiredState != store.DesiredRunning || nodes[w.NodeID] == store.NodeUnavailable {
			continue
		}
		counted := Available(w, nodes)
		if def.Serves(w.ServiceRevision) {
			c.Updated++
			if counted {
				c.UpdatedReady++
			}
		} else {
			c.Old++
			if counted {
				c.OldReady++
			}
		}
	}
	return c
}

// surge is how many replicas above the wanted count may run now. A paused
// rollout adds none.
func surge(p store.RolloutPolicy, paused bool) int {
	if paused {
		return 0
	}
	return p.Effective().MaxSurge
}

// CreateBudget is how many replicas of the current spec may be created now: as
// many as are missing, as far as the surge allows. It is what keeps a rollout
// gradual. A service with no old replicas is not rolling out, and gets all it
// is missing.
func CreateBudget(p store.RolloutPolicy, paused bool, c Census) int {
	room := c.Want + surge(p, paused) - c.Updated - c.Old
	return max(0, min(c.Want-c.Updated, room))
}

// Excess is how many old replicas must go whatever their state: more of them
// than the wanted count. It is what a scale-down in the middle of a rollout
// leaves behind, and it applies to a paused rollout too. It does not look at
// the surge: a rollout that is over its surge (the policy was tightened) gets
// back under it by retiring old replicas as new ones become ready, within
// Stoppable, not by giving up capacity the other limit protects.
func Excess(c Census) int { return max(0, c.Old-c.Want) }

// Stoppable is how many ready old replicas may be stopped for the rollout now:
// as many as leave at least the wanted count minus MaxUnavailable ready. None
// while paused. Old replicas that are not ready are not counted here; they
// provide nothing and are always stoppable.
func Stoppable(p store.RolloutPolicy, paused bool, c Census) int {
	if paused {
		return 0
	}
	minimum := max(0, c.Want-p.Effective().MaxUnavailable)
	return max(0, min(c.OldReady, c.Available()-minimum))
}

// Facts is what is known about a rollout at one moment.
type Facts struct {
	ServiceID string
	Revision  int
	Census

	// Failed, Unhealthy and Starting describe the replicas of the current spec
	// that are not ready, by name and why.
	Failed    []string
	Unhealthy []string
	Starting  []string
	// Waiting are the replicas of the current revision the scheduler could not
	// place.
	Waiting []store.Unschedulable
}

// Complete is whether every wanted replica is ready and no replica of another
// spec is left.
func (f Facts) Complete() bool { return f.Old == 0 && f.UpdatedReady >= f.Want }

// Assess collects the facts about the current revision of a service. waiting
// are the scheduler's explanations (of any service) and nodes each node's
// health.
func Assess(def store.Definition, workloads []store.Workload, waiting []store.Unschedulable, nodes map[string]string) Facts {
	f := Facts{ServiceID: def.Service.ID, Revision: def.Service.CurrentRevision, Census: Count(def, workloads, nodes)}
	for _, w := range workloads {
		if w.ServiceID != def.Service.ID || w.DesiredState != store.DesiredRunning || nodes[w.NodeID] == store.NodeUnavailable ||
			!def.Serves(w.ServiceRevision) || Ready(w) {
			continue
		}
		note := w.Name
		if w.Observed != nil && w.Observed.Message != "" {
			note += " (" + w.Observed.Message + ")"
		}
		switch {
		case w.Phase() == store.WorkloadFailed:
			f.Failed = append(f.Failed, note)
		case w.Phase() == store.WorkloadRunning && w.Observed.Health == store.HealthUnhealthy:
			f.Unhealthy = append(f.Unhealthy, w.Name)
		default:
			f.Starting = append(f.Starting, w.Name)
		}
	}
	for _, u := range waiting {
		if u.ServiceID == def.Service.ID && u.Revision == f.Revision {
			f.Waiting = append(f.Waiting, u)
		}
	}
	sort.Strings(f.Failed)
	sort.Strings(f.Unhealthy)
	sort.Strings(f.Starting)
	return f
}

func plural(n int, one, many string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, one)
	}
	return fmt.Sprintf("%d %s", n, many)
}

func named(names []string) string {
	if len(names) > maxNamed {
		return strings.Join(names[:maxNamed], ", ") + fmt.Sprintf(" and %d more", len(names)-maxNamed)
	}
	return strings.Join(names, ", ")
}

// Describe says where the rollout stands and, if it is waiting, what for. The
// text names no times, so it stays the same for as long as the facts do.
func (f Facts) Describe(p store.RolloutPolicy, paused bool) string {
	if f.Complete() {
		return fmt.Sprintf("revision %d is rolled out: %d of %d replicas are ready", f.Revision, f.UpdatedReady, f.Want)
	}
	p = p.Effective()
	parts := []string{fmt.Sprintf("%d of %d replicas of revision %d are ready", f.UpdatedReady, f.Want, f.Revision)}
	if f.Old == 1 {
		parts = append(parts, "1 replica of an older spec still runs")
	} else if f.Old > 1 {
		parts = append(parts, fmt.Sprintf("%d replicas of an older spec still run", f.Old))
	}
	if paused {
		parts = append([]string{"paused"}, parts...)
	}
	if len(f.Waiting) > 0 {
		reason := fmt.Sprintf("%s cannot be placed: %s", plural(len(f.Waiting), "replica", "replicas"), f.Waiting[0].Message)
		if f.Old > 0 && p.MaxUnavailable == 0 {
			reason += fmt.Sprintf(" (the old replicas stay, since max_unavailable is 0 and max_surge is %d: free capacity, add nodes, or allow max_unavailable of 1 or more)", p.MaxSurge)
		}
		parts = append(parts, reason)
	}
	if len(f.Failed) > 0 {
		parts = append(parts, fmt.Sprintf("%s failed: %s", plural(len(f.Failed), "replica", "replicas"), named(f.Failed)))
	}
	if len(f.Unhealthy) > 0 {
		verb := "fail their health check"
		if len(f.Unhealthy) == 1 {
			verb = "fails its health check"
		}
		parts = append(parts, fmt.Sprintf("%s %s: %s", plural(len(f.Unhealthy), "replica", "replicas"), verb, named(f.Unhealthy)))
	}
	if len(f.Starting) > 0 {
		parts = append(parts, fmt.Sprintf("%s not ready yet: %s", plural(len(f.Starting), "replica is", "replicas are"), named(f.Starting)))
	}
	return strings.Join(parts, "; ")
}

// Advance moves the record of a rollout on to now, given the facts. prev is the
// stored record, nil for a revision that has none yet. It returns the new record
// and the audit event the change deserves, or "" when there is none. The caller
// writes the record when it differs from prev.
//
// A rollout makes progress when it has more replicas of the new spec, more of
// them ready, or fewer old ones left than ever before. If it makes none for
// longer than the progress deadline it is failed, unless it is paused: a pause
// stops the clock, and resuming starts it again from the moment of the resume.
// A failed rollout goes on being carried out, and returns to progressing if it
// moves again. A complete or superseded record never changes.
func Advance(prev *store.Rollout, f Facts, p store.RolloutPolicy, paused bool, now time.Time) (next store.Rollout, event string) {
	if prev != nil {
		next = *prev
	} else {
		next = store.Rollout{
			ServiceID: f.ServiceID, Revision: f.Revision, State: store.RolloutProgressing, Paused: paused,
			Updated: f.Updated, Ready: f.UpdatedReady, Old: f.Old, StartedAt: now, ProgressAt: now,
		}
	}
	if next.State == store.RolloutComplete || next.State == store.RolloutSuperseded {
		return next, ""
	}

	if next.Paused != paused {
		next.Paused = paused
		if !paused {
			next.ProgressAt = now
		}
	}
	if f.Complete() {
		finished := now
		next.State, next.Reason, next.FinishedAt = store.RolloutComplete, "", &finished
		next.Updated, next.Ready, next.Old = f.Updated, f.UpdatedReady, f.Old
		next.Message = f.Describe(p, paused)
		return next, EventCompleted
	}

	if f.Updated > next.Updated || f.UpdatedReady > next.Ready || f.Old < next.Old {
		next.Updated, next.Ready, next.Old = max(next.Updated, f.Updated), max(next.Ready, f.UpdatedReady), min(next.Old, f.Old)
		next.ProgressAt = now
		if next.State == store.RolloutFailed {
			next.State, next.Reason, next.Message = store.RolloutProgressing, "", ""
			event = EventRecovered
		}
	}

	deadline := time.Duration(p.Effective().ProgressDeadlineSeconds) * time.Second
	if next.State == store.RolloutProgressing && !paused && now.Sub(next.ProgressAt) > deadline {
		next.State, next.Reason = store.RolloutFailed, store.RolloutDeadlineExceeded
		next.Message = fmt.Sprintf("no progress for %s: %s", deadline, f.Describe(p, paused))
		event = EventFailed
	}
	return next, event
}

// Supersede closes a record that another revision has overtaken, or that the
// service's deletion has ended. It returns false for a record that is already
// closed.
func Supersede(r store.Rollout, now time.Time) (store.Rollout, bool) {
	if r.State == store.RolloutComplete || r.State == store.RolloutSuperseded {
		return r, false
	}
	finished := now
	r.State, r.FinishedAt = store.RolloutSuperseded, &finished
	return r, true
}
