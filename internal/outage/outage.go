// Package outage remembers when the manager last could not use its store.
//
// A node is judged by how long it has been silent, and silence is the time since
// its last heartbeat was recorded. While the store is down a heartbeat cannot be
// recorded, however many the agents send, so a node looks silent for exactly as
// long as the outage. Without this memory the first pass after the store came
// back would see every node silent for the whole outage, give up on all of them
// and move every replica of the fleet at once, although nothing was wrong with
// any node. With it, silence that the manager could not have heard is not counted,
// the same way silence from before the manager started is not counted.
//
// The package uses only the standard library; it is shared by the scheduling
// loop, the reconciler and the API through scheduler.Timing.
package outage

import (
	"sync"
	"time"
)

// Tracker is the time of the latest store failure the manager saw. It is safe
// for concurrent use; the zero value is ready.
type Tracker struct {
	mu sync.Mutex
	at time.Time
}

// New returns a Tracker.
func New() *Tracker { return &Tracker{} }

// Mark records that the store failed at the given time. Times only move forward,
// so marks arriving out of order, from several loops, leave the latest.
func (t *Tracker) Mark(at time.Time) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if at.After(t.at) {
		t.at = at
	}
}

// Since is when the store last failed: silence before it is not counted. It is
// the zero time if the store has not failed.
func (t *Tracker) Since() time.Time {
	if t == nil {
		return time.Time{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.at
}
