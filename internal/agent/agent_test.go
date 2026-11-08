package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dockfleet/dockfleet/internal/protocol"
)

type fakeManager struct {
	mu             sync.Mutex
	registerErrs   []error // consumed one per RegisterNode call
	registerCalls  int
	heartbeatCalls int
	heartbeatErrs  []error // consumed one per Heartbeat call
	interval       time.Duration
}

func (f *fakeManager) RegisterNode(context.Context, protocol.NodeInfo) (time.Duration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registerCalls++
	if len(f.registerErrs) > 0 {
		err := f.registerErrs[0]
		f.registerErrs = f.registerErrs[1:]
		if err != nil {
			return 0, err
		}
	}
	return f.interval, nil
}

func (f *fakeManager) Heartbeat(context.Context, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heartbeatCalls++
	if len(f.heartbeatErrs) > 0 {
		err := f.heartbeatErrs[0]
		f.heartbeatErrs = f.heartbeatErrs[1:]
		return err
	}
	return nil
}

func (f *fakeManager) counts() (registers, heartbeats int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.registerCalls, f.heartbeatCalls
}

type fakeDocker struct{ err error }

func (d fakeDocker) Ping(context.Context) error { return d.err }
func (d fakeDocker) Close() error               { return nil }

func newTestAgent(m Manager, d fakeDocker) *Agent {
	a := New(protocol.NodeInfo{ID: "node-1"}, m, d, slog.New(slog.NewTextHandler(io.Discard, nil)))
	a.retryMin = time.Millisecond
	a.retryMax = 4 * time.Millisecond
	return a
}

func startAgent(t *testing.T, a *Agent) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	stopped := false
	stop = func() error {
		if stopped {
			return nil
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("agent did not stop after context cancellation")
			return nil
		}
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRunRegistersThenHeartbeatsAndStopsCleanly(t *testing.T) {
	m := &fakeManager{interval: 2 * time.Millisecond}
	stop := startAgent(t, newTestAgent(m, fakeDocker{}))

	waitFor(t, "three heartbeats", func() bool { _, h := m.counts(); return h >= 3 })
	if err := stop(); err != nil {
		t.Fatalf("Run returned %v, want nil on cancellation", err)
	}
	if r, _ := m.counts(); r != 1 {
		t.Errorf("registered %d times, want 1", r)
	}
}

func TestRunRetriesRegistrationUntilManagerIsAvailable(t *testing.T) {
	m := &fakeManager{
		interval:     2 * time.Millisecond,
		registerErrs: []error{errors.New("down"), errors.New("down"), errors.New("down")},
	}
	startAgent(t, newTestAgent(m, fakeDocker{}))

	waitFor(t, "a heartbeat after retries", func() bool { _, h := m.counts(); return h >= 1 })
	if r, _ := m.counts(); r != 4 {
		t.Errorf("registered %d times, want 4 (3 failures + 1 success)", r)
	}
}

func TestRunReRegistersWhenManagerForgetsNode(t *testing.T) {
	m := &fakeManager{
		interval:      2 * time.Millisecond,
		heartbeatErrs: []error{status.Error(codes.NotFound, "node is not registered")},
	}
	startAgent(t, newTestAgent(m, fakeDocker{}))

	waitFor(t, "re-registration", func() bool { r, _ := m.counts(); return r >= 2 })
}

func TestRunSurvivesHeartbeatFailures(t *testing.T) {
	m := &fakeManager{
		interval:      2 * time.Millisecond,
		heartbeatErrs: []error{errors.New("blip"), errors.New("blip")},
	}
	startAgent(t, newTestAgent(m, fakeDocker{}))

	waitFor(t, "heartbeats after transient errors", func() bool { _, h := m.counts(); return h >= 4 })
	if r, _ := m.counts(); r != 1 {
		t.Errorf("transient errors must not trigger re-registration, got %d registrations", r)
	}
}

func TestRunStopsWhileManagerIsDown(t *testing.T) {
	m := &fakeManager{registerErrs: []error{errors.New("down")}}
	a := newTestAgent(m, fakeDocker{})
	a.retryMin = time.Hour
	a.retryMax = time.Hour
	stop := startAgent(t, a)

	waitFor(t, "first registration attempt", func() bool { r, _ := m.counts(); return r >= 1 })
	if err := stop(); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
}

func TestRunToleratesUnreachableDocker(t *testing.T) {
	m := &fakeManager{interval: 2 * time.Millisecond}
	startAgent(t, newTestAgent(m, fakeDocker{err: errors.New("no socket")}))

	waitFor(t, "a heartbeat", func() bool { _, h := m.counts(); return h >= 1 })
}
