// Package dockerfake is an in-memory docker.Runtime for tests of the agent and
// of whole deployments. It is imported by tests only.
//
// It behaves like the real adapter wherever the agent can tell the
// difference: containers are addressed by workload ID, an operation can only
// reach a managed container of this node, Create applies the container policy
// and refuses a second container for a workload or a name that a container
// DockFleet does not manage already holds, and an image that is not on the
// node must be pulled first. It can also hold containers that are not
// DockFleet's (bystanders), which nothing may touch, and make operations fail
// on demand.
package dockerfake

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dockfleet/dockfleet/internal/docker"
)

var _ docker.Runtime = (*Fake)(nil)

// Info describes one container, managed or not.
type Info struct {
	ID         string
	Name       string
	Image      string
	WorkloadID string // empty for a container that is not DockFleet's
	Managed    bool   // carries this node's DockFleet labels
	State      docker.State
	ExitCode   int
	OOMKilled  bool
	// Health is the health check result: empty for a container without a check.
	Health string
}

type entry struct {
	Info
	nodeID string
	spec   docker.ContainerSpec
}

// Journal records the changes made on several engines in one order, so a test
// can tell which of two nodes acted first.
type Journal struct {
	mu      sync.Mutex
	entries []string
}

// Entries returns the changes so far, in order, as "<node> <operation> <workload>".
func (j *Journal) Entries() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]string(nil), j.entries...)
}

// Index is the position of the first entry equal to want, or -1.
func (j *Journal) Index(want string) int {
	for i, e := range j.Entries() {
		if e == want {
			return i
		}
	}
	return -1
}

// Fake is an in-memory Docker Engine for one node.
type Fake struct {
	nodeID  string
	policy  *docker.Policy
	journal *Journal

	mu       sync.Mutex
	entries  []*entry
	images   map[string]bool
	seq      int
	calls    []string
	failures map[string][]error
}

// New returns an engine for the node with no images and no containers.
func New(nodeID string) *Fake {
	return &Fake{nodeID: nodeID, policy: docker.DefaultPolicy(), images: map[string]bool{}, failures: map[string][]error{}}
}

// WithJournal makes the engine record its changes in j as well.
func (f *Fake) WithJournal(j *Journal) *Fake {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.journal = j
	return f
}

// AddImage puts an image on the node, so Create does not need a pull.
func (f *Fake) AddImage(ref string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.images[ref] = true
}

// AddUnmanaged adds a running container that is not DockFleet's, or that
// belongs to another node (give it that node's DockFleet labels). It is never
// managed by this node: no operation of the Runtime may see or change it.
func (f *Fake) AddUnmanaged(name, image string, labels map[string]string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	e := &entry{
		Info:   Info{ID: fmt.Sprintf("fake%04d", f.seq), Name: name, Image: image, State: docker.StateRunning},
		nodeID: labels[docker.LabelNodeID],
	}
	f.entries = append(f.entries, e)
	return e.ID
}

// FailNext makes the next n calls of an operation ("ping", "list", "pull",
// "create", "start", "stop", "remove" or "inspect") return err.
func (f *Fake) FailNext(op string, err error, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := 0; i < n; i++ {
		f.failures[op] = append(f.failures[op], err)
	}
}

// Pending is how many injected failures of an operation have not been used yet.
func (f *Fake) Pending(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.failures[op])
}

// Crash makes a running managed container exit with the given code.
func (f *Fake) Crash(workloadID string, exitCode int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e := f.managed(workloadID); e != nil && e.State == docker.StateRunning {
		e.State, e.ExitCode = docker.StateExited, exitCode
	}
}

// SetHealth sets what the health check of a managed container reports
// ("starting", "healthy" or "unhealthy"). A container with a health check starts
// out healthy; one without has none, and SetHealth leaves it alone.
func (f *Fake) SetHealth(workloadID, health string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e := f.managed(workloadID); e != nil && e.spec.HealthCheck != nil {
		e.Health = health
	}
}

// OOMKill makes a running managed container exit as if the kernel killed it.
func (f *Fake) OOMKill(workloadID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e := f.managed(workloadID); e != nil && e.State == docker.StateRunning {
		e.State, e.ExitCode, e.OOMKilled = docker.StateExited, 137, true
	}
}

// Containers returns every container on the node, managed or not, in creation order.
func (f *Fake) Containers() []Info {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Info, 0, len(f.entries))
	for _, e := range f.entries {
		out = append(out, e.Info)
	}
	return out
}

// Managed returns the containers DockFleet manages on this node, ordered by workload ID.
func (f *Fake) Managed() []Info {
	var out []Info
	for _, c := range f.Containers() {
		if c.Managed {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].WorkloadID < out[j].WorkloadID })
	return out
}

// Spec returns the spec a managed container was created from.
func (f *Fake) Spec(workloadID string) (docker.ContainerSpec, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e := f.managed(workloadID); e != nil {
		return e.spec, true
	}
	return docker.ContainerSpec{}, false
}

// Calls returns the changes made so far, in order, as "<operation> <workload>":
// pull, create, start, stop and remove. Reads are not recorded.
func (f *Fake) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// Count is how many times an operation changed something.
func (f *Fake) Count(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if strings.HasPrefix(c, op+" ") {
			n++
		}
	}
	return n
}

// managed finds the managed container of this node for a workload. The caller holds the lock.
func (f *Fake) managed(workloadID string) *entry {
	for _, e := range f.entries {
		if e.Managed && e.WorkloadID == workloadID {
			return e
		}
	}
	return nil
}

// take returns the injected failure for the operation, if any. The caller holds the lock.
func (f *Fake) take(op string) error {
	queue := f.failures[op]
	if len(queue) == 0 {
		return nil
	}
	f.failures[op] = queue[1:]
	return queue[0]
}

func (f *Fake) record(op, subject string) {
	f.calls = append(f.calls, op+" "+subject)
	if f.journal != nil {
		f.journal.mu.Lock()
		f.journal.entries = append(f.journal.entries, f.nodeID+" "+op+" "+subject)
		f.journal.mu.Unlock()
	}
}

func (f *Fake) Ping(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.take("ping")
}

func (f *Fake) Close() error { return nil }

func container(e *entry) docker.Container {
	return docker.Container{
		ID: e.ID, Name: e.Name, WorkloadID: e.WorkloadID, WorkloadName: e.spec.WorkloadName, NodeID: e.nodeID,
		Image: e.Image, State: e.State,
	}
}

func (f *Fake) List(context.Context) ([]docker.Container, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.take("list"); err != nil {
		return nil, err
	}
	out := []docker.Container{}
	for _, e := range f.entries {
		if e.Managed {
			out = append(out, container(e))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].WorkloadID < out[j].WorkloadID })
	return out, nil
}

func (f *Fake) PullImage(_ context.Context, ref string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.take("pull"); err != nil {
		return err
	}
	f.record("pull", ref)
	f.images[ref] = true
	return nil
}

func (f *Fake) Create(_ context.Context, spec docker.ContainerSpec) (docker.Container, error) {
	if err := f.policy.Validate(spec); err != nil {
		return docker.Container{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.take("create"); err != nil {
		return docker.Container{}, err
	}
	name := "dockfleet-" + spec.WorkloadID
	if f.managed(spec.WorkloadID) != nil {
		return docker.Container{}, fmt.Errorf("%w: workload %s already has a container", docker.ErrConflict, spec.WorkloadID)
	}
	for _, e := range f.entries {
		if e.Name == name {
			return docker.Container{}, fmt.Errorf("%w: the name %s is used by a container DockFleet does not manage", docker.ErrConflict, name)
		}
	}
	if !f.images[spec.Image] {
		return docker.Container{}, fmt.Errorf("%w: %s", docker.ErrImageNotFound, spec.Image)
	}
	f.seq++
	e := &entry{
		Info: Info{
			ID: fmt.Sprintf("fake%04d", f.seq), Name: name, Image: spec.Image, WorkloadID: spec.WorkloadID,
			Managed: true, State: docker.StateCreated,
		},
		nodeID: f.nodeID,
		spec:   spec,
	}
	f.entries = append(f.entries, e)
	f.record("create", spec.WorkloadID)
	return container(e), nil
}

func (f *Fake) Start(_ context.Context, workloadID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.take("start"); err != nil {
		return err
	}
	e := f.managed(workloadID)
	if e == nil {
		return fmt.Errorf("%w: %s", docker.ErrNotFound, workloadID)
	}
	f.record("start", workloadID)
	e.State, e.ExitCode, e.OOMKilled = docker.StateRunning, 0, false
	e.Health = ""
	if e.spec.HealthCheck != nil {
		e.Health = docker.HealthHealthy
	}
	return nil
}

func (f *Fake) Stop(_ context.Context, workloadID string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.take("stop"); err != nil {
		return err
	}
	e := f.managed(workloadID)
	if e == nil {
		return fmt.Errorf("%w: %s", docker.ErrNotFound, workloadID)
	}
	f.record("stop", workloadID)
	if e.State == docker.StateRunning {
		e.State, e.ExitCode = docker.StateExited, 137
	}
	return nil
}

func (f *Fake) Remove(_ context.Context, workloadID string, force bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.take("remove"); err != nil {
		return err
	}
	for i, e := range f.entries {
		if !e.Managed || e.WorkloadID != workloadID {
			continue
		}
		if e.State == docker.StateRunning && !force {
			return fmt.Errorf("%w: %s is running", docker.ErrStateConflict, workloadID)
		}
		f.record("remove", workloadID)
		f.entries = append(f.entries[:i], f.entries[i+1:]...)
		return nil
	}
	return fmt.Errorf("%w: %s", docker.ErrNotFound, workloadID)
}

func (f *Fake) Inspect(_ context.Context, workloadID string) (docker.ContainerState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.take("inspect"); err != nil {
		return docker.ContainerState{}, err
	}
	e := f.managed(workloadID)
	if e == nil {
		return docker.ContainerState{}, fmt.Errorf("%w: %s", docker.ErrNotFound, workloadID)
	}
	return docker.ContainerState{
		Container: container(e),
		Running:   e.State == docker.StateRunning,
		ExitCode:  e.ExitCode,
		OOMKilled: e.OOMKilled,
		Health:    e.Health,
	}, nil
}

func (f *Fake) Logs(_ context.Context, workloadID string, _ docker.LogOptions) (docker.Logs, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.managed(workloadID) == nil {
		return docker.Logs{}, fmt.Errorf("%w: %s", docker.ErrNotFound, workloadID)
	}
	return docker.Logs{}, nil
}

func (f *Fake) Stats(_ context.Context, workloadID string) (docker.Usage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.managed(workloadID) == nil {
		return docker.Usage{}, fmt.Errorf("%w: %s", docker.ErrNotFound, workloadID)
	}
	return docker.Usage{}, nil
}
