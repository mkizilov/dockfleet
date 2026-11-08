package docker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

const defaultStopTimeout = 10 * time.Second

// backend is the narrow, SDK-free surface of the Docker Engine the adapter
// needs. sdkBackend implements it with the Docker SDK; unit tests use a fake.
//
// Contract: a backend maps "no such container" to ErrNotFound, "no such image"
// on create to ErrImageNotFound and name/ID conflicts to ErrConflict. The
// adapter, not the backend, enforces ownership and policy.
type backend interface {
	ping(ctx context.Context) error
	// list returns all containers (any state) carrying every given label.
	list(ctx context.Context, labels map[string]string) ([]rawContainer, error)
	create(ctx context.Context, req createRequest) (id string, err error)
	start(ctx context.Context, id string) error
	stop(ctx context.Context, id string, timeout time.Duration) error
	remove(ctx context.Context, id string, force bool) error
	inspect(ctx context.Context, id string) (rawInspect, error)
	// logs returns Docker's multiplexed log stream.
	logs(ctx context.Context, id string, req logRequest) (io.ReadCloser, error)
	// stats returns one JSON stats document.
	stats(ctx context.Context, id string) (io.ReadCloser, error)
	pull(ctx context.Context, ref string) error
	close() error
}

type rawContainer struct {
	ID      string
	Names   []string
	Image   string
	Labels  map[string]string
	State   string
	Created time.Time
}

type rawInspect struct {
	rawContainer
	Running      bool
	ExitCode     int
	Error        string
	OOMKilled    bool
	RestartCount int
	Health       string
	StartedAt    time.Time
	FinishedAt   time.Time
}

// createRequest is a spec the policy already accepted, with the name and
// labels the adapter chose. The backend applies the fixed hardening.
type createRequest struct {
	Name   string
	Labels map[string]string
	Spec   ContainerSpec
}

type logRequest struct {
	// Tail is the number of last lines; negative means all of them.
	Tail       int
	Since      time.Time
	Timestamps bool
	// Follow keeps the stream open for output the container writes later.
	Follow bool
}

// AdapterConfig configures NewAdapter.
type AdapterConfig struct {
	// Host is the local Docker Engine: unix:///path/to/docker.sock or
	// tcp://127.0.0.1:port. Remote hosts are refused.
	Host string
	// NodeID is stamped on every container and scopes every query: this
	// adapter only ever sees containers created for this node.
	NodeID string
	// Policy decides which specs are accepted; nil means DefaultPolicy.
	Policy *Policy
}

// Adapter manages DockFleet's containers on the local Docker Engine.
type Adapter struct {
	b      backend
	nodeID string
	policy *Policy
}

var _ Runtime = (*Adapter)(nil)

// NewAdapter connects to the local Docker Engine through the Docker SDK.
// Connecting is lazy: an unreachable daemon shows up as errors from calls
// (start with Ping to check).
func NewAdapter(cfg AdapterConfig) (*Adapter, error) {
	if err := validateHost(cfg.Host); err != nil {
		return nil, err
	}
	b, err := newSDKBackend(cfg.Host)
	if err != nil {
		return nil, err
	}
	a, err := newAdapter(b, cfg.NodeID, cfg.Policy)
	if err != nil {
		_ = b.close()
		return nil, err
	}
	return a, nil
}

func newAdapter(b backend, nodeID string, policy *Policy) (*Adapter, error) {
	if !idPattern.MatchString(nodeID) {
		return nil, fmt.Errorf("%w: node ID %q must be 1-63 characters of [A-Za-z0-9._-] starting with a letter or digit", ErrInvalid, nodeID)
	}
	if policy == nil {
		policy = DefaultPolicy()
	}
	return &Adapter{b: b, nodeID: nodeID, policy: policy}, nil
}

func (a *Adapter) Ping(ctx context.Context) error { return a.b.ping(ctx) }

func (a *Adapter) Close() error { return a.b.close() }

func containerName(workloadID string) string { return namePrefix + workloadID }

// selector is the label set that identifies this node's managed containers.
func (a *Adapter) selector() map[string]string {
	return map[string]string{LabelManaged: managedValue, LabelNodeID: a.nodeID}
}

// owns reports whether labels say the container was created by DockFleet for
// this node. It is checked on everything the backend returns, in addition to
// the label filter the backend applies, so a container without our labels is
// never acted on even if a filter were ignored.
func (a *Adapter) owns(labels map[string]string) bool {
	return labels[LabelManaged] == managedValue && labels[LabelNodeID] == a.nodeID && labels[LabelWorkloadID] != ""
}

func (a *Adapter) labels(spec ContainerSpec) map[string]string {
	labels := a.selector()
	labels[LabelSchema] = schemaVersion
	labels[LabelWorkloadID] = spec.WorkloadID
	if spec.WorkloadName != "" {
		labels[LabelWorkloadName] = spec.WorkloadName
	}
	return labels
}

func (a *Adapter) toContainer(r rawContainer) Container {
	name := ""
	if len(r.Names) > 0 {
		name = strings.TrimPrefix(r.Names[0], "/")
	}
	return Container{
		ID:           r.ID,
		Name:         name,
		WorkloadID:   r.Labels[LabelWorkloadID],
		WorkloadName: r.Labels[LabelWorkloadName],
		NodeID:       r.Labels[LabelNodeID],
		Image:        r.Image,
		State:        parseState(r.State),
		CreatedAt:    r.Created,
	}
}

func parseState(s string) State {
	switch State(s) {
	case StateCreated, StateRunning, StatePaused, StateRestarting, StateRemoving, StateExited, StateDead:
		return State(s)
	}
	return StateUnknown
}

func (a *Adapter) List(ctx context.Context) ([]Container, error) {
	raw, err := a.b.list(ctx, a.selector())
	if err != nil {
		return nil, fmt.Errorf("list containers: %w", err)
	}
	out := make([]Container, 0, len(raw))
	for _, r := range raw {
		if a.owns(r.Labels) {
			out = append(out, a.toContainer(r))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].WorkloadID < out[j].WorkloadID })
	return out, nil
}

// find returns the one managed container of this node for the workload.
func (a *Adapter) find(ctx context.Context, workloadID string) (Container, error) {
	if !idPattern.MatchString(workloadID) {
		return Container{}, fmt.Errorf("%w: workload ID %q", ErrInvalid, workloadID)
	}
	selector := a.selector()
	selector[LabelWorkloadID] = workloadID
	raw, err := a.b.list(ctx, selector)
	if err != nil {
		return Container{}, fmt.Errorf("find container for workload %s: %w", workloadID, err)
	}
	var matches []Container
	for _, r := range raw {
		if a.owns(r.Labels) && r.Labels[LabelWorkloadID] == workloadID {
			matches = append(matches, a.toContainer(r))
		}
	}
	switch len(matches) {
	case 0:
		return Container{}, fmt.Errorf("%w: %s", ErrNotFound, workloadID)
	case 1:
		return matches[0], nil
	default:
		return Container{}, fmt.Errorf("%w: %s (%d containers)", ErrAmbiguous, workloadID, len(matches))
	}
}

func (a *Adapter) PullImage(ctx context.Context, ref string) error {
	if ref == "" || len(ref) > maxImageLength || strings.HasPrefix(ref, "-") || strings.ContainsAny(ref, " \t\r\n\x00") {
		return fmt.Errorf("%w: image reference %q", ErrInvalid, ref)
	}
	if err := a.b.pull(ctx, ref); err != nil {
		return fmt.Errorf("pull image %s: %w", ref, err)
	}
	return nil
}

func (a *Adapter) Create(ctx context.Context, spec ContainerSpec) (Container, error) {
	if err := a.policy.Validate(spec); err != nil {
		return Container{}, err
	}
	existing, err := a.find(ctx, spec.WorkloadID)
	switch {
	case err == nil:
		return Container{}, fmt.Errorf("%w: workload %s already has container %s", ErrConflict, spec.WorkloadID, existing.ID)
	case !errors.Is(err, ErrNotFound):
		return Container{}, err
	}

	labels := a.labels(spec)
	name := containerName(spec.WorkloadID)
	id, err := a.b.create(ctx, createRequest{Name: name, Labels: labels, Spec: spec})
	switch {
	case errors.Is(err, ErrImageNotFound):
		return Container{}, fmt.Errorf("%w: %s", ErrImageNotFound, spec.Image)
	case errors.Is(err, ErrConflict):
		return Container{}, fmt.Errorf("%w: the name %s is already used by a container DockFleet does not manage; it was left untouched", ErrConflict, name)
	case err != nil:
		return Container{}, fmt.Errorf("create container for workload %s: %w", spec.WorkloadID, err)
	}
	return Container{
		ID:           id,
		Name:         name,
		WorkloadID:   spec.WorkloadID,
		WorkloadName: spec.WorkloadName,
		NodeID:       a.nodeID,
		Image:        spec.Image,
		State:        StateCreated,
		CreatedAt:    time.Now().UTC(),
	}, nil
}

func (a *Adapter) Start(ctx context.Context, workloadID string) error {
	c, err := a.find(ctx, workloadID)
	if err != nil {
		return err
	}
	if err := a.b.start(ctx, c.ID); err != nil {
		return fmt.Errorf("start container for workload %s: %w", workloadID, err)
	}
	return nil
}

func (a *Adapter) Stop(ctx context.Context, workloadID string, timeout time.Duration) error {
	c, err := a.find(ctx, workloadID)
	if err != nil {
		return err
	}
	if timeout < 0 {
		timeout = defaultStopTimeout
	}
	if err := a.b.stop(ctx, c.ID, timeout); err != nil {
		return fmt.Errorf("stop container for workload %s: %w", workloadID, err)
	}
	return nil
}

func (a *Adapter) Remove(ctx context.Context, workloadID string, force bool) error {
	c, err := a.find(ctx, workloadID)
	if err != nil {
		return err
	}
	if err := a.b.remove(ctx, c.ID, force); err != nil {
		return fmt.Errorf("remove container for workload %s: %w", workloadID, err)
	}
	return nil
}

func (a *Adapter) Inspect(ctx context.Context, workloadID string) (ContainerState, error) {
	c, err := a.find(ctx, workloadID)
	if err != nil {
		return ContainerState{}, err
	}
	info, err := a.b.inspect(ctx, c.ID)
	if err != nil {
		return ContainerState{}, fmt.Errorf("inspect container for workload %s: %w", workloadID, err)
	}
	if !a.owns(info.Labels) || info.Labels[LabelWorkloadID] != workloadID {
		return ContainerState{}, fmt.Errorf("%w: %s", ErrNotFound, workloadID)
	}
	return ContainerState{
		Container:    a.toContainer(info.rawContainer),
		Running:      info.Running,
		ExitCode:     info.ExitCode,
		Error:        info.Error,
		OOMKilled:    info.OOMKilled,
		RestartCount: info.RestartCount,
		Health:       info.Health,
		StartedAt:    info.StartedAt,
		FinishedAt:   info.FinishedAt,
	}, nil
}

func (a *Adapter) Logs(ctx context.Context, workloadID string, opts LogOptions) (Logs, error) {
	c, err := a.find(ctx, workloadID)
	if err != nil {
		return Logs{}, err
	}
	rc, err := a.b.logs(ctx, c.ID, logRequest{Tail: opts.tail(), Since: opts.Since, Timestamps: opts.Timestamps})
	if err != nil {
		return Logs{}, fmt.Errorf("read logs of workload %s: %w", workloadID, err)
	}
	defer rc.Close()
	logs, err := demux(rc, opts.maxBytes())
	if err != nil {
		return Logs{}, fmt.Errorf("read logs of workload %s: %w", workloadID, err)
	}
	return logs, nil
}

// StreamLogs starts reading the container's output. The container is found by
// the same labels as for every other operation, so the logs of a container that
// DockFleet does not manage cannot be read. The returned reader holds the
// daemon's connection open until it is closed or the context ends.
func (a *Adapter) StreamLogs(ctx context.Context, workloadID string, opts LogStreamOptions) (LogReader, error) {
	c, err := a.find(ctx, workloadID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	rc, err := a.b.logs(ctx, c.ID, logRequest{Tail: opts.Tail, Since: opts.Since, Timestamps: true, Follow: opts.Follow})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("read logs of workload %s: %w", workloadID, err)
	}
	return newLogReader(rc, cancel, opts.maxLine()), nil
}

func (a *Adapter) Stats(ctx context.Context, workloadID string) (Usage, error) {
	c, err := a.find(ctx, workloadID)
	if err != nil {
		return Usage{}, err
	}
	rc, err := a.b.stats(ctx, c.ID)
	if err != nil {
		return Usage{}, fmt.Errorf("read stats of workload %s: %w", workloadID, err)
	}
	defer rc.Close()
	u, err := parseStats(rc)
	if err != nil {
		return Usage{}, fmt.Errorf("read stats of workload %s: %w", workloadID, err)
	}
	return u, nil
}
