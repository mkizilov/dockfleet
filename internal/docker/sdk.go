package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/go-connections/nat"
)

// This is the only file that uses the Docker SDK (github.com/docker/docker).
// Replacing the SDK, for example with github.com/moby/moby/client, means
// rewriting this file only.

const (
	logMaxSize = "10m"
	logMaxFile = "3"
)

// sdkBackend implements backend with the Docker SDK.
type sdkBackend struct {
	cli *client.Client
}

var _ backend = (*sdkBackend)(nil)

// newSDKBackend builds a client for host, which the caller has validated.
// Extra options come last; tests use them to inject a fake HTTP transport.
// Unlike client.FromEnv it ignores DOCKER_HOST and the other environment
// variables, so the daemon the agent talks to is always the configured one.
func newSDKBackend(host string, extra ...client.Opt) (*sdkBackend, error) {
	opts := append([]client.Opt{client.WithHost(host), client.WithAPIVersionNegotiation()}, extra...)
	cli, err := client.NewClientWithOpts(opts...)
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}
	return &sdkBackend{cli: cli}, nil
}

// mapError translates SDK errors into the adapter's sentinels. conflict is
// the sentinel to use for HTTP 409, which means different things per operation.
func mapError(err, conflict error) error {
	switch {
	case err == nil:
		return nil
	case errdefs.IsNotFound(err):
		return fmt.Errorf("%w: %v", ErrNotFound, err)
	case errdefs.IsConflict(err):
		return fmt.Errorf("%w: %v", conflict, err)
	}
	return err
}

func (s *sdkBackend) ping(ctx context.Context) error {
	_, err := s.cli.Ping(ctx)
	return err
}

func (s *sdkBackend) close() error { return s.cli.Close() }

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (s *sdkBackend) list(ctx context.Context, labels map[string]string) ([]rawContainer, error) {
	args := filters.NewArgs()
	for _, k := range sortedKeys(labels) {
		args.Add("label", k+"="+labels[k])
	}
	list, err := s.cli.ContainerList(ctx, container.ListOptions{All: true, Filters: args})
	if err != nil {
		return nil, err
	}
	out := make([]rawContainer, 0, len(list))
	for _, c := range list {
		out = append(out, rawContainer{
			ID:      c.ID,
			Names:   c.Names,
			Image:   c.Image,
			Labels:  c.Labels,
			State:   c.State,
			Created: time.Unix(c.Created, 0).UTC(),
		})
	}
	return out, nil
}

// buildConfigs turns an accepted spec into Docker's create configuration. The
// hardening here is fixed and does not depend on the spec: no privileged
// mode, every capability dropped (then only the policy-allowed ones added),
// no-new-privileges, no restart, bounded logs, bounded PIDs, CPU and memory
// limits with swap disabled, and no host namespaces. Published ports bind to
// loopback unless the spec carries an address the policy accepted.
func buildConfigs(req createRequest) (*container.Config, *container.HostConfig) {
	spec := req.Spec

	exposed := nat.PortSet{}
	bindings := nat.PortMap{}
	for _, p := range spec.Ports {
		port := nat.Port(fmt.Sprintf("%d/%s", p.ContainerPort, p.protocol()))
		hostPort := ""
		if p.HostPort != 0 {
			hostPort = strconv.Itoa(int(p.HostPort))
		}
		exposed[port] = struct{}{}
		bindings[port] = append(bindings[port], nat.PortBinding{HostIP: p.hostIP(), HostPort: hostPort})
	}

	env := make([]string, 0, len(spec.Env))
	for _, k := range sortedKeys(spec.Env) {
		env = append(env, k+"="+spec.Env[k])
	}

	var mounts []mount.Mount
	tmpfs := map[string]string{}
	for _, m := range spec.Mounts {
		switch m.Type {
		case MountBind:
			mounts = append(mounts, mount.Mount{
				Type:        mount.TypeBind,
				Source:      m.Source,
				Target:      m.Target,
				ReadOnly:    m.ReadOnly,
				BindOptions: &mount.BindOptions{Propagation: mount.PropagationRPrivate},
			})
		case MountVolume:
			mounts = append(mounts, mount.Mount{Type: mount.TypeVolume, Source: m.Source, Target: m.Target, ReadOnly: m.ReadOnly})
		case MountTmpfs:
			tmpfs[m.Target] = "rw,noexec,nosuid,nodev,size=" + strconv.FormatInt(m.tmpfsSize(), 10)
		}
	}

	var capAdd []string
	for _, c := range spec.CapAdd {
		capAdd = append(capAdd, normalizeCapability(c))
	}

	pids := spec.pidsLimit()
	cfg := &container.Config{
		Image:        spec.Image,
		Cmd:          spec.Command,
		Env:          env,
		Labels:       req.Labels,
		ExposedPorts: exposed,
		User:         spec.User,
		Tty:          false,
		OpenStdin:    false,
	}
	if h := spec.HealthCheck; h != nil {
		// Docker runs the probe inside the container and reports its result in
		// the container's state; the exec form never goes through a shell.
		cfg.Healthcheck = &container.HealthConfig{
			Test:        append([]string{"CMD"}, h.Command...),
			Interval:    h.Interval,
			Timeout:     h.Timeout,
			StartPeriod: h.StartPeriod,
			Retries:     h.Retries,
		}
	}
	host := &container.HostConfig{
		NetworkMode:    container.NetworkMode(spec.networkMode()),
		PortBindings:   bindings,
		RestartPolicy:  container.RestartPolicy{Name: "no"},
		Privileged:     false,
		CapDrop:        []string{"ALL"},
		CapAdd:         capAdd,
		SecurityOpt:    []string{"no-new-privileges:true"},
		ReadonlyRootfs: spec.ReadOnlyRootFS,
		Mounts:         mounts,
		Tmpfs:          tmpfs,
		LogConfig:      container.LogConfig{Type: "json-file", Config: map[string]string{"max-size": logMaxSize, "max-file": logMaxFile}},
		Resources: container.Resources{
			Memory:     spec.MemoryBytes,
			MemorySwap: spec.MemoryBytes,
			NanoCPUs:   spec.CPUMillis * 1_000_000,
			PidsLimit:  &pids,
		},
	}
	return cfg, host
}

func (s *sdkBackend) create(ctx context.Context, req createRequest) (string, error) {
	cfg, host := buildConfigs(req)
	resp, err := s.cli.ContainerCreate(ctx, cfg, host, nil, nil, req.Name)
	switch {
	case err == nil:
		return resp.ID, nil
	case errdefs.IsNotFound(err):
		return "", fmt.Errorf("%w: %v", ErrImageNotFound, err)
	}
	return "", mapError(err, ErrConflict)
}

func (s *sdkBackend) start(ctx context.Context, id string) error {
	return mapError(s.cli.ContainerStart(ctx, id, container.StartOptions{}), ErrStateConflict)
}

func (s *sdkBackend) stop(ctx context.Context, id string, timeout time.Duration) error {
	seconds := int(timeout / time.Second)
	return mapError(s.cli.ContainerStop(ctx, id, container.StopOptions{Timeout: &seconds}), ErrStateConflict)
}

func (s *sdkBackend) remove(ctx context.Context, id string, force bool) error {
	err := s.cli.ContainerRemove(ctx, id, container.RemoveOptions{Force: force, RemoveVolumes: true})
	return mapError(err, ErrStateConflict)
}

func parseDockerTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil || t.Year() <= 1 {
		return time.Time{}
	}
	return t.UTC()
}

func (s *sdkBackend) inspect(ctx context.Context, id string) (rawInspect, error) {
	info, err := s.cli.ContainerInspect(ctx, id)
	if err != nil {
		return rawInspect{}, mapError(err, ErrStateConflict)
	}
	if info.ContainerJSONBase == nil {
		return rawInspect{}, errors.New("docker returned an empty container description")
	}
	out := rawInspect{
		rawContainer: rawContainer{
			ID:      info.ID,
			Names:   []string{info.Name},
			Created: parseDockerTime(info.Created),
		},
		RestartCount: info.RestartCount,
	}
	if info.Config != nil {
		out.Image = info.Config.Image
		out.Labels = info.Config.Labels
	}
	if st := info.State; st != nil {
		out.State = st.Status
		out.Running = st.Running
		out.ExitCode = st.ExitCode
		out.Error = st.Error
		out.OOMKilled = st.OOMKilled
		out.StartedAt = parseDockerTime(st.StartedAt)
		out.FinishedAt = parseDockerTime(st.FinishedAt)
		if st.Health != nil {
			out.Health = st.Health.Status
		}
	}
	return out, nil
}

func (s *sdkBackend) logs(ctx context.Context, id string, req logRequest) (io.ReadCloser, error) {
	tail := "all"
	if req.Tail >= 0 {
		tail = strconv.Itoa(req.Tail)
	}
	opts := container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Timestamps: req.Timestamps,
		Follow:     req.Follow,
		Tail:       tail,
	}
	if !req.Since.IsZero() {
		opts.Since = req.Since.UTC().Format(time.RFC3339Nano)
	}
	rc, err := s.cli.ContainerLogs(ctx, id, opts)
	return rc, mapError(err, ErrStateConflict)
}

// stats asks for a non-streaming sample, which the daemon fills with a
// previous sample too, so CPU usage can be computed as a percentage.
func (s *sdkBackend) stats(ctx context.Context, id string) (io.ReadCloser, error) {
	resp, err := s.cli.ContainerStats(ctx, id, false)
	if err != nil {
		return nil, mapError(err, ErrStateConflict)
	}
	return resp.Body, nil
}

func (s *sdkBackend) pull(ctx context.Context, ref string) error {
	rc, err := s.cli.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		if errdefs.IsNotFound(err) {
			return fmt.Errorf("%w: %v", ErrImageNotFound, err)
		}
		return err
	}
	defer rc.Close()
	// The pull is reported as a stream of JSON messages; failures arrive in
	// the stream with HTTP status 200.
	dec := json.NewDecoder(rc)
	for {
		var msg struct {
			Error string `json:"error"`
		}
		if err := dec.Decode(&msg); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read pull progress: %w", err)
		}
		if msg.Error != "" {
			if strings.Contains(msg.Error, "not found") || strings.Contains(msg.Error, "manifest unknown") {
				return fmt.Errorf("%w: %s", ErrImageNotFound, msg.Error)
			}
			return errors.New(msg.Error)
		}
	}
}
