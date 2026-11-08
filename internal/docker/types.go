package docker

import "time"

// Labels DockFleet puts on every container it creates. They are the only way
// the adapter recognizes "its" containers, so the names and values are a
// stable contract: never rename them, only add new ones (and bump
// schemaVersion if the meaning of an existing one changes).
const (
	LabelManaged      = "io.dockfleet.managed"       // "true" on every managed container
	LabelSchema       = "io.dockfleet.schema"        // label schema version
	LabelNodeID       = "io.dockfleet.node-id"       // node that created and owns the container
	LabelWorkloadID   = "io.dockfleet.workload-id"   // stable workload identifier from the manager
	LabelWorkloadName = "io.dockfleet.workload-name" // human-readable workload name (informational)

	managedValue  = "true"
	schemaVersion = "1"
	namePrefix    = "dockfleet-"
)

// State is a container's lifecycle state as reported by Docker.
type State string

const (
	StateCreated    State = "created"
	StateRunning    State = "running"
	StatePaused     State = "paused"
	StateRestarting State = "restarting"
	StateRemoving   State = "removing"
	StateExited     State = "exited"
	StateDead       State = "dead"
	StateUnknown    State = "unknown"
)

// Mount types a ContainerSpec may request.
const (
	MountBind   = "bind"
	MountVolume = "volume"
	MountTmpfs  = "tmpfs"
)

// PortMapping publishes one container port on the host.
type PortMapping struct {
	ContainerPort uint16
	HostPort      uint16 // 0 lets Docker pick a free port
	Protocol      string // "tcp" (default) or "udp"
	HostIP        string // default 127.0.0.1; anything else needs Policy.AllowPublicPorts
}

// Mount attaches storage to the container.
type Mount struct {
	Type      string // MountBind, MountVolume or MountTmpfs
	Source    string // host directory (bind), volume name "dockfleet-..." (volume), empty (tmpfs)
	Target    string // absolute path inside the container
	ReadOnly  bool
	SizeBytes int64 // tmpfs only; 0 means 64 MiB
}

// Health check results in ContainerState.Health.
const (
	HealthStarting  = "starting"
	HealthHealthy   = "healthy"
	HealthUnhealthy = "unhealthy"
)

// HealthCheck is a command Docker runs inside the container at an interval;
// exit status 0 is healthy. After Retries failures in a row the container is
// unhealthy, which ContainerState.Health reports. Failures during StartPeriod
// do not count.
type HealthCheck struct {
	Command     []string // exec form: the program and its arguments, never a shell string
	Interval    time.Duration
	Timeout     time.Duration
	StartPeriod time.Duration
	Retries     int
}

// ContainerSpec describes a container to create. It is deliberately a
// restricted subset of what Docker accepts. Fields that could weaken the
// isolation of the host (Privileged through SecurityOpt) exist only so that
// requests for them can be rejected with a clear reason; with the default
// Policy they must stay zero.
type ContainerSpec struct {
	WorkloadID   string
	WorkloadName string
	Image        string
	Command      []string
	Env          map[string]string
	User         string

	CPUMillis      int64 // 1000 = one CPU; required
	MemoryBytes    int64 // hard limit, swap included; required
	PidsLimit      int64 // 0 means 512
	ReadOnlyRootFS bool
	NetworkMode    string // "", "bridge" or "none"

	Ports  []PortMapping
	Mounts []Mount

	// HealthCheck is optional; without it the container has no health state.
	HealthCheck *HealthCheck

	Privileged  bool
	CapAdd      []string
	Devices     []string
	PidMode     string
	IpcMode     string
	UTSMode     string
	UsernsMode  string
	SecurityOpt []string
}

// Container identifies a DockFleet-managed container.
type Container struct {
	ID           string // Docker's container ID
	Name         string
	WorkloadID   string
	WorkloadName string
	NodeID       string
	Image        string
	State        State
	CreatedAt    time.Time
}

// ContainerState is the detailed state of a managed container.
type ContainerState struct {
	Container
	Running      bool
	ExitCode     int
	Error        string // Docker's error message for a failed start or run
	OOMKilled    bool
	RestartCount int
	Health       string // "", "starting", "healthy" or "unhealthy"
	StartedAt    time.Time
	FinishedAt   time.Time
}

// LogOptions bounds a log request.
type LogOptions struct {
	Tail       int       // last N lines; 0 means 100, at most 10000
	Since      time.Time // zero means from the start
	Timestamps bool
	MaxBytes   int // cap on returned bytes; 0 means 1 MiB, at most 8 MiB
}

// LogChunk is one piece of container output.
type LogChunk struct {
	Stream string // "stdout" or "stderr"
	Text   string
}

// Logs is a bounded, ordered slice of container output.
type Logs struct {
	Chunks    []LogChunk
	Truncated bool // output beyond MaxBytes was dropped
}

// LogStreamOptions says which part of a container's output StreamLogs reads.
type LogStreamOptions struct {
	// Tail is how many of the last lines to start with, counted after Since:
	// negative means all of them, 0 none.
	Tail int
	// Since excludes lines written before this time; zero means no bound.
	Since time.Time
	// Follow keeps reading as the container writes, until it stops.
	Follow bool
	// MaxLineBytes cuts longer lines; 0 means 16 KiB, at most 64 KiB.
	MaxLineBytes int
}

// LogLine is one line of container output, with the time the daemon recorded it.
type LogLine struct {
	Time      time.Time
	Stream    string // "stdout" or "stderr"
	Text      string // valid UTF-8, without the line break
	Truncated bool   // the line was longer than MaxLineBytes; the rest was dropped
}

// LogReader reads a container's output a line at a time, holding at most one
// line and one read buffer in memory: what the caller does not read stays in
// the daemon. Next blocks until a line is available and returns io.EOF at the
// end of the output (for a following reader, when the container stops). Close
// ends a read that is blocked in Next.
type LogReader interface {
	Next() (LogLine, error)
	Close() error
}

// Usage is one resource sample of a running container.
type Usage struct {
	ReadAt time.Time

	CPUTotalNanos uint64  // cumulative CPU time consumed
	CPUPercent    float64 // of one CPU; meaningful only when HasCPUPercent
	HasCPUPercent bool    // the daemon supplied a previous sample to compare against

	MemoryUsageBytes uint64 // working set: usage minus reclaimable page cache
	MemoryLimitBytes uint64
	MemoryPercent    float64

	Pids       uint64
	NetRxBytes uint64
	NetTxBytes uint64
}

func (s ContainerSpec) pidsLimit() int64 {
	if s.PidsLimit > 0 {
		return s.PidsLimit
	}
	return defaultPidsLimit
}

// networkMode is "none" or "bridge". Any other value is rejected by the
// policy earlier; this keeps the Docker layer safe even if it is not.
func (s ContainerSpec) networkMode() string {
	if s.NetworkMode == "none" {
		return "none"
	}
	return "bridge"
}

func (p PortMapping) protocol() string {
	if p.Protocol == "" {
		return "tcp"
	}
	return p.Protocol
}

func (p PortMapping) hostIP() string {
	if p.HostIP == "" {
		return "127.0.0.1"
	}
	return p.HostIP
}

func (m Mount) tmpfsSize() int64 {
	if m.SizeBytes > 0 {
		return m.SizeBytes
	}
	return defaultTmpfsBytes
}

const (
	defaultPidsLimit  = 512
	defaultTmpfsBytes = 64 << 20
)
