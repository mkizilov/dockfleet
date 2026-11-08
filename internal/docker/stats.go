package docker

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// statsPayload mirrors the parts of the Docker Engine API stats document that
// DockFleet reads. The JSON field names are part of the Engine API contract.
type statsPayload struct {
	Read        time.Time `json:"read"`
	CPUStats    cpuStats  `json:"cpu_stats"`
	PreCPUStats cpuStats  `json:"precpu_stats"`
	MemoryStats struct {
		Usage uint64            `json:"usage"`
		Limit uint64            `json:"limit"`
		Stats map[string]uint64 `json:"stats"`
	} `json:"memory_stats"`
	PidsStats struct {
		Current uint64 `json:"current"`
	} `json:"pids_stats"`
	Networks map[string]struct {
		RxBytes uint64 `json:"rx_bytes"`
		TxBytes uint64 `json:"tx_bytes"`
	} `json:"networks"`
}

type cpuStats struct {
	CPUUsage struct {
		TotalUsage uint64 `json:"total_usage"`
	} `json:"cpu_usage"`
	SystemUsage uint64 `json:"system_cpu_usage"`
	OnlineCPUs  uint32 `json:"online_cpus"`
}

const maxStatsBytes = 1 << 20

// parseStats decodes one stats document.
func parseStats(r io.Reader) (Usage, error) {
	var p statsPayload
	if err := json.NewDecoder(io.LimitReader(r, maxStatsBytes)).Decode(&p); err != nil {
		return Usage{}, fmt.Errorf("decode container stats: %w", err)
	}
	return p.usage(), nil
}

func (p statsPayload) usage() Usage {
	u := Usage{
		ReadAt:           p.Read.UTC(),
		CPUTotalNanos:    p.CPUStats.CPUUsage.TotalUsage,
		MemoryUsageBytes: workingSet(p.MemoryStats.Usage, p.MemoryStats.Stats),
		MemoryLimitBytes: p.MemoryStats.Limit,
		Pids:             p.PidsStats.Current,
	}
	if u.MemoryLimitBytes > 0 {
		u.MemoryPercent = float64(u.MemoryUsageBytes) / float64(u.MemoryLimitBytes) * 100
	}
	for _, n := range p.Networks {
		u.NetRxBytes += n.RxBytes
		u.NetTxBytes += n.TxBytes
	}

	// Percentage of one CPU over the interval between the daemon's two samples.
	cpuDelta := float64(p.CPUStats.CPUUsage.TotalUsage) - float64(p.PreCPUStats.CPUUsage.TotalUsage)
	sysDelta := float64(p.CPUStats.SystemUsage) - float64(p.PreCPUStats.SystemUsage)
	if p.PreCPUStats.SystemUsage > 0 && cpuDelta >= 0 && sysDelta > 0 {
		cpus := float64(p.CPUStats.OnlineCPUs)
		if cpus == 0 {
			cpus = 1
		}
		u.CPUPercent = cpuDelta / sysDelta * cpus * 100
		u.HasCPUPercent = true
	}
	return u
}

// workingSet subtracts reclaimable page cache from raw memory usage, the way
// `docker stats` does: cgroup v2 reports inactive_file, cgroup v1
// total_inactive_file.
func workingSet(usage uint64, stats map[string]uint64) uint64 {
	for _, key := range []string{"inactive_file", "total_inactive_file"} {
		if cache, ok := stats[key]; ok {
			if cache < usage {
				return usage - cache
			}
			return 0
		}
	}
	return usage
}
