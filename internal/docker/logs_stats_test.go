package docker

import (
	"bytes"
	"errors"
	"io"
	"math"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

func TestDemux(t *testing.T) {
	stream := bytes.Join([][]byte{
		frame(streamStdout, "line one\n"),
		frame(streamStderr, "warning\n"),
		frame(streamStdout, ""),
		frame(streamStdout, "line two\n"),
	}, nil)

	got, err := demux(bytes.NewReader(stream), 1024)
	if err != nil {
		t.Fatal(err)
	}
	want := []LogChunk{
		{Stream: "stdout", Text: "line one\n"},
		{Stream: "stderr", Text: "warning\n"},
		{Stream: "stdout", Text: "line two\n"},
	}
	if !reflect.DeepEqual(got.Chunks, want) || got.Truncated {
		t.Fatalf("demux = %+v", got)
	}
}

func TestDemuxEmptyStream(t *testing.T) {
	got, err := demux(strings.NewReader(""), 10)
	if err != nil || len(got.Chunks) != 0 || got.Truncated {
		t.Fatalf("demux(empty) = %+v, %v", got, err)
	}
}

func TestDemuxLimits(t *testing.T) {
	stream := bytes.Join([][]byte{frame(streamStdout, "abcdef"), frame(streamStderr, "ghij")}, nil)

	t.Run("limit inside a frame", func(t *testing.T) {
		got, err := demux(bytes.NewReader(stream), 4)
		if err != nil || !got.Truncated || totalBytes(got) != 4 || got.Chunks[0].Text != "abcd" {
			t.Fatalf("demux = %+v, %v", got, err)
		}
	})
	t.Run("limit exactly at a frame boundary with more to come", func(t *testing.T) {
		got, err := demux(bytes.NewReader(stream), 6)
		if err != nil || !got.Truncated || totalBytes(got) != 6 {
			t.Fatalf("demux = %+v, %v", got, err)
		}
	})
	t.Run("limit exactly at the end is not truncation", func(t *testing.T) {
		got, err := demux(bytes.NewReader(stream), 10)
		if err != nil || got.Truncated || totalBytes(got) != 10 {
			t.Fatalf("demux = %+v, %v", got, err)
		}
	})
	t.Run("a huge frame is cut without being read in full", func(t *testing.T) {
		header := frame(streamStdout, "")
		header[4], header[5], header[6], header[7] = 0xff, 0xff, 0xff, 0xff
		body := append(header, strings.Repeat("x", 64)...)
		got, err := demux(io.MultiReader(bytes.NewReader(body), iotest.ErrReader(errors.New("must not read past the limit"))), 16)
		if err != nil || !got.Truncated || totalBytes(got) != 16 {
			t.Fatalf("demux = %+v, %v", got, err)
		}
	})
}

func TestDemuxMalformedStreams(t *testing.T) {
	good := frame(streamStdout, "hello")
	tests := map[string][]byte{
		"truncated header":  good[:5],
		"truncated payload": good[:len(good)-2],
		"unknown stream":    frame(7, "x"),
		"tty raw output":    []byte("plain text from a container with a TTY\n"),
	}
	for name, stream := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := demux(bytes.NewReader(stream), 1024); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestLogOptionLimits(t *testing.T) {
	tests := []struct {
		opts     LogOptions
		tail     int
		maxBytes int
	}{
		{LogOptions{}, 100, 1 << 20},
		{LogOptions{Tail: 5, MaxBytes: 10}, 5, 10},
		{LogOptions{Tail: -3, MaxBytes: -3}, 100, 1 << 20},
		{LogOptions{Tail: 1 << 30, MaxBytes: 1 << 30}, 10000, 8 << 20},
	}
	for _, tt := range tests {
		if tt.opts.tail() != tt.tail || tt.opts.maxBytes() != tt.maxBytes {
			t.Errorf("%+v: tail %d, maxBytes %d; want %d, %d", tt.opts, tt.opts.tail(), tt.opts.maxBytes(), tt.tail, tt.maxBytes)
		}
	}
}

const statsCgroupV2 = `{
  "read": "2026-03-01T10:00:02.5Z",
  "preread": "2026-03-01T10:00:01.5Z",
  "pids_stats": {"current": 7},
  "networks": {
    "eth0": {"rx_bytes": 1000, "tx_bytes": 200},
    "eth1": {"rx_bytes": 24, "tx_bytes": 8}
  },
  "memory_stats": {"usage": 52428800, "limit": 134217728, "stats": {"anon": 1, "inactive_file": 10485760}},
  "cpu_stats": {"cpu_usage": {"total_usage": 3000000000}, "system_cpu_usage": 40000000000, "online_cpus": 4},
  "precpu_stats": {"cpu_usage": {"total_usage": 2000000000}, "system_cpu_usage": 36000000000, "online_cpus": 4}
}`

func TestParseStatsCgroupV2(t *testing.T) {
	u, err := parseStats(strings.NewReader(statsCgroupV2))
	if err != nil {
		t.Fatal(err)
	}
	if !u.ReadAt.Equal(time.Date(2026, 3, 1, 10, 0, 2, 500_000_000, time.UTC)) {
		t.Errorf("ReadAt = %v", u.ReadAt)
	}
	if u.CPUTotalNanos != 3_000_000_000 || u.Pids != 7 || u.NetRxBytes != 1024 || u.NetTxBytes != 208 {
		t.Errorf("unexpected counters: %+v", u)
	}
	// working set = 52428800 - 10485760 inactive page cache
	if u.MemoryUsageBytes != 41943040 || u.MemoryLimitBytes != 134217728 {
		t.Errorf("memory = %d / %d", u.MemoryUsageBytes, u.MemoryLimitBytes)
	}
	if math.Abs(u.MemoryPercent-31.25) > 1e-9 {
		t.Errorf("MemoryPercent = %v, want 31.25", u.MemoryPercent)
	}
	// 1e9 ns of CPU over 4e9 ns of system time on 4 CPUs = one full CPU = 100%
	if !u.HasCPUPercent || math.Abs(u.CPUPercent-100) > 1e-9 {
		t.Errorf("CPUPercent = %v (valid %v), want 100", u.CPUPercent, u.HasCPUPercent)
	}
}

func TestParseStatsCgroupV1AndMissingPreviousSample(t *testing.T) {
	const doc = `{
  "read": "2026-03-01T10:00:00Z",
  "memory_stats": {"usage": 1000, "limit": 4000, "stats": {"total_inactive_file": 400}},
  "cpu_stats": {"cpu_usage": {"total_usage": 500}, "system_cpu_usage": 9000, "online_cpus": 2},
  "precpu_stats": {"cpu_usage": {"total_usage": 0}, "system_cpu_usage": 0}
}`
	u, err := parseStats(strings.NewReader(doc))
	if err != nil {
		t.Fatal(err)
	}
	if u.MemoryUsageBytes != 600 {
		t.Errorf("MemoryUsageBytes = %d, want 600", u.MemoryUsageBytes)
	}
	if u.HasCPUPercent || u.CPUPercent != 0 {
		t.Errorf("without a previous sample there is no percentage: %+v", u)
	}
}

func TestParseStatsEdgeCases(t *testing.T) {
	// Cache larger than usage must not underflow; no limit means no percentage.
	u, err := parseStats(strings.NewReader(`{"memory_stats":{"usage":10,"limit":0,"stats":{"inactive_file":50}}}`))
	if err != nil || u.MemoryUsageBytes != 0 || u.MemoryPercent != 0 {
		t.Errorf("usage = %+v, %v", u, err)
	}
	// Online CPU count missing: assume one CPU rather than dividing by zero.
	u, err = parseStats(strings.NewReader(`{"cpu_stats":{"cpu_usage":{"total_usage":200},"system_cpu_usage":2000},"precpu_stats":{"cpu_usage":{"total_usage":100},"system_cpu_usage":1000}}`))
	if err != nil || !u.HasCPUPercent || math.Abs(u.CPUPercent-10) > 1e-9 {
		t.Errorf("usage = %+v, %v", u, err)
	}
	// Counters that went backwards (container restarted) produce no percentage.
	u, err = parseStats(strings.NewReader(`{"cpu_stats":{"cpu_usage":{"total_usage":50},"system_cpu_usage":2000},"precpu_stats":{"cpu_usage":{"total_usage":100},"system_cpu_usage":1000}}`))
	if err != nil || u.HasCPUPercent {
		t.Errorf("usage = %+v, %v", u, err)
	}
	for _, bad := range []string{"", "not json", `{"read": "yesterday"}`} {
		if _, err := parseStats(strings.NewReader(bad)); err == nil {
			t.Errorf("parseStats(%q) succeeded, want an error", bad)
		}
	}
}
