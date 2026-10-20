package main

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestPlan(t *testing.T) {
	const gib = 1 << 30
	for _, test := range []struct {
		name        string
		budget      int64
		cpus        float64
		workers     int
		maxRSS      int64
		wantWorkers int
		wantMaxRSS  int64
	}{
		{"the laptop of 2026-10-19", 18 * gib, 7, 0, -1, 4, 4 * gib},
		{"CPUs are the limit", 128 * gib, 4, 0, -1, 4, 4 * gib},
		{"memory is the limit", 8 * gib, 32, 0, -1, 2, 4 * gib},
		{"a VPS with 2 GiB and 1 CPU", 1 * gib, 0.5, 0, -1, 1, 1 * gib},
		{"a VPS with 512 MiB", 256 << 20, 0.5, 0, -1, 1, 256 << 20},
		{"unknown memory", 0, 7, 0, -1, 7, 4 * gib},
		{"the user chose the workers", 18 * gib, 7, 6, -1, 6, 3 * gib},
		{"the user chose the limit", 18 * gib, 7, 0, 1 * gib, 7, 1 * gib},
		{"the user chose no limit", 18 * gib, 7, 0, 0, 4, 0},
		{"the user chose both", 1 * gib, 0.5, 3, 2 * gib, 3, 2 * gib},
	} {
		workers, maxRSS := plan(test.budget, test.cpus, test.workers, test.maxRSS)
		if workers != test.wantWorkers || maxRSS != test.wantMaxRSS {
			t.Errorf("%s: got %d workers with %d bytes, want %d workers with %d bytes", test.name, workers, maxRSS, test.wantWorkers, test.wantMaxRSS)
		}
	}
}

func TestRestAfterKeepsTheCPUBudget(t *testing.T) {
	for _, test := range []struct {
		workers int
		cpus    float64
		want    time.Duration
	}{
		{4, 7, 0},
		{1, 1, 0},
		{1, 0.5, time.Minute},
		{2, 0.5, 3 * time.Minute},
		{4, 1, 3 * time.Minute},
	} {
		if got := restAfter(time.Minute, test.workers, test.cpus); got != test.want {
			t.Errorf("%d workers and %g CPUs: rest %v, want %v", test.workers, test.cpus, got, test.want)
		}
	}
}

func TestMemoryAllowsWorker(t *testing.T) {
	const mib = 1 << 20
	for _, test := range []struct {
		total, available, maxRSS int64
		want                     bool
	}{
		{1024 * mib, 700 * mib, 512 * mib, true},
		{1024 * mib, 600 * mib, 512 * mib, false}, // a tenth of the memory stays free for other processes
		{0, 0, 512 * mib, true},                   // unknown memory
		{1024 * mib, 0, 512 * mib, true},          // unknown available memory
	} {
		if got := memoryAllowsWorker(test.total, test.available, test.maxRSS); got != test.want {
			t.Errorf("%+v: got %v", test, got)
		}
	}
}

func TestWaitForMemory(t *testing.T) {
	stop, stopCancel := context.WithCancel(context.Background())
	defer stopCancel()
	s := newSupervisor(options{workers: 1, maxRSS: 512 << 20}, nil, stop, stopCancel, context.Background())
	s.logger = log.New(io.Discard, "", 0)
	s.setBuild(&build{Commit: "test123"}, nil)
	s.memory = func() (int64, int64) { return 1 << 30, 900 << 20 }
	if !s.waitForMemory() {
		t.Fatal("the worker waited although the system has the memory")
	}
	s.memory = func() (int64, int64) { return 1 << 30, 100 << 20 }
	time.AfterFunc(100*time.Millisecond, stopCancel)
	start := time.Now()
	if s.waitForMemory() {
		t.Fatal("the worker started although the system has too little memory")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("the stop ended the wait after %v", elapsed)
	}
}

func TestCgroupAndMeminfoParsing(t *testing.T) {
	if dir, ok := cgroupDir([]byte("0::/system.slice/blackout.service\n")); !ok || dir != "/sys/fs/cgroup/system.slice/blackout.service" {
		t.Errorf("cgroupDir: %q, %v", dir, ok)
	}
	if _, ok := cgroupDir([]byte("12:memory:/docker/abc\n")); ok {
		t.Error("cgroupDir accepted a cgroup v1 line")
	}
	meminfo := []byte("MemTotal:        2014208 kB\nMemFree:          112160 kB\nMemAvailable:     905312 kB\n")
	if got := meminfoValue(meminfo, "MemAvailable:"); got != 905312<<10 {
		t.Errorf("MemAvailable: %d", got)
	}
	if limit, ok := cgroupCPULimit([]byte("50000 100000\n")); !ok || limit != 0.5 {
		t.Errorf("cpu.max with half a CPU: %g, %v", limit, ok)
	}
	if _, ok := cgroupCPULimit([]byte("max 100000\n")); ok {
		t.Error("cpu.max without a limit gave a limit")
	}
}

// A limit of an ancestor cgroup also applies, for example MemoryMax on a systemd slice.
func TestCgroupLimitsOfAncestors(t *testing.T) {
	root := t.TempDir()
	unit := filepath.Join(root, "blackout.slice", "blackout.service")
	if err := os.MkdirAll(unit, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{
		"blackout.slice/memory.max":                      "1073741824\n",
		"blackout.slice/memory.current":                  "805306368\n",
		"blackout.slice/cpu.max":                         "50000 100000\n",
		"blackout.slice/blackout.service/memory.max":     "max\n",
		"blackout.slice/blackout.service/memory.current": "104857600\n",
		"blackout.slice/blackout.service/cpu.max":        "max 100000\n",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	dirs := cgroupAncestors(unit, root)
	if len(dirs) == 0 || dirs[0] != unit || dirs[len(dirs)-1] != root {
		t.Fatalf("cgroupAncestors: %q, want %q first and %q last", dirs, unit, root)
	}
	// The slice allows 1 GiB and uses 768 MiB, so 256 MiB is available.
	if limit, available := cgroupMemory(dirs); limit != 1<<30 || available != 256<<20 {
		t.Errorf("cgroupMemory: limit %d, available %d, want %d and %d", limit, available, 1<<30, 256<<20)
	}
	if limit, ok := cgroupCPULimitOf(dirs); !ok || limit != 0.5 {
		t.Errorf("cgroupCPULimitOf: %g, %v, want 0.5", limit, ok)
	}
}

func TestCPUCount(t *testing.T) {
	count := cpuCount()
	t.Logf("%g CPUs", count)
	if count <= 0 || count > float64(runtime.NumCPU()) {
		t.Fatalf("cpuCount() = %g", count)
	}
}

func TestSystemMemory(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("the supervisor reads the memory on macOS and Linux only")
	}
	// No computer that runs the tests has less than 256 MiB or more than 64 TiB.
	total, available := systemMemory()
	t.Logf("memory %d MiB, available %d MiB", total>>20, available>>20)
	if total < 256<<20 || total > 64<<40 || available <= 0 || available > total {
		t.Fatalf("systemMemory() = %d, %d", total, available)
	}
}

func TestPreferAsOOMVictim(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("only Linux has oom_score_adj")
	}
	if err := preferAsOOMVictim(); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile("/proc/self/oom_score_adj"); err != nil || strings.TrimSpace(string(data)) != "1000" {
		t.Fatalf("oom_score_adj: %q, %v", data, err)
	}
}
