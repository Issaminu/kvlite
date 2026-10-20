package main

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestPlan(t *testing.T) {
	const gib = 1 << 30
	for _, test := range []struct {
		name        string
		budget      int64
		cpus        int
		workers     int
		maxRSS      int64
		wantWorkers int
		wantMaxRSS  int64
	}{
		{"the laptop of 2026-10-19", 18 * gib, 14, 0, -1, 4, 4 * gib},
		{"CPUs are the limit", 128 * gib, 4, 0, -1, 4, 4 * gib},
		{"memory is the limit", 8 * gib, 32, 0, -1, 2, 4 * gib},
		{"a VPS with 2 GiB and 1 CPU", 1 * gib, 1, 0, -1, 1, 1 * gib},
		{"a VPS with 512 MiB", 256 << 20, 1, 0, -1, 1, 256 << 20},
		{"unknown memory", 0, 7, 0, -1, 7, 4 * gib},
		{"the user chose the workers", 18 * gib, 7, 6, -1, 6, 3 * gib},
		{"the user chose the limit", 18 * gib, 7, 0, 1 * gib, 7, 1 * gib},
		{"the user chose no limit", 18 * gib, 7, 0, 0, 4, 0},
		{"the user chose both", 1 * gib, 1, 3, 2 * gib, 3, 2 * gib},
	} {
		workers, maxRSS := plan(test.budget, test.cpus, test.workers, test.maxRSS)
		if workers != test.wantWorkers || maxRSS != test.wantMaxRSS {
			t.Errorf("%s: got %d workers with %d bytes, want %d workers with %d bytes", test.name, workers, maxRSS, test.wantWorkers, test.wantMaxRSS)
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
}

func TestCgroupMemory(t *testing.T) {
	limited, unlimited := t.TempDir(), t.TempDir()
	for path, text := range map[string]string{
		filepath.Join(limited, "memory.max"):       "1073741824\n",
		filepath.Join(limited, "memory.current"):   "805306368\n",
		filepath.Join(unlimited, "memory.max"):     "max\n",
		filepath.Join(unlimited, "memory.current"): "104857600\n",
	} {
		if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// The cgroup allows 1 GiB and uses 768 MiB, so 256 MiB is available.
	if limit, available := cgroupMemory(limited); limit != 1<<30 || available != 256<<20 {
		t.Errorf("cgroupMemory: limit %d, available %d, want %d and %d", limit, available, 1<<30, 256<<20)
	}
	for _, dir := range []string{unlimited, ""} {
		if limit, available := cgroupMemory(dir); limit != 0 || available != 0 {
			t.Errorf("cgroupMemory(%q): limit %d, available %d, want no limit", dir, limit, available)
		}
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
