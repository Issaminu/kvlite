package main

import (
	"runtime"
	"testing"
)

func TestDefaultWorkers(t *testing.T) {
	const gib = 1 << 30
	for _, test := range []struct {
		name   string
		cores  int
		ram    int64
		maxRSS int64
		want   int
	}{
		{"the laptop of 2026-10-19", 14, 36 * gib, 3 * gib, 6},
		{"cores are the limit", 8, 256 * gib, 3 * gib, 4},
		{"RAM is the limit", 64, 16 * gib, 3 * gib, 2},
		{"RAM for less than one worker", 8, 4 * gib, 3 * gib, 1},
		{"one core", 1, 36 * gib, 3 * gib, 1},
		{"unknown RAM", 14, 0, 3 * gib, 7},
		{"no memory limit", 14, 36 * gib, 0, 7},
	} {
		if got := defaultWorkers(test.cores, test.ram, test.maxRSS); got != test.want {
			t.Errorf("%s: got %d workers, want %d", test.name, got, test.want)
		}
	}
}

func TestPhysicalMemory(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("the supervisor reads the RAM size on macOS and Linux only")
	}
	// No computer that runs the tests has less than 256 MiB or more than 64 TiB.
	if ram := physicalMemory(); ram < 256<<20 || ram > 64<<40 {
		t.Fatalf("physicalMemory() = %d", ram)
	}
}
