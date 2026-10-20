package main

import (
	"io"
	"runtime"
	"slices"
	"strconv"
	"testing"
	"time"
)

func TestPickLaneFollowsWeights(t *testing.T) {
	lanes := []lane{{"default", 3, 300}, {"long", 1, 3000}, {"tiny", 2, 10}}
	counts := map[string]int{}
	for n := range totalWeight(lanes) {
		counts[pickLane(lanes, n).Name]++
	}
	for _, lane := range lanes {
		if counts[lane.Name] != lane.Weight {
			t.Errorf("lane %s: %d of %d values select it, want %d", lane.Name, counts[lane.Name], totalWeight(lanes), lane.Weight)
		}
	}
}

func TestParseOptions(t *testing.T) {
	opts, lanes, err := parseOptions([]string{"-workers", "2", "-steps", "50", "-memlimit", "512MiB", "-max-rss", "1GiB"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if opts.workers != 2 || len(lanes) != 1 || lanes[0].Steps != 50 || opts.memoryLimit != "512MiB" || opts.maxRSS != 1<<30 {
		t.Fatalf("options: got %+v and lanes %+v", opts, lanes)
	}
	// Without -steps, most seeds are short and a fifth of them are long.
	if _, lanes, err = parseOptions(nil, io.Discard); err != nil || !slices.Equal(lanes, []lane{{"default", 80, 300}, {"long", 20, 3000}}) {
		t.Fatalf("default lanes: got %+v, %v", lanes, err)
	}
	// The defaults follow the memory and the CPUs of the computer.
	total, _ := systemMemory()
	workers, maxRSS := plan(total/2, runtime.GOMAXPROCS(0), 0, -1)
	opts, _, err = parseOptions(nil, io.Discard)
	if err != nil || opts.workers != workers || opts.maxRSS != maxRSS || opts.memoryBudget != total/2 || opts.memoryLimit != strconv.FormatInt(maxRSS/2, 10) {
		t.Fatalf("default options: got %+v, %v", opts, err)
	}
	// A heavy 3000-step seed took 39 minutes on an efficiency core of an Apple M4 Max, so a short default timeout stops slow runs that make progress.
	if opts.timeout != 2*time.Hour {
		t.Fatalf("default timeout: got %v, want 2h", opts.timeout)
	}
	// With enough memory, the CPUs are the limit: one worker for each CPU that the Go runtime uses.
	opts, _, err = parseOptions([]string{"-memory", "1TiB"}, io.Discard)
	if err != nil || opts.workers != runtime.GOMAXPROCS(0) {
		t.Fatalf("workers with enough memory: got %d, want %d (%v)", opts.workers, runtime.GOMAXPROCS(0), err)
	}
	// A small budget gives one worker with the whole budget.
	opts, _, err = parseOptions([]string{"-memory", "512MiB"}, io.Discard)
	if err != nil || opts.workers != 1 || opts.maxRSS != 512<<20 || opts.memoryLimit != strconv.Itoa(256<<20) {
		t.Fatalf("options of a small computer: got %+v, %v", opts, err)
	}
	for _, args := range [][]string{{"-workers", "0"}, {"-memlimit", "2GB"}, {"-max-rss", "3GB"}, {"-max-rss", "-1"}, {"-max-rss", "1MiB"}, {"-memory", "off"}, {"-steps", "-1"}, {"-steps", "x"}, {"extra"}, {"-no-such-flag"}} {
		if _, _, err := parseOptions(args, io.Discard); err == nil {
			t.Errorf("parseOptions(%q) accepted bad flags", args)
		}
	}
}
