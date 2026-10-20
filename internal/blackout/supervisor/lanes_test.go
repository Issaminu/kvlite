package main

import (
	"io"
	"slices"
	"strconv"
	"testing"
)

func TestParseLanes(t *testing.T) {
	got, err := parseLanes("default=80:steps=300,long=20:steps=3000,plain=5")
	if err != nil {
		t.Fatal(err)
	}
	want := []lane{{"default", 80, 300}, {"long", 20, 3000}, {"plain", 5, defaultLaneSteps}}
	if !slices.Equal(got, want) {
		t.Fatalf("lanes: got %+v, want %+v", got, want)
	}
	for _, spec := range []string{
		"",
		"default",
		"=5",
		"default=0",
		"default=-1",
		"default=x",
		"default=1:steps=0",
		"default=1:steps=x",
		"default=1:speed=3",
		"a=1,a=2",
	} {
		if _, err := parseLanes(spec); err == nil {
			t.Errorf("parseLanes(%q) accepted a bad spec", spec)
		}
	}
}

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
	opts, lanes, err := parseOptions([]string{"-workers", "2", "-lanes", "default=1:steps=50", "-memlimit", "512MiB", "-max-rss", "1GiB"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if opts.workers != 2 || len(lanes) != 1 || lanes[0].Steps != 50 || opts.memoryLimit != "512MiB" || opts.maxRSS != 1<<30 {
		t.Fatalf("options: got %+v and lanes %+v", opts, lanes)
	}
	// The defaults follow the memory and the CPUs of the computer.
	total, _ := systemMemory()
	cpus := cpuCount() / 2
	workers, maxRSS := plan(total/2, cpus, 0, -1)
	opts, _, err = parseOptions(nil, io.Discard)
	if err != nil || opts.workers != workers || opts.maxRSS != maxRSS || opts.memoryBudget != total/2 || opts.cpus != cpus || opts.memoryLimit != strconv.FormatInt(maxRSS/2, 10) {
		t.Fatalf("default options: got %+v, %v", opts, err)
	}
	// A small budget gives one worker with the whole budget.
	opts, _, err = parseOptions([]string{"-memory", "512MiB", "-cpus", "0.5"}, io.Discard)
	if err != nil || opts.workers != 1 || opts.maxRSS != 512<<20 || opts.memoryLimit != strconv.Itoa(256<<20) {
		t.Fatalf("options of a small computer: got %+v, %v", opts, err)
	}
	for _, args := range [][]string{{"-workers", "0"}, {"-memlimit", "2GB"}, {"-max-rss", "3GB"}, {"-max-rss", "-1"}, {"-max-rss", "1MiB"}, {"-memory", "off"}, {"-cpus", "0"}, {"-lanes", "x"}, {"extra"}, {"-no-such-flag"}} {
		if _, _, err := parseOptions(args, io.Discard); err == nil {
			t.Errorf("parseOptions(%q) accepted bad flags", args)
		}
	}
}
