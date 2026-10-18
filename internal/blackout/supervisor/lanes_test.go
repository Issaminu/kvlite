package main

import (
	"io"
	"slices"
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
	opts, lanes, err := parseOptions([]string{"-workers", "2", "-lanes", "default=1:steps=50", "-memlimit", "512MiB"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if opts.workers != 2 || len(lanes) != 1 || lanes[0].Steps != 50 || opts.memoryLimit != "512MiB" {
		t.Fatalf("options: got %+v and lanes %+v", opts, lanes)
	}
	for _, args := range [][]string{{"-workers", "0"}, {"-memlimit", "2GB"}, {"-lanes", "x"}, {"extra"}, {"-no-such-flag"}} {
		if _, _, err := parseOptions(args, io.Discard); err == nil {
			t.Errorf("parseOptions(%q) accepted bad flags", args)
		}
	}
}
