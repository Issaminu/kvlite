package main

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestTotalsSurviveARestart requires that the totals of an output directory keep growing over a restart of the supervisor.
func TestTotalsSurviveARestart(t *testing.T) {
	out := t.TempDir()
	start := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	first, err := loadTotals(out, start)
	if err != nil {
		t.Fatal(err)
	}
	first.add(runOutcome{Kind: kindPass, Steps: 300, Result: &workerResult{Passed: true, Commits: 10, Recoveries: 4, Crashes: 3, PowerFailures: 1}}, 90*time.Second)
	first.add(runOutcome{Kind: kindFail, Steps: 3000, Result: &workerResult{Failure: "x", Commits: 5, Recoveries: 2, Crashes: 2}}, 30*time.Second)
	// A memory kill used worker time, but the run did not finish, so it adds no steps.
	first.add(runOutcome{Kind: kindMemory, Steps: 3000}, 60*time.Second)
	first.addWall(time.Hour)
	if err := first.save(out); err != nil {
		t.Fatal(err)
	}

	second, err := loadTotals(out, start.Add(48*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	second.add(runOutcome{Kind: kindPass, Steps: 300, Result: &workerResult{Passed: true, Commits: 1, Recoveries: 1, Crashes: 1, PowerFailures: 1}}, 30*time.Second)
	second.addWall(30 * time.Minute)
	got := *second
	got.WallHours, got.WorkerHours = 0, 0
	want := totals{Since: "2026-10-20T12:00:00Z", Runs: 4, Steps: 3600, Commits: 16, Recoveries: 7, Crashes: 6, PowerFailures: 2, Failures: 1, MemoryKills: 1}
	if got != want {
		t.Errorf("totals after a restart:\ngot  %+v\nwant %+v", got, want)
	}
	if math.Abs(second.WallHours-1.5) > 1e-9 || math.Abs(second.WorkerHours-210.0/3600) > 1e-9 {
		t.Errorf("hours: wall %v, worker %v; want 1.5 and %v", second.WallHours, second.WorkerHours, 210.0/3600)
	}
}

// TestBrokenTotalsAreKept requires that a broken totals.json is kept aside, so that a bug cannot erase the totals.
func TestBrokenTotalsAreKept(t *testing.T) {
	out := t.TempDir()
	if err := os.WriteFile(filepath.Join(out, "totals.json"), []byte("not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	fresh, err := loadTotals(out, time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Runs != 0 || fresh.Since != "2026-10-21T00:00:00Z" {
		t.Errorf("totals from a broken file: %+v", fresh)
	}
	if data, err := os.ReadFile(filepath.Join(out, "totals.json.broken")); err != nil || string(data) != "not json" {
		t.Errorf("the broken file was not kept: %q, %v", data, err)
	}
}
