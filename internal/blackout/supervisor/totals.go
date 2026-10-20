package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// totalsFile is the name of the file in -out that counts all testing over every restart and commit.
const totalsFile = "totals.json"

// totals counts all testing in one output directory, over every restart of the supervisor and every commit.
// Blackout has no simulated clock, so the time is real time: WorkerHours is the run time of all workers together.
type totals struct {
	Since         string  `json:"since"`          // the first start of a supervisor with this output directory
	WallHours     float64 `json:"wall_hours"`     // the time that a supervisor ran
	WorkerHours   float64 `json:"worker_hours"`   // the run time of all workers together; 2 workers for 1 hour give 2 hours
	Runs          int64   `json:"runs"`           // runs of new seeds and of shrinks
	Steps         int64   `json:"steps"`          // safety-phase steps of the runs that finished
	Commits       int64   `json:"commits"`        // acknowledged commits that the checks compared with the model
	Crashes       int64   `json:"crashes"`        // injected crashes, power failures included
	PowerFailures int64   `json:"power_failures"` // injected power failures
	Recoveries    int64   `json:"recoveries"`     // recoveries that the checks compared with the model
	Failures      int64   `json:"failures"`       // runs with a fail, panic, timeout, or error result
	MemoryKills   int64   `json:"memory_kills"`
}

// loadTotals reads the totals of dir, or starts new totals at now when dir has none.
// It renames a broken file to totals.json.broken, so that a bug cannot erase the totals.
func loadTotals(dir string, now time.Time) (*totals, error) {
	path := filepath.Join(dir, totalsFile)
	fresh := &totals{Since: now.UTC().Format(time.RFC3339)}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return fresh, nil
	}
	if err != nil {
		return nil, err
	}
	var loaded totals
	if err := json.Unmarshal(data, &loaded); err != nil || loaded.Since == "" {
		if err := os.Rename(path, path+".broken"); err != nil {
			return nil, fmt.Errorf("keep the broken %s: %w", path, err)
		}
		return fresh, nil
	}
	return &loaded, nil
}

// add counts one run that took duration.
// A run that the supervisor stopped counts only its worker time.
func (counts *totals) add(outcome runOutcome, duration time.Duration) {
	counts.WorkerHours += duration.Hours()
	if outcome.Kind == kindCanceled {
		return
	}
	counts.Runs++
	switch outcome.Kind {
	case kindMemory:
		counts.MemoryKills++
	case kindPass:
	default:
		counts.Failures++
	}
	if result := outcome.Result; result != nil {
		counts.Steps += int64(outcome.Steps)
		counts.Commits += int64(result.Commits)
		counts.Crashes += int64(result.Crashes)
		counts.PowerFailures += int64(result.PowerFailures)
		counts.Recoveries += int64(result.Recoveries)
	}
}

// addWall counts time that the supervisor ran.
func (counts *totals) addWall(duration time.Duration) {
	counts.WallHours += duration.Hours()
}

// save writes the totals to dir.
func (counts *totals) save(dir string) error {
	return writeJSONFile(filepath.Join(dir, totalsFile), counts)
}
