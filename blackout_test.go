package kvlite_test

// This file runs Blackout, KVLite's deterministic simulation test (package internal/blackout).
//
// Replay a failure with its printed seed:
//
//	go test -run 'TestBlackout$' -blackout.seed=N -blackout.trace=200 -v .

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Issaminu/kvlite"
	"github.com/Issaminu/kvlite/internal/blackout"
	"github.com/Issaminu/kvlite/internal/blackout/disk"
	"github.com/Issaminu/kvlite/internal/fileio"
)

var (
	blackoutSeed        = flag.Uint64("blackout.seed", 0, "run only this Blackout seed (zero runs -blackout.seeds seeds from -blackout.start)")
	blackoutStart       = flag.Uint64("blackout.start", 1, "first Blackout seed of the range")
	blackoutSeeds       = flag.Int("blackout.seeds", 32, "number of Blackout seeds in the range")
	blackoutSteps       = flag.Int("blackout.steps", blackout.DefaultSteps, "safety-phase steps per Blackout run")
	blackoutTrace       = flag.Int("blackout.trace", 0, "with -blackout.seed, print this many of the last steps and storage events on failure")
	blackoutMaxFailures = flag.Int("blackout.max-failures", 3, "stop the seed range after this many failed seeds")
	blackoutJSON        = flag.String("blackout.json", "", "with -blackout.seed, write the result to this file as JSON (the autonomous runner reads it)")
	blackoutDeepCheck   = flag.Bool("blackout.deep-check", false, "run the slow checks after every step: the process-crash image and the memory database")
	blackoutStressSeeds = flag.Int("blackout.stress-seeds", 4, "number of seeds of TestBlackoutStress")
	blackoutStressSize  = flag.Int("blackout.stress-operations", 0, "operations of each stress client (zero means the default)")
	blackoutStressCount = flag.Int("blackout.stress-clients", 0, "goroutines of each stress run (zero means the default)")
)

// blackoutResult is the JSON that -blackout.json writes. The autonomous runner (internal/blackout/supervisor) reads it.
type blackoutResult struct {
	Seed          uint64 `json:"seed"`
	Steps         int    `json:"steps"`
	Passed        bool   `json:"passed"`
	Failure       string `json:"failure,omitempty"` // first line of the report
	Report        string `json:"report,omitempty"`
	Config        string `json:"config"`
	TraceHash     string `json:"trace_hash"`
	GoVersion     string `json:"go_version"`
	GOOS          string `json:"goos"`
	GOARCH        string `json:"goarch"`
	PageSize      int    `json:"page_size"` // the operating-system page size: the default checkpoint threshold depends on it, so a replay needs the same size
	Commits       int    `json:"commits"`
	Recoveries    int    `json:"recoveries"`
	Crashes       int    `json:"crashes"`
	PowerFailures int    `json:"power_failures"`
}

// writeBlackoutResult writes result to path through a temporary file, so a reader never sees a partial file.
func writeBlackoutResult(path string, result blackout.Result, steps int) error {
	data := blackoutResult{
		Seed:          result.Seed,
		Steps:         steps,
		Passed:        result.Err == nil,
		Config:        result.Config.String(),
		TraceHash:     fmt.Sprintf("%016x", result.TraceHash),
		GoVersion:     runtime.Version(),
		GOOS:          runtime.GOOS,
		GOARCH:        runtime.GOARCH,
		PageSize:      os.Getpagesize(),
		Commits:       result.Stats.Commits,
		Recoveries:    result.Stats.Recoveries,
		Crashes:       result.Stats.Disk.ProcessCrashes + result.Stats.Disk.PowerFailures,
		PowerFailures: result.Stats.Disk.PowerFailures,
	}
	if result.Err != nil {
		data.Report = result.Err.Error()
		data.Failure, _, _ = strings.Cut(data.Report, "\n")
	}
	encoded, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, encoded, 0o644); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

// target connects the Blackout engine to the internals that export_test.go exports.
var target = blackout.Target{
	Open:            kvlite.BlackoutOpen,
	Checkpoint:      kvlite.BlackoutCheckpoint,
	CheckPages:      kvlite.BlackoutCheckPages,
	BucketNames:     kvlite.BlackoutBucketNames,
	CheckpointCount: kvlite.BlackoutCheckpointCount,
	Abandon:         kvlite.BlackoutAbandon,
	Quiesce:         kvlite.BlackoutQuiesce,
	Batch: func(db *kvlite.DB, callbacks []func(*kvlite.Tx) error) []blackout.BatchResult {
		results := make([]blackout.BatchResult, len(callbacks))
		for index, result := range kvlite.BlackoutBatch(db, callbacks) {
			results[index] = blackout.BatchResult(result)
		}
		return results
	},
	DataPageChecksums: kvlite.BlackoutDataPageChecksums,
}

func TestBlackout(t *testing.T) {
	if *blackoutSeed != 0 {
		result := blackout.Run(target, *blackoutSeed, blackout.Settings{
			Steps:      *blackoutSteps,
			KeepTrace:  *blackoutTrace > 0,
			TraceLines: *blackoutTrace,
			DeepCheck:  *blackoutDeepCheck,
		})
		t.Logf("seed %d config: %s\n%s", result.Seed, result.Config, result.Stats)
		if *blackoutJSON != "" {
			if err := writeBlackoutResult(*blackoutJSON, result, *blackoutSteps); err != nil {
				t.Fatalf("write the JSON result: %v", err)
			}
		}
		if result.Err != nil {
			t.Fatal(result.Err)
		}
		return
	}
	seeds := *blackoutSeeds
	if testing.Short() {
		seeds = min(seeds, 8)
	}
	var total blackout.Stats
	failures := 0
	for index := range seeds {
		result := blackout.Run(target, *blackoutStart+uint64(index), blackout.Settings{Steps: *blackoutSteps, DeepCheck: *blackoutDeepCheck})
		total.Add(result.Stats)
		if result.Err != nil {
			t.Error(result.Err)
			if failures++; failures == *blackoutMaxFailures {
				t.Fatalf("stopped after %d failed seeds", failures)
			}
		}
	}
	t.Logf("%d seeds of %d steps:\n%s", seeds, *blackoutSteps, total)
}

// TestBlackoutIsDeterministic runs seeds twice and requires the same trace, so that a printed seed replays a failure.
// It is a test of Blackout itself.
func TestBlackoutIsDeterministic(t *testing.T) {
	for seed := uint64(1); seed <= 4; seed++ {
		settings := blackout.Settings{Steps: 120, KeepTrace: true}
		first := blackout.Run(target, seed, settings)
		second := blackout.Run(target, seed, settings)
		if (first.Err == nil) != (second.Err == nil) {
			t.Errorf("seed %d: results differ: %v, %v", seed, first.Err, second.Err)
		}
		for index := range max(len(first.Trace), len(second.Trace)) {
			if index < len(first.Trace) && index < len(second.Trace) && first.Trace[index] == second.Trace[index] {
				continue
			}
			from := max(0, index-8)
			t.Errorf("seed %d: runs differ at event %d\nfirst run:\n%s\nsecond run:\n%s", seed, index,
				strings.Join(first.Trace[from:min(len(first.Trace), index+3)], "\n"),
				strings.Join(second.Trace[from:min(len(second.Trace), index+3)], "\n"))
			break
		}
		if first.TraceHash != second.TraceHash {
			t.Errorf("seed %d: trace hashes differ", seed)
		}
	}
}

// TestBlackoutCoverage runs seeds and requires that they reach every important case: each operation, change, scan, contract, outcome, fault, and option combination.
// A change to Blackout that makes a case unreachable fails this test, even when every seed passes.
// The test ignores failed seeds: TestBlackout reports them.
// It is a test of Blackout itself.
func TestBlackoutCoverage(t *testing.T) {
	if testing.Short() {
		t.Skip("the coverage test runs many seeds")
	}
	const seeds = 96
	results := make([]blackout.Result, 0, seeds)
	for seed := uint64(1); seed <= seeds; seed++ {
		results = append(results, blackout.Run(target, seed, blackout.Settings{Steps: 150}))
	}
	for _, missing := range blackout.MissingCoverage(results) {
		t.Errorf("%d seeds did not reach: %s", seeds, missing)
	}
}

// TestBlackoutWatchdogStopsAHang runs a step that never ends in a child process.
// The watchdog must print the goroutine stacks and stop the process, so a hang is a failure, not a test that never ends.
// It is a test of Blackout itself.
func TestBlackoutWatchdogStopsAHang(t *testing.T) {
	if os.Getenv("BLACKOUT_WATCHDOG_CHILD") == "1" {
		hang := target
		hang.Open = func(string, os.FileMode, *kvlite.Options, fileio.FS, int) (*kvlite.DB, error) { select {} }
		blackout.Run(hang, 1, blackout.Settings{StepTimeout: 100 * time.Millisecond})
		return
	}
	command := exec.Command(os.Args[0], "-test.run=^TestBlackoutWatchdogStopsAHang$")
	command.Env = append(os.Environ(), "BLACKOUT_WATCHDOG_CHILD=1")
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("the hung run exited cleanly:\n%s", output)
	}
	for _, want := range []string{"did not finish in 100ms", "goroutine stacks:", "TestBlackoutWatchdogStopsAHang"} {
		if !strings.Contains(string(output), want) {
			t.Errorf("the watchdog output lacks %q:\n%s", want, output)
		}
	}
}

// TestBlackoutStress runs real goroutines against one database at the same time (see [blackout.Stress]).
// The goroutine schedule does not repeat, so a failure may not replay. Run it with the race detector too:
//
//	go test -race -run TestBlackoutStress .
func TestBlackoutStress(t *testing.T) {
	seeds := uint64(*blackoutStressSeeds)
	if testing.Short() {
		seeds = 1
	}
	for seed := uint64(1); seed <= seeds; seed++ {
		result, err := blackout.Stress(target, seed, blackout.StressSettings{Clients: *blackoutStressCount, Operations: *blackoutStressSize})
		if err != nil {
			t.Error(err)
			continue
		}
		t.Log(result)
	}
}

// TestBlackoutDetectsDurabilityDefects gives Blackout storage that lies about durability.
// Blackout must report lost data for each lie, or a passing run proves nothing.
// A seed counts only when the same seed passes without the lie, so an open KVLite bug cannot stand in for the lie.
// It is a test of Blackout itself.
func TestBlackoutDetectsDurabilityDefects(t *testing.T) {
	for _, test := range []struct {
		name string
		lie  disk.SyncLie
	}{
		{"file syncs do nothing", disk.SyncLieAllFiles},
		{"main-file syncs do nothing", disk.SyncLieMainFile},
		{"directory syncs do nothing", disk.SyncLieDirectory},
	} {
		t.Run(test.name, func(t *testing.T) {
			settings := func(lie disk.SyncLie) blackout.Settings {
				return blackout.Settings{Steps: 150, Change: func(config *blackout.Config) {
					config.Sync = kvlite.SyncFull
					config.PowerFailures = true
					config.CheckpointBytes = 1
					config.CrashNow = disk.OneIn(8)
					// Only crashes and power failures, so the control run does not hit open bugs of other faults.
					config.IOFaults = false
					config.OptionChanges = false
					config.CorruptAtRest = disk.Never
					config.Disk = disk.Config{SectorSize: config.Disk.SectorSize, KeepChange: config.Disk.KeepChange, InOrder: config.Disk.InOrder, SyncLie: lie}
				}}
			}
			for seed := uint64(1); seed <= 40; seed++ {
				result := blackout.Run(target, seed, settings(test.lie))
				if result.Err == nil {
					continue
				}
				if control := blackout.Run(target, seed, settings(disk.SyncLieNone)); control.Err != nil {
					continue
				}
				t.Logf("seed %d detected the defect: %s", seed, strings.SplitN(result.Err.Error(), "\n", 2)[0])
				return
			}
			t.Fatal("40 seeds did not detect the durability defect")
		})
	}
}
