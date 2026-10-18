package kvlite_test

// This file runs Blackout, KVLite's deterministic simulation test (package internal/blackout).
//
// Replay a failure with its printed seed:
//
//	go test -run 'TestBlackout$' -blackout.seed=N -blackout.trace=200 -v .

import (
	"flag"
	"strings"
	"testing"

	"github.com/Issaminu/kvlite"
	"github.com/Issaminu/kvlite/internal/blackout"
	"github.com/Issaminu/kvlite/internal/blackout/disk"
)

var (
	blackoutSeed  = flag.Uint64("blackout.seed", 0, "run only this Blackout seed (zero runs -blackout.seeds seeds from -blackout.start)")
	blackoutStart = flag.Uint64("blackout.start", 1, "first Blackout seed of the range")
	blackoutSeeds = flag.Int("blackout.seeds", 32, "number of Blackout seeds in the range")
	blackoutSteps = flag.Int("blackout.steps", blackout.DefaultSteps, "safety-phase steps per Blackout run")
	blackoutTrace = flag.Int("blackout.trace", 0, "with -blackout.seed, print this many of the last steps and storage events on failure")
)

// target connects the Blackout engine to the internals that export_test.go exports.
var target = blackout.Target{
	Open:            kvlite.BlackoutOpen,
	Checkpoint:      kvlite.BlackoutCheckpoint,
	CheckPages:      kvlite.BlackoutCheckPages,
	CheckpointCount: kvlite.BlackoutCheckpointCount,
	Abandon:         kvlite.BlackoutAbandon,
	Quiesce:         kvlite.BlackoutQuiesce,
}

func TestBlackout(t *testing.T) {
	if *blackoutSeed != 0 {
		result := blackout.Run(target, *blackoutSeed, blackout.Settings{
			Steps:      *blackoutSteps,
			KeepTrace:  *blackoutTrace > 0,
			TraceLines: *blackoutTrace,
		})
		t.Logf("seed %d config: %s\n%s", result.Seed, result.Config, result.Stats)
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
		result := blackout.Run(target, *blackoutStart+uint64(index), blackout.Settings{Steps: *blackoutSteps})
		total.Add(result.Stats)
		if result.Err != nil {
			t.Error(result.Err)
			if failures++; failures == 3 {
				t.Fatal("stopped after 3 failed seeds")
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

// TestBlackoutDetectsDurabilityDefects gives Blackout storage that lies about durability.
// Blackout must report lost data for each lie, or a passing run proves nothing.
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
			for seed := uint64(1); seed <= 40; seed++ {
				result := blackout.Run(target, seed, blackout.Settings{Steps: 150, Change: func(config *blackout.Config) {
					config.Sync = kvlite.SyncFull
					config.PowerFailures = true
					config.CheckpointBytes = 1
					config.CrashNow = disk.OneIn(8)
					config.Disk.SyncLie = test.lie
				}})
				if result.Err != nil {
					t.Logf("seed %d detected the defect: %s", seed, strings.SplitN(result.Err.Error(), "\n", 2)[0])
					return
				}
			}
			t.Fatal("40 seeds did not detect the durability defect")
		})
	}
}
