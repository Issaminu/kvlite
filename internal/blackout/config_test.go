package blackout

import (
	"errors"
	"os"
	"slices"
	"testing"

	"github.com/Issaminu/kvlite"
	"github.com/Issaminu/kvlite/internal/blackout/disk"
	"github.com/Issaminu/kvlite/internal/fileio"
)

// TestReadCorruptionNeedsFixedChecksums requires read corruption only in a run whose data pages keep their checksums for the whole run.
// Without those checksums, KVLite cannot find a flipped bit, so the run would fail without a bug.
func TestReadCorruptionNeedsFixedChecksums(t *testing.T) {
	corrupted := false
	for seed := uint64(1); seed <= 200; seed++ {
		config := newConfig(seed)
		if config.fixedChecksums() {
			corrupted = corrupted || config.Disk.ReadCorruption != disk.Never
		} else if config.Disk.ReadCorruption != disk.Never {
			t.Fatalf("seed %d: read corruption %v without fixed data-page checksums", seed, config.Disk.ReadCorruption)
		}
	}
	if !corrupted {
		t.Fatal("no seed of 200 has read corruption")
	}
}

func TestDisableWALGeneratorsAndOptions(t *testing.T) {
	var configured, sessions [2]bool
	for seed := uint64(1); seed <= 256; seed++ {
		config := newConfig(seed)
		if config.DisableWAL && config.Sync != kvlite.SyncNone {
			t.Fatalf("seed %d: DisableWAL with sync %s", seed, syncNames[config.Sync])
		}
		if config.Sync == kvlite.SyncNone {
			if config.DisableWAL {
				configured[1] = true
			} else {
				configured[0] = true
			}
		}
		r := newRunner(Target{}, seed, config, Settings{})
		r.session = firstSession(&config)
		if got := r.options(); got.DisableWAL != config.DisableWAL || got.Synchronous != config.Sync {
			t.Fatalf("seed %d: options %+v differ from config %s", seed, got, config)
		}
		config.OptionChanges = true
		r.config = config
		for range 20 {
			r.session = r.nextSession()
			options := r.options()
			if options == nil {
				continue
			}
			if options.DisableWAL && options.Synchronous != kvlite.SyncNone {
				t.Fatalf("seed %d: invalid session options %+v", seed, options)
			}
			readOnly := r.readOnlyOptions()
			if !readOnly.ReadOnly || readOnly.DisableWAL != options.DisableWAL {
				t.Fatalf("seed %d: read-only options %+v differ from %+v", seed, readOnly, options)
			}
			if options.Synchronous == kvlite.SyncNone {
				if options.DisableWAL {
					sessions[1] = true
				} else {
					sessions[0] = true
				}
			}
		}
	}
	if configured != [2]bool{true, true} || sessions != [2]bool{true, true} {
		t.Fatalf("missing WAL choices: configurations=%v sessions=%v", configured, sessions)
	}
}

func TestStressGeneratesBothWALChoices(t *testing.T) {
	stop := errors.New("stop after observing options")
	var seen [2]bool
	for seed := uint64(1); seed <= 32; seed++ {
		target := Target{Open: func(_ string, _ os.FileMode, options *kvlite.Options, _ fileio.FS, _ int) (*kvlite.DB, error) {
			if options.DisableWAL && options.Synchronous != kvlite.SyncNone {
				t.Fatalf("seed %d: invalid stress options %+v", seed, options)
			}
			if options.Synchronous == kvlite.SyncNone {
				if options.DisableWAL {
					seen[1] = true
				} else {
					seen[0] = true
				}
			}
			return nil, stop
		}}
		if err := Stress(target, seed, StressSettings{}); !errors.Is(err, stop) {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
	if seen != [2]bool{true, true} {
		t.Fatalf("stress omitted a SyncNone WAL choice: %v", seen)
	}
}

func TestWALCoverageRequiresSuccessfulRuns(t *testing.T) {
	result := Result{Config: newConfig(1), Err: errors.New("failed run")}
	result.Stats.SyncNoneSessions = [2][2]int{{1, 1}, {1, 1}}
	result.Stats.UnsyncedWALCrashes = 1
	result.Stats.InvalidWALOpens = 1
	for _, failed := range []bool{true, false} {
		if !failed {
			result.Err = nil
		}
		missing := MissingCoverage([]Result{result})
		for _, name := range []string{
			"SyncNone with DisableWAL=false checksums=false",
			"SyncNone with DisableWAL=false checksums=true",
			"SyncNone with DisableWAL=true checksums=false",
			"SyncNone with DisableWAL=true checksums=true",
			"process crash with WAL-backed SyncNone",
			"Open that refused DisableWAL outside SyncNone",
		} {
			if slices.Contains(missing, name) != failed {
				t.Fatalf("failed=%t: coverage for %q is incorrect", failed, name)
			}
		}
	}
}
