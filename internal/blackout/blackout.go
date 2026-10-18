// Package blackout is KVLite's deterministic simulation test.
//
// One seed selects a configuration, a workload, every crash, and every storage decision.
// KVLite runs on the simulated storage of package disk, which can crash the process, cut the power, and tear or lose unsynced writes.
// A model holds the expected state and every older state that recovery can still return.
//
// One run has two phases, like TigerBeetle's VOPR:
//   - The safety phase runs random operations with crashes and power failures.
//     Every read must match the model, and every recovery must return exactly one state that the model allows.
//   - The liveness phase turns every fault off and requires progress: every operation must succeed, and a clean close and reopen must keep the latest state.
//
// Four random generators come from the seed:
//   - The config generator picks the database options, the workload mix, and the crash and storage settings, once at the start.
//   - The workload generator plays the user: it picks each operation, its keys and values, and whether a transaction aborts.
//   - The crash generator plays the machine: it decides when the process crashes and whether a crash is a power failure.
//   - The disk generator plays the storage hardware: it decides what a crash inside a call and a power failure keep.
//
// The same seed, code, platform, and settings give the same run.
// The package uses KVLite's public API and the internals in [Target], which the test binary supplies.
package blackout

import (
	"os"
	"time"

	"github.com/Issaminu/kvlite"
	"github.com/Issaminu/kvlite/internal/fileio"
)

// Target gives the engine the KVLite internals that the public API does not expose.
// The root package's test binary fills it from export_test.go, so KVLite's API stays unchanged.
type Target struct {
	// Open opens the database at path through fsys. A new file uses mode and pageSize.
	Open func(path string, mode os.FileMode, options *kvlite.Options, fsys fileio.FS, pageSize int) (*kvlite.DB, error)
	// Checkpoint copies the committed WAL pages into the main file now.
	Checkpoint func(db *kvlite.DB) error
	// CheckPages walks every tree page and requires that each reachable page is allocated once and that each allocated page is reachable.
	// It reads without the node cache, so it does not change what later operations read.
	CheckPages func(db *kvlite.DB) error
	// BucketNames returns the names of all top-level buckets in key order, read without the node cache.
	BucketNames func(db *kvlite.DB) ([]string, error)
	// CheckpointCount returns the number of checkpoints that this handle completed.
	CheckpointCount func(db *kvlite.DB) uint64
	// Abandon drops a handle without Close, as a crashed process does, and stops its write worker.
	Abandon func(db *kvlite.DB)
	// Quiesce waits until the write worker finishes its current batch, including the checkpoint after it.
	Quiesce func(db *kvlite.DB)
	// Batch runs the callbacks as one SyncFull WAL commit through the batch executor, in order, and returns one result for each callback.
	Batch func(db *kvlite.DB, callbacks []func(*kvlite.Tx) error) []BatchResult
	// DataPageChecksums reports whether db checks data-page checksums now.
	DataPageChecksums func(db *kvlite.DB) bool
}

// BatchResult is the result of one callback of [Target.Batch].
type BatchResult struct {
	Err        error
	Panicked   bool
	PanicValue any
	Goexited   bool
}

// Settings are the knobs of one run.
type Settings struct {
	// Steps is the number of safety-phase steps. Zero means [DefaultSteps].
	Steps int
	// KeepTrace keeps every step and every storage event of the run in [Result.Trace].
	KeepTrace bool
	// TraceLines is the number of trace lines that a failure report prints.
	// It requires KeepTrace.
	TraceLines int
	// Change, when not nil, changes the configuration that the seed selected.
	// Tests of the checker use it, for example to make the storage lie about syncs.
	Change func(*Config)
	// DeepCheck runs the slow checks after every step: the process-crash image and the memory database.
	// Without it, they run after some steps.
	// The checks never change the run, so a failure replays with and without DeepCheck.
	DeepCheck bool
	// StepTimeout is the time limit of one step. Zero means [DefaultStepTimeout].
	// A step over the limit, such as a deadlock, prints every goroutine stack and stops the process.
	StepTimeout time.Duration
}

const (
	// DefaultSteps is the default number of safety-phase steps.
	DefaultSteps = 300
	// LivenessSteps is the number of liveness-phase steps.
	LivenessSteps = 40
	// DefaultStepTimeout is the default time limit of one step.
	DefaultStepTimeout = 2 * time.Minute
)

// Result describes one run.
type Result struct {
	Seed   uint64
	Config Config
	Stats  Stats
	// TraceHash is a hash of every step and storage decision. Two runs of one seed must have the same hash.
	TraceHash uint64
	// Trace holds every step and storage event when [Settings.KeepTrace] is set.
	Trace []string
	// Err is nil when the run passed.
	// Otherwise it is a report with the failure, the configuration, the recent steps, and a replay command.
	Err error
}

// Run runs one seed and returns its result.
func Run(target Target, seed uint64, settings Settings) Result {
	if settings.Steps <= 0 {
		settings.Steps = DefaultSteps
	}
	if settings.StepTimeout <= 0 {
		settings.StepTimeout = DefaultStepTimeout
	}
	config := newConfig(seed)
	if settings.Change != nil {
		settings.Change(&config)
	}
	runner := newRunner(target, seed, config, settings)
	err := runner.run()
	return Result{
		Seed:      seed,
		Config:    runner.config,
		Stats:     runner.stats,
		TraceHash: runner.traceHash ^ runner.fsys.TraceHash()*31 ^ runner.model.hash*17,
		Trace:     runner.trace,
		Err:       err,
	}
}
