package blackout

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/Issaminu/kvlite"
	"github.com/Issaminu/kvlite/internal/blackout/disk"
)

const (
	// databasePath is the database file on the simulated storage.
	databasePath = "/sim/db"
	// openTries is the number of failed opens before recovery must succeed with crashes turned off.
	openTries = 12
)

// runner runs one seed.
type runner struct {
	target          Target
	seed            uint64
	config          Config
	settings        Settings
	work            *workload
	crash           *crasher
	fsys            *disk.FS
	db              *kvlite.DB
	model           *stateModel
	step            int
	powerLost       bool   // a power failure happened since the last verified open
	uncertainOpen   bool   // a commit result is unknown, and no open has resolved it yet
	checkpointCount uint64 // checkpoints of the current handle
	failure         error
	events          []string
	trace           []string // every step and storage event, when the settings ask for it
	traceHash       uint64
	stats           Stats
}

func newRunner(target Target, seed uint64, config Config, settings Settings) *runner {
	runner := &runner{
		target:    target,
		seed:      seed,
		config:    config,
		settings:  settings,
		model:     newStateModel(),
		traceHash: 14695981039346656037,
		stats:     Stats{CrashesInCalls: make(map[string]int)},
	}
	if settings.KeepTrace {
		runner.trace = []string{}
		runner.config.Disk.OnEvent = func(event disk.Event) {
			runner.trace = append(runner.trace, "  "+event.String())
		}
	}
	runner.work = newWorkload(seed, &runner.config, osPageSize)
	runner.crash = newCrasher(seed, &runner.config)
	runner.fsys = disk.New(streamSeed(seed, streamDisk), runner.config.Disk)
	return runner
}

// event records one step for the failure report and the determinism check.
func (runner *runner) event(format string, args ...any) {
	text := fmt.Sprintf(format, args...)
	for index := range len(text) {
		runner.traceHash = (runner.traceHash ^ uint64(text[index])) * 1099511628211
	}
	if len(runner.events) == 60 {
		copy(runner.events, runner.events[1:])
		runner.events = runner.events[:59]
	}
	runner.events = append(runner.events, text)
	if runner.trace != nil {
		runner.trace = append(runner.trace, text)
	}
}

// fail records the first failed check. The run stops after the current step.
func (runner *runner) fail(format string, args ...any) {
	if runner.failure == nil {
		runner.failure = fmt.Errorf(format, args...)
		runner.event("FAILED: %v", runner.failure)
	}
}

// report formats a failure with everything that a replay needs.
func (runner *runner) report() error {
	var text strings.Builder
	fmt.Fprintf(&text, "blackout seed %d failed at step %d: %v\n", runner.seed, runner.step, runner.failure)
	fmt.Fprintf(&text, "config: %s\n", runner.config)
	fmt.Fprintf(&text, "model: latest=%d floor=%d branches=%d buckets=%d\n", runner.model.latest, runner.model.floor, len(runner.model.branches), len(runner.model.state))
	if runner.trace != nil && runner.settings.TraceLines > 0 {
		text.WriteString("trace:\n")
		for _, event := range runner.trace[max(0, len(runner.trace)-runner.settings.TraceLines):] {
			fmt.Fprintf(&text, "  %s\n", event)
		}
	} else {
		text.WriteString("recent steps:\n")
		for _, event := range runner.events {
			fmt.Fprintf(&text, "  %s\n", event)
		}
		text.WriteString("recent storage events:\n")
		for _, event := range runner.fsys.RecentEvents() {
			fmt.Fprintf(&text, "  %s\n", event)
		}
	}
	fmt.Fprintf(&text, "replay: go test -run 'TestBlackout$' -blackout.seed=%d -blackout.steps=%d -blackout.trace=200 -v .", runner.seed, runner.settings.Steps)
	return errors.New(text.String())
}

func (runner *runner) options() *kvlite.Options {
	return &kvlite.Options{
		Synchronous:              runner.config.Sync,
		DataPageChecksums:        runner.config.Checksums,
		CheckpointThresholdBytes: runner.config.CheckpointBytes,
	}
}

// run runs the safety phase and the liveness phase.
func (runner *runner) run() (err error) {
	defer func() {
		if value := recover(); value != nil {
			runner.fail("panic: %v", value)
		}
		if runner.db != nil {
			runner.target.Abandon(runner.db)
			runner.db = nil
		}
		runner.stats.Disk = runner.fsys.Stats()
		if runner.failure != nil {
			err = runner.report()
		}
	}()
	runner.event("config %s", runner.config)
	runner.openAndVerify(false)
	for step := 0; step < runner.settings.Steps && runner.failure == nil; step++ {
		runner.step = step
		runner.event("step %d", step)
		runner.safetyStep()
	}
	if runner.failure == nil {
		runner.livenessPhase()
	}
	return nil
}

// safetyStep runs one step of the safety phase: a crash now, or one operation with an optional crash inside it.
func (runner *runner) safetyStep() {
	switch event, calls := runner.crash.beforeStep(); event {
	case crashNow:
		runner.stats.CrashesNow++
		runner.event("crash now")
		runner.fsys.Crash()
		runner.afterStep()
		return
	case crashSchedule:
		runner.event("schedule a crash after %d mutating calls", calls)
		runner.fsys.ScheduleCrash(calls)
	}
	runner.runOperation(runner.work.nextOperation(runner.config.OperationWeights))
	runner.afterStep()
}

func (runner *runner) runOperation(op operation) {
	runner.stats.Steps[op]++
	switch op {
	case opUpdate:
		runner.update()
	case opGet:
		runner.get()
	case opView:
		runner.view()
	case opCheckpoint:
		runner.checkpoint()
	case opReopen:
		runner.reopen()
	}
}

// afterStep recovers from a crash and moves the durable floor.
func (runner *runner) afterStep() {
	if runner.failure != nil {
		return
	}
	if crashed, _ := runner.fsys.Crashed(); crashed {
		runner.recoverCrash()
		return
	}
	runner.updateFloor()
}

// updateFloor moves the durable floor to the latest state when KVLite promises its durability.
func (runner *runner) updateFloor() {
	if runner.db == nil {
		return
	}
	switch runner.config.Sync {
	case kvlite.SyncFull:
		// Every acknowledged commit is durable.
		runner.model.setFloor()
	case kvlite.SyncNormal:
		// A completed checkpoint syncs the WAL and the main file.
		if count := runner.target.CheckpointCount(runner.db); count > runner.checkpointCount {
			runner.checkpointCount = count
			runner.model.setFloor()
		}
	}
}

// closeDB closes the handle.
// It reports false when a crash interrupted Close.
func (runner *runner) closeDB() bool {
	err := runner.db.Close()
	runner.event("close: %v", err)
	if err == nil {
		runner.db = nil
		if runner.config.Sync != kvlite.SyncNone {
			// Close checkpoints with syncs, so the latest state is durable.
			runner.model.setFloor()
		}
		return true
	}
	if crashed, _ := runner.fsys.Crashed(); !crashed {
		runner.fail("Close: unexpected error: %v", err)
	}
	return false
}

// recoverCrash drops the crashed handle, restarts the simulated process, and checks the recovered state.
func (runner *runner) recoverCrash() {
	_, event := runner.fsys.Crashed()
	if event.Op != "crash" {
		kind := event.Op
		if strings.HasSuffix(event.Name, "-wal") {
			kind += " wal"
		} else if strings.HasPrefix(event.Name, databasePath) {
			kind += " main"
		}
		runner.stats.CrashesInCalls[kind]++
	}
	if runner.db != nil {
		runner.target.Abandon(runner.db)
		runner.db = nil
	}
	powerFailure := runner.crash.powerFailure(disk.Ratio{Num: 2, Den: 3})
	runner.powerLost = runner.powerLost || powerFailure
	runner.event("restart after %s (power failure: %t)", event, powerFailure)
	runner.fsys.Restart(powerFailure)
	runner.openAndVerify(true)
}

// openAndVerify opens the database and checks the recovered state.
func (runner *runner) openAndVerify(afterCrash bool) {
	for attempt := 0; ; attempt++ {
		if attempt == openTries {
			// Recovery must finish once the machine stops crashing.
			runner.fsys.CancelCrash()
		} else if afterCrash {
			if crash, calls := runner.crash.duringRecovery(); crash {
				runner.fsys.ScheduleCrash(calls)
			}
		}
		db, err := runner.target.Open(databasePath, runner.options(), runner.fsys)
		if err == nil {
			runner.db = db
			runner.fsys.CancelCrash()
			break
		}
		runner.stats.FailedOpens++
		runner.event("open: %v", err)
		if crashed, _ := runner.fsys.Crashed(); crashed && attempt < openTries {
			runner.stats.CrashesInOpen++
			powerFailure := runner.crash.powerFailure(disk.Ratio{Num: 1, Den: 2})
			runner.powerLost = runner.powerLost || powerFailure
			runner.fsys.Restart(powerFailure)
			afterCrash = true
			continue
		}
		runner.fail("Open: unexpected error after %d attempts: %v", attempt+1, err)
		return
	}
	// Recovery can checkpoint inside Open.
	// Only a later checkpoint makes later commits durable.
	runner.checkpointCount = runner.target.CheckpointCount(runner.db)
	runner.verify(afterCrash)
}

// livenessPhase turns every fault off and requires progress: every operation succeeds, and Close and Open keep the latest state.
func (runner *runner) livenessPhase() {
	runner.event("liveness phase")
	runner.crash.enabled = false
	runner.fsys.CancelCrash()
	weights := slices.Clone(runner.config.OperationWeights)
	weights[opReopen] = 0
	for step := 0; step < LivenessSteps && runner.failure == nil; step++ {
		runner.runOperation(runner.work.nextOperation(weights))
		if crashed, _ := runner.fsys.Crashed(); crashed {
			runner.fail("the process crashed in the liveness phase")
		}
		runner.updateFloor()
	}
	if runner.failure != nil {
		return
	}
	if !runner.closeDB() {
		runner.fail("Close crashed in the liveness phase")
		return
	}
	runner.openAndVerify(false)
	if runner.failure != nil || !runner.config.PowerFailures {
		return
	}
	// Close made the latest state durable, so a power failure after Close keeps it.
	if !runner.closeDB() {
		runner.fail("Close crashed in the liveness phase")
		return
	}
	runner.fsys.Restart(true)
	runner.powerLost = true
	runner.openAndVerify(true)
	if runner.failure == nil && runner.model.latest != runner.model.floor {
		runner.fail("model floor %d differs from latest %d after the final check", runner.model.floor, runner.model.latest)
	}
}
