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
	// simDir is the directory of the database files on the simulated storage.
	simDir = "/sim"
	// openTries is the number of failed opens before recovery must succeed with faults and crashes turned off.
	openTries = 12
	// closeTries is the number of failed closes before Close must succeed with faults turned off.
	closeTries = 4
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
	faults          bool          // injected errors are enabled outside the checks
	stopped         bool          // the handle returned ErrWALWriteFailed
	powerLost       bool          // a power failure happened since the last verified open
	uncertainOpen   bool          // a commit result is unknown, and no open has resolved it yet
	checkpointCount uint64        // checkpoints of the current handle
	checkRand       *disk.Rand    // picks the steps of the slow checks
	optionsRand     *disk.Rand    // the options generator: picks the options of each open
	session         session       // the options of the current open
	sessions        int           // opens of the main handle so far
	tainted         bool          // a SyncNone session wrote data that no sync has made durable yet
	openPath        string        // the path that the run gives Open: absolute, relative, or a symbolic link
	realPath        string        // the absolute path of the main file
	backups         int           // backups made so far, for unique names
	lastBackup      *backupRecord // the last complete backup, which must survive a power failure
	crashedBackup   *backupRecord // a backup that a crash stopped; if its file exists, it must be complete
	readOnlyState   dbState       // the state that a read-only recovery showed, which the writable recovery must match
	memory          *kvlite.DB    // the memory database of the differential check
	lastStats       *kvlite.Stats // the last DB.Stats of the current handle
	droppedCaches   int           // disk.Stats.DroppedCaches at the last verified open
	readCorruptions int           // disk.Stats.ReadCorruptions at the start of the current step or open
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
		model:     newStateModel(config.Checksums),
		traceHash: 14695981039346656037,
		stats:     Stats{CrashesInCalls: make(map[string]int)},
	}
	if settings.KeepTrace {
		runner.trace = []string{}
		runner.config.Disk.OnEvent = func(event disk.Event) {
			runner.trace = append(runner.trace, "  "+event.String())
		}
	}
	runner.work = newWorkload(seed, &runner.config, runner.config.PageSize)
	runner.crash = newCrasher(seed, &runner.config)
	runner.checkRand = disk.NewRand(streamSeed(seed, streamCheck))
	runner.optionsRand = disk.NewRand(streamSeed(seed, streamOptions))
	runner.openPath, runner.realPath = runner.config.paths()
	if runner.config.PathMode == pathRelative {
		runner.config.Disk.WorkingDir = simDir
	}
	runner.fsys = disk.New(streamSeed(seed, streamDisk), runner.config.Disk)
	if runner.config.PathMode == pathSymlink {
		runner.fsys.Symlink("data/db", runner.openPath)
	}
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
	fmt.Fprintf(&text, "model: latest=%d floor=%d branches=%d buckets=%d\n", runner.model.latest, runner.model.floor, len(runner.model.branches), len(runner.model.bucketPaths()))
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

// openDB opens the database of the run on fsys with the options of the current session.
func (runner *runner) openDB(fsys *disk.FS, readOnly bool) (*kvlite.DB, error) {
	options := runner.options()
	if readOnly {
		options = runner.readOnlyOptions()
	}
	return runner.target.Open(runner.openPath, runner.config.FileMode, options, fsys, runner.config.PageSize)
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
		if runner.memory != nil {
			_ = runner.memory.Close()
		}
		runner.stats.Disk = runner.fsys.Stats()
		if runner.failure != nil {
			err = runner.report()
		}
	}()
	runner.event("config %s", runner.config)
	runner.guard(func() { runner.openAndVerify(false) })
	runner.faults = runner.config.IOFaults
	runner.fsys.SetFaults(runner.faults)
	for step := 0; step < runner.settings.Steps && runner.failure == nil; step++ {
		runner.step = step
		runner.event("step %d", step)
		runner.guard(runner.safetyStep)
	}
	if runner.failure == nil {
		runner.livenessPhase()
	}
	return nil
}

// safetyStep runs one step of the safety phase: a crash now, or one operation with an optional crash inside it.
func (runner *runner) safetyStep() {
	runner.readCorruptions = runner.fsys.Stats().ReadCorruptions
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
	case opBatch:
		runner.batch()
	case opChecksums:
		runner.toggleChecksums()
	case opContracts:
		runner.contracts()
	case opBackup:
		runner.backup()
	case opReadOnly:
		runner.readOnly()
	case opReaders:
		runner.readers()
	}
}

// afterStep recovers from a crash or a stopped handle and moves the durable floor.
func (runner *runner) afterStep() {
	if runner.failure != nil {
		return
	}
	if crashed, _ := runner.fsys.Crashed(); crashed {
		runner.recoverCrash()
		return
	}
	if runner.stopped {
		runner.recoverStopped()
		return
	}
	runner.updateFloor()
	runner.checkAfterStep()
}

// recoverStopped follows the documented procedure after ErrWALWriteFailed: check that the handle stopped, then close it and open the file again.
// The crash generator can crash the process instead.
func (runner *runner) recoverStopped() {
	runner.stopped = false
	if err := runner.db.View(func(*kvlite.Tx) error { return nil }); !errors.Is(err, kvlite.ErrWALWriteFailed) {
		runner.fail("View after ErrWALWriteFailed returned %v, want ErrWALWriteFailed", err)
		return
	}
	if runner.crash.enabled && runner.crash.afterWALFailure() {
		runner.event("crash after the WAL write failure")
		runner.fsys.Crash()
		runner.recoverCrash()
		return
	}
	runner.reopen()
}

// updateFloor moves the durable floor to the latest state when KVLite promises its durability.
func (runner *runner) updateFloor() {
	if runner.db == nil {
		return
	}
	if runner.tainted {
		runner.clearTaint()
		return
	}
	switch runner.session.sync {
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
// It retries after injected errors and finally turns faults off, because Close must succeed once the storage works.
// It reports false when a crash interrupted Close.
func (runner *runner) closeDB() bool {
	for attempt := 0; ; attempt++ {
		if attempt == closeTries {
			runner.fsys.SetFaults(false)
			defer runner.fsys.SetFaults(runner.faults)
		}
		err := runner.db.Close()
		runner.event("close: %v", err)
		if err == nil {
			runner.db = nil
			if runner.syncing() {
				// Close checkpoints with syncs, so the latest state is durable.
				runner.model.setFloor()
			} else {
				runner.clearTaint()
			}
			return true
		}
		if crashed, _ := runner.fsys.Crashed(); crashed {
			return false
		}
		runner.stats.FailedCloses++
		if !runner.explained(err) || attempt >= closeTries {
			runner.fail("Close: unexpected error after %d attempts: %v", attempt+1, err)
			return false
		}
	}
}

// recoverCrash drops the crashed handle, restarts the simulated process, and checks the recovered state.
func (runner *runner) recoverCrash() {
	_, event := runner.fsys.Crashed()
	if event.Op != "crash" {
		kind := event.Op
		if strings.HasSuffix(event.Name, "-wal") {
			kind += " wal"
		} else if strings.HasPrefix(event.Name, runner.realPath) {
			kind += " main"
		}
		runner.stats.CrashesInCalls[kind]++
	}
	if runner.db != nil {
		runner.target.Abandon(runner.db)
		runner.db = nil
	}
	runner.stopped = false
	// A tainted storage gets no power failure, because SyncNone promises nothing after one.
	powerFailure := runner.crash.powerFailure(disk.Ratio{Num: 2, Den: 3}) && !runner.tainted
	runner.powerLost = runner.powerLost || powerFailure
	runner.event("restart after %s (power failure: %t)", event, powerFailure)
	runner.fsys.Restart(powerFailure)
	runner.corruptAtRest()
	if runner.crash.enabled && runner.crash.rand.Chance(runner.config.ReadOnlyRecovery) {
		runner.readOnlyRecovery()
	}
	runner.openAndVerify(true)
	if runner.failure == nil {
		runner.checkBackupAfterCrash()
	}
}

// openAndVerify opens the database and checks the recovered state.
func (runner *runner) openAndVerify(afterCrash bool) {
	runner.startSession()
	for attempt := 0; ; attempt++ {
		if runner.fsys.Damaged() {
			runner.probeDamage(afterCrash)
			if runner.failure != nil {
				return
			}
		}
		runner.readCorruptions = runner.fsys.Stats().ReadCorruptions
		if attempt == openTries {
			// Recovery must finish once the storage works and the machine stops crashing.
			runner.fsys.SetFaults(false)
			runner.fsys.CancelCrash()
			defer runner.fsys.SetFaults(runner.faults)
		} else if afterCrash {
			if crash, calls := runner.crash.duringRecovery(); crash {
				runner.fsys.ScheduleCrash(calls)
			}
		}
		db, err := runner.openDB(runner.fsys, false)
		if err == nil {
			runner.db = db
			runner.fsys.CancelCrash()
			break
		}
		runner.stats.FailedOpens++
		runner.event("open: %v", err)
		if crashed, _ := runner.fsys.Crashed(); crashed && attempt < openTries {
			runner.stats.CrashesInOpen++
			powerFailure := runner.crash.powerFailure(disk.Ratio{Num: 1, Den: 2}) && !runner.tainted
			runner.powerLost = runner.powerLost || powerFailure
			if powerFailure {
				// The power failure changed the files, so the read-only recovery saw other files.
				runner.readOnlyState = nil
			}
			runner.fsys.Restart(powerFailure)
			afterCrash = true
			continue
		}
		if runner.explained(err) && attempt < openTries {
			continue
		}
		runner.fail("Open: unexpected error after %d attempts: %v", attempt+1, err)
		return
	}
	// Recovery can checkpoint inside Open.
	// Only a later checkpoint makes later commits durable.
	runner.checkpointCount = runner.target.CheckpointCount(runner.db)
	runner.lastStats = nil
	runner.verify(afterCrash)
	if runner.failure == nil {
		runner.resetMemory()
	}
}

// livenessPhase turns every fault off and requires progress: every operation succeeds, and Close and Open keep the latest state.
func (runner *runner) livenessPhase() {
	runner.event("liveness phase")
	runner.crash.enabled = false
	runner.fsys.CancelCrash()
	runner.faults = false
	runner.fsys.SetFaults(false)
	if crashed, _ := runner.fsys.Crashed(); crashed {
		runner.recoverCrash()
	}
	if runner.stopped {
		runner.recoverStopped()
	}
	weights := slices.Clone(runner.config.OperationWeights)
	weights[opReopen] = 0
	weights[opReadOnly] = 0
	for step := 0; step < LivenessSteps && runner.failure == nil; step++ {
		runner.step = runner.settings.Steps + step
		runner.guard(func() {
			runner.runOperation(runner.work.nextOperation(weights))
			if crashed, _ := runner.fsys.Crashed(); crashed {
				runner.fail("the process crashed in the liveness phase")
			}
			if runner.stopped && runner.failure == nil {
				runner.fail("a WAL write failed in the liveness phase, without injected faults")
			}
			runner.updateFloor()
			runner.checkAfterStep()
		})
	}
	if runner.failure != nil {
		return
	}
	if !runner.closeDB() {
		runner.fail("Close crashed in the liveness phase")
		return
	}
	runner.openAndVerify(false)
	if runner.failure != nil || !runner.config.PowerFailures || runner.tainted {
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
