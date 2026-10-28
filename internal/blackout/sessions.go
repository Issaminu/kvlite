package blackout

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/Issaminu/kvlite"
	"github.com/Issaminu/kvlite/internal/blackout/disk"
)

// An application can change its options between opens, for example after an upgrade.
// With Config.OptionChanges, each open of the main handle starts a session with new options from the options generator.
//
// DisableWAL selects direct main-file writes, which can corrupt earlier data after a failed write or process crash.
// Those sessions run with healthy storage and no injected crashes.
// WAL-backed SyncNone sessions keep process crashes and I/O faults, but neither form permits power failures.
// Every successful operation still has to match the model.
// A SyncNone session also taints the storage until its writes become durable.
// While the storage is tainted, no power failure happens, the floor does not move, and no sync fails.
// A failed sync is a storage failure too: it can drop the unsynced pages of the SyncNone session, even from the cache of the running process.
// The taint ends when a syncing session has made everything durable, which the disk reports ([disk.FS.Durable]).

// optionsForm is how a session gives its options to Open.
type optionsForm uint8

const (
	formExplicit optionsForm = iota // every option has a value
	formNil                         // nil Options: every default
	formZero                        // a zero Options value: every default
	formCount
)

var formNames = [formCount]string{"explicit", "nil", "zero"}

// session holds the options of one open.
type session struct {
	form            optionsForm
	sync            kvlite.Sync // the sync mode that Open resolves
	disableWAL      bool        // direct main-file writes
	checksums       bool        // the checksum option; Open uses it only for a new file
	checkpointBytes uint64      // zero means the default threshold
	maxBatch        int         // zero means the default
}

func (options session) String() string {
	if options.form != formExplicit {
		return formNames[options.form] + " options (SyncFull and every default)"
	}
	return fmt.Sprintf("sync=%s disable-wal=%t checksums=%t checkpoint=%d max-batch=%d", syncNames[options.sync], options.disableWAL, options.checksums, options.checkpointBytes, options.maxBatch)
}

// largeWAL is a checkpoint threshold that no run reaches, so the WAL grows until an explicit checkpoint, a Close, or a recovery.
const largeWAL = 1 << 40

// firstSession returns the options that the config generator selected.
func firstSession(config *Config) session {
	return session{form: formExplicit, sync: config.Sync, disableWAL: config.DisableWAL, checksums: config.Checksums, checkpointBytes: config.CheckpointBytes}
}

// nextSession picks the options of the next open.
// Without OptionChanges, every open uses the first options.
//
// Two rules keep the model exact:
//   - Until the floor passes state 0, the database file can still be lost, and Open then makes a new file from the session options.
//     So the checksum option keeps its first value, and nil and zero options, which take the operating-system page size, wait.
//   - The liveness phase uses syncing modes only, so its final power failure must keep the latest state.
func (runner *runner) nextSession() session {
	config := &runner.config
	if !config.OptionChanges {
		return firstSession(config)
	}
	rand := runner.optionsRand
	next := session{form: optionsForm(rand.Weighted([]uint64{6, 1, 1}))}
	if next.form != formExplicit && runner.model.floor == 0 {
		next.form = formExplicit
	}
	if next.form != formExplicit {
		next.sync = kvlite.SyncFull
		return next
	}
	next.sync = [...]kvlite.Sync{kvlite.SyncFull, kvlite.SyncNormal, kvlite.SyncNone}[rand.Weighted([]uint64{3, 2, 1})]
	if !runner.crash.enabled && next.sync == kvlite.SyncNone {
		next.sync = kvlite.SyncNormal
	}
	next.checksums = config.Checksums
	if runner.model.floor > 0 {
		next.checksums = rand.Bool()
	}
	next.checkpointBytes = [...]uint64{1, 16 << 10, 64 << 10, 256 << 10, 0}[rand.IntN(5)]
	if config.ValueWeights[len(config.ValueWeights)-1] == 0 && rand.Chance(disk.OneIn(8)) {
		// Values of several MiB would make a large WAL too big for memory.
		next.checkpointBytes = largeWAL
	}
	next.maxBatch = [...]int{0, 1, 2, 100}[rand.IntN(4)]
	next.disableWAL = next.sync == kvlite.SyncNone && rand.Bool()
	return next
}

// startSession starts the options of a new open of the main handle.
func (runner *runner) startSession() {
	previous := runner.session
	runner.session = runner.nextSession()
	if runner.sessions > 0 && runner.session != previous {
		runner.stats.OptionChanges++
	}
	runner.sessions++
	runner.stats.Forms[runner.session.form]++
	if runner.session.checkpointBytes == largeWAL {
		runner.stats.LargeWALSessions++
	}
	if runner.session.form == formExplicit && runner.session.maxBatch == 1 {
		runner.stats.BatchSizeOneSessions++
	}
	if runner.session.sync == kvlite.SyncNone {
		runner.tainted = true
		runner.fsys.SetSyncFaults(false)
	}
	if runner.sessions > 1 {
		// The first Open is fault-free. Later opens use the new session's policy.
		runner.configureSessionFaults()
	}
	runner.event("options: %s", runner.session)
}

// configureSessionFaults keeps recovery checks within the selected mode's guarantees.
// Direct writes use healthy storage because they have no recovery guarantee after failed writes.
// TestDisableWALPartialWriteStopsHandle covers its stopped handle.
func (runner *runner) configureSessionFaults() {
	runner.faults = runner.config.IOFaults && runner.crash.enabled && !runner.session.disableWAL
	runner.fsys.SetFaults(runner.faults)
	if runner.session.disableWAL {
		runner.fsys.CancelCrash()
	}
}

// options returns the Options of the current session.
func (runner *runner) options() *kvlite.Options {
	switch runner.session.form {
	case formNil:
		return nil
	case formZero:
		return &kvlite.Options{}
	}
	return &kvlite.Options{
		Synchronous:              runner.session.sync,
		DisableWAL:               runner.session.disableWAL,
		DataPageChecksums:        runner.session.checksums,
		CheckpointThresholdBytes: runner.session.checkpointBytes,
		MaxWriteBatchSize:        runner.session.maxBatch,
	}
}

// readOnlyOptions returns the Options of the current session for a read-only handle.
func (runner *runner) readOnlyOptions() *kvlite.Options {
	options := runner.options()
	if options == nil {
		options = &kvlite.Options{}
	}
	options.ReadOnly = true
	return options
}

// checkpointThreshold returns the checkpoint threshold that DB.Stats must report in the current session.
// The default of KVLite is about 1,000 operating-system pages, and the simulated disk gives that page size.
func (runner *runner) checkpointThreshold() uint64 {
	if runner.session.checkpointBytes == 0 {
		return 1000 * uint64(runner.fsys.PageSize())
	}
	return runner.session.checkpointBytes
}

// syncing reports whether the acknowledged state of the session can become durable: the session syncs, and no earlier SyncNone session tainted the storage.
func (runner *runner) syncing() bool {
	return runner.session.sync != kvlite.SyncNone && !runner.tainted
}

// clearTaint ends the taint of a SyncNone session once a syncing session has made the storage durable.
// The latest state is then on storage, so the floor moves to it.
func (runner *runner) clearTaint() {
	if !runner.tainted || runner.session.sync == kvlite.SyncNone || !runner.fsys.Durable() {
		return
	}
	runner.tainted = false
	runner.fsys.SetSyncFaults(true)
	runner.stats.TaintsCleared++
	runner.model.setFloor()
	runner.event("a syncing session made the storage of the SyncNone session durable")
}

// openInvalid checks that Open refuses invalid options and an empty path, and changes no file.
func (runner *runner) openInvalid() {
	rand := runner.optionsRand
	options := &kvlite.Options{}
	var name string
	switch rand.IntN(4) {
	case 0:
		options.Synchronous = kvlite.Sync(rand.Range(4, 255))
		name = "sync mode"
	case 1:
		options.MaxWriteBatchSize = -rand.Range(1, 1000)
		name = "MaxWriteBatchSize"
	case 2:
		options.LockTimeout = -time.Duration(rand.Range(1, 1_000_000_000))
		name = "LockTimeout"
	default:
		options.Synchronous = [...]kvlite.Sync{kvlite.SyncDefault, kvlite.SyncFull, kvlite.SyncNormal}[rand.IntN(3)]
		options.DisableWAL = true
		name = "DisableWAL"
	}
	runner.stats.InvalidOpens++
	names, mutations := runner.fsys.Names(), runner.fsys.Mutations()
	for _, test := range []struct {
		path    string
		options *kvlite.Options
		want    error
		what    string
	}{
		{runner.openPath, options, kvlite.ErrInvalid, "an invalid " + name},
		{"", runner.options(), kvlite.ErrPathRequired, "an empty path"},
	} {
		db, err := runner.target.Open(test.path, runner.config.FileMode, test.options, runner.fsys, runner.config.PageSize)
		if err == nil {
			_ = db.Close()
			runner.fail("Open accepted %s", test.what)
			return
		}
		if !errors.Is(err, test.want) {
			runner.fail("Open with %s returned %v, want %v", test.what, err, test.want)
			return
		}
	}
	if runner.fsys.Mutations() != mutations || !slices.Equal(names, runner.fsys.Names()) {
		runner.fail("Open with invalid options changed the files")
		return
	}
	if options.DisableWAL {
		runner.stats.InvalidWALOpens++
	}
}
