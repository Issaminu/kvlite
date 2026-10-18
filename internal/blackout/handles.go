package blackout

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/Issaminu/kvlite"
)

// backupRecord is one backup file and the state that it must hold.
type backupRecord struct {
	path  string
	state dbState
}

// backup runs DB.Backup and checks the new file.
// A backup holds the committed state at its start. A failed backup leaves a complete file or no file.
// With SyncFull or SyncNormal, the last complete backup must survive every later power failure.
func (runner *runner) backup() {
	runner.backups++
	path := fmt.Sprintf("backup-%d", runner.backups)
	if runner.config.PathMode != pathRelative {
		path = simDir + "/" + path
	}
	record := &backupRecord{path: path, state: cloneState(runner.model.state)}
	err := runner.db.Backup(path)
	runner.event("Backup(%s): %v", path, err)
	if err != nil && !runner.explained(err) {
		runner.fail("Backup: unexpected error: %v", err)
		return
	}
	if crashed, _ := runner.fsys.Crashed(); crashed {
		// The crash recovery checks the file, if the crash left one.
		runner.crashedBackup = record
		return
	}
	_, _, exists := runner.fsys.FileData(path)
	if err == nil && !exists {
		runner.fail("Backup returned nil, but %s does not exist", path)
		return
	}
	if !exists {
		return
	}
	runner.checkBackup(record)
	runner.lastBackup = runner.keepBackup(runner.lastBackup, record, err == nil)
}

// keepBackup returns the backup to keep for the power-failure check: the new one when it is complete and durable, else the old one.
// It removes the backup that it does not keep, so the simulated storage stays small.
func (runner *runner) keepBackup(old, record *backupRecord, complete bool) *backupRecord {
	if !complete || runner.config.Sync == kvlite.SyncNone {
		return old
	}
	if old != nil {
		runner.fsys.SetFaults(false)
		_ = runner.fsys.Remove(old.path)
		runner.fsys.SetFaults(runner.faults)
	}
	return record
}

// checkBackup opens a backup read-only and requires its state.
func (runner *runner) checkBackup(record *backupRecord) {
	runner.fsys.SetFaults(false)
	defer runner.fsys.SetFaults(runner.faults)
	options := runner.options()
	options.ReadOnly = true
	db, err := runner.target.Open(record.path, runner.config.FileMode, options, runner.fsys, runner.config.PageSize)
	if err != nil {
		runner.fail("open backup %s: %v", record.path, err)
		return
	}
	defer db.Close()
	state, err := runner.readState(db)
	if err == nil {
		err = runner.target.CheckPages(db)
	}
	switch {
	case err != nil:
		runner.fail("backup %s: %v", record.path, err)
	case !statesEqual(state, record.state):
		runner.fail("backup %s does not hold the state at its start:\n%s", record.path, stateDiff(record.state, state))
	}
	if perm, ok := runner.fsys.Perm(record.path); ok && perm != runner.config.FileMode {
		runner.fail("backup %s has mode %o, want the mode %o of the main file", record.path, perm, runner.config.FileMode)
	}
}

// checkBackupAfterCrash checks the last complete backup after a crash: it must still exist and be complete.
// It also removes the temporary files that a crash inside Backup left.
func (runner *runner) checkBackupAfterCrash() {
	for _, name := range runner.fsys.Names() {
		if strings.HasPrefix(filepath.Base(name), ".kvlite-backup-") {
			runner.fsys.SetFaults(false)
			_ = runner.fsys.Remove(name)
			runner.fsys.SetFaults(runner.faults)
		}
	}
	if record := runner.crashedBackup; record != nil {
		runner.crashedBackup = nil
		if _, _, exists := runner.fsys.FileData(record.path); exists {
			// Backup publishes the file with a hard link only after the copy is complete and synced.
			runner.checkBackup(record)
			runner.fsys.SetFaults(false)
			_ = runner.fsys.Remove(record.path)
			runner.fsys.SetFaults(runner.faults)
		}
	}
	if runner.lastBackup == nil || runner.failure != nil {
		return
	}
	if _, _, exists := runner.fsys.FileData(runner.lastBackup.path); !exists {
		runner.fail("the complete backup %s did not survive the crash", runner.lastBackup.path)
		return
	}
	runner.checkBackup(runner.lastBackup)
}

// readOnly closes the handle, opens the file read-only, checks it, and then opens it writable again.
// A read-only handle must show the latest state and refuse every write.
func (runner *runner) readOnly() {
	runner.event("read-only handle")
	writable := runner.db
	if !runner.closeDB() {
		if runner.failure == nil {
			runner.recoverCrash()
		}
		return
	}
	runner.checkClosed(writable)
	db, err := runner.openDB(runner.fsys, true)
	if err != nil {
		if !runner.explained(err) {
			runner.fail("read-only Open: unexpected error: %v", err)
			return
		}
		runner.event("read-only open: %v", err)
		runner.openAndVerify(false)
		return
	}
	runner.db = db
	runner.checkReadOnly()
	for range runner.work.rand.Range(1, 3) {
		if runner.failure != nil {
			break
		}
		switch runner.work.rand.IntN(3) {
		case 0:
			runner.view()
		case 1:
			runner.get()
		default:
			runner.backup()
		}
	}
	runner.db = nil
	if err := db.Close(); err != nil && runner.failure == nil && !runner.explained(err) {
		runner.fail("read-only Close: unexpected error: %v", err)
	}
	if runner.failure == nil {
		runner.openAndVerify(false)
	}
}

// checkReadOnly requires the latest state from the read-only handle, and ErrDatabaseReadOnly from each write.
func (runner *runner) checkReadOnly() {
	runner.fsys.SetFaults(false)
	defer runner.fsys.SetFaults(runner.faults)
	state, err := runner.readState(runner.db)
	switch {
	case err != nil:
		runner.fail("read the read-only handle: %v", err)
		return
	case !statesEqual(state, runner.model.state):
		runner.fail("the read-only handle differs from the latest state:\n%s", stateDiff(runner.model.state, state))
		return
	}
	bucket, key := []byte(runner.work.bucketNames[0]), []byte("key")
	for _, write := range []struct {
		name string
		err  error
	}{
		{"Update", runner.db.Update(func(*kvlite.Tx) error { return nil })},
		{"Put", runner.db.Put(bucket, key, []byte("value"))},
		{"Delete", runner.db.Delete(bucket, key)},
		{"DeleteBucket", runner.db.DeleteBucket(bucket)},
		{"SetDataPageChecksums", runner.db.SetDataPageChecksums(!runner.model.checksums())},
	} {
		if !errors.Is(write.err, kvlite.ErrDatabaseReadOnly) {
			runner.fail("%s on a read-only handle returned %v, want ErrDatabaseReadOnly", write.name, write.err)
		}
	}
}

// readOnlyRecovery opens the crashed storage read-only before the writable recovery.
// Read-only recovery keeps the WAL in memory and does not checkpoint, so it is a separate recovery path.
// Both read the same files, so the writable recovery must show the same state.
func (runner *runner) readOnlyRecovery() {
	if runner.fsys.Damaged() {
		return
	}
	runner.fsys.SetFaults(false)
	defer runner.fsys.SetFaults(runner.faults)
	db, err := runner.openDB(runner.fsys, true)
	if errors.Is(err, fs.ErrNotExist) {
		// A power failure lost the name of the main file. A read-only open cannot create it, and the writable open makes a new file.
		runner.event("read-only recovery: no main file")
		return
	}
	if err != nil {
		runner.fail("read-only recovery: Open: %v", err)
		return
	}
	state, err := runner.readState(db)
	closeErr := db.Close()
	switch {
	case err != nil:
		runner.fail("read-only recovery: %v", err)
		return
	case closeErr != nil:
		runner.fail("read-only recovery: Close: %v", closeErr)
		return
	}
	lowest, allowBranches := runner.allowedStates(true)
	if _, _, found := runner.model.find(state, lowest, allowBranches); !found {
		runner.fail("read-only recovery shows a state that matches no allowed state %d..%d\ndifference from the latest state:\n%s", lowest, runner.model.latest, stateDiff(runner.model.state, state))
		return
	}
	runner.stats.ReadOnlyRecoveries++
	runner.readOnlyState = state
	runner.event("read-only recovery: an allowed state (state hash %x)", stateHash(state))
}
