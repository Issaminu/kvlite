package kvlite

import "errors"

// checkpointWAL copies committed WAL pages into the main file.
// SyncFull and SyncNormal make the WAL and main file durable before WAL cleanup.
// SyncNone does not issue storage sync calls.
// The WAL removes its committed state only after the required steps succeed.
// A failed write or sync can be retried.
//
// The checkpoint refreshes the mapping before another operation can read it.
//
// The caller must hold the exclusive database operation lock.
func (db *DB) checkpointWAL() error {
	if db.wal.Stats().CommittedRecordCount == 0 {
		return nil
	}
	checkpointErr := db.wal.Checkpoint(db.file)
	mapErr := db.refreshMainFileMapping()
	return errors.Join(checkpointErr, mapErr)
}
