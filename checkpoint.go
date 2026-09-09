package kvlite

import "errors"

// checkpointWAL copies committed WAL pages into the main file.
// It first makes the WAL durable.
// It then makes the main file durable.
// The WAL removes its committed state only after these steps succeed.
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
