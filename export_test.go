package kvlite

import (
	"github.com/Issaminu/kvlite/internal/fileio"
)

// This file exports internals to the Blackout simulation test in blackout_test.go.
// Go compiles it only for tests, so KVLite's public API does not change.

// BlackoutOpen opens the database at path through fsys.
func BlackoutOpen(path string, options *Options, fsys fileio.FS) (*DB, error) {
	return open(path, 0o600, options, fsys)
}

// BlackoutCheckpoint copies the committed WAL pages into the main file now.
func BlackoutCheckpoint(db *DB) error {
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	return db.checkpointWAL()
}

// BlackoutCheckPages walks every tree page and compares the walk with the allocation bitmap.
func BlackoutCheckPages(db *DB) error {
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	return db.validatePageOwnership()
}

// BlackoutCheckpointCount returns the number of checkpoints that db completed.
func BlackoutCheckpointCount(db *DB) uint64 {
	db.operationMu.RLock()
	defer db.operationMu.RUnlock()
	return db.wal.Stats().CheckpointCount
}

// BlackoutAbandon drops db without Close, as a crashed process does, and stops its write worker.
func BlackoutAbandon(db *DB) {
	db.lifecycleMu.Lock()
	db.closed = true
	db.lifecycleMu.Unlock()
	db.stopWriteBatcherAndWait()
}

// BlackoutQuiesce waits until the write worker finishes its current batch, including the checkpoint after it.
// The worker holds operationMu until then.
func BlackoutQuiesce(db *DB) {
	db.operationMu.Lock()
	db.operationMu.Unlock()
}
