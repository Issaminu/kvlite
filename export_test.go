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

// BlackoutBatchResult is the result of one callback of [BlackoutBatch].
type BlackoutBatchResult struct {
	Err        error
	Panicked   bool
	PanicValue any
	Goexited   bool
}

// BlackoutBatch runs the callbacks as one SyncFull batch through the batch executor, in order.
// It calls the executor directly, so the grouping does not depend on goroutine scheduling.
func BlackoutBatch(db *DB, callbacks []func(*Tx) error) []BlackoutBatchResult {
	results := make([]BlackoutBatchResult, len(callbacks))
	if err := db.beginOperation(); err != nil {
		for index := range results {
			results[index].Err = err
		}
		return results
	}
	defer db.endOperation()
	requests := make([]*writeRequest, len(callbacks))
	for index, callback := range callbacks {
		requests[index] = &writeRequest{transaction: callback, result: make(chan writeResult, 1)}
	}
	db.operationMu.Lock()
	db.executeWriteBatch(requests)
	db.operationMu.Unlock()
	for index, request := range requests {
		result := <-request.result
		results[index] = BlackoutBatchResult{Err: result.err, Panicked: result.panicked, PanicValue: result.panicValue, Goexited: result.goexited}
	}
	return results
}

// BlackoutDataPageChecksums reports whether db checks data-page checksums now.
func BlackoutDataPageChecksums(db *DB) bool {
	db.operationMu.RLock()
	defer db.operationMu.RUnlock()
	return db.meta.DataPageChecksums()
}
