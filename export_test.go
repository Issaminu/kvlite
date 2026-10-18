package kvlite

import (
	"os"
	"slices"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
)

// This file exports internals to the Blackout simulation test in blackout_test.go.
// Go compiles it only for tests, so KVLite's public API does not change.

// BlackoutOpen opens the database at path through fsys.
// A new file uses mode and pageSize, so each run selects them.
func BlackoutOpen(path string, mode os.FileMode, options *Options, fsys fileio.FS, pageSize int) (*DB, error) {
	withPageSize := *options
	withPageSize.pageSize = int64(pageSize)
	return open(path, mode, &withPageSize, fsys)
}

// BlackoutCheckpoint copies the committed WAL pages into the main file now.
func BlackoutCheckpoint(db *DB) error {
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	return db.checkpointWAL()
}

// BlackoutCheckPages walks every tree page and compares the walk with the allocation bitmap.
// It reads without the node cache, so it can run after every step and not change what later operations read.
func BlackoutCheckPages(db *DB) error {
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	return db.validatePageOwnershipWith(db.blackoutReadNode)
}

// BlackoutBucketNames returns the names of the top-level buckets in key order.
// It reads without the node cache.
func BlackoutBucketNames(db *DB) ([]string, error) {
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	var names []string
	pending := []page.ID{db.meta.Root()}
	for len(pending) > 0 {
		pageID := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		node, err := db.blackoutReadNode(pageID)
		if err != nil {
			return nil, err
		}
		if !node.IsLeaf() {
			pending = append(pending, node.Children...)
			continue
		}
		for index := range node.EntryCount() {
			names = append(names, string(node.EntryAt(index).Key()))
		}
	}
	slices.Sort(names)
	return names, nil
}

// blackoutReadNode reads a tree page like readNode, but without the node cache.
func (db *DB) blackoutReadNode(pageID page.ID) (*btree.Node, error) {
	if db.memoryNodes != nil {
		return db.readNode(pageID)
	}
	if record, ok := db.wal.Lookup(pageID); ok {
		return db.wal.CommittedPageToNode(&record)
	}
	data, err := db.readMainNodePage(pageID)
	if err != nil {
		return nil, err
	}
	return btree.DecodeNode(slices.Clone(data), pageID, db.meta.PageSize(), db.readMainNodePage, db.meta.DataPageChecksums())
}

// BlackoutCheckpointCount returns the number of checkpoints that db completed.
func BlackoutCheckpointCount(db *DB) uint64 {
	db.operationMu.RLock()
	defer db.operationMu.RUnlock()
	return db.wal.Stats().CheckpointCount
}

// BlackoutAbandon drops db without Close, as a crashed process does, and stops its write worker.
// A crashed process loses its mappings too, so it releases the main-file mapping.
func BlackoutAbandon(db *DB) {
	db.lifecycleMu.Lock()
	db.closed = true
	db.lifecycleMu.Unlock()
	db.stopWriteBatcherAndWait()
	db.operationMu.Lock()
	// After a simulated restart the mapping is already gone, so an error is expected.
	_ = db.unmapMainFile()
	db.mappedFile = nil
	db.operationMu.Unlock()
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
