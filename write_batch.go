package kvlite

import (
	"runtime"
	"sync"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

const (
	// defaultWriteBatchSize bounds the number of callbacks and private states held by one durable commit.
	defaultWriteBatchSize = 128
)

// writeCallbackResultChannels reuses empty result channels after each callback completes.
var writeCallbackResultChannels = sync.Pool{
	New: func() any {
		return make(chan writeResult, 1)
	},
}

var durableWriteRequests = sync.Pool{
	New: func() any {
		return &writeRequest{result: make(chan writeResult, 1)}
	},
}

type writeRequest struct {
	transaction func(*Tx) error
	result      chan writeResult
}

type writeResult struct {
	err        error
	panicValue any
	panicked   bool
	goexited   bool
	// dependsOnCommit is true when the callback changed the batch or ran after a pending change that it could observe. A failed batch commit must fail that callback because its result can depend on that change.
	dependsOnCommit bool
}

// writeBatchState is the last state produced by successful callbacks in one batch. It stays private until the WAL commit succeeds.
type writeBatchState struct {
	meta      *page.Meta
	rootNode  *btree.Node
	dirty     map[page.ID]*btree.Node
	metaDirty bool
}

// startWriteBatcher starts the request worker for a writable database in SyncFull mode. Other modes commit without this worker.
func (db *DB) startWriteBatcher() {
	if db.options.ReadOnly || db.options.Synchronous != SyncFull {
		return
	}
	db.writeRequests = make(chan *writeRequest, defaultWriteBatchSize)
	db.stopWriteBatcher = make(chan struct{})
	db.writeBatcherDone = make(chan struct{})
	go db.runWriteBatcher()
}

// stopWriteBatcherAndWait stops the request worker. The caller must first stop new submissions and wait for all active operations to finish.
func (db *DB) stopWriteBatcherAndWait() {
	if db.stopWriteBatcher == nil {
		return
	}
	close(db.stopWriteBatcher)
	<-db.writeBatcherDone
}

// submitDurableUpdate waits for the batch worker to run transaction and commit its dependent state. It forwards a callback panic or Goexit to the caller.
func (db *DB) submitDurableUpdate(transaction func(*Tx) error) error {
	request := durableWriteRequests.Get().(*writeRequest)
	request.transaction = transaction
	db.writeRequests <- request
	result := <-request.result
	request.transaction = nil
	durableWriteRequests.Put(request)
	if result.panicked {
		panic(result.panicValue)
	}
	if result.goexited {
		runtime.Goexit()
	}
	return result.err
}

// runWriteBatcher owns the durable request queue. It runs each collected batch while holding the database operation lock.
func (db *DB) runWriteBatcher() {
	defer close(db.writeBatcherDone)
	for {
		select {
		case first := <-db.writeRequests:
			requests := db.collectWriteBatch(first)
			// Hold operationMu across callback execution, WAL commit, and publication. This keeps all transaction callbacks in order and stops a View from running between these steps.
			db.operationMu.Lock()
			db.executeWriteBatch(requests)
			db.operationMu.Unlock()
		case <-db.stopWriteBatcher:
			return
		}
	}
}

// collectWriteBatch returns first and the requests that are ready in the queue. It yields once so concurrent callers can enter the queue without a timer.
func (db *DB) collectWriteBatch(first *writeRequest) []*writeRequest {
	requests := make([]*writeRequest, 1, defaultWriteBatchSize)
	requests[0] = first
	runtime.Gosched()

	for len(requests) < defaultWriteBatchSize {
		select {
		case request := <-db.writeRequests:
			requests = append(requests, request)
		default:
			return requests
		}
	}
	return requests
}

// executeWriteBatch runs callbacks in request order and commits their combined successful changes as one WAL transaction. The caller must hold operationMu.
func (db *DB) executeWriteBatch(requests []*writeRequest) {
	state := writeBatchState{
		meta:     db.meta,
		rootNode: db.rootNode,
		dirty:    make(map[page.ID]*btree.Node),
	}
	results := make([]writeResult, len(requests))
	hasPendingWrites := false

	for index, request := range requests {
		// Create the Tx only after earlier requests finish so it starts from their successful state. Keeping it separate also lets a failed callback discard only its own changes.
		tx := newWriteTx(db, state.meta, state.rootNode, state.dirty)
		result := executeWriteCallback(request.transaction, tx)
		if result.err != nil || result.panicked || result.goexited {
			results[index] = result
			continue
		}

		transactionChangedState := len(tx.store.dirty) > 0 || tx.metaDirty
		result.dependsOnCommit = hasPendingWrites || transactionChangedState
		results[index] = result
		if !transactionChangedState {
			continue
		}

		state.meta = tx.meta
		state.rootNode = tx.rootNode
		state.metaDirty = state.metaDirty || tx.metaDirty
		for pageID, node := range tx.store.dirty {
			state.dirty[pageID] = node
		}
		hasPendingWrites = true
	}

	var commitErr error
	needsCheckpoint := false
	if hasPendingWrites {
		// state.dirty contains one final image for each changed page. One WAL transaction gives all dependent callbacks the same commit result.
		records := encodeWALRecords(state.dirty, state.meta, state.metaDirty)
		needsCheckpoint, commitErr = db.wal.Commit(records)
		if commitErr == nil {
			// Publish the new state only after the WAL append and required synchronization succeed.
			db.meta = state.meta
			db.rootNode = state.rootNode
		}
	}

	for index, request := range requests {
		result := results[index]
		if result.err == nil && !result.panicked && !result.goexited && result.dependsOnCommit {
			result.err = commitErr
		}
		request.result <- result
	}
	if needsCheckpoint && commitErr == nil {
		// The WAL commit is complete. Callers do not need to wait for main-file maintenance.
		_ = db.checkpointWAL()
	}
}

// executeWriteCallback runs transaction without allowing runtime.Goexit to stop the long-lived batch worker.
func executeWriteCallback(transaction func(*Tx) error, tx *Tx) writeResult {
	// A separate goroutine lets the coordinator detect runtime.Goexit and forward it to the caller without losing the long-lived coordinator.
	resultReady := writeCallbackResultChannels.Get().(chan writeResult)
	go func() {
		callbackReturned := false
		var result writeResult
		defer func() {
			if !callbackReturned {
				result = writeResult{goexited: true}
			}
			resultReady <- result
		}()

		result = callWriteCallback(transaction, tx)
		callbackReturned = true
	}()

	result := <-resultReady
	writeCallbackResultChannels.Put(resultReady)
	return result
}

// callWriteCallback closes tx after transaction returns or panics. It converts a panic into a writeResult so the original Update caller can continue it.
func callWriteCallback(transaction func(*Tx) error, tx *Tx) (result writeResult) {
	returned := false
	defer func() {
		tx.closed = true
		if !returned {
			result.panicked = true
			result.panicValue = recover()
		}
	}()

	result.err = transaction(tx)
	returned = true
	return result
}
