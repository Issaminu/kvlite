package kvlite

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

// Tx gives a [DB.View] or [DB.Update] callback access to one transaction. Use [Tx.Bucket] to open a top-level bucket and [Tx.DeleteBucket] to remove one. A Tx and every bucket or cursor obtained from it are valid only until the callback returns. Do not save, copy, or share them with another goroutine.
type Tx struct {
	db              *DB
	meta            *page.Meta
	store           txTreeStore
	tree            btree.Tree
	allocation      *allocationChanges
	allocationOwned bool
	metaOwned       bool
	metaDirty       bool
	readOnly        bool
	closed          bool
	changes         uint64             // changes counts the changes to private state, so keepFailure can see whether a failed write changed the transaction.
	failure         error              // failure is the ErrTxFailed error of the first write that failed after it changed the transaction.
	borrowedValue   bool               // A managed put can retain its input until commit.
	bucket          *Bucket            // most transactions will only interact with a single bucket, we store it here instead of the [Tx.buckets] map attribute
	buckets         map[string]*Bucket // created after a second top-level bucket so every caller in this tx observes the same in-memory state
}

// newTx borrows committed state for a read transaction and creates private mutable state for a write transaction.
func newTx(db *DB, readOnly bool) *Tx {
	if !readOnly {
		return newWriteTx(db, writeBatchState{meta: db.meta})
	}

	tx := &Tx{db: db, meta: db.meta, readOnly: true}
	tx.store = txTreeStore{
		tx:        tx,
		committed: db.storage.nodeReader(),
	}
	tx.tree = *btree.NewTree(&tx.store)
	return tx
}

// newWriteTx starts one private write transaction from the committed state or earlier callbacks in the same write batch.
func newWriteTx(db *DB, state writeBatchState) *Tx {
	tx := &Tx{db: db, meta: state.meta, allocation: state.allocation}
	tx.store = txTreeStore{tx: tx, committed: db.storage.nodeReader(), baseDirty: state.dirty, dirty: make(map[page.ID]dirtyNode)}
	tx.tree = state.tree.Fork(&tx.store)
	tx.tree.SkipIdenticalPuts = db.memoryNodes != nil || (db.options != nil && db.options.Synchronous != SyncFull)
	return tx
}

// newCommitTx returns the transaction that commits the combined state of a write batch.
// The state's changes become this transaction's own changes.
func newCommitTx(db *DB, state writeBatchState) *Tx {
	tx := &Tx{db: db, meta: state.meta, allocation: state.allocation, allocationOwned: state.allocation != nil, metaDirty: state.metaDirty}
	tx.store = txTreeStore{tx: tx, committed: db.storage.nodeReader(), dirty: state.dirty}
	tx.tree = state.tree.Fork(&tx.store)
	return tx
}

// nodeReader returns the reader for this transaction's view of the tree.
// A read-only transaction has no private changes, so it reads committed storage directly.
// Its operation lock keeps that storage unchanged, and traversal does not check empty private maps at every node.
func (tx *Tx) nodeReader() btree.NodeReader {
	if tx.readOnly {
		return tx.store.committed
	}
	return &tx.store
}

// Update runs fn in one read-write transaction. The callback can read and write buckets. A read in fn can see a write made earlier in the same fn.
//
// If fn returns nil, Update commits all changes together. Other database operations can then read them. If fn returns an error, Update discards all changes. It returns that error unless a shared WAL commit fails, as described below.
//
// A write that fails after it changed the transaction, for example because of a storage read error, stops the transaction. Every later operation in fn returns [ErrTxFailed]. Update then discards all changes and returns [ErrTxFailed], also when fn returns nil. A write that rejects its input, for example with [ErrKeyTooLarge], changes nothing, so fn can continue.
//
// If a write or sync of the write-ahead log fails during the commit, Update returns an error that matches [ErrWALWriteFailed]. The changes can be saved or lost, because the failed write can still reach storage. The DB then stops. Later transactions, reads, writes, and backups return the same error without calling fn. Close the DB and open it again. Then check the data before you repeat a change that is not safe to apply twice.
//
// With [SyncNone] and [Options.DisableWAL] enabled, Update waits for active backups before it calls fn. A direct main-file commit failure returns [ErrDatabaseWriteFailed] and stops later data operations. The file can be partly changed or corrupt. A process crash during a direct commit can also damage data from earlier successful transactions.
//
// Only one Update callback runs at a time. Update waits for active [DB.View] callbacks to finish. New View callbacks wait until Update returns.
//
// For a file database in [SyncFull] mode, concurrent Update calls can share one WAL commit. KVLite still runs each callback alone. A later callback can read changes from an earlier successful callback in the same batch before those changes are durable. If the WAL commit fails, Update returns the commit error for each callback that changed data or ran after an earlier change in the batch. That error replaces an error that fn returned, because fn could read changes that the failed commit discards. Act outside the database only after Update returns nil. A memory database commits each callback without a log or file operations.
//
// Do not start another transaction from fn. Use the Tx passed to fn. If fn panics, Update discards its changes before the panic continues.
func (db *DB) Update(transaction func(tx *Tx) error) error {
	if err := db.beginWriteOperation(); err != nil {
		return err
	}
	defer db.endOperation()
	if db.memoryNodes == nil && db.options.Synchronous == SyncFull {
		return db.submitDurableUpdate(transaction)
	}
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	return db.updateDirect(transaction)
}

// prepareDirectWrite waits for backups when writes bypass the WAL and checks prior write errors.
// The caller must hold operationMu. The lock is released while a backup finishes.
func (db *DB) prepareDirectWrite() error {
	for db.options.DisableWAL && db.activeBackups > 0 {
		done := db.backupsDone
		db.operationMu.Unlock()
		<-done
		db.operationMu.Lock()
	}
	return db.writeFailure()
}

func (db *DB) updateDirect(transaction func(tx *Tx) error) error {
	if err := db.prepareDirectWrite(); err != nil {
		return err
	}
	tx := newTx(db, false)
	defer func() { tx.closed = true }()

	if err := transaction(tx); err != nil {
		return err
	}
	// A callback can ignore a failed write and return nil. The half-changed transaction must not commit.
	if tx.failure != nil {
		return tx.failure
	}
	needsCheckpoint, err := tx.commit()
	if err != nil {
		return err
	}
	if needsCheckpoint {
		// The WAL commit succeeded. A checkpoint failure must not turn this
		// committed transaction into a reported failure.
		// The sync of the WAL truncate saves the header sync of the next commit.
		if db.checkpointWAL() == nil {
			_ = db.wal.SyncTruncate()
		}
	}

	return nil
}

// commit prepares, writes, and publishes this transaction's private state.
// The caller holds operationMu and has excluded backups when writes bypass the WAL.
// A true result requests checkpoint maintenance; the caller chooses when to acknowledge success.
func (tx *Tx) commit() (bool, error) {
	db := tx.db
	if err := tx.prepareCommit(); err != nil {
		return false, err
	}
	for _, change := range tx.store.dirty {
		change.final.OwnImageBytes()
	}
	needsCheckpoint := false
	var nodes []wal.NodeRecord
	if db.memoryNodes != nil {
		// Update holds the write lock until all node and allocation changes are visible.
		// Retired nodes must leave memory before a later transaction can reuse their page IDs.
		if tx.allocationOwned {
			tx.allocation.forEachRetired(func(id page.ID) { delete(db.memoryNodes, id) })
		}
	} else {
		var encodedMeta []byte
		nodes, encodedMeta = makeWALCommitData(tx.store.dirty, tx.meta, tx.metaDirty)
		var allocation *allocationChanges
		var allocationPages []wal.AllocationRecord
		if tx.allocationOwned {
			allocation = tx.allocation
			allocationPages = tx.allocation.pageRecords()
		}
		if len(nodes) == 0 && len(allocationPages) == 0 && len(encodedMeta) == 0 {
			return false, nil
		}
		var err error
		needsCheckpoint, err = db.persist(tx.meta, nodes, allocationPages, encodedMeta, allocation)
		if err != nil {
			return false, err
		}
	}
	db.meta = tx.meta
	if tx.allocationOwned {
		db.allocation = tx.allocation.publish()
		db.stats.PagesReused += tx.allocation.pagesReused
		db.stats.PagesRetired += tx.allocation.pagesRetired
		db.stats.TailPagesReclaimed += tx.allocation.tailPagesRemoved
	}
	if db.memoryNodes != nil {
		for _, dirty := range tx.store.dirty {
			db.memoryNodes[dirty.final.PageID()] = dirty.final
		}
	}
	// Page-ID order makes cache eviction, and so the later file reads, repeatable.
	for _, node := range nodes {
		if tx.borrowedValue && node.Final.HasBorrowedValues() {
			db.invalidateWriteNode(node.Final.PageID())
			continue
		}
		db.cacheWriteNode(node.Final)
	}
	return needsCheckpoint, nil
}

// writableAllocation gives this callback its own allocation changes.
// A failed callback cannot change allocation work from an earlier callback in the batch.
func (tx *Tx) writableAllocation() *allocationChanges {
	tx.writableMeta()
	if tx.allocationOwned {
		return tx.allocation
	}
	if tx.allocation != nil {
		tx.allocation = tx.allocation.clone()
	} else {
		tx.allocation = newAllocationChanges(tx.db.allocation)
	}
	tx.allocationOwned = true
	return tx.allocation
}

// writableMeta copies metadata before this callback can change it.
// Stable value updates keep the committed metadata without an allocation.
func (tx *Tx) writableMeta() *page.Meta {
	if !tx.metaOwned {
		meta := *tx.meta
		tx.meta = &meta
		tx.metaOwned = true
	}
	return tx.meta
}

// makeWALCommitData returns one original and final node pair for each changed data page, in page-ID order.
// Repeated node changes produce one node record.
// The order does not depend on map order, so the same changes always write the same WAL bytes and fill the node cache in the same order.
// When metadata changed, encodedMeta contains its new image. Otherwise, encodedMeta is nil.
func makeWALCommitData(dirty map[page.ID]dirtyNode, meta *page.Meta, metaDirty bool) (nodes []wal.NodeRecord, encodedMeta []byte) {
	if len(dirty) == 0 && !metaDirty {
		return nil, nil
	}

	nodes = make([]wal.NodeRecord, 0, len(dirty))
	for _, dirtyNode := range dirty {
		nodes = append(nodes, wal.NodeRecord{Original: dirtyNode.original, Final: dirtyNode.final})
	}
	slices.SortFunc(nodes, func(left, right wal.NodeRecord) int {
		return cmp.Compare(left.Final.PageID(), right.Final.PageID())
	})
	if metaDirty {
		meta.AdvanceGeneration()
		meta.RefreshChecksum()
		encodedMeta = page.EncodeMeta(meta)
	}
	return nodes, encodedMeta
}

// View runs fn in one read-only transaction. Every read in fn sees the same database state. Writes through the Tx or its buckets return [ErrTxNotWritable].
//
// Several View callbacks can run at the same time. A View callback waits while [DB.Update] runs. Update waits for all active View callbacks to return.
//
// Do not start another transaction from fn. Use the Tx passed to fn. View returns the error from fn. If fn panics, View closes the transaction before the panic continues. After [ErrWALWriteFailed] or [ErrDatabaseWriteFailed], View returns that error without calling fn.
func (db *DB) View(transaction func(tx *Tx) error) error {
	if err := db.beginOperation(); err != nil {
		return err
	}
	defer db.endOperation()

	db.operationMu.RLock()
	defer db.operationMu.RUnlock()

	if err := db.writeFailure(); err != nil {
		return err
	}
	tx := newTx(db, true)
	defer func() { tx.closed = true }()
	return transaction(tx)
}

// Writable reports whether tx accepts writes. It returns true only inside an active [DB.Update] callback, and returns false for a [DB.View] transaction or after either callback has returned.
func (tx *Tx) Writable() bool {
	return !tx.readOnly && !tx.closed
}

func (tx *Tx) writableError() error {
	if tx.closed {
		return ErrTxClosed
	}
	if tx.readOnly {
		return ErrTxNotWritable
	}
	return tx.failure
}

// keepFailure records *err when it stopped a write after that write changed this transaction.
// changes is the value of tx.changes from before the write started.
// The private tree can then be half changed, so every later operation and the end of the callback return [ErrTxFailed].
// An error before the first change, such as a rejected key, leaves the transaction usable.
func (tx *Tx) keepFailure(changes uint64, err *error) {
	if *err != nil && tx.changes != changes && tx.failure == nil {
		tx.failure = fmt.Errorf("%w: %w", ErrTxFailed, *err)
	}
}

// putCatalogEntry writes entry to the transaction's top-level tree and records
// a replacement catalog root in the transaction metadata.
func (tx *Tx) putCatalogEntry(entry btree.Entry) error {
	newRoot, err := tx.tree.PutEntry(tx.meta.Root(), entry)
	if err != nil {
		return err
	}
	if newRoot != tx.meta.Root() {
		tx.writableMeta().SetRoot(newRoot)
		tx.metaDirty = true
	}
	return nil
}
