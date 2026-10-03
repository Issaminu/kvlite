package kvlite

import (
	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

// Tx gives a [DB.View] or [DB.Update] callback access to one transaction. Use [Tx.Bucket] to open a top-level bucket and [Tx.DeleteBucket] to remove one. A Tx and every bucket or cursor obtained from it are valid only until the callback returns. Do not save, copy, or share them with another goroutine.
type Tx struct {
	db              *DB
	meta            *page.Meta
	rootNode        *btree.Node
	store           txTreeStore
	tree            *btree.Tree
	allocation      *allocationChanges
	allocationOwned bool
	metaDirty       bool
	readOnly        bool
	closed          bool
	bucket          *Bucket            // most transactions will only interact with a single bucket, we store it here instead of the [Tx.buckets] map attribute
	buckets         map[string]*Bucket // created after a second top-level bucket so every caller in this tx observes the same in-memory state
}

// newTx borrows committed state for a read transaction and creates private mutable state for a write transaction.
func newTx(db *DB, readOnly bool) *Tx {
	if !readOnly {
		return newWriteTx(db, db.meta, db.rootNode, nil, nil)
	}

	tx := &Tx{db: db, meta: db.meta, rootNode: db.rootNode, readOnly: true}
	tx.store = txTreeStore{
		tx: tx,
	}
	tx.tree = btree.NewTree(&tx.store)
	return tx
}

// newWriteTx starts one private write transaction from the committed state or earlier callbacks in the same write batch.
func newWriteTx(db *DB, baseMeta *page.Meta, baseRoot *btree.Node, baseDirty map[page.ID]dirtyNode, baseAllocation *allocationChanges) *Tx {
	meta := *baseMeta
	tx := &Tx{db: db, meta: &meta, rootNode: baseRoot, allocation: baseAllocation}
	tx.store = txTreeStore{
		tx:        tx,
		baseDirty: baseDirty,
		nodes:     make(map[page.ID]*btree.Node),
		dirty:     make(map[page.ID]dirtyNode),
	}
	tx.tree = btree.NewTree(&tx.store)

	// B+tree writes clone this read-only root before they change it.
	tx.store.nodes[baseRoot.PageID()] = baseRoot
	return tx
}

// Update runs fn in one read-write transaction. The callback can read and write buckets. A read in fn can see a write made earlier in the same fn.
//
// If fn returns nil, Update commits all changes together. Other database operations can then read them. If fn returns an error, Update discards all changes and returns that error.
//
// Only one Update callback runs at a time. Update waits for active [DB.View] callbacks to finish. New View callbacks wait until Update returns.
//
// In [SyncFull] mode, concurrent Update calls can share one WAL commit. KVLite still runs each callback alone. A later callback can read changes from an earlier successful callback in the same batch. If the WAL commit fails, each callback that depends on that commit receives the same error.
//
// Do not start another transaction from fn. Use the Tx passed to fn. If fn panics, Update discards its changes before the panic continues.
func (db *DB) Update(transaction func(tx *Tx) error) error {
	if err := db.beginOperation(); err != nil {
		return err
	}
	defer db.endOperation()
	if db.options.ReadOnly {
		return ErrDatabaseReadOnly
	}
	if db.options.Synchronous == SyncFull {
		return db.submitDurableUpdate(transaction)
	}
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	return db.updateDirect(transaction)
}

func (db *DB) updateDirect(transaction func(tx *Tx) error) error {
	tx := newTx(db, false)
	defer func() { tx.closed = true }()

	if err := transaction(tx); err != nil {
		return err
	}
	var retired map[page.ID]struct{}
	if tx.allocationOwned && len(tx.allocation.retired) != 0 {
		// A deleted node must not also appear as a changed node in this commit.
		for pageID := range tx.allocation.retired {
			delete(tx.store.dirty, pageID)
		}
		if tx.allocation.finalize(tx.meta) {
			tx.metaDirty = true
		}
	}
	if tx.allocationOwned {
		retired = tx.allocation.walRetiredPages()
	}

	nodes, encodedMeta := makeWALCommitData(tx.store.dirty, tx.meta, tx.metaDirty)
	var allocationPages []wal.AllocationRecord
	if tx.allocationOwned {
		allocationPages = tx.allocation.pageRecords()
	}
	if len(nodes) == 0 && len(allocationPages) == 0 && len(encodedMeta) == 0 {
		return nil
	}

	needsCheckpoint, err := db.wal.Commit(nodes, allocationPages, encodedMeta, retired)
	if err != nil {
		return err
	}
	db.meta = tx.meta
	db.rootNode = tx.rootNode
	if tx.allocationOwned {
		db.allocation = tx.allocation.publish()
		db.stats.PagesReused += tx.allocation.pagesReused
		db.stats.PagesRetired += tx.allocation.pagesRetired
		db.stats.TailPagesReclaimed += tx.allocation.tailPagesRemoved
	}
	for _, dirty := range tx.store.dirty {
		db.cacheWriteNode(dirty.final)
	}
	if needsCheckpoint {
		// The WAL is already durable. A checkpoint failure must not turn this
		// committed transaction into a reported failure.
		_ = db.checkpointWAL()
	}

	return nil
}

// writableAllocation gives this callback its own allocation changes.
// A failed callback cannot change allocation work from an earlier callback in the batch.
func (tx *Tx) writableAllocation() *allocationChanges {
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

// makeWALCommitData returns one original and final node pair for each changed data page.
// Repeated node changes produce one node record.
// When metadata changed, encodedMeta contains its new image. Otherwise, encodedMeta is nil.
func makeWALCommitData(dirty map[page.ID]dirtyNode, meta *page.Meta, metaDirty bool) (nodes []wal.NodeRecord, encodedMeta []byte) {
	if len(dirty) == 0 && !metaDirty {
		return nil, nil
	}

	nodes = make([]wal.NodeRecord, 0, len(dirty))
	for _, dirtyNode := range dirty {
		nodes = append(nodes, wal.NodeRecord{Original: dirtyNode.original, Final: dirtyNode.final})
	}
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
// Do not start another transaction from fn. Use the Tx passed to fn. View returns the error from fn. If fn panics, View closes the transaction before the panic continues.
func (db *DB) View(transaction func(tx *Tx) error) error {
	if err := db.beginOperation(); err != nil {
		return err
	}
	defer db.endOperation()

	db.operationMu.RLock()
	defer db.operationMu.RUnlock()

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
	return nil
}

// putCatalogEntry writes entry to the transaction's top-level tree and records
// a replacement catalog root in the transaction metadata.
func (tx *Tx) putCatalogEntry(entry btree.Entry) error {
	newRoot, err := tx.tree.PutEntry(tx.rootNode, entry)
	if err != nil {
		return err
	}
	tx.rootNode = newRoot
	if newRoot.PageID() != tx.meta.Root() {
		tx.meta.SetRoot(newRoot.PageID())
		tx.metaDirty = true
	}
	return nil
}
