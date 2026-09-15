package kvlite

import (
	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

// Tx gives a [DB.View] or [DB.Update] callback access to one transaction. Use [Tx.Bucket] to open a top-level bucket. A Tx and every bucket or cursor obtained from it are valid only until the callback returns. Do not save, copy, or share them with another goroutine.
type Tx struct {
	db        *DB
	meta      *page.Meta
	rootNode  *btree.Node
	store     txTreeStore
	tree      *btree.Tree
	metaDirty bool
	readOnly  bool
	closed    bool
	bucket    *Bucket            // most transactions will only interact with a single bucket, we store it here instead of the [Tx.buckets] map attribute
	buckets   map[string]*Bucket // created after a second top-level bucket so every caller in this tx observes the same in-memory state
}

// newTx borrows committed state for a read transaction and creates private mutable state for a write transaction.
func newTx(db *DB, readOnly bool) *Tx {
	if !readOnly {
		return newWriteTx(db, db.meta, db.rootNode, nil)
	}

	tx := &Tx{db: db, meta: db.meta, rootNode: db.rootNode, readOnly: true}
	tx.store = txTreeStore{
		tx: tx,
	}
	tx.tree = btree.NewTree(&tx.store)
	return tx
}

// newWriteTx starts one private write transaction from baseMeta, baseRoot, and any page images prepared by earlier callbacks in the same write batch.
func newWriteTx(db *DB, baseMeta *page.Meta, baseRoot *btree.Node, baseNodes map[page.ID]*btree.Node) *Tx {
	meta := *baseMeta
	tx := &Tx{db: db, meta: &meta, rootNode: baseRoot}
	tx.store = txTreeStore{
		tx:        tx,
		baseNodes: baseNodes,
		nodes:     make(map[page.ID]*btree.Node),
		dirty:     make(map[page.ID]*btree.Node),
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

	records := tx.walRecords()
	if len(records) == 0 {
		return nil
	}

	needsCheckpoint, err := db.wal.Commit(records)
	if err != nil {
		return err
	}
	db.meta = tx.meta
	db.rootNode = tx.rootNode
	if needsCheckpoint {
		// The WAL is already durable. A checkpoint failure must not turn this
		// committed transaction into a reported failure.
		_ = db.checkpointWAL()
	}

	return nil
}

// walRecords encodes one final image for each changed node page and two metadata copies when metadata changed.
// Update calls it after the transaction callback succeeds, so callback failure does no encoding or WAL work.
func (tx *Tx) walRecords() []wal.Record {
	return encodeWALRecords(tx.store.dirty, tx.meta, tx.metaDirty)
}

// encodeWALRecords encodes the final private image of each changed node page. Repeated node changes produce one record, while changed metadata produces one record for each metadata page.
func encodeWALRecords(dirty map[page.ID]*btree.Node, meta *page.Meta, metaDirty bool) []wal.Record {
	if len(dirty) == 0 && !metaDirty {
		return nil
	}

	recordCapacity := len(dirty)
	if metaDirty {
		recordCapacity += 2
	}
	records := make([]wal.Record, 0, recordCapacity)
	for pageID, node := range dirty {
		records = append(records, wal.Record{
			Header: wal.RecordHeader{Type: wal.RecordTypeData, PageID: pageID},
			Node:   node,
		})
	}
	if metaDirty {
		meta.AdvanceGeneration()
		meta.RefreshChecksum()
		encodedMeta := page.EncodeMeta(meta)
		for _, pageID := range [...]page.ID{page.Meta0ID, page.Meta1ID} {
			records = append(records, wal.Record{
				Header:      wal.RecordHeader{Type: wal.RecordTypeMeta, PageID: pageID},
				PageContent: encodedMeta,
			})
		}
	}
	return records
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
