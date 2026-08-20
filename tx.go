package kvlite

import (
	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

// Tx is a managed transaction passed to a [DB.Update] or [DB.View] callback. It is valid only while that callback runs, so callers must not retain it or any [Bucket] obtained from it. Transactions must not be nested, and a Tx must not be copied or used concurrently.
type Tx struct {
	db        *DB
	meta      *page.Meta
	rootNode  *btree.Node
	store     *txTreeStore
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
	tx.store = &txTreeStore{
		tx: tx,
	}
	tx.tree = btree.NewTree(tx.store)
	return tx
}

// newWriteTx starts one private write transaction from baseMeta, baseRoot, and any page images prepared by earlier callbacks in the same write batch.
func newWriteTx(db *DB, baseMeta *page.Meta, baseRoot *btree.Node, baseNodes map[page.ID]*btree.Node) *Tx {
	meta := *baseMeta
	tx := &Tx{db: db, meta: &meta, rootNode: baseRoot}
	tx.store = &txTreeStore{
		tx:        tx,
		baseNodes: baseNodes,
		nodes:     make(map[page.ID]*btree.Node),
		dirty:     make(map[page.ID]*btree.Node),
	}
	tx.tree = btree.NewTree(tx.store)

	// B+tree writes clone this read-only root before they change it.
	tx.store.nodes[baseRoot.PageID()] = baseRoot
	return tx
}

// Update runs transaction as one managed read-write transaction. Writes made inside the callback are visible to later reads in the same callback, but other database operations can observe them only after Update commits.
//
// If transaction returns an error, Update discards every change and returns that error. If it panics, Update discards every change before the panic continues to the caller. When transaction returns nil, Update writes the transaction to the write-ahead log and returns any error that prevents that commit.
//
// KVLite runs concurrent Update callbacks in serial order. In [SyncFull] mode, concurrent successful callbacks can form one atomic write batch. Later callbacks in that batch see earlier successful changes. A callback error discards only that callback's changes. A write-ahead log error returns to every successful callback whose result depends on that batch, and none of the batch changes become visible.
//
// An automatic checkpoint can run after the write-ahead log commit. If that checkpoint fails, the transaction remains committed, Update returns nil, and KVLite keeps the log data so a later write or [DB.Close] can retry the checkpoint.
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

	// The WAL commit made these final node images committed. Publish child nodes
	// only now so a failed append or sync cannot replace readable cache entries.
	// The catalog root remains in db.rootNode and does not consume a cache slot.
	for _, node := range tx.store.dirty {
		if node.PageID() == db.rootNode.PageID() {
			continue
		}
		db.pageCache.Put(node)
	}
	if needsCheckpoint {
		// The WAL is already durable. A checkpoint failure must not turn this
		// committed transaction into a reported failure.
		_ = db.checkpointWAL()
	}

	return nil
}

// walRecords encodes one final image for each changed page and includes metadata only when it changed.
// Update calls it after the transaction callback succeeds, so callback failure does no encoding or WAL work.
func (tx *Tx) walRecords() []wal.Record {
	return encodeWALRecords(tx.store.dirty, tx.meta, tx.metaDirty)
}

// encodeWALRecords encodes the final private image of each changed page. Repeated changes to one page produce one record for the transaction or batch.
func encodeWALRecords(dirty map[page.ID]*btree.Node, meta *page.Meta, metaDirty bool) []wal.Record {
	if len(dirty) == 0 && !metaDirty {
		return nil
	}

	recordCapacity := len(dirty)
	if metaDirty {
		recordCapacity++
	}
	records := make([]wal.Record, 0, recordCapacity)
	for pageID, node := range dirty {
		records = append(records, wal.Record{
			Header:      wal.RecordHeader{Type: wal.RecordTypeData, PageID: pageID},
			PageContent: btree.EncodeNode(node),
		})
	}
	if metaDirty {
		meta.RefreshChecksum()
		records = append(records, wal.Record{
			Header:      wal.RecordHeader{Type: wal.RecordTypeMeta, PageID: page.MetaID},
			PageContent: page.EncodeMeta(meta),
		})
	}
	return records
}

// View runs transaction as one managed read-only transaction and returns its error. Writes through the Tx or its buckets return [ErrTxNotWritable], while reads remain available until the callback returns. If the callback panics, View closes the transaction before the panic continues to the caller.
func (db *DB) View(transaction func(tx *Tx) error) error {
	if err := db.beginOperation(); err != nil {
		return err
	}
	defer db.endOperation()

	db.operationMu.Lock()
	defer db.operationMu.Unlock()

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
	newRoot, err := tx.putTreeEntry(tx.rootNode, entry)
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

func (tx *Tx) putTreeEntry(rootNode *btree.Node, entry btree.Entry) (*btree.Node, error) {
	return tx.tree.PutEntry(rootNode, entry)
}

func (tx *Tx) findTreeEntry(rootNode *btree.Node, key []byte) (btree.Entry, bool, error) {
	return tx.tree.FindEntry(rootNode, key)
}

func (tx *Tx) findTreeEntryRef(rootNode *btree.Node, key []byte) (btree.Entry, bool, error) {
	return tx.tree.FindEntryRef(rootNode, key)
}
