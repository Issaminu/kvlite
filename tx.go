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
	buckets   map[string]*Bucket // per-tx cache: one *Bucket handle per top-level name, so every caller in this tx observes the same in-memory state
}

// newTx creates an isolated metadata, root, and page-cache view of the current committed database state.
func newTx(db *DB, readOnly bool) *Tx {
	meta := *db.meta
	tx := &Tx{db: db, meta: &meta, readOnly: readOnly}
	tx.store = &txTreeStore{
		tx:    tx,
		nodes: make(map[page.ID]*btree.Node),
		dirty: make(map[page.ID]*btree.Node),
	}
	tx.tree = btree.NewTree(tx.store)

	// Writable transactions clone the cached catalog root because tree operations
	// can change a node in place. Read-only transactions share the cached root;
	// callers must treat Bucket.Get results as read-only (see doc.go).
	rootNode := db.rootNode
	if !readOnly {
		rootNode = db.rootNode.Clone()
	}
	tx.store.nodes[rootNode.PageID()] = rootNode
	tx.rootNode = rootNode
	return tx
}

// Update runs transaction as one managed read-write transaction. Writes made inside the callback are visible to later reads in the same callback, but other database operations can observe them only after Update commits.
//
// If transaction returns an error, Update discards every change and returns that error. If it panics, Update discards every change before the panic continues to the caller. When transaction returns nil, Update writes the transaction to the write-ahead log and returns any error that prevents that commit.
//
// An automatic checkpoint can run after the write-ahead log commit. If that checkpoint fails, the transaction remains committed, Update returns nil, and KVLite keeps the log data so a later write or [DB.Close] can retry the checkpoint.
func (db *DB) Update(transaction func(tx *Tx) error) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.options.ReadOnly {
		return ErrDatabaseReadOnly
	}

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

// walRecords encodes one final image for each changed page and includes metadata only when it changed.
// Update calls it after the transaction callback succeeds, so callback failure does no encoding or WAL work.
func (tx *Tx) walRecords() []wal.Record {
	if len(tx.store.dirty) == 0 && !tx.metaDirty {
		return nil
	}

	recordCapacity := len(tx.store.dirty)
	if tx.metaDirty {
		recordCapacity++
	}
	records := make([]wal.Record, 0, recordCapacity)
	for pageID, node := range tx.store.dirty {
		records = append(records, wal.Record{
			Header:      wal.RecordHeader{Type: wal.RecordTypeData, PageID: pageID},
			PageContent: btree.EncodeNode(node),
		})
	}
	if tx.metaDirty {
		tx.meta.RefreshChecksum()
		records = append(records, wal.Record{
			Header:      wal.RecordHeader{Type: wal.RecordTypeMeta, PageID: page.MetaID},
			PageContent: page.EncodeMeta(tx.meta),
		})
	}
	return records
}

// View runs transaction as one managed read-only transaction and returns its error. Writes through the Tx or its buckets return [ErrTxNotWritable], while reads remain available until the callback returns. If the callback panics, View closes the transaction before the panic continues to the caller.
func (db *DB) View(transaction func(tx *Tx) error) error {
	if err := db.ensureOpen(); err != nil {
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
	return nil
}

// putCatalogEntry writes entry to the transaction's top-level tree. putTreeEntry
// can return a replacement root after a split, but it does not install that
// root. This method installs it and updates meta.root so recovery uses the root
// that contains the entry.
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
