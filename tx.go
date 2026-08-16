package kvlite

import (
	"errors"
	"fmt"
	"slices"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

const (
	BucketLeafFlag = btree.BucketLeafFlag
)

type Tx struct {
	db       *DB
	readOnly bool
	closed   bool
	buckets  map[string]*Bucket // per-tx cache: one *Bucket handle per top-level name, so every caller in this tx observes the same in-memory state
}

type writeTransactionSnapshot struct {
	meta page.Meta
	wal  wal.Snapshot
}

func (db *DB) Update(transaction func(tx *Tx) error) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.options.ReadOnly {
		return ErrDatabaseReadOnly
	}

	tx := &Tx{db: db, readOnly: false}
	defer func() { tx.closed = true }()

	snapshot, err := db.snapshotWriteTransaction()
	if err != nil {
		return err
	}
	callbackReturned := false
	defer func() {
		if callbackReturned {
			return
		}

		panicValue := recover() // nil when there's no panic
		rollbackErr := db.rollbackWriteTransaction(&snapshot)
		if rollbackErr != nil {
			if panicValue != nil {
				panic(errors.Join(fmt.Errorf("transaction panic: %v", panicValue), rollbackErr))
			}
			panic(rollbackErr)
		}
		if panicValue != nil {
			panic(panicValue)
		}
	}()

	transactionErr := transaction(tx)
	callbackReturned = true
	if transactionErr != nil {
		// transaction failed, revert back to snapshot
		if rbErr := db.rollbackWriteTransaction(&snapshot); rbErr != nil {
			return errors.Join(transactionErr, rbErr)
		}
		return transactionErr
	}

	// transaction succeeded
	needsCheckpoint, err := db.wal.PersistCollectedRecords()
	if err != nil {
		if rbErr := db.rollbackWriteTransaction(&snapshot); rbErr != nil {
			return errors.Join(err, rbErr)
		}
		return err
	}
	if needsCheckpoint {
		// The WAL is already durable. A checkpoint failure must not turn this
		// committed transaction into a reported failure.
		_ = db.checkpointWAL()
	}

	return nil
}

func (db *DB) snapshotWriteTransaction() (writeTransactionSnapshot, error) {
	walSnapshot, err := db.wal.Snapshot()
	if err != nil {
		return writeTransactionSnapshot{}, err
	}
	return writeTransactionSnapshot{
		meta: *db.meta,
		wal:  walSnapshot,
	}, nil
}

func (db *DB) rollbackWriteTransaction(snapshot *writeTransactionSnapshot) error {
	walErr := db.wal.Restore(snapshot.wal)
	db.meta = &snapshot.meta
	root, err := db.readNode(snapshot.meta.Root())
	if err != nil {
		return errors.Join(walErr, fmt.Errorf("reload root node: %w", err))
	}
	db.rootNode = root
	return walErr
}

func (db *DB) View(transaction func(tx *Tx) error) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	tx := &Tx{db: db, readOnly: true}
	defer func() { tx.closed = true }()
	return transaction(tx)
}

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

// putCatalogEntry writes entry to the database's top-level tree. putTreeEntry
// can return a replacement root after a split, but it does not install that
// root. This method installs it, updates meta.root, and then stages the meta
// record so recovery uses the root that contains the entry.
func (tx *Tx) putCatalogEntry(entry Entry) error {
	newRoot, err := tx.db.putTreeEntry(tx.db.rootNode, entry)
	if err != nil {
		return err
	}
	tx.db.rootNode = newRoot
	tx.db.meta.SetRoot(newRoot.PageID())
	tx.db.wal.InsertMetaRecord(tx.db.meta)
	return nil
}

func (tx *Tx) Put(key, value []byte) error {
	if err := tx.writableError(); err != nil {
		return err
	}
	return tx.putCatalogEntry(btree.NewEntry(0, key, value))
}

func (tx *Tx) Get(key []byte) ([]byte, error) {
	if tx.closed {
		return nil, ErrTxClosed
	}

	entry, found, err := tx.db.findTreeEntry(tx.db.rootNode, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrKeyNotFound
	}
	if entry.Flags()&BucketLeafFlag != 0 {
		return nil, ErrIncompatibleValue
	}
	return entry.Value(), nil
}

// cacheBucket registers b as the one handle for its name within this transaction.
func (tx *Tx) cacheBucket(b *Bucket) {
	if tx.buckets == nil {
		tx.buckets = make(map[string]*Bucket)
	}
	tx.buckets[string(b.name)] = b
}

type Bucket struct {
	tx           *Tx
	name         []byte
	rootNode     *Node
	parentBucket *Bucket
	children     map[string]*Bucket // per-parent cache: one *Bucket handle per nested name
}

// cacheChild registers b as the one handle for its name within this bucket.
func (bucket *Bucket) cacheChild(b *Bucket) {
	if bucket.children == nil {
		bucket.children = make(map[string]*Bucket)
	}
	bucket.children[string(b.name)] = b
}

// writeBackRoot records this bucket's (possibly moved) root pgid into the entry
// that points at it. That entry lives in the parent's tree: the DB catalog for a
// top-level bucket, or the parent bucket's own tree for a nested bucket.
func (b *Bucket) writeBackRoot() error {
	entry := btree.NewEntry(BucketLeafFlag, b.name, page.EncodeID(b.rootNode.PageID()))
	if b.parentBucket == nil {
		return b.tx.putCatalogEntry(entry)
	}
	return b.parentBucket.putBucketEntry(entry)
}

func (tx *Tx) CreateBucket(bucketName []byte) (*Bucket, error) {
	return tx.createBucket(nil, bucketName)
}

func (tx *Tx) createBucket(parent *Bucket, bucketName []byte) (*Bucket, error) {
	if err := tx.writableError(); err != nil {
		return nil, err
	}

	var existing *Bucket
	var err error
	if parent == nil {
		existing, err = tx.lookupBucket(bucketName)
	} else {
		existing, err = parent.Bucket(bucketName)
	}
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, ErrBucketExists
	}

	newPgid := tx.db.allocate()
	rootNode := btree.NewLeafNode(newPgid)
	tx.db.wal.InsertNodeRecord(rootNode)

	bucket := &Bucket{
		tx:           tx,
		name:         slices.Clone(bucketName),
		rootNode:     rootNode,
		parentBucket: parent,
	}

	if parent == nil {
		err = tx.putCatalogEntry(btree.NewEntry(BucketLeafFlag, bucket.name, page.EncodeID(newPgid)))
	} else {
		err = parent.putBucketEntry(btree.NewEntry(BucketLeafFlag, bucket.name, page.EncodeID(newPgid)))
	}
	if err != nil {
		return nil, err
	}

	if parent == nil {
		tx.cacheBucket(bucket)
		return bucket, nil
	}

	parent.cacheChild(bucket)
	return bucket, nil
}

// Bucket looks up a top-level bucket by name. Like bbolt, it returns nil if no
// bucket exists under that name — whether nothing is stored there, the name holds
// a plain value instead, or the lookup failed — with no way to tell those apart.
// It returns the same *Bucket handle on every call within this transaction, so
// writes through one handle are visible through any other handle for the same
// name. CreateBucket needs the finer-grained result, so it calls lookupBucket
// directly instead of going through this method.
func (tx *Tx) Bucket(bucketName []byte) *Bucket {
	b, err := tx.lookupBucket(bucketName)
	if err != nil {
		return nil
	}
	return b
}

// lookupBucket is Bucket's engine: same cache and tree descent, but it reports
// *why* a name didn't resolve to a bucket, via (nil, nil) for "no entry",
// (nil, ErrIncompatibleValue) for "a plain value is there instead", or (nil, err)
// for a real lookup failure.
func (tx *Tx) lookupBucket(bucketName []byte) (*Bucket, error) {
	if tx.closed {
		return nil, ErrTxClosed
	}
	if b, ok := tx.buckets[string(bucketName)]; ok {
		return b, nil
	}
	bucket, err := tx.loadBucket(tx.db.rootNode, bucketName, nil)
	if err != nil || bucket == nil {
		return bucket, err
	}
	tx.cacheBucket(bucket)
	return bucket, nil
}

func (tx *Tx) loadBucket(rootNode *Node, bucketName []byte, parent *Bucket) (*Bucket, error) {
	entry, found, err := tx.db.findTreeEntry(rootNode, bucketName)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	if entry.Flags()&BucketLeafFlag == 0 {
		// exists, but it's a regular value
		return nil, ErrIncompatibleValue
	}

	// exists and is actually a bucket

	pgid, err := page.DecodeID(entry.Value())
	if err != nil {
		return nil, err
	}

	bucketRootNode, err := tx.db.readNode(pgid)
	if err != nil {
		return nil, err
	}

	return &Bucket{tx: tx, name: slices.Clone(bucketName), rootNode: bucketRootNode, parentBucket: parent}, nil
}

func (bucket *Bucket) CreateBucket(bucketName []byte) (*Bucket, error) {
	return bucket.tx.createBucket(bucket, bucketName)
}

// Bucket looks up a nested bucket by name. It returns (nil, nil) if no entry
// exists under that name, (nil, ErrIncompatibleValue) if the name holds a plain
// value instead of a bucket, and otherwise the same *Bucket handle on every call
// for the same parent and name, so writes through one handle are visible through
// any other handle for the same nested bucket.
func (bucket *Bucket) Bucket(bucketName []byte) (*Bucket, error) {
	if bucket.tx.closed {
		return nil, ErrTxClosed
	}
	if b, ok := bucket.children[string(bucketName)]; ok {
		return b, nil
	}
	child, err := bucket.tx.loadBucket(bucket.rootNode, bucketName, bucket)
	if err != nil || child == nil {
		return child, err
	}
	bucket.cacheChild(child)
	return child, nil
}

func (bucket *Bucket) Put(key, value []byte) error {
	if err := bucket.tx.writableError(); err != nil {
		return err
	}
	return bucket.putBucketEntry(btree.NewEntry(0, key, value))
}

func (bucket *Bucket) putBucketEntry(entry Entry) error {
	newRoot, err := bucket.tx.db.putTreeEntry(bucket.rootNode, entry)
	if err != nil {
		return err
	}
	if newRoot == bucket.rootNode {
		bucket.tx.db.wal.InsertMetaRecord(bucket.tx.db.meta)
		return nil
	}

	bucket.rootNode = newRoot
	return bucket.writeBackRoot()
}

// Get fetches key's value from this bucket. Like bbolt, it returns nil both when
// the key is absent and when the lookup fails outright; it also returns nil (not
// an error) if key names a nested bucket rather than a value, since Get has no
// error channel to report that distinction through.
func (bucket *Bucket) Get(key []byte) []byte {
	if bucket.tx.closed {
		return nil
	}
	entry, found, err := bucket.tx.db.findTreeEntry(bucket.rootNode, key)
	if err != nil || !found {
		return nil
	}

	// value is a bucket, we should refuse
	if entry.Flags()&BucketLeafFlag != 0 {
		return nil
	}
	return entry.Value()
}
