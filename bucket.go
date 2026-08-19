package kvlite

import (
	"slices"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

// cacheBucket registers b as the one handle for its name within this transaction.
func (tx *Tx) cacheBucket(b *Bucket) {
	if tx.buckets == nil {
		tx.buckets = make(map[string]*Bucket)
	}
	tx.buckets[string(b.name)] = b
}

// Bucket represents a named key space inside a transaction. A bucket can contain key/value pairs and nested buckets, and each nested bucket has its own key space.
//
// A Bucket is valid only during the [DB.Update] or [DB.View] callback in which the caller obtained it. Callers must not retain it after that callback returns, and they must not copy it or use it concurrently.
type Bucket struct {
	tx           *Tx
	name         []byte
	rootNode     *btree.Node
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

// writeBackRoot records this bucket's current root page in the entry that points to it. That entry belongs to the database catalog for a top-level bucket, or to the parent bucket for a nested bucket.
func (b *Bucket) writeBackRoot() error {
	entry := btree.NewEntry(btree.BucketLeafFlag, b.name, page.EncodeID(b.rootNode.PageID()))
	if b.parentBucket == nil {
		return b.tx.putCatalogEntry(entry)
	}
	return b.parentBucket.putBucketEntry(entry)
}

// CreateBucket creates a top-level bucket named bucketName in a writable transaction. The new bucket is part of the transaction, so [DB.Update] commits or rolls it back with the other changes in that transaction.
//
// CreateBucket copies bucketName before it returns. It returns [ErrBucketNameRequired] for an empty name, [ErrBucketExists] when that bucket already exists, and [ErrIncompatibleValue] when a plain value uses the same name. It also returns [ErrTxNotWritable] for a read-only transaction and [ErrTxClosed] after the transaction callback returns.
func (tx *Tx) CreateBucket(bucketName []byte) (*Bucket, error) {
	return tx.createBucket(nil, bucketName)
}

func (tx *Tx) createBucket(parent *Bucket, bucketName []byte) (*Bucket, error) {
	if err := tx.writableError(); err != nil {
		return nil, err
	}
	if len(bucketName) == 0 {
		return nil, ErrBucketNameRequired
	}

	var existing *Bucket
	var err error
	if parent == nil {
		existing, err = tx.lookupBucket(bucketName)
	} else {
		existing, err = parent.lookupBucket(bucketName)
	}
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, ErrBucketExists
	}

	newPgid := tx.store.AllocatePage()
	rootNode := btree.NewLeafNode(newPgid)
	tx.store.StageNode(rootNode)

	bucket := &Bucket{
		tx:           tx,
		name:         slices.Clone(bucketName),
		rootNode:     rootNode,
		parentBucket: parent,
	}

	if parent == nil {
		err = tx.putCatalogEntry(btree.NewEntry(btree.BucketLeafFlag, bucket.name, page.EncodeID(newPgid)))
	} else {
		err = parent.putBucketEntry(btree.NewEntry(btree.BucketLeafFlag, bucket.name, page.EncodeID(newPgid)))
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

// Bucket returns the top-level bucket named bucketName. Repeated lookups in one transaction return the same bucket handle, so all callers in that transaction observe the same pending changes.
//
// Bucket returns [ErrBucketNotFound] when the name is absent and [ErrIncompatibleValue] when the name belongs to a plain value. An empty name returns [ErrBucketNameRequired], and a call after the transaction callback returns fails with [ErrTxClosed].
func (tx *Tx) Bucket(bucketName []byte) (*Bucket, error) {
	if tx.closed {
		return nil, ErrTxClosed
	}
	if len(bucketName) == 0 {
		return nil, ErrBucketNameRequired
	}
	b, err := tx.lookupBucket(bucketName)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, ErrBucketNotFound
	}
	return b, nil
}

// lookupBucket resolves a top-level bucket without treating absence as an error. It returns (nil, nil) when no entry exists, which lets createBucket distinguish a free name from a conflicting value.
func (tx *Tx) lookupBucket(bucketName []byte) (*Bucket, error) {
	if tx.closed {
		return nil, ErrTxClosed
	}
	if b, ok := tx.buckets[string(bucketName)]; ok {
		return b, nil
	}
	bucket, err := tx.loadBucket(tx.rootNode, bucketName, nil)
	if err != nil || bucket == nil {
		return bucket, err
	}
	tx.cacheBucket(bucket)
	return bucket, nil
}

// loadBucket reads a bucket entry and opens the tree that it names. It returns (nil, nil) when no entry exists and [ErrIncompatibleValue] when the name belongs to a plain value.
func (tx *Tx) loadBucket(rootNode *btree.Node, bucketName []byte, parent *Bucket) (*Bucket, error) {
	entry, found, err := tx.findTreeEntry(rootNode, bucketName)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	if entry.Flags()&btree.BucketLeafFlag == 0 {
		// exists, but it's a regular value
		return nil, ErrIncompatibleValue
	}

	// exists and is actually a bucket

	pgid, err := page.DecodeID(entry.Value())
	if err != nil {
		return nil, err
	}

	bucketRootNode, err := tx.store.ReadNode(pgid)
	if err != nil {
		return nil, err
	}

	return &Bucket{tx: tx, name: slices.Clone(bucketName), rootNode: bucketRootNode, parentBucket: parent}, nil
}

// CreateBucket creates a nested bucket named bucketName inside bucket. The new bucket is part of the owning transaction, so [DB.Update] commits or rolls it back with the other changes in that transaction.
//
// CreateBucket copies bucketName before it returns. It returns [ErrBucketNameRequired] for an empty name, [ErrBucketExists] when that bucket already exists, and [ErrIncompatibleValue] when a plain value uses the same name. It also returns [ErrTxNotWritable] for a read-only transaction and [ErrTxClosed] after the transaction callback returns.
func (bucket *Bucket) CreateBucket(bucketName []byte) (*Bucket, error) {
	return bucket.tx.createBucket(bucket, bucketName)
}

// Bucket returns the nested bucket named bucketName. Repeated lookups through the same parent in one transaction return the same bucket handle, so all callers in that transaction observe the same pending changes.
//
// Bucket returns [ErrBucketNotFound] when the name is absent and [ErrIncompatibleValue] when the name belongs to a plain value. An empty name returns [ErrBucketNameRequired], and a call after the transaction callback returns fails with [ErrTxClosed].
func (bucket *Bucket) Bucket(bucketName []byte) (*Bucket, error) {
	child, err := bucket.lookupBucket(bucketName)
	if err != nil {
		return nil, err
	}
	if child == nil {
		return nil, ErrBucketNotFound
	}
	return child, nil
}

func (bucket *Bucket) lookupBucket(bucketName []byte) (*Bucket, error) {
	if bucket.tx.closed {
		return nil, ErrTxClosed
	}
	if len(bucketName) == 0 {
		return nil, ErrBucketNameRequired
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

// Put stores value under key in bucket. The write is visible to later reads in the same transaction, but Put does not commit it; [DB.Update] commits or rolls back all transaction changes together.
//
// Put copies key and value before it returns, and it treats a nil value as empty. It returns [ErrTxNotWritable] for a read-only transaction and [ErrTxClosed] after the transaction callback returns. An empty key returns [ErrKeyRequired], while oversized data returns [ErrKeyTooLarge], [ErrValueTooLarge], or [ErrEntryTooLargeForPage]. If key names a nested bucket, Put returns [ErrIncompatibleValue] instead of replacing it.
func (bucket *Bucket) Put(key, value []byte) error {
	if err := bucket.tx.writableError(); err != nil {
		return err
	}
	return bucket.putBucketEntry(btree.NewEntry(0, key, value))
}

func (bucket *Bucket) putBucketEntry(entry btree.Entry) error {
	newRoot, err := bucket.tx.putTreeEntry(bucket.rootNode, entry)
	if err != nil {
		return err
	}
	if newRoot == bucket.rootNode {
		return nil
	}

	bucket.rootNode = newRoot
	return bucket.writeBackRoot()
}

// Get returns the value stored under key in bucket, including a value written earlier in the same transaction. It returns [ErrKeyNotFound] when the key is absent and [ErrIncompatibleValue] when the key names a nested bucket. An empty key returns [ErrKeyRequired], an oversized key returns [ErrKeyTooLarge], and a call after the transaction callback returns fails with [ErrTxClosed].
//
// Get returns a read-only slice that is valid only until the transaction callback returns. The caller must not modify or retain it. A stored empty value returns a non-nil slice with length zero.
func (bucket *Bucket) Get(key []byte) ([]byte, error) {
	if bucket.tx.closed {
		return nil, ErrTxClosed
	}
	entry, found, err := bucket.tx.findTreeEntryRef(bucket.rootNode, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrKeyNotFound
	}

	if entry.Flags()&btree.BucketLeafFlag != 0 {
		return nil, ErrIncompatibleValue
	}
	return entry.Value(), nil
}
