package kvlite

import (
	"errors"
	"slices"
)

const (
	BucketLeafFlag = 0x01
)

type Tx struct {
	db       *DB
	readOnly bool
	buckets  map[string]*Bucket // per-tx cache: one *Bucket handle per top-level name, so every caller in this tx observes the same in-memory state
}

func (tx *Tx) Writable() bool {
	return !tx.readOnly
}

func (tx *Tx) Put(key, value []byte) error {
	if !tx.Writable() {
		return ErrTxNotWritable
	}
	// need to call `_put()` instead of `Put()` so that we can specify that we don't want to commit the changes
	newRoot, err := tx.db._put(tx.db.rootNode, key, value, 0, false)
	if err != nil {
		return err
	}
	if newRoot == tx.db.rootNode {
		return nil
	}
	tx.db.rootNode = newRoot
	tx.db.meta.root = newRoot.pgid
	return nil
}

func (tx *Tx) Get(key []byte) ([]byte, error) {
	value, err := tx.db.Get(key)
	return value, err
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
	db := b.tx.db

	if b.parentBucket == nil {
		// Top-level bucket. The entry lives in the DB catalog (db.rootNode). When
		// that root splits, _put updates db.rootNode and db.meta.root itself, so we
		// only record the pointer here and let _put own the catalog root.
		_, err := db._put(db.rootNode, b.name, encode(b.rootNode.pgid), BucketLeafFlag, false)
		return err
	}

	// Nested bucket. The entry lives in the parent bucket's own tree. A split moves
	// the parent's root to a new page, so adopt it and write the parent's pointer one
	// level further up. This recurses until it reaches the DB catalog.
	parent := b.parentBucket
	newParentRoot, err := db._put(parent.rootNode, b.name, encode(b.rootNode.pgid), BucketLeafFlag, false)
	if err != nil {
		return err
	}
	if newParentRoot != parent.rootNode {
		parent.rootNode = newParentRoot
		return parent.writeBackRoot()
	}
	return nil
}

func (tx *Tx) CreateBucket(bucketName []byte) (*Bucket, error) {
	if !tx.Writable() {
		return nil, ErrTxNotWritable
	}
	// Check whether the name is already taken, by a bucket or by a plain value.
	// This goes through lookupBucket rather than the public Bucket() method,
	// because Bucket() (like bbolt's) collapses "wrong type" and "not found" into
	// the same nil result. CreateBucket needs to tell them apart: ErrBucketExists
	// for an existing bucket, ErrIncompatibleValue (from lookupBucket) for an
	// existing plain value.
	existing, err := tx.lookupBucket(bucketName)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, ErrBucketExists
	}

	// create the bucket
	newPgid := tx.db.allocate()

	bucketRootNode := tx.db.newLeafNode(newPgid)
	tx.db.wal.insertNodeRecord(bucketRootNode)

	bucket := &Bucket{
		tx:           tx,
		name:         slices.Clone(bucketName), // own the name: writeBackRoot reads it again on every later split
		rootNode:     bucketRootNode,
		parentBucket: nil, // top-level: entry lives in the DB catalog
	}

	newRootNode, err := tx.db._put(tx.db.rootNode, bucketName, encode(newPgid), BucketLeafFlag, false)
	if err != nil {
		return nil, err
	}
	if newRootNode != nil {
		tx.db.rootNode = newRootNode
		tx.db.meta.root = newRootNode.pgid
	}

	tx.cacheBucket(bucket)
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
	if b, ok := tx.buckets[string(bucketName)]; ok {
		return b, nil
	}

	value, flags, err := tx.db._get(tx.db.rootNode, bucketName)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}

	if flags&BucketLeafFlag == 0 {
		// exists, but it's a regular value
		return nil, ErrIncompatibleValue
	}

	// exists and is actually a bucket

	pgid, err := decode[Pgid](value)
	if err != nil {
		return nil, err
	}

	bucketRootNode, err := tx.db.readNode(pgid)
	if err != nil {
		return nil, err
	}

	bucket := &Bucket{tx: tx, name: slices.Clone(bucketName), rootNode: bucketRootNode, parentBucket: nil}
	tx.cacheBucket(bucket)
	return bucket, nil
}

func (bucket *Bucket) CreateBucket(bucketName []byte) (*Bucket, error) {
	if !bucket.tx.Writable() {
		return nil, ErrTxNotWritable
	}

	// check if the bucket exists already
	existing, err := bucket.Bucket(bucketName)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, ErrBucketExists
	}

	// create the bucket
	newPgid := bucket.tx.db.allocate()

	bucketRootNode := bucket.tx.db.newLeafNode(newPgid)
	bucket.tx.db.wal.insertNodeRecord(bucketRootNode)

	newBucket := &Bucket{
		tx:           bucket.tx,
		name:         slices.Clone(bucketName), // own the name: writeBackRoot reads it again on every later split
		rootNode:     bucketRootNode,
		parentBucket: bucket,
	}

	// Insert the child's name->root entry into this bucket's own tree. That insert
	// can split this bucket; if its root moves, write the new root back up the chain.
	newRoot, err := bucket.tx.db._put(bucket.rootNode, newBucket.name, encode(newBucket.rootNode.pgid), BucketLeafFlag, false)
	if err != nil {
		return nil, err
	}
	if newRoot != bucket.rootNode {
		bucket.rootNode = newRoot
		if err := bucket.writeBackRoot(); err != nil {
			return nil, err
		}
	}

	// Return the newly created child bucket, not the parent we just re-wired.
	bucket.cacheChild(newBucket)
	return newBucket, nil
}

// Bucket looks up a nested bucket by name. It returns (nil, nil) if no entry
// exists under that name, (nil, ErrIncompatibleValue) if the name holds a plain
// value instead of a bucket, and otherwise the same *Bucket handle on every call
// for the same parent and name, so writes through one handle are visible through
// any other handle for the same nested bucket.
func (bucket *Bucket) Bucket(bucketName []byte) (*Bucket, error) {
	if b, ok := bucket.children[string(bucketName)]; ok {
		return b, nil
	}

	value, flags, err := bucket.tx.db._get(bucket.rootNode, bucketName)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return nil, nil
		}

		return nil, err
	}

	if flags&BucketLeafFlag == 0 {
		// exists, but it's a regular value
		return nil, ErrIncompatibleValue
	}

	// exists and is actually a bucket
	pgid, err := decode[Pgid](value)
	if err != nil {
		return nil, err
	}

	bucketRootNode, err := bucket.tx.db.readNode(pgid)
	if err != nil {
		return nil, err
	}

	child := &Bucket{tx: bucket.tx, name: slices.Clone(bucketName), rootNode: bucketRootNode, parentBucket: bucket}
	bucket.cacheChild(child)
	return child, nil
}

func (bucket *Bucket) Put(key, value []byte) error {
	if !bucket.tx.Writable() {
		return ErrTxNotWritable
	}

	newRoot, err := bucket.tx.db._put(bucket.rootNode, key, value, 0, false)
	if err != nil {
		return err
	}
	if newRoot == bucket.rootNode {
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
	value, flags, err := bucket.tx.db._get(bucket.rootNode, key)
	if err != nil {
		return nil
	}

	// value is a bucket, we should refuse
	if flags&BucketLeafFlag != 0 {
		return nil
	}
	return value
}
