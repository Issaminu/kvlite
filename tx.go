package kvlite

import "errors"

const (
	BucketLeafFlag = 0x01
)

type Tx struct {
	db       *DB
	readOnly bool
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

type Bucket struct {
	tx           *Tx
	name         []byte
	rootNode     *Node
	parentBucket *Bucket
}

// writeBackRoot records this bucket's (possibly moved) root pgid into the entry that points at it
func (b *Bucket) writeBackRoot() error {
	db := b.tx.db
	container := db.rootNode
	if b.parentBucket != nil {
		container = b.parentBucket.rootNode
	}
	_, err := db._put(container, b.name, encode(b.rootNode.pgid), BucketLeafFlag, false)
	return err
}

func (tx *Tx) CreateBucket(bucketName []byte) (*Bucket, error) {
	// check if the bucket exists already
	bucket := tx.Bucket(bucketName)
	if bucket != nil {
		return nil, ErrBucketExists
	}

	// create the bucket
	newPgid := tx.db.allocate()

	bucketRootNode := tx.db.newLeafNode(newPgid)
	tx.db.wal.insertNodeRecord(bucketRootNode)

	bucket = &Bucket{
		tx:           tx,
		name:         bucketName,
		rootNode:     bucketRootNode,
		parentBucket: nil, // top-level: entry lives in the DB catalog
	}

	newRootNode, err := tx.db._put(tx.db.rootNode, []byte(bucketName), encode(newPgid), BucketLeafFlag, false)
	if err != nil {
		return nil, err
	}
	if newRootNode != nil {
		tx.db.rootNode = newRootNode
		tx.db.meta.root = newRootNode.pgid
	}

	return bucket, nil
}

func (tx *Tx) Bucket(bucketName []byte) *Bucket {
	value, flags, err := tx.db._get(tx.db.rootNode, bucketName)
	if err != nil && errors.Is(err, ErrKeyNotFound) {
		return nil
	}

	// if bucket is missing, return nil
	if value == nil {
		return nil
	}

	if flags&BucketLeafFlag == 0 {
		// exists, but it's a regular value
		return nil
	}

	// exists and is actually a bucket

	pgid, err := decode[Pgid](value)
	if err != nil {
		return nil
	}

	bucketRootNode, err := tx.db.readNode(pgid)
	if err != nil {
		return nil
	}

	return &Bucket{tx: tx, name: bucketName, rootNode: bucketRootNode, parentBucket: nil}
}

func (bucket *Bucket) CreateBucket(bucketName []byte) (*Bucket, error) {
	// check if the bucket exists already
	newBucket, err := bucket.Bucket(bucketName)
	if err != nil {
		return nil, err
	}
	if newBucket != nil {
		return nil, ErrBucketExists
	}

	// create the bucket
	newPgid := bucket.tx.db.allocate()

	bucketRootNode := bucket.tx.db.newLeafNode(newPgid)
	bucket.tx.db.wal.insertNodeRecord(bucketRootNode)

	newBucket = &Bucket{
		tx:           bucket.tx,
		name:         bucketName,
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
	return newBucket, nil
}

func (bucket *Bucket) Bucket(bucketName []byte) (*Bucket, error) {
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

	return &Bucket{tx: bucket.tx, name: bucketName, rootNode: bucketRootNode, parentBucket: bucket}, nil
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

func (bucket *Bucket) Get(key []byte) []byte {
	value, flags, _ := bucket.tx.db._get(bucket.rootNode, key)

	// value is a bucket, we should refuse
	if flags&BucketLeafFlag != 0 {
		return nil
	}
	return value
}
