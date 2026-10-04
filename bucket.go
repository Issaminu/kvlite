package kvlite

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

// cacheBucket registers b as the one handle for its name within this transaction.
func (tx *Tx) cacheBucket(b *Bucket) {
	if tx.bucket == nil {
		tx.bucket = b
		return
	}
	if tx.buckets == nil {
		tx.buckets = make(map[string]*Bucket)
		tx.buckets[string(tx.bucket.name)] = tx.bucket
	}
	tx.buckets[string(b.name)] = b
}

// Bucket is a named group of keys inside a transaction. A bucket can store values and other buckets. Each nested bucket has its own keys.
//
// A Bucket is valid only until its [DB.View] or [DB.Update] callback returns. [Tx.DeleteBucket] or [Bucket.DeleteBucket] can end its use earlier. Do not save, copy, or share it with another goroutine.
//
// Ordered reads include the names of nested buckets. A nested bucket has a nil value. A stored empty value has a non-nil value with length zero.
type Bucket struct {
	tx           *Tx
	name         []byte
	rootPageID   page.ID // Set only while a read-only bucket has not loaded rootNode.
	rootNode     *btree.Node
	parentBucket *Bucket
	children     map[string]*Bucket // per-parent cache: one *Bucket handle per nested name
	treeVersion  uint64             // A cursor captures this value and rejects a stale path after a successful tree change.
	deleted      bool
}

// liveError rejects a handle after this bucket or one of its parents is removed.
func (bucket *Bucket) liveError() error {
	if bucket.tx.closed {
		return ErrTxClosed
	}
	for current := bucket; current != nil; current = current.parentBucket {
		if current.deleted {
			return ErrBucketNotFound
		}
	}
	return nil
}

// cacheChild registers b as the one handle for its name within this bucket.
func (bucket *Bucket) cacheChild(b *Bucket) {
	if bucket.children == nil {
		bucket.children = make(map[string]*Bucket)
	}
	bucket.children[string(b.name)] = b
}

// writeBackRoot updates the existing fixed-size root pointer after the root page changes.
// The transaction already loaded this entry before it changed the bucket tree.
func (bucket *Bucket) writeBackRoot() {
	entry := btree.NewEntry(btree.BucketLeafFlag, bucket.name, page.EncodeID(bucket.rootNode.PageID()))
	var err error
	if bucket.parentBucket == nil {
		err = bucket.tx.putCatalogEntry(entry)
	} else {
		err = bucket.parentBucket.putBucketEntry(entry)
	}
	if err != nil {
		panic("kvlite: failed to update an existing bucket root pointer: " + err.Error())
	}
}

// setRoot records a tree change and updates the bucket entry if the root page changed.
func (bucket *Bucket) setRoot(newRoot *btree.Node) {
	oldRootPageID := bucket.rootNode.PageID()
	bucket.rootNode = newRoot
	if newRoot.PageID() != oldRootPageID {
		bucket.writeBackRoot()
	}
	bucket.treeVersion++
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
	if parent != nil {
		if err := parent.liveError(); err != nil {
			return nil, err
		}
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

	newPgid, err := tx.store.AllocatePage()
	if err != nil {
		return nil, err
	}
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
	if tx.bucket != nil && bytes.Equal(tx.bucket.name, bucketName) {
		return tx.bucket, nil
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
	entry, found, err := tx.tree.FindEntryRef(rootNode, bucketName)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return tx.openBucket(entry, parent)
}

func (tx *Tx) openBucket(entry btree.Entry, parent *Bucket) (*Bucket, error) {
	rootPageID, err := decodeBucketRootPageID(entry)
	if err != nil {
		return nil, err
	}
	if tx.readOnly {
		return &Bucket{tx: tx, name: entry.Key(), rootPageID: rootPageID, parentBucket: parent}, nil
	}

	rootNode, err := tx.store.ReadNode(rootPageID)
	if err != nil {
		return nil, err
	}
	return &Bucket{tx: tx, name: entry.Key(), rootNode: rootNode, parentBucket: parent}, nil
}

func decodeBucketRootPageID(entry btree.Entry) (page.ID, error) {
	if entry.Flags()&btree.BucketLeafFlag == 0 {
		return 0, btree.ErrIncompatibleValue
	}
	return page.DecodeID(entry.Value())
}

func valueFromEntry(entry btree.Entry, found bool) ([]byte, error) {
	if !found {
		return nil, btree.ErrKeyNotFound
	}
	if entry.Flags()&btree.BucketLeafFlag != 0 {
		return nil, btree.ErrIncompatibleValue
	}
	return entry.Value(), nil
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
	if err := bucket.liveError(); err != nil {
		return nil, err
	}
	if len(bucketName) == 0 {
		return nil, ErrBucketNameRequired
	}
	if b, ok := bucket.children[string(bucketName)]; ok {
		return b, nil
	}
	entry, found, err := bucket.findEntry(bucketName)
	if err != nil || !found {
		return nil, err
	}
	child, err := bucket.tx.openBucket(entry, bucket)
	if err != nil {
		return nil, err
	}
	bucket.cacheChild(child)
	return child, nil
}

// Put stores value under key in bucket. The write is visible to later reads in the same transaction, but Put does not commit it; [DB.Update] commits or rolls back all transaction changes together.
//
// Put copies key and value before it returns, and it treats a nil value as empty.
// It returns [ErrTxNotWritable] for a read-only transaction and [ErrTxClosed] after the transaction callback returns.
// An empty key returns [ErrKeyRequired], while oversized data returns [ErrKeyTooLarge], [ErrValueTooLarge], or [ErrEntryTooLargeForPage].
// [ErrEntryTooLargeForPage] also applies when the entry fits a leaf but its key cannot fit a required branch separator.
// If key names a nested bucket, Put returns [ErrIncompatibleValue] instead of replacing it.
// A returned error does not change the bucket, so the caller can continue to use the transaction.
func (bucket *Bucket) Put(key, value []byte) error {
	if err := bucket.tx.writableError(); err != nil {
		return err
	}
	if err := bucket.liveError(); err != nil {
		return err
	}
	return bucket.putBucketEntry(btree.NewEntry(0, key, value))
}

func (bucket *Bucket) putBucketEntry(entry btree.Entry) error {
	newRoot, err := bucket.tx.tree.PutEntry(bucket.rootNode, entry)
	if err != nil {
		return err
	}
	bucket.setRoot(newRoot)
	return nil
}

// Delete removes one plain key and value from this bucket in the current write transaction. Later reads in the same transaction see the change. Other database operations see it after [DB.Update] commits.
//
// A missing key returns nil. A key that names a nested bucket returns [ErrIncompatibleValue]. Delete returns [ErrTxNotWritable] for a read-only transaction and [ErrTxClosed] after the callback returns. It returns [ErrKeyRequired] for an empty key and [ErrKeyTooLarge] for a large key.
func (bucket *Bucket) Delete(key []byte) error {
	if err := bucket.tx.writableError(); err != nil {
		return err
	}
	if err := bucket.liveError(); err != nil {
		return err
	}
	newRoot, deleted, err := bucket.tx.tree.DeleteEntry(bucket.rootNode, key)
	if err != nil || !deleted {
		return err
	}
	bucket.setRoot(newRoot)
	return nil
}

// DeleteBatch removes plain keys from this bucket in the current write transaction.
// A missing key has no effect. A repeated key has the same effect as one key.
// It returns the same key and bucket errors as [Bucket.Delete].
// If it returns an error, the caller must return that error from [DB.Update].
func (bucket *Bucket) DeleteBatch(keys [][]byte) error {
	if err := bucket.tx.writableError(); err != nil {
		return err
	}
	if err := bucket.liveError(); err != nil {
		return err
	}
	newRoot, changed, err := bucket.tx.tree.DeleteEntries(bucket.rootNode, keys)
	if !changed || newRoot == nil {
		return err
	}
	bucket.setRoot(newRoot)
	return err
}

// DeleteBucket removes a top-level bucket and all of its contents in this write transaction.
// It also removes nested buckets and releases their pages when [DB.Update] commits.
//
// A missing bucket returns [ErrBucketNotFound]. A plain value with this name returns [ErrIncompatibleValue]. An empty name returns [ErrBucketNameRequired]. A read-only transaction returns [ErrTxNotWritable]. A closed transaction returns [ErrTxClosed]. Handles to the removed bucket and its children return [ErrBucketNotFound] until the transaction ends.
// Return a storage error from the [DB.Update] callback to discard the transaction.
func (tx *Tx) DeleteBucket(bucketName []byte) error {
	return tx.deleteBucket(nil, bucketName)
}

// DeleteBucket removes a nested bucket and all of its contents in this write transaction.
// It also removes child buckets and releases their pages when [DB.Update] commits.
//
// A missing bucket returns [ErrBucketNotFound]. A plain value with this name returns [ErrIncompatibleValue]. An empty name returns [ErrBucketNameRequired]. A read-only transaction returns [ErrTxNotWritable]. A closed transaction returns [ErrTxClosed]. Handles to the removed bucket and its children return [ErrBucketNotFound] until the transaction ends.
// Return a storage error from the [DB.Update] callback to discard the transaction.
func (bucket *Bucket) DeleteBucket(bucketName []byte) error {
	return bucket.tx.deleteBucket(bucket, bucketName)
}

func (tx *Tx) deleteBucket(parent *Bucket, bucketName []byte) error {
	if err := tx.writableError(); err != nil {
		return err
	}
	if parent != nil {
		if err := parent.liveError(); err != nil {
			return err
		}
	}
	if len(bucketName) == 0 {
		return ErrBucketNameRequired
	}

	var target *Bucket
	var err error
	if parent == nil {
		target, err = tx.lookupBucket(bucketName)
	} else {
		target, err = parent.lookupBucket(bucketName)
	}
	if err != nil {
		return err
	}
	if target == nil {
		return ErrBucketNotFound
	}

	// Read every owned page before changing the parent entry. A read error leaves the tree unchanged.
	pages, err := tx.collectBucketPages(target.rootNode.PageID())
	if err != nil {
		return err
	}
	if parent == nil {
		oldRootPageID := tx.rootNode.PageID()
		newRoot, found, err := tx.tree.DeleteBucketEntry(tx.rootNode, bucketName)
		if err != nil {
			return err
		}
		if !found {
			return ErrBucketNotFound
		}
		tx.rootNode = newRoot
		if newRoot.PageID() != oldRootPageID {
			tx.meta.SetRoot(newRoot.PageID())
			tx.metaDirty = true
		}
	} else {
		newRoot, found, err := tx.tree.DeleteBucketEntry(parent.rootNode, bucketName)
		if err != nil {
			return err
		}
		if !found {
			return ErrBucketNotFound
		}
		parent.setRoot(newRoot)
	}

	for _, pageID := range pages {
		if err := tx.store.FreePage(pageID); err != nil {
			return err
		}
	}
	target.deleted = true
	if parent == nil {
		if tx.bucket == target {
			tx.bucket = nil
		}
		delete(tx.buckets, string(bucketName))
	} else {
		delete(parent.children, string(bucketName))
	}
	return nil
}

// collectBucketPages checks every page owned by a bucket before the caller removes its name.
// A set detects duplicate page owners before any page is retired.
func (tx *Tx) collectBucketPages(root page.ID) ([]page.ID, error) {
	allocation := tx.writableAllocation()
	stack := []page.ID{root}
	seen := make(map[page.ID]struct{})
	var pages []page.ID
	for len(stack) != 0 {
		last := len(stack) - 1
		pageID := stack[last]
		stack = stack[:last]
		if _, exists := seen[pageID]; exists {
			return nil, fmt.Errorf("bucket page %d has more than one owner: %w", pageID, ErrInvalid)
		}
		if pageID < firstTreePageID || allocation.isSegmentPage(pageID) || !allocation.allocated(pageID) {
			return nil, fmt.Errorf("bucket page %d is not allocated tree data: %w", pageID, ErrInvalid)
		}
		if _, retired := allocation.retired[pageID]; retired {
			return nil, fmt.Errorf("bucket page %d is already retired: %w", pageID, ErrInvalid)
		}
		seen[pageID] = struct{}{}
		if err := tx.store.VisitNodeReferences(pageID, func(child page.ID) error {
			stack = append(stack, child)
			return nil
		}); err != nil {
			return nil, err
		}
		pages = append(pages, pageID)
	}
	return pages, nil
}

// Get returns the value stored under key in bucket, including a value written earlier in the same transaction. It returns [ErrKeyNotFound] when the key is absent and [ErrIncompatibleValue] when the key names a nested bucket. An empty key returns [ErrKeyRequired], an oversized key returns [ErrKeyTooLarge], and a call after the transaction callback returns fails with [ErrTxClosed].
//
// Get returns a read-only slice that is valid only until the transaction callback returns. The caller must not modify or retain it. A stored empty value returns a non-nil slice with length zero.
func (bucket *Bucket) Get(key []byte) ([]byte, error) {
	if err := bucket.liveError(); err != nil {
		return nil, err
	}
	entry, found, err := bucket.findEntry(key)
	if err != nil {
		return nil, err
	}
	return valueFromEntry(entry, found)
}

func (bucket *Bucket) findEntry(key []byte) (btree.Entry, bool, error) {
	if bucket.rootNode != nil {
		return bucket.tx.tree.FindEntryRef(bucket.rootNode, key)
	}
	return bucket.tx.tree.FindEntryRefFromPage(bucket.rootPageID, key)
}

func (bucket *Bucket) loadRootNode() error {
	if err := bucket.liveError(); err != nil {
		return err
	}
	if bucket.rootNode != nil {
		return nil
	}
	rootNode, err := bucket.tx.store.ReadNode(bucket.rootPageID)
	if err != nil {
		return err
	}
	bucket.rootNode = rootNode
	bucket.rootPageID = page.Meta0ID
	return nil
}
