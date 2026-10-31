package kvlite

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

// cacheBucket registers b as the one handle for its name within this transaction.
// Keep the first bucket directly. Store only other buckets in the map.
func (tx *Tx) cacheBucket(b *Bucket) {
	if tx.bucket == nil {
		tx.bucket = b
		return
	}
	if tx.buckets == nil {
		tx.buckets = make(map[string]*Bucket)
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
	rootPageID   page.ID
	parentBucket *Bucket
	child        *Bucket            // First nested handle, kept without a map.
	children     map[string]*Bucket // Other nested handles in this transaction.
	treeVersion  uint64             // A cursor captures this value and rejects a stale path after a successful tree change.
	deleted      bool
}

// KeyValue is one key and value supplied to [Bucket.PutBatch] or [DB.PutBatch].
// A nil Value stores an empty value.
// An empty Key is invalid.
type KeyValue struct{ Key, Value []byte }

// liveError rejects a handle after this bucket or one of its parents is removed.
func (bucket *Bucket) liveError() error {
	if bucket.tx.closed {
		return ErrTxClosed
	}
	if bucket.tx.failure != nil {
		return bucket.tx.failure
	}
	for current := bucket; current != nil; current = current.parentBucket {
		if current.deleted {
			return ErrBucketNotFound
		}
	}
	return nil
}

// cacheChild registers b as the one handle for its name within this bucket.
// A short read transaction often opens only one child at each level of a bucket
// path. Keep that child directly so the read does not allocate a map at every
// level and discard those maps when the transaction ends. Cache other children
// in a map so repeated lookups still return the same handles.
func (bucket *Bucket) cacheChild(b *Bucket) {
	if bucket.child == nil {
		bucket.child = b
		return
	}
	if bucket.children == nil {
		bucket.children = make(map[string]*Bucket)
	}
	bucket.children[string(b.name)] = b
}

// writeBackRoot updates the existing fixed-size root pointer after the root page changes.
// It reads the parent tree again, so a storage error can stop it.
// The bucket tree has already changed then, so the caller must discard the transaction.
func (bucket *Bucket) writeBackRoot() error {
	entry := btree.NewEntry(btree.BucketLeafFlag, bucket.name, page.EncodeID(bucket.rootPageID))
	if bucket.parentBucket == nil {
		return bucket.tx.putCatalogEntry(entry)
	}
	return bucket.parentBucket.putBucketEntry(entry)
}

// setRoot records a tree change and updates the bucket entry if the root page changed.
// An error leaves the bucket tree changed, so the caller must discard the transaction.
func (bucket *Bucket) setRoot(newRoot page.ID) error {
	oldRootPageID := bucket.rootPageID
	bucket.rootPageID = newRoot
	var err error
	if newRoot != oldRootPageID {
		err = bucket.writeBackRoot()
	}
	bucket.treeVersion++
	return err
}

// CreateBucket creates a top-level bucket named bucketName in a writable transaction. The new bucket is part of the transaction, so [DB.Update] commits or rolls it back with the other changes in that transaction.
//
// CreateBucket copies bucketName before it returns. It returns [ErrBucketNameRequired] for an empty name, [ErrBucketExists] when that bucket already exists, and [ErrIncompatibleValue] when a plain value uses the same name. It also returns [ErrTxNotWritable] for a read-only transaction and [ErrTxClosed] after the transaction callback returns.
func (tx *Tx) CreateBucket(bucketName []byte) (*Bucket, error) {
	return tx.createBucket(nil, bucketName)
}

func (tx *Tx) createBucket(parent *Bucket, bucketName []byte) (_ *Bucket, err error) {
	if err := tx.writableError(); err != nil {
		return nil, err
	}
	defer tx.keepFailure(tx.changes, &err)
	if parent != nil {
		if err := parent.liveError(); err != nil {
			return nil, err
		}
	}
	if len(bucketName) == 0 {
		return nil, ErrBucketNameRequired
	}

	var existing *Bucket
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
		rootPageID:   rootNode.PageID(),
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
	if tx.failure != nil {
		return nil, tx.failure
	}
	if tx.bucket != nil && bytes.Equal(tx.bucket.name, bucketName) {
		return tx.bucket, nil
	}
	if b, ok := tx.buckets[string(bucketName)]; ok {
		return b, nil
	}
	bucket, err := tx.loadBucket(tx.meta.Root(), bucketName, nil)
	if err != nil || bucket == nil {
		return bucket, err
	}
	tx.cacheBucket(bucket)
	return bucket, nil
}

// loadBucket reads a bucket entry from the tree at parentRoot and opens the tree that the entry names. It returns (nil, nil) when no entry exists and [ErrIncompatibleValue] when the name belongs to a plain value.
func (tx *Tx) loadBucket(parentRoot page.ID, bucketName []byte, parent *Bucket) (*Bucket, error) {
	// The bucket keeps the stored name, so the lookup must return the key.
	entry, found, err := tx.tree.FindEntryRef(parentRoot, bucketName, true)
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
	return &Bucket{tx: tx, name: entry.Key(), rootPageID: rootPageID, parentBucket: parent}, nil
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
	if bucket.child != nil && bytes.Equal(bucket.child.name, bucketName) {
		return bucket.child, nil
	}
	if b, ok := bucket.children[string(bucketName)]; ok {
		return b, nil
	}
	entry, found, err := bucket.findEntry(bucketName, true)
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
// An empty key returns [ErrKeyRequired].
// A key larger than [MaxKeySize] returns [ErrKeyTooLarge].
// A value larger than [MaxValueSize] returns [ErrValueTooLarge].
// Larger leaf values and branch keys use continuation pages at commit.
// If key names a nested bucket, Put returns [ErrIncompatibleValue] instead of replacing it.
// An error that rejects key or value changes nothing, so the caller can continue to use the transaction.
// A storage error after Put changed the transaction stops the transaction, and later operations return [ErrTxFailed].
func (bucket *Bucket) Put(key, value []byte) (err error) {
	if err := bucket.tx.writableError(); err != nil {
		return err
	}
	if err := bucket.liveError(); err != nil {
		return err
	}
	defer bucket.tx.keepFailure(bucket.tx.changes, &err)
	return bucket.putBucketEntry(btree.NewEntry(0, key, value))
}

// PutBatch stores entries in this bucket in the current write transaction.
// Later reads in the transaction see the changes.
// For repeated keys, the last entry supplies the stored value.
// A nil value stores an empty value, and an empty batch has no effect.
//
// PutBatch copies keys and values before it returns and does not change the input order.
// The caller must keep entries and their bytes unchanged until it returns.
// Key, value, bucket-conflict and transaction errors match [Bucket.Put].
// An error after changes begin stops the transaction: later operations return [ErrTxFailed], and [DB.Update] discards the changes.
func (bucket *Bucket) PutBatch(entries []KeyValue) error {
	return bucket.putBatch(entries, false)
}

// putBatch shares ordering and insertion between callback-owned and managed batches.
// Only a managed call can borrow large values because it commits before caller code resumes.
func (bucket *Bucket) putBatch(entries []KeyValue, managed bool) (err error) {
	if err := bucket.tx.writableError(); err != nil {
		return err
	}
	if err := bucket.liveError(); err != nil {
		return err
	}
	defer bucket.tx.keepFailure(bucket.tx.changes, &err)
	for _, entry := range entries {
		if len(entry.Key) == 0 {
			return ErrKeyRequired
		}
		if len(entry.Key) > MaxKeySize {
			return ErrKeyTooLarge
		}
		if len(entry.Value) > MaxValueSize {
			return ErrValueTooLarge
		}
	}
	ordered := entries
	compare := func(a, b KeyValue) int { return bytes.Compare(a.Key, b.Key) }
	if !slices.IsSortedFunc(entries, compare) {
		ordered = slices.Clone(entries)
		// Stable ordering leaves the last caller-supplied value last for each key.
		slices.SortStableFunc(ordered, compare)
	}
	for index, pair := range ordered {
		if index+1 < len(ordered) && bytes.Equal(pair.Key, ordered[index+1].Key) {
			continue
		}
		entry := btree.NewEntry(0, pair.Key, pair.Value)
		if managed && bucket.tx.db.memoryNodes == nil && int64(len(pair.Value)) > bucket.tx.meta.PageSize() {
			entry = btree.NewEntryBorrowingValue(pair.Key, pair.Value)
			bucket.tx.borrowedValue = true
		}
		if err := bucket.putBucketEntry(entry); err != nil {
			return err
		}
	}
	return nil
}

func (bucket *Bucket) putBucketEntry(entry btree.Entry) error {
	newRoot, err := bucket.tx.tree.PutEntry(bucket.rootPageID, entry)
	if err != nil {
		return err
	}
	return bucket.setRoot(newRoot)
}

// Delete removes one plain key and value from this bucket in the current write transaction. Later reads in the same transaction see the change. Other database operations see it after [DB.Update] commits.
//
// A missing key returns nil. A key that names a nested bucket returns [ErrIncompatibleValue]. Delete returns [ErrTxNotWritable] for a read-only transaction and [ErrTxClosed] after the callback returns. It returns [ErrKeyRequired] for an empty key and [ErrKeyTooLarge] for a large key.
func (bucket *Bucket) Delete(key []byte) (err error) {
	if err := bucket.tx.writableError(); err != nil {
		return err
	}
	if err := bucket.liveError(); err != nil {
		return err
	}
	defer bucket.tx.keepFailure(bucket.tx.changes, &err)
	newRoot, deleted, err := bucket.tx.tree.DeleteEntry(bucket.rootPageID, key)
	if err != nil || !deleted {
		return err
	}
	return bucket.setRoot(newRoot)
}

// DeleteBatch removes plain keys from this bucket in the current write transaction.
// A missing key has no effect. A repeated key has the same effect as one key.
// An empty batch has no effect.
// DeleteBatch does not change the input order or retain caller bytes after it returns.
// The caller must keep keys and their bytes unchanged until it returns.
// It returns the same key and bucket errors as [Bucket.Delete].
// An error after changes begin stops the transaction: later operations return [ErrTxFailed], and [DB.Update] discards the changes.
func (bucket *Bucket) DeleteBatch(keys [][]byte) (err error) {
	if err := bucket.tx.writableError(); err != nil {
		return err
	}
	if err := bucket.liveError(); err != nil {
		return err
	}
	defer bucket.tx.keepFailure(bucket.tx.changes, &err)
	newRoot, changed, err := bucket.tx.tree.DeleteEntries(bucket.rootPageID, keys)
	if !changed || newRoot == 0 {
		return err
	}
	// A partial change can fail after earlier leaves changed. Record the new root and report both errors.
	return errors.Join(err, bucket.setRoot(newRoot))
}

// DeleteBucket removes a top-level bucket and all of its contents in this write transaction.
// It also removes nested buckets and releases their pages when [DB.Update] commits.
//
// A missing bucket returns [ErrBucketNotFound]. A plain value with this name returns [ErrIncompatibleValue]. An empty name returns [ErrBucketNameRequired]. A read-only transaction returns [ErrTxNotWritable]. A closed transaction returns [ErrTxClosed]. Handles to the removed bucket and its children return [ErrBucketNotFound] until the transaction ends.
// A storage error after the removal begins stops the transaction: later operations return [ErrTxFailed], and [DB.Update] discards the changes.
func (tx *Tx) DeleteBucket(bucketName []byte) error {
	return tx.deleteBucket(nil, bucketName)
}

// DeleteBucket removes a nested bucket and all of its contents in this write transaction.
// It also removes child buckets and releases their pages when [DB.Update] commits.
//
// A missing bucket returns [ErrBucketNotFound]. A plain value with this name returns [ErrIncompatibleValue]. An empty name returns [ErrBucketNameRequired]. A read-only transaction returns [ErrTxNotWritable]. A closed transaction returns [ErrTxClosed]. Handles to the removed bucket and its children return [ErrBucketNotFound] until the transaction ends.
// A storage error after the removal begins stops the transaction: later operations return [ErrTxFailed], and [DB.Update] discards the changes.
func (bucket *Bucket) DeleteBucket(bucketName []byte) error {
	return bucket.tx.deleteBucket(bucket, bucketName)
}

func (tx *Tx) deleteBucket(parent *Bucket, bucketName []byte) (err error) {
	if err := tx.writableError(); err != nil {
		return err
	}
	defer tx.keepFailure(tx.changes, &err)
	if parent != nil {
		if err := parent.liveError(); err != nil {
			return err
		}
	}
	if len(bucketName) == 0 {
		return ErrBucketNameRequired
	}

	var target *Bucket
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
	pages, err := tx.collectBucketPages(target.rootPageID)
	if err != nil {
		return err
	}
	if parent == nil {
		newRoot, found, err := tx.tree.DeleteBucketEntry(tx.meta.Root(), bucketName)
		if err != nil {
			return err
		}
		if !found {
			return ErrBucketNotFound
		}
		if newRoot != tx.meta.Root() {
			tx.writableMeta().SetRoot(newRoot)
			tx.metaDirty = true
		}
	} else {
		newRoot, found, err := tx.tree.DeleteBucketEntry(parent.rootPageID, bucketName)
		if err != nil {
			return err
		}
		if !found {
			return ErrBucketNotFound
		}
		if err := parent.setRoot(newRoot); err != nil {
			return err
		}
	}

	// pages is the complete checked physical ownership set, including continuations.
	// Retire each ID once; FreePage would also retire an owning node's continuations.
	for _, pageID := range pages {
		if err := tx.writableAllocation().retire(pageID); err != nil {
			return err
		}
		delete(tx.store.dirty, pageID)
	}
	target.deleted = true
	if parent == nil {
		if tx.bucket == target {
			tx.bucket = nil
		}
		delete(tx.buckets, string(bucketName))
	} else {
		if parent.child == target {
			parent.child = nil
		}
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
	addPage := func(pageID page.ID) error {
		if _, exists := seen[pageID]; exists {
			return fmt.Errorf("bucket page %d has more than one owner: %w", pageID, ErrInvalid)
		}
		if pageID < firstTreePageID || allocation.isSegmentPage(pageID) || !allocation.allocated(pageID) {
			return fmt.Errorf("bucket page %d is not allocated tree data: %w", pageID, ErrInvalid)
		}
		if allocation.isRetired(pageID) {
			return fmt.Errorf("bucket page %d is already retired: %w", pageID, ErrInvalid)
		}
		seen[pageID] = struct{}{}
		pages = append(pages, pageID)
		return nil
	}
	for len(stack) != 0 {
		last := len(stack) - 1
		pageID := stack[last]
		stack = stack[:last]
		if err := addPage(pageID); err != nil {
			return nil, err
		}
		if err := tx.store.VisitNodeReferences(pageID, func(child page.ID) error {
			stack = append(stack, child)
			return nil
		}, addPage); err != nil {
			return nil, err
		}
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
	// Get uses only the value and flags. Skip the key so a continued key is not copied.
	entry, found, err := bucket.findEntry(key, false)
	if err != nil {
		return nil, err
	}
	return valueFromEntry(entry, found)
}

// GetBatch returns the values for keys from this bucket, in the input order.
// It sees earlier writes in the owning transaction. A missing key has a nil result; a stored empty value has a non-nil result of length zero.
// Empty input returns an empty result slice. Invalid keys and nested-bucket names return the same errors as [Bucket.Get], with no results.
//
// Keep keys and their bytes unchanged until GetBatch returns; the function does not retain them or change their order.
// Returned value bytes are read-only and valid until the transaction callback returns, as with Get. Repeated keys can share the same value bytes.
func (bucket *Bucket) GetBatch(keys [][]byte) ([][]byte, error) {
	if err := bucket.liveError(); err != nil {
		return nil, err
	}
	for _, key := range keys {
		if len(key) == 0 {
			return nil, ErrKeyRequired
		}
		if len(key) > MaxKeySize {
			return nil, ErrKeyTooLarge
		}
	}
	values := make([][]byte, len(keys))
	// One or two reads do not repay sorting and page grouping.
	if len(keys) <= 2 {
		for i, key := range keys {
			value, err := bucket.Get(key)
			if err == ErrKeyNotFound {
				continue
			}
			if err != nil {
				return nil, err
			}
			values[i] = value
		}
		return values, nil
	}
	// Sort positions rather than caller keys, then place each result back in its original slot.
	order := make([]int, len(keys))
	for i := range order {
		order[i] = i
	}
	if !slices.IsSortedFunc(keys, bytes.Compare) {
		slices.SortFunc(order, func(a, b int) int { return bytes.Compare(keys[a], keys[b]) })
	}
	if err := btree.ReadBatch(bucket.tx.nodeReader(), bucket.rootPageID, keys, order, values); err != nil {
		return nil, err
	}
	return values, nil
}

// findEntry finds key in bucket through the transaction's view of the tree.
// includeKey has the same meaning as in [btree.Tree.FindEntryRef].
func (bucket *Bucket) findEntry(key []byte, includeKey bool) (btree.Entry, bool, error) {
	return bucket.tx.tree.FindEntryRef(bucket.rootPageID, key, includeKey)
}
