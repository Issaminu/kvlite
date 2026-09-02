package kvlite

import (
	"bytes"

	"github.com/Issaminu/kvlite/internal/btree"
)

// Cursor reads the entries in one bucket in key order. An entry is either a key/value pair or the name of a nested bucket. Use [Cursor.First], [Cursor.Last], or [Cursor.Seek] to select a starting entry. Then use [Cursor.Next] or [Cursor.Prev] to move one entry at a time.
//
// A Cursor belongs to its bucket's transaction. Do not use it after the transaction callback returns. Do not copy it or share it with another goroutine.
//
// The returned keys and values are read-only. They are valid only until the transaction ends. Use [bytes.Clone] to keep them longer.
//
// A nested bucket is returned with a nil value. A stored empty value has a non-nil value with length zero.
//
// Do not change the bucket while a cursor is in use. A change makes the saved cursor position out of date. Later movement returns [ErrCursorInvalidated]. Create a new cursor after the change.
type Cursor struct {
	bucket        *Bucket
	treeCursor    btree.Cursor
	bucketVersion uint64
}

// Cursor creates a cursor for bucket. A new cursor has no current entry. Call [Cursor.First], [Cursor.Last], or [Cursor.Seek] before [Cursor.Next] or [Cursor.Prev].
func (bucket *Bucket) Cursor() (*Cursor, error) {
	if bucket.tx.closed {
		return nil, ErrTxClosed
	}
	if err := bucket.loadRootNode(); err != nil {
		return nil, err
	}
	return &Cursor{
		bucket:        bucket,
		treeCursor:    *bucket.tx.tree.Cursor(bucket.rootNode),
		bucketVersion: bucket.treeVersion,
	}, nil
}

// First moves to the entry with the smallest key. It returns a nil key and value when the bucket is empty.
func (cursor *Cursor) First() ([]byte, []byte, error) {
	if err := cursor.checkValid(); err != nil {
		return nil, nil, err
	}
	return cursor.result(cursor.treeCursor.First())
}

// Last moves to the entry with the largest key. It returns a nil key and value when the bucket is empty.
func (cursor *Cursor) Last() ([]byte, []byte, error) {
	if err := cursor.checkValid(); err != nil {
		return nil, nil, err
	}
	return cursor.result(cursor.treeCursor.Last())
}

// Seek moves to the first key that is equal to target or sorts after it. A nil or empty target has the same result as [Cursor.First]. Seek returns a nil key and value when no matching entry remains.
func (cursor *Cursor) Seek(target []byte) ([]byte, []byte, error) {
	if err := cursor.checkValid(); err != nil {
		return nil, nil, err
	}
	return cursor.result(cursor.treeCursor.Seek(target))
}

// Next moves to the next entry in key order. It returns a nil key and value when the cursor has no current entry or is at the end of the bucket.
func (cursor *Cursor) Next() ([]byte, []byte, error) {
	if err := cursor.checkValid(); err != nil {
		return nil, nil, err
	}
	return cursor.result(cursor.treeCursor.Next())
}

// Prev moves to the previous entry in key order. It returns a nil key and value when the cursor has no current entry or is at the start of the bucket.
func (cursor *Cursor) Prev() ([]byte, []byte, error) {
	if err := cursor.checkValid(); err != nil {
		return nil, nil, err
	}
	return cursor.result(cursor.treeCursor.Prev())
}

func (cursor *Cursor) checkValid() error {
	if cursor.bucket.tx.closed {
		return ErrTxClosed
	}
	if cursor.bucketVersion != cursor.bucket.treeVersion {
		return ErrCursorInvalidated
	}
	return nil
}

func (cursor *Cursor) result(entry *btree.Entry, found bool, err error) ([]byte, []byte, error) {
	if err != nil || !found {
		return nil, nil, err
	}
	if entry.Flags()&btree.BucketLeafFlag != 0 {
		return entry.Key(), nil, nil
	}
	value := entry.Value()
	if value == nil {
		value = []byte{}
	}
	return entry.Key(), value, nil
}

// ScanPrefix reads matching entries without changing the bucket.
//
// A key matches when it starts with prefix. For example, the prefix "user/" matches "user/1" and "user/settings". It does not match "admin/1".
//
// ScanPrefix calls fn once for each match, from the smallest key to the largest. A nil or empty prefix visits every entry in the bucket.
//
// ScanPrefix uses the transaction that owns bucket. In a [DB.Update] callback, the scan can read values written earlier in that callback. Do not change this bucket until ScanPrefix returns because a change makes the scan position out of date.
//
// The key and value passed to fn are read-only and belong to the current transaction. Use [bytes.Clone] to keep them after the transaction ends.
//
// If fn returns an error, ScanPrefix stops and returns that error. A nil fn returns [ErrScanCallbackRequired].
func (bucket *Bucket) ScanPrefix(prefix []byte, fn func(key, value []byte) error) error {
	if bucket.tx.closed {
		return ErrTxClosed
	}
	if fn == nil {
		return ErrScanCallbackRequired
	}
	cursor, err := bucket.Cursor()
	if err != nil {
		return err
	}
	key, value, err := cursor.Seek(prefix)
	for err == nil && key != nil && bytes.HasPrefix(key, prefix) {
		if err := fn(key, value); err != nil {
			return err
		}
		key, value, err = cursor.Next()
	}
	return err
}

// ScanRange reads entries without changing the bucket.
//
// ScanRange starts at start and stops before end. The start key is included when it exists. The end key is never included.
//
// A nil or empty start begins at the smallest key. A nil or empty end continues through the largest key. ScanRange visits nothing when a non-empty end sorts before start or is equal to start.
//
// ScanRange calls fn once for each match, from the smallest key to the largest.
//
// ScanRange uses the transaction that owns bucket. In a [DB.Update] callback, the scan can read values written earlier in that callback. Do not change this bucket until ScanRange returns because a change makes the scan position out of date.
//
// The key and value passed to fn are read-only and belong to the current transaction. Use [bytes.Clone] to keep them after the transaction ends.
//
// If fn returns an error, ScanRange stops and returns that error. A nil fn returns [ErrScanCallbackRequired].
func (bucket *Bucket) ScanRange(start, end []byte, fn func(key, value []byte) error) error {
	if bucket.tx.closed {
		return ErrTxClosed
	}
	if fn == nil {
		return ErrScanCallbackRequired
	}
	if len(end) > 0 && bytes.Compare(start, end) >= 0 {
		return nil
	}
	cursor, err := bucket.Cursor()
	if err != nil {
		return err
	}
	key, value, err := cursor.Seek(start)
	for err == nil && key != nil && (len(end) == 0 || bytes.Compare(key, end) < 0) {
		if err := fn(key, value); err != nil {
			return err
		}
		key, value, err = cursor.Next()
	}
	return err
}
