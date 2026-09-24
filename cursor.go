package kvlite

import (
	"bytes"
	"errors"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
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
	return bucket.ScanRange(prefix, prefixRangeEnd(prefix), fn)
}

// scanMappedTreeRange visits entries in the half-open range [start, end) from the main database file.
// It reads leaf entries from page bytes and decodes only branch pages. The caller must first prove that committed WAL pages cannot replace pages in the main file.
// The key and value passed to visit refer to database page bytes. They remain valid while the current database operation holds its read lock.
func (db *DB) scanMappedTreeRange(pageID page.ID, start, end []byte, visit func(uint32, []byte, []byte) error) error {
	data, err := db.readMainPage(pageID)
	if err != nil {
		return err
	}
	err = btree.VisitMappedLeafRange(data, pageID, start, end, visit)
	if err == nil || !errors.Is(err, btree.ErrNotLeafNode) {
		return err
	}

	// A branch contains child page IDs instead of values. Decode only the branch so the scan can select its children.
	branch, err := db.readNode(pageID)
	if err != nil {
		return err
	}
	firstChild := 0
	if len(start) > 0 {
		// Branch separators let the scan skip every child whose entries sort before start.
		firstChild, err = branch.FindChildIndex(start)
		if err != nil {
			return err
		}
	}
	for childIndex := firstChild; childIndex < len(branch.Children); childIndex++ {
		// The separator before a child is its lower bound. This child and all later children are outside the range when that separator is not less than end.
		if len(end) > 0 && childIndex > 0 && bytes.Compare(branch.EntryAt(childIndex-1).Key(), end) >= 0 {
			break
		}
		if err := db.scanMappedTreeRange(branch.Children[childIndex], start, end, visit); err != nil {
			return err
		}
	}
	return nil
}

// prefixRangeEnd returns the smallest exclusive end for a prefix scan. For example, "acct/" ends at "acct0".
// It returns nil when no finite end exists, such as for an empty prefix or a prefix that contains only 0xff bytes. It does not change prefix.
func prefixRangeEnd(prefix []byte) []byte {
	end := bytes.Clone(prefix)
	for index := len(end) - 1; index >= 0; index-- {
		if end[index] == 0xff {
			continue
		}
		end[index]++
		return end[:index+1]
	}
	return nil
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
	// Mapped pages do not include newer page images from the WAL. Use the mapped scan only when the read-only view has no committed WAL overlay.
	if bucket.tx.readOnly && bucket.tx.db.wal.Stats().CommittedRecordCount == 0 {
		rootPageID := bucket.rootPageID
		if bucket.rootNode != nil {
			// A read-only bucket clears rootPageID after it loads rootNode. The loaded node keeps the same page ID.
			rootPageID = bucket.rootNode.PageID()
		}
		return bucket.tx.db.scanMappedTreeRange(rootPageID, start, end, func(flags uint32, key, value []byte) error {
			// Preserve the public scan result rules. A nested bucket has a nil value. An empty stored value has a non-nil empty value.
			if flags&btree.BucketLeafFlag != 0 {
				return fn(key, nil)
			}
			if value == nil {
				value = []byte{}
			}
			return fn(key, value)
		})
	}
	// The cursor reads the WAL overlay and transaction changes. Keep it as the general path when the mapped main file is not the complete view.
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
