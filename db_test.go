package kvlite

// Some tests here are adapted from etcd-io/bbolt
// (https://github.com/etcd-io/bbolt), MIT License, Copyright (c) 2013 Ben Johnson.
//
// This suite covers the database file, metadata, WAL, transactions, buckets, locks, and recovery.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Issaminu/kvlite/internal/blackout/disk"
	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

var testBucketName = []byte("test-data")
var errDiscardTx = errors.New("discard transaction")

func TestContinuedMainFileReads(t *testing.T) {
	for _, testCase := range []struct {
		mapped, fromWAL bool
	}{{true, false}, {false, false}, {true, true}, {false, true}} {
		mapped, fromWAL := testCase.mapped, testCase.fromWAL
		t.Run(fmt.Sprintf("mapped=%t/WAL=%t", mapped, fromWAL), func(t *testing.T) {
			path, bucketName, entries, _ := writeContinuedReadDatabase(t, false)
			db, err := Open(path, 0600, &Options{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if fromWAL {
				// WAL values differ from the main file. Get must use the WAL tree.
				for _, id := range []page.ID{db.meta.Root(), 4, 5, 6} {
					data, err := db.readMainPage(id)
					if err != nil {
						t.Fatal(err)
					}
					node, err := btree.DecodeNode(bytes.Clone(data), id, db.meta.PageSize(), db.readMainPage, db.meta.DataPageChecksums())
					if err != nil {
						t.Fatal(err)
					}
					if id == 5 || id == 6 {
						for index := 0; index < node.EntryCount(); index++ {
							node.EntryAt(index).Value()[0] ^= 1
						}
					}
					ids := append([]page.ID{id}, node.OverflowPages()...)
					images, err := btree.EncodeWALNodePages(node, db.meta.PageSize(), ids)
					if err != nil {
						t.Fatal(err)
					}
					for index, pageID := range ids {
						db.wal.LoadCommittedRecord(wal.WALRecord{Header: wal.RecordHeader{Type: wal.RecordTypeNode, PageID: pageID}, Payload: images[index]})
					}
				}
				for _, entry := range entries {
					entry.Value()[0] ^= 1
				}
			}
			if !mapped {
				if err := db.unmapMainFile(); err != nil {
					t.Fatal(err)
				}
			}
			for _, entry := range entries {
				value, err := db.Get(bucketName, entry.Key())
				if err != nil || !bytes.Equal(value, entry.Value()) {
					t.Fatalf("Get(%q): got %q, %v", entry.Key(), value, err)
				}
				value[0] ^= 1
				again, err := db.Get(bucketName, entry.Key())
				if err != nil || !bytes.Equal(again, entry.Value()) {
					t.Fatalf("Get result changed stored bytes: %v", err)
				}
			}
			if _, err := db.Get(bucketName, []byte("missing")); !errors.Is(err, ErrKeyNotFound) {
				t.Fatalf("missing key: %v", err)
			}
			// Point reads must not decode the bucket's branch or leaves.
			if len(db.writeNodes) != 1 {
				t.Fatalf("point reads cached %d nodes, want only the catalog root", len(db.writeNodes))
			}
			if err := db.View(func(tx *Tx) error {
				bucket := mustBucket(t, tx, bucketName)
				for _, entry := range entries {
					value, err := bucket.Get(entry.Key())
					if err != nil || !bytes.Equal(value, entry.Value()) {
						t.Fatalf("Bucket.Get(%q): %v", entry.Key(), err)
					}
				}
				cursor, err := bucket.Cursor()
				if err != nil {
					return err
				}
				key, value, err := cursor.First()
				for _, entry := range entries {
					if err != nil || !bytes.Equal(key, entry.Key()) || !bytes.Equal(value, entry.Value()) {
						t.Fatalf("forward cursor: got %q, %v", key, err)
					}
					key, value, err = cursor.Next()
				}
				if err != nil || key != nil {
					t.Fatalf("cursor end: got %q, %v", key, err)
				}
				key, value, err = cursor.Last()
				for index := len(entries) - 1; index >= 0; index-- {
					entry := entries[index]
					if err != nil || !bytes.Equal(key, entry.Key()) || !bytes.Equal(value, entry.Value()) {
						t.Fatalf("reverse cursor: got %q, %v", key, err)
					}
					key, value, err = cursor.Prev()
				}
				if err != nil || key != nil {
					t.Fatalf("cursor start: got %q, %v", key, err)
				}
				key, value, err = cursor.Seek(entries[1].Key())
				if err != nil || !bytes.Equal(key, entries[1].Key()) || !bytes.Equal(value, entries[1].Value()) {
					t.Fatalf("cursor seek: got %q, %v", key, err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestContinuedMainFileReadsRejectDamagedPage(t *testing.T) {
	path, bucketName, entries, continuationID := writeContinuedReadDatabase(t, true)
	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	// Change body bytes without updating the continuation page's checksum.
	if _, err := file.WriteAt([]byte{0xff}, int64(continuationID)*64+63); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Get(bucketName, entries[1].Key()); !errors.Is(err, ErrChecksum) {
		t.Fatalf("damaged mapped value: got %v, want checksum error", err)
	}
	if err := db.View(func(tx *Tx) error {
		cursor, err := mustBucket(t, tx, bucketName).Cursor()
		if err != nil {
			return err
		}
		_, _, err = cursor.Last()
		return err
	}); !errors.Is(err, ErrChecksum) {
		t.Fatalf("damaged decoded leaf: got %v, want checksum error", err)
	}
	for _, reverse := range []bool{false, true} {
		err := db.View(func(tx *Tx) error {
			bucket, err := tx.Bucket(bucketName)
			if err != nil {
				return err
			}
			visit := func(_, _ []byte) error { return nil }
			if reverse {
				return bucket.ScanReverse(visit)
			}
			return bucket.ScanRange(nil, nil, visit)
		})
		if !errors.Is(err, ErrChecksum) {
			t.Fatalf("damaged scan, reverse=%t: got %v, want checksum error", reverse, err)
		}
	}
}

// writeContinuedReadDatabase writes a catalog, a branch, and two leaves.
// The catalog, branch, and right leaf need continuation pages. The small
// pages force several ID groups. Storage IDs have gaps and change direction.
// This fixture tests reads independently of the pending insertion/WAL work.
func writeContinuedReadDatabase(t *testing.T, checksums bool) (string, []byte, []btree.Entry, page.ID) {
	t.Helper()
	const pageSize int64 = 64
	path := filepath.Join(t.TempDir(), "continued.db")
	bucketName := bytes.Repeat([]byte("b"), 128)
	entries := []btree.Entry{
		btree.NewEntry(0, []byte("a"), []byte("left")),
		btree.NewEntry(0, bytes.Repeat([]byte("m"), 128), bytes.Repeat([]byte("v"), 1500)),
		btree.NewEntry(0, []byte("z"), []byte("last")),
	}
	meta := page.NewMeta(pageSize)
	meta.SetDataPageChecksums(checksums)
	catalog := btree.NewLeafNode(meta.Root())
	left, right := btree.NewLeafNode(5), btree.NewLeafNode(6)
	for index, entry := range entries {
		node := right
		if index == 0 {
			node = left
		}
		if err := node.InsertEntry(entry); err != nil {
			t.Fatal(err)
		}
	}
	branch := btree.NewRootNode(4, left, right, entries[1].Key())
	if err := catalog.InsertEntry(btree.NewEntry(btree.BucketLeafFlag, bucketName, page.EncodeID(branch.PageID()))); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	write := func(id page.ID, data []byte) {
		if _, err := file.WriteAt(data, int64(id)*pageSize); err != nil {
			t.Fatal(err)
		}
	}
	allocation := newAllocationBitmap(pageSize)
	nextID := page.ID(7)
	var damagedPageID page.ID
	for _, node := range []*btree.Node{catalog, branch, left, right} {
		ids := make([]page.ID, btree.NodePageCount(node, pageSize))
		ids[0] = node.PageID()
		for index := 1; index < len(ids); index++ {
			ids[index] = nextID
			nextID = 7 + (nextID-7+73)%300
		}
		var encoded [][]byte
		var err error
		if checksums {
			encoded, err = btree.EncodeNodePages(node, pageSize, ids)
		} else {
			encoded, err = btree.EncodeWALNodePages(node, pageSize, ids)
		}
		if err != nil {
			t.Fatal(err)
		}
		for index, id := range ids {
			data := encoded[index]
			if len(data) < int(pageSize) {
				padded := make([]byte, pageSize)
				copy(padded, data)
				data = padded
			}
			write(id, data)
			allocation.markAllocated(id)
			meta.SetLastPage(max(meta.LastPage(), id))
		}
		if node == right {
			damagedPageID = ids[len(ids)-1]
		}
	}
	write(firstAllocationSegmentID, allocation.encodeSegment(0))
	meta.RefreshChecksum()
	for _, id := range []page.ID{page.Meta0ID, page.Meta1ID} {
		write(id, page.EncodeMeta(meta))
	}
	return path, bucketName, entries, damagedPageID
}

func TestMakeWALCommitData_DoesNotCalculateDatabasePageChecksum(t *testing.T) {
	meta := page.NewMeta(4096)
	node := btree.NewLeafNode(1)
	if err := node.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("value"))); err != nil {
		t.Fatal(err)
	}

	nodes, encodedMeta := makeWALCommitData(map[page.ID]dirtyNode{node.PageID(): {final: node}}, meta, false)
	if len(nodes) != 1 || encodedMeta != nil {
		t.Fatalf("WAL inputs: got %d node records and %d metadata bytes, want 1 and 0", len(nodes), len(encodedMeta))
	}
	if nodes[0].Original != nil || nodes[0].Final != node {
		t.Fatal("node record did not keep the final node for direct encoding")
	}
	payload := btree.EncodeWALNode(nodes[0].Final)
	if got := binary.LittleEndian.Uint32(payload[btree.NodeHeaderSize-4 : btree.NodeHeaderSize]); got != 0 {
		t.Fatalf("WAL node page checksum: got %x, want zero", got)
	}
}

func TestMakeWALCommitData_EncodesChangedMetadata(t *testing.T) {
	meta := page.NewMeta(4096)
	nodes, encodedMeta := makeWALCommitData(nil, meta, true)
	if len(nodes) != 0 || len(encodedMeta) == 0 {
		t.Fatalf("WAL inputs: got %d node records and %d metadata bytes, want 0 and encoded metadata", len(nodes), len(encodedMeta))
	}

	decoded, err := page.DecodeMeta(encodedMeta)
	if err != nil {
		t.Fatal(err)
	}
	if err := decoded.Validate(); err != nil {
		t.Fatalf("validate encoded metadata: %v", err)
	}
	if got := decoded.Generation(); got != 1 {
		t.Fatalf("metadata generation: got %d, want 1", got)
	}
}

func TestWriteTx_PreservesOriginalNodeAcrossWriteBatchCallbacks(t *testing.T) {
	meta := page.NewMeta(4096)
	root := btree.NewLeafNode(meta.Root())
	db := &DB{meta: meta, rootNode: root}
	base := btree.NewLeafNode(10)
	if err := base.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("old"))); err != nil {
		t.Fatal(err)
	}

	first := newWriteTx(db, meta, root, nil, nil)
	middle := first.store.WritableNode(base)
	if err := middle.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("mid"))); err != nil {
		t.Fatal(err)
	}

	second := newWriteTx(db, first.meta, first.rootNode, first.store.dirty, nil)
	middle, err := second.store.ReadNode(base.PageID())
	if err != nil {
		t.Fatal(err)
	}
	latest := second.store.WritableNode(middle)
	if err := latest.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("new"))); err != nil {
		t.Fatal(err)
	}

	nodes, encodedMeta := makeWALCommitData(second.store.dirty, second.meta, second.metaDirty)
	if len(nodes) != 1 || encodedMeta != nil {
		t.Fatalf("WAL inputs: got %d node records and %d metadata bytes, want 1 and 0", len(nodes), len(encodedMeta))
	}
	if nodes[0].Original != base || nodes[0].Final != latest {
		t.Fatal("node record did not preserve the original and final nodes")
	}
}

func TestOpen_RecoversBytePatch(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("key"), []byte("old-value")); err != nil {
		t.Fatal(err)
	}
	db.operationMu.Lock()
	if err := db.checkpointWAL(); err != nil {
		db.operationMu.Unlock()
		t.Fatal(err)
	}
	db.operationMu.Unlock()
	if err := db.Put(testBucketName, []byte("key"), []byte("mid-value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("key"), []byte("new-value")); err != nil {
		t.Fatal(err)
	}
	records, err := db.wal.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	foundPatch := false
	for _, record := range records {
		foundPatch = foundPatch || record.Header.Type == wal.RecordTypeBytePatch
	}
	if !foundPatch {
		t.Fatal("update did not write a byte patch")
	}
	if err := db.unmapMainFile(); err != nil {
		t.Fatal(err)
	}
	if err := db.closeFiles(); err != nil {
		t.Fatal(err)
	}

	readOnly, err := Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	got, err := readOnly.Get(testBucketName, []byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("new-value")) {
		t.Fatalf("read-only recovered value: got %q, want %q", got, "new-value")
	}
	if err := readOnly.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err = reopened.Get(testBucketName, []byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("new-value")) {
		t.Fatalf("recovered value: got %q, want %q", got, "new-value")
	}
}

func TestReadNode_ReusesOwnedCommittedNodeAfterMappingRefresh(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	if err := db.checkpointWAL(); err != nil {
		t.Fatal(err)
	}
	clear(db.writeNodes)
	db.writeNodeSlots = nil
	db.nextWriteNodeSlot = 0

	first, err := db.readNode(db.meta.Root())
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.readNode(db.meta.Root())
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("second write-page read decoded a new node")
	}
	if err := db.refreshMainFileMapping(); err != nil {
		t.Fatal(err)
	}
	if first.EntryCount() != second.EntryCount() {
		t.Fatal("cached node changed after mapping refresh")
	}
}

func TestWriteNodeCache_BoundsPagesAndGivesReferencesAnotherChance(t *testing.T) {
	db := &DB{writeNodes: make(map[page.ID]*writeNodeCacheEntry)}
	for pageID := page.ID(1); pageID <= maxCachedWriteNodePages; pageID++ {
		db.cacheWriteNode(btree.NewLeafNode(pageID))
	}

	db.cacheWriteNode(btree.NewLeafNode(maxCachedWriteNodePages + 1))
	if _, ok := db.writeNodes[1]; ok {
		t.Fatal("first CLOCK candidate remains cached")
	}
	if _, ok := db.cachedWriteNode(2); !ok {
		t.Fatal("referenced node is not cached")
	}
	db.cacheWriteNode(btree.NewLeafNode(maxCachedWriteNodePages + 2))

	if _, ok := db.writeNodes[2]; !ok {
		t.Fatal("referenced node was evicted")
	}
	if _, ok := db.writeNodes[3]; ok {
		t.Fatal("unreferenced node remains cached")
	}
	if got := len(db.writeNodes); got != maxCachedWriteNodePages {
		t.Fatalf("cached write pages: got %d, want %d", got, maxCachedWriteNodePages)
	}
}

// tempfile returns a temporary file path for a database.
func tempfile() string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("kvlite-%d.db", time.Now().UnixNano()))
}

// openDB opens a test database in SyncNone mode with the default checksum policy.
// Tests that require synchronized storage use Open with the default SyncFull mode.
func openDB(path string) (*DB, error) {
	return openDBWithChecksums(path, false)
}

// openDBWithChecksums selects data-page protection when creating a test file.
// Existing files retain their stored mode.
func openDBWithChecksums(path string, enabled bool) (*DB, error) {
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone, DataPageChecksums: enabled})
	if err != nil {
		return nil, err
	}

	err = db.View(func(tx *Tx) error {
		_, err := tx.Bucket(testBucketName)
		return err
	})
	if err == nil {
		return db, nil
	}
	if !errors.Is(err, ErrBucketNotFound) {
		_ = db.Close()
		return nil, err
	}

	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket(testBucketName)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// fileSize returns the current size of the database file in bytes.
func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

func writeEmptyDatabaseWithPageSize(t *testing.T, path string, pageSize int64) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	meta := page.NewMeta(pageSize)
	meta.RefreshChecksum()
	encodedMeta := page.EncodeMeta(meta)
	for _, pageID := range [...]page.ID{page.Meta0ID, page.Meta1ID} {
		if _, err := file.WriteAt(encodedMeta, int64(pageID)*pageSize); err != nil {
			t.Fatal(err)
		}
	}
	allocation := newAllocationBitmap(pageSize)
	if _, err := file.WriteAt(allocation.encodeSegment(0), int64(firstAllocationSegmentID)*pageSize); err != nil {
		t.Fatal(err)
	}
	root := btree.NewLeafNode(meta.Root())
	if err := btree.WriteNode(io.NewOffsetWriter(file, int64(root.PageID())*pageSize), root, pageSize, true, false); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
}

func createBucket(tb testing.TB, db *DB, name []byte) {
	tb.Helper()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket(name)
		return err
	}); err != nil {
		tb.Fatal(err)
	}
}

func mustBucket(t *testing.T, tx *Tx, name []byte) *Bucket {
	t.Helper()
	bucket, err := tx.Bucket(name)
	if err != nil {
		t.Fatal(err)
	}
	return bucket
}

func mustNestedBucket(t *testing.T, parent *Bucket, name []byte) *Bucket {
	t.Helper()
	bucket, err := parent.Bucket(name)
	if err != nil {
		t.Fatal(err)
	}
	return bucket
}

func mustBucketValue(t *testing.T, bucket *Bucket, key []byte) []byte {
	t.Helper()
	value, err := bucket.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func mustBucketRoot(t *testing.T, db *DB, name []byte) *btree.Node {
	t.Helper()
	var root *btree.Node
	if err := db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(name)
		if err != nil {
			return err
		}
		if err := bucket.loadRootNode(); err != nil {
			return err
		}
		root = bucket.rootNode
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestBucketGet_CachedLeafDoesNotAllocate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("key")
	if err := db.Put(testBucketName, key, []byte("value")); err != nil {
		t.Fatal(err)
	}

	err = db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		if _, err := bucket.Get(key); err != nil {
			return err
		}

		var getErr error
		allocations := testing.AllocsPerRun(100, func() {
			_, getErr = bucket.Get(key)
		})
		if getErr != nil {
			return getErr
		}
		if allocations != 0 {
			t.Fatalf("cached Bucket.Get allocations: got %v, want 0", allocations)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBucketGet_ReadOnlyBranchDoesNotAllocate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var key []byte
	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		value := bytes.Repeat([]byte("v"), int(tx.meta.PageSize()/3))
		for index := 0; bucket.rootNode.IsLeaf(); index++ {
			key = fmt.Appendf(nil, "key-%04d", index)
			if err := bucket.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.checkpointWAL(); err != nil {
		t.Fatal(err)
	}

	err = db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}

		var getErr error
		allocations := testing.AllocsPerRun(100, func() {
			_, getErr = bucket.Get(key)
		})
		if getErr != nil {
			return getErr
		}
		if allocations != 0 {
			t.Fatalf("read-only branch Bucket.Get allocations: got %v, want 0", allocations)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTxBucketKeepsMultipleTopLevelHandlesStable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("other"))
		return err
	}); err != nil {
		t.Fatal(err)
	}

	err = db.View(func(tx *Tx) error {
		first := mustBucket(t, tx, testBucketName)
		other := mustBucket(t, tx, []byte("other"))
		if first != mustBucket(t, tx, testBucketName) {
			t.Fatal("repeated lookup returned a different first bucket handle")
		}
		if other != mustBucket(t, tx, []byte("other")) {
			t.Fatal("repeated lookup returned a different second bucket handle")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestViewOneBucketLookupUsesAtMostThreeAllocations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var viewErr error
	allocations := testing.AllocsPerRun(100, func() {
		viewErr = db.View(func(tx *Tx) error {
			_, err := tx.Bucket(testBucketName)
			return err
		})
	})
	if viewErr != nil {
		t.Fatal(viewErr)
	}
	if allocations > 3 {
		t.Fatalf("one-bucket View allocations: got %v, want at most 3", allocations)
	}
}

// TestBucketPut_ReusesTransactionOwnedRoot catches a write path that copies an
// already private leaf again for each Put in the same transaction.
func TestBucketPut_ReusesTransactionOwnedRoot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *Tx) error {
		bucket := mustBucket(t, tx, testBucketName)
		committedRoot := bucket.rootNode

		if err := bucket.Put([]byte("first"), []byte("value")); err != nil {
			return err
		}
		privateRoot := bucket.rootNode
		if privateRoot == committedRoot {
			t.Fatal("first write did not give the transaction a private root")
		}

		if err := bucket.Put([]byte("second"), []byte("value")); err != nil {
			return err
		}
		if bucket.rootNode != privateRoot {
			t.Fatal("second write copied the transaction-owned root again")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestBucketPut_RootSplitUpdatesCatalog catches a bucket root split that does
// not record the replacement root page in the catalog.
func TestBucketPut_RootSplitUpdatesCatalog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *Tx) error {
		bucket := mustBucket(t, tx, testBucketName)
		rootPageID := bucket.rootNode.PageID()
		value := bytes.Repeat([]byte("v"), int(tx.meta.PageSize()/3))

		for index := 0; bucket.rootNode.IsLeaf(); index++ {
			key := fmt.Appendf(nil, "key-%04d", index)
			if err := bucket.Put(key, value); err != nil {
				return err
			}
		}

		if got := bucket.rootNode.PageID(); got == rootPageID {
			t.Fatalf("bucket root page ID after split: got old page %d, want a new page", got)
		}
		entry, found, err := tx.tree.FindEntryRef(tx.rootNode, testBucketName, true)
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("catalog lost bucket after its root split")
		}
		catalogRoot, err := page.DecodeID(entry.Value())
		if err != nil {
			return err
		}
		if got, want := catalogRoot, bucket.rootNode.PageID(); got != want {
			t.Fatalf("catalog root page ID: got %d, want %d", got, want)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBucketPut_RejectedEntryLeavesTransactionUsable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket(testBucketName)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	rejectedKey := bytes.Repeat([]byte("k"), MaxKeySize+1)
	rejectedValue := []byte("value")
	err = db.Update(func(tx *Tx) error {
		bucket := mustBucket(t, tx, testBucketName)
		beforeMeta := page.EncodeMeta(tx.meta)

		if err := bucket.Put(rejectedKey, rejectedValue); !errors.Is(err, ErrKeyTooLarge) {
			t.Fatalf("oversized Put: got %v, want ErrKeyTooLarge", err)
		}
		if afterMeta := page.EncodeMeta(tx.meta); !bytes.Equal(afterMeta, beforeMeta) {
			t.Fatal("rejected Put changed transaction metadata")
		}

		return bucket.Put([]byte("good"), []byte("value"))
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := db.Get(testBucketName, rejectedKey); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("rejected key lookup: got %v, want ErrKeyTooLarge", err)
	}
	if got, err := db.Get(testBucketName, []byte("good")); err != nil || !bytes.Equal(got, []byte("value")) {
		t.Fatalf("valid write after rejected Put: got %q, err %v", got, err)
	}
}

// TestPut_Overwrite_BoundedGrowth checks that repeated writes to one key do not grow the main file without a bound.
func TestPut_Overwrite_BoundedGrowth(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put(testBucketName, []byte("a"), []byte("first")); err != nil {
		t.Fatal(err)
	}
	small := fileSize(t, path)

	for i := 0; i < 1000; i++ {
		if err := db.Put(testBucketName, []byte("a"), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	big := fileSize(t, path)

	if big > small*4 {
		t.Fatalf("file grew unboundedly overwriting one key 1000 times: %d -> %d bytes", small, big)
	}
}

// TestPutGet_Stress checks reads after keys arrive in reverse order and some values change.
// It checks all values again after reopen.
func TestPutGet_Stress(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}

	const n = 500
	want := make(map[string]string, n)

	// Insert in descending order (deliberately not sorted-ascending).
	for i := n - 1; i >= 0; i-- {
		k := fmt.Sprintf("key-%04d", i)
		v := fmt.Sprintf("val-%d", i)
		if err := db.Put(testBucketName, []byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
		want[k] = v
	}
	// Overwrite the even-numbered keys.
	for i := 0; i < n; i += 2 {
		k := fmt.Sprintf("key-%04d", i)
		v := fmt.Sprintf("val-%d-updated", i)
		if err := db.Put(testBucketName, []byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
		want[k] = v
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for k, wantV := range want {
		got, err := db.Get(testBucketName, []byte(k))
		if err != nil {
			t.Fatalf("get %q: %v", k, err)
		}
		if string(got) != wantV {
			t.Fatalf("key %q: got %q, want %q", k, got, wantV)
		}
	}
}

// TestFile_SinglePage checks the fixed page layout for a small bucket.
func TestFile_SinglePage(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		if err := db.Put(testBucketName, []byte(kv[0]), []byte(kv[1])); err != nil {
			t.Fatal(err)
		}
	}
	pageBytes := db.meta.PageSize()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Pages 0 and 1 are metadata, page 2 is allocation, page 3 is the bucket catalog, and page 4 is the data leaf.
	if size := fileSize(t, path); size != 5*pageBytes {
		t.Fatalf("a small bucket should occupy exactly five %d-byte pages (two metadata + allocation + catalog + data leaf), got %d bytes "+
			"(nodes must be padded to page boundaries)", pageBytes, size)
	}
}

// TestSplit_TreeGrows checks that a full root leaf becomes a branch with readable children.
func TestSplit_TreeGrows(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 150 x 256B ≈ 38 KB — blows past one page at any OS page size (4 KB or 16 KB).
	val := bytes.Repeat([]byte("x"), 256)
	for i := 0; i < 150; i++ {
		if err := db.Put(testBucketName, fmt.Appendf(nil, "key-%05d", i), val); err != nil {
			t.Fatal(err)
		}
	}

	if mustBucketRoot(t, db, testBucketName).IsLeaf() {
		t.Fatal("root is still a single leaf after about 38 KB of entries")
	}
	if got, err := db.Get(testBucketName, []byte("key-00042")); err != nil || !bytes.Equal(got, val) {
		t.Fatalf("key-00042 unreadable after split: err=%v", err)
	}
}

// TestSplit_SurvivesReopen: after splits, close + reopen must locate the (now
// multi-node) root and descend to read every key back.
func TestSplit_SurvivesReopen(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}

	const n = 150
	val := bytes.Repeat([]byte("y"), 256)
	for i := 0; i < n; i++ {
		if err := db.Put(testBucketName, fmt.Appendf(nil, "key-%05d", i), val); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < n; i++ {
		got, err := db.Get(testBucketName, fmt.Appendf(nil, "key-%05d", i))
		if err != nil {
			t.Fatalf("key-%05d after reopen: %v", i, err)
		}
		if !bytes.Equal(got, val) {
			t.Fatalf("key-%05d: wrong value after reopen", i)
		}
	}
}

// walkNodes visits each node in a tree and checks that each child page can be read.
func walkNodes(t *testing.T, db *DB, root *btree.Node, visit func(n *btree.Node)) {
	t.Helper()
	var rec func(n *btree.Node)
	rec = func(n *btree.Node) {
		visit(n)
		if n.IsLeaf() {
			return
		}
		for _, childPgid := range n.Children {
			child, err := db.readNode(childPgid)
			if err != nil || child == nil {
				t.Fatalf("walk: could not read child pgid %d (broken split wiring?): %v", childPgid, err)
			}
			rec(child)
		}
	}
	rec(root)
}

// TestSplit_RootBecomesBranch checks both sides of a root leaf split.
func TestSplit_RootBecomesBranch(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pageBytes := int(db.meta.PageSize())
	val := bytes.Repeat([]byte("x"), 256)
	// Size the count off the page size so this forces ~one split on any page size.
	entryBytes := 4 + len("key-00000") + 4 + len(val)
	n := pageBytes/entryBytes + pageBytes/entryBytes/2 // ~1.5 pages

	for i := 0; i < n; i++ {
		if err := db.Put(testBucketName, fmt.Appendf(nil, "key-%05d", i), val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	root := mustBucketRoot(t, db, testBucketName)
	if root.IsLeaf() {
		t.Fatal("root is still a leaf after overflowing a page — it must split into a branch root")
	}
	if len(root.Children) < 2 {
		t.Fatalf("branch root must point at >= 2 children, got %d", len(root.Children))
	}
	for _, i := range []int{0, n - 1} { // first and last: neither half may be lost
		k := fmt.Appendf(nil, "key-%05d", i)
		if got, err := db.Get(testBucketName, k); err != nil || !bytes.Equal(got, val) {
			t.Fatalf("key %q lost across split: err=%v", k, err)
		}
	}
}

// TestSplit_EveryNodeFitsInOnePage checks that each reachable node fits in one page.
func TestSplit_EveryNodeFitsInOnePage(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	val := bytes.Repeat([]byte("z"), 256)
	for i := 0; i < 300; i++ {
		if err := db.Put(testBucketName, fmt.Appendf(nil, "key-%05d", i), val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	pageBytes := int(db.meta.PageSize())
	walkNodes(t, db, mustBucketRoot(t, db, testBucketName), func(nd *btree.Node) {
		if sz := nd.EncodedSize(); sz > pageBytes {
			t.Fatalf("node serializes to %d bytes > one %d-byte page — split must keep every node <= a page", sz, pageBytes)
		}
	})

}

// TestSplit_PropagatesToParent: forces enough leaf splits that at least one happens
// on a NON-root leaf, whose separator must propagate up into the existing branch
// root. Proven by reaching >= 3 reachable leaves (walk reads each by pgid) and by
// every key still being readable in-session.
func TestSplit_PropagatesToParent(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const n = 400
	val := bytes.Repeat([]byte("w"), 256)
	for i := 0; i < n; i++ {
		if err := db.Put(testBucketName, fmt.Appendf(nil, "key-%05d", i), val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	if mustBucketRoot(t, db, testBucketName).IsLeaf() {
		t.Fatal("root must be a branch after 400 entries")
	}
	leaves := 0
	walkNodes(t, db, mustBucketRoot(t, db, testBucketName), func(nd *btree.Node) {
		if nd.IsLeaf() {
			leaves++
		}
	})

	if leaves < 3 {
		t.Fatalf("expected >= 3 leaves (a child-leaf split must add one, propagating a separator up), got %d", leaves)
	}
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "key-%05d", i)
		if got, err := db.Get(testBucketName, k); err != nil || !bytes.Equal(got, val) {
			t.Fatalf("key %q unreadable after propagating splits: err=%v", k, err)
		}
	}
}

// TestSplit_Cascades: force the tree to depth 3 — enough leaves that the ROOT BRANCH
// itself overflows a page and must split, minting a new root above two BRANCH children.
// This is the first test that exercises a BRANCH split (not just a leaf split). Large,
// same-size keys bloat the separators so the root branch fills fast.
func TestSplit_Cascades(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}

	pageBytes := int(db.meta.PageSize())
	keySize := pageBytes / 16 // safely < pageSize/2 (no oversized-entry edge), fat enough to fill branches fast
	mkKey := func(i int) []byte {
		k := fmt.Appendf(nil, "key-%08d-", i)
		for len(k) < keySize {
			k = append(k, 'x')
		}
		return k
	}

	const n = 400
	val := []byte("v")
	for i := 0; i < n; i++ {
		if err := db.Put(testBucketName, mkKey(i), val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	// Depth >= 3: root is a branch AND at least one of its children is ALSO a branch
	// (which only happens once the root branch itself has split).
	root := mustBucketRoot(t, db, testBucketName)
	if root.IsLeaf() {
		t.Fatal("root must be a branch")
	}
	child, err := db.readNode(root.Children[0])
	if err != nil || child == nil {
		t.Fatalf("could not read root's first child: %v", err)
	}
	if child.IsLeaf() {
		t.Fatalf("tree only reached depth 2 — %d fat keys should overflow the root branch and force a BRANCH split (depth 3)", n)
	}

	// Every key must still route correctly through the branch-split tree.
	for i := 0; i < n; i++ {
		if got, err := db.Get(testBucketName, mkKey(i)); err != nil || !bytes.Equal(got, val) {
			t.Fatalf("key %d unreadable after a branch split (mis-wired separator?): err=%v", i, err)
		}
	}

	// And survive a reopen.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < n; i++ {
		if got, err := db.Get(testBucketName, mkKey(i)); err != nil || !bytes.Equal(got, val) {
			t.Fatalf("key %d unreadable after reopen: err=%v", i, err)
		}
	}
}

// These WAL lifecycle tests use SyncNone. A commit appends changed pages to the WAL. A clean Close copies those pages into the main file and deletes the WAL. SyncNone does not sync either file.

// TestWAL_WrittenThenCheckpointed: the plumbing. During operation the WAL exists and is
// non-empty; a clean Close checkpoints it away → single file at rest, data intact.
func TestWAL_WrittenThenCheckpointed(t *testing.T) {
	path := tempfile()
	wal := path + "-wal"
	defer os.RemoveAll(path)
	defer os.RemoveAll(wal)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}

	// During operation: the WAL exists and holds the committed change.
	if fi, err := os.Stat(wal); err != nil || fi.Size() == 0 {
		t.Fatalf("expected a non-empty WAL at %s during operation, stat err=%v", wal, err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// After a clean close: WAL checkpointed away → single file at rest.
	if _, err := os.Stat(wal); !os.IsNotExist(err) {
		t.Fatalf("WAL should be gone after a clean close (single file at rest), stat err=%v", err)
	}
	// And the data survived the checkpoint.
	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if v, err := db.Get(testBucketName, []byte("a")); err != nil || !bytes.Equal(v, []byte("1")) {
		t.Fatalf("key a lost after checkpoint+reopen: got %q, err %v", v, err)
	}
}

func TestCheckpoint_TruncateFailureKeepsCommittedWAL(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put(testBucketName, []byte("k"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	walBefore, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}

	if err := db.wal.Close(); err != nil {
		t.Fatal(err)
	}
	readOnlyWAL, err := os.Open(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	db.wal.ReplaceFileForTesting(readOnlyWAL)

	if err := db.checkpointWAL(); err == nil {
		t.Fatal("checkpoint did not report the WAL cleanup failure")
	}
	stats, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	// The main file holds every committed page, so the WAL pages leave memory although the truncate failed.
	if stats.CheckpointFailureCount != 1 || stats.LastCheckpointError == nil || stats.WALPageCount != 0 {
		t.Fatalf("checkpoint failure state: %+v", stats)
	}
	walAfter, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(walAfter, walBefore) {
		t.Fatal("failed WAL cleanup changed the committed WAL")
	}
	if got, err := db.Get(testBucketName, []byte("k")); err != nil || !bytes.Equal(got, []byte("value")) {
		t.Fatalf("committed value missing after WAL cleanup failure: got %q, err %v", got, err)
	}
	if err := readOnlyWAL.Close(); err != nil {
		t.Fatal(err)
	}
	writableWAL, err := os.OpenFile(path+"-wal", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	db.wal.ReplaceFileForTesting(writableWAL)
	if err := db.Close(); err != nil {
		t.Fatalf("retry close after WAL cleanup failure: %v", err)
	}
}

// appliedTruncateFailureFS gives WAL files a Truncate that changes the size and then returns an error once, as an ftruncate that fails after its size change does.
type appliedTruncateFailureFS struct {
	fileio.OSFS
	err    error
	failed bool
}

func (fsys *appliedTruncateFailureFS) OpenFile(name string, flag int, perm os.FileMode) (fileio.File, error) {
	file, err := fsys.OSFS.OpenFile(name, flag, perm)
	if err != nil || !strings.HasSuffix(name, "-wal") {
		return file, err
	}
	return &appliedTruncateFailureFile{File: file, fsys: fsys}, nil
}

type appliedTruncateFailureFile struct {
	fileio.File
	fsys *appliedTruncateFailureFS
}

func (file *appliedTruncateFailureFile) Truncate(size int64) error {
	if err := file.File.Truncate(size); err != nil {
		return err
	}
	if !file.fsys.failed {
		file.fsys.failed = true
		return file.fsys.err
	}
	return nil
}

// A process crash must keep a commit after a checkpoint whose WAL truncate failed but changed the size.
// Blackout found this: the commit went to the old WAL offset without a header, and recovery found no WAL.
func TestCheckpoint_WALTruncateFailureKeepsLaterCommits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	truncateErr := errors.New("truncate failed")
	// The checkpoint of the new database in Open must not fail.
	fsys := &appliedTruncateFailureFS{err: truncateErr, failed: true}
	db, err := open(path, 0600, &Options{CheckpointThresholdBytes: 1 << 40}, fsys)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fsys.failed = false
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket(testBucketName)
		if err != nil {
			return err
		}
		return bucket.Put([]byte("before-checkpoint"), []byte("value"))
	}); err != nil {
		t.Fatal(err)
	}
	db.operationMu.Lock()
	err = db.checkpointWAL()
	db.operationMu.Unlock()
	if !errors.Is(err, truncateErr) {
		t.Fatalf("checkpoint: got %v, want %v", err, truncateErr)
	}
	if err := db.Put(testBucketName, []byte("after-checkpoint"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	// Copy the files as a process crash leaves them, and recover the copy.
	crashPath := filepath.Join(t.TempDir(), "database")
	for _, suffix := range []string{"", "-wal"} {
		data, err := os.ReadFile(path + suffix)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(crashPath+suffix, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	recovered, err := Open(crashPath, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	for _, key := range []string{"before-checkpoint", "after-checkpoint"} {
		if value, err := recovered.Get(testBucketName, []byte(key)); err != nil || string(value) != "value" {
			t.Errorf("Get %q after the crash: %q, %v", key, value, err)
		}
	}
}

// TestWAL_RecoversAfterCrash: the payoff. We reconstruct the on-disk state of a power
// loss — the WAL committed, but the main file never got the change — and prove that
// opening the DB REPLAYS the WAL to recover the committed data. (We capture the file
// bytes directly, so this exercises the recovery *logic* independent of fsync.)
func TestWAL_RecoversAfterCrash(t *testing.T) {
	path := tempfile()
	wal := path + "-wal"
	defer os.RemoveAll(path)
	defer os.RemoveAll(wal)

	// Capture a pristine, pre-write main file.
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	emptyMain, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Commit data; grab the WAL (holding the committed change) before any checkpoint.
	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	walBytes, err := os.ReadFile(wal)
	if err != nil {
		t.Fatalf("expected a WAL after a commit: %v", err)
	}
	_ = db.Close()

	// Reconstruct a crashed state: main is still pre-commit, but the WAL landed.
	crash := tempfile()
	crashWal := crash + "-wal"
	defer os.RemoveAll(crash)
	defer os.RemoveAll(crashWal)
	if err := os.WriteFile(crash, emptyMain, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(crashWal, walBytes, 0600); err != nil {
		t.Fatal(err)
	}

	// Opening it must replay the WAL and recover the committed key.
	rec, err := openDB(crash)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Close()
	if v, err := rec.Get(testBucketName, []byte("a")); err != nil || !bytes.Equal(v, []byte("1")) {
		t.Fatalf("committed key not recovered from WAL after crash: got %q, err %v", v, err)
	}
}

// TestWAL_RecoversAfterSplitCrash: the harder recovery case. Enough data to force a
// split, so the ROOT POINTER moves (root leaf -> new branch root at a new pgid). Replay
// must reconstruct not just the leaf/branch pages but the META (new root pgid) — and the
// in-memory meta must reflect it afterward. Otherwise recovery rebuilds the tree but the
// root pointer still aims at the old (now-leaf) page.
func TestWAL_RecoversAfterSplitCrash(t *testing.T) {
	path := tempfile()
	wal := path + "-wal"
	defer os.RemoveAll(path)
	defer os.RemoveAll(wal)

	// Pristine, pre-write main file.
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	emptyMain, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Write enough to force at least one split (root must become a branch).
	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	const n = 100
	val := bytes.Repeat([]byte("x"), 512) // 100 * ~525B ≈ 50 KB, well past any page
	for i := 0; i < n; i++ {
		if err := db.Put(testBucketName, fmt.Appendf(nil, "key-%05d", i), val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if mustBucketRoot(t, db, testBucketName).IsLeaf() {
		t.Fatal("test setup: expected a split — root should be a branch")
	}
	walBytes, err := os.ReadFile(wal)
	if err != nil {
		t.Fatalf("expected a WAL after commits: %v", err)
	}
	_ = db.Close()

	// Reconstruct the crash: main is pre-write, the WAL holds every committed change.
	crash := tempfile()
	crashWal := crash + "-wal"
	defer os.RemoveAll(crash)
	defer os.RemoveAll(crashWal)
	if err := os.WriteFile(crash, emptyMain, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(crashWal, walBytes, 0600); err != nil {
		t.Fatal(err)
	}

	// Recovery must rebuild the complete multi-level tree from the recorded root page.
	rec, err := openDB(crash)
	if err != nil {
		t.Fatalf("open crashed db: %v", err)
	}
	defer rec.Close()
	if mustBucketRoot(t, rec, testBucketName).IsLeaf() {
		t.Fatal("recovered root is a leaf after WAL replay")
	}
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "key-%05d", i)
		if got, err := rec.Get(testBucketName, k); err != nil || !bytes.Equal(got, val) {
			t.Fatalf("key %d not recovered after split-crash: err=%v", i, err)
		}
	}
}

// TestWAL_TornTransaction_DiscardedAtomically: the commit-frame invariant, and the
// whole reason a single Put needs a "transaction". A WAL that contains a transaction's
// page frames but NOT its commit marker (crashed mid-commit, or a torn tail) must be
// discarded ENTIRELY on recovery — never applied in part. Applying half a transaction
// = a corrupt tree from one Put (a child page written, its parent/meta not).
//
// We commit T1 (key "a"), capture the WAL, commit T2 (key "b"), capture the WAL again.
// The WAL is append-only, so walAfterT2 == walAfterT1 ++ (T2 frames ++ T2 commit).
// Truncating walAfterT2 anywhere inside the T2 segment reproduces a torn commit: T1
// fully committed, T2 incomplete. EVERY such truncation must recover to *exactly* the
// post-T1 state — "a" present, "b" absent — and Open must SUCCEED (a torn tail is
// normal recovery, not an error).
func TestWAL_TornTransaction_DiscardedAtomically(t *testing.T) {
	path := tempfile()
	wal := path + "-wal"
	defer os.RemoveAll(path)
	defer os.RemoveAll(wal)

	// Pristine, pre-write main file.
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	emptyMain, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Commit T1, then T2, capturing the append-only WAL after each.
	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	walAfterT1, err := os.ReadFile(wal)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	walAfterT2, err := os.ReadFile(wal)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	if len(walAfterT2) <= len(walAfterT1) {
		t.Fatalf("test setup: WAL did not grow across T2 (%d -> %d)", len(walAfterT1), len(walAfterT2))
	}

	// recover rebuilds a crashed on-disk state (pre-write main + the given WAL bytes)
	// on a throwaway path and returns the opened DB.
	recover := func(t *testing.T, walBytes []byte) (*DB, error) {
		t.Helper()
		crash := tempfile()
		crashWal := crash + "-wal"
		t.Cleanup(func() { os.RemoveAll(crash); os.RemoveAll(crashWal) })
		if err := os.WriteFile(crash, emptyMain, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(crashWal, walBytes, 0600); err != nil {
			t.Fatal(err)
		}
		return openDB(crash)
	}

	// Positive control: the FULL WAL (T2's commit marker intact) recovers both keys.
	// If this fails the harness itself is wrong, not the torn-tail logic.
	t.Run("full-wal-recovers-both", func(t *testing.T) {
		rec, err := recover(t, walAfterT2)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer rec.Close()
		if v, err := rec.Get(testBucketName, []byte("a")); err != nil || !bytes.Equal(v, []byte("1")) {
			t.Fatalf("a: got %q err %v, want \"1\"", v, err)
		}
		if v, err := rec.Get(testBucketName, []byte("b")); err != nil || !bytes.Equal(v, []byte("2")) {
			t.Fatalf("b: got %q err %v, want \"2\"", v, err)
		}
	})

	// The invariant: any truncation inside T2 -> recover to exactly post-T1.
	t2 := len(walAfterT2) - len(walAfterT1) // size of the T2 segment
	for _, off := range []int{0, 1, t2 / 2, t2 - 1} {
		off := off
		t.Run(fmt.Sprintf("torn-at-+%d", off), func(t *testing.T) {
			rec, err := recover(t, walAfterT2[:len(walAfterT1)+off])
			if err != nil {
				t.Fatalf("open must succeed on a torn tail, got: %v", err)
			}
			defer rec.Close()
			if v, err := rec.Get(testBucketName, []byte("a")); err != nil || !bytes.Equal(v, []byte("1")) {
				t.Fatalf("committed T1 lost: a = %q, err %v", v, err)
			}
			if _, err := rec.Get(testBucketName, []byte("b")); !errors.Is(err, ErrKeyNotFound) {
				t.Fatalf("uncommitted T2 leaked into recovery: b should be absent, got err %v", err)
			}
		})
	}
}

// TestCheckpoint_BoundsWALAndPreservesData: the checkpoint bounds WAL growth and
// never loses data. We shrink the checkpoint threshold, then write far past it. A
// checkpoint must fire, drain the committed data into the main file, and reset the
// WAL — so the WAL at rest stays near the threshold instead of growing with the
// data. Every key must be readable before the close and after a reopen.
func TestCheckpoint_BoundsWALAndPreservesData(t *testing.T) {
	path := tempfile()
	wal := path + "-wal"
	defer os.RemoveAll(path)
	defer os.RemoveAll(wal)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}

	// Force frequent checkpoints so we can prove the WAL is bounded.
	db.wal.SetCheckpointThresholdBytes(8 * 1024) // 8 KiB

	const n = 400
	val := bytes.Repeat([]byte("x"), 2048) // ~2 KiB per value: the WAL grows fast
	want := make(map[string]string, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%05d", i)
		if err := db.Put(testBucketName, []byte(k), val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		want[k] = string(val)
	}

	// We wrote ~800 KiB, far past the 8 KiB threshold. If the checkpoint fires and
	// resets the WAL, the WAL at rest holds only the records since the last one.
	// Without a checkpoint it grows with the data (RED).
	if got := fileSize(t, wal); got > 128*1024 {
		t.Fatalf("WAL was not checkpointed: %d bytes at rest after writing ~%d KiB "+
			"(expected it to reset near the %d-byte threshold)", got, n*len(val)/1024,
			db.wal.Stats().CheckpointThresholdBytes)
	}

	// All data readable before the close.
	for k, wantV := range want {
		if got, err := db.Get(testBucketName, []byte(k)); err != nil || string(got) != wantV {
			t.Fatalf("get %q before close: got %q, err %v", k, got, err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: the checkpoint must have moved everything into the main file, so a
	// clean close + reopen keeps all data even though the WAL is gone at rest.
	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for k, wantV := range want {
		if got, err := db.Get(testBucketName, []byte(k)); err != nil || string(got) != wantV {
			t.Fatalf("get %q after reopen: got %q, err %v", k, got, err)
		}
	}
}

// TestCommitDefersMainWriteUntilCheckpoint checks that a commit below the checkpoint threshold leaves the main file unchanged.
// Reads use the committed WAL data. Close moves that data to the main file.
func TestCommitDefersMainWriteUntilCheckpoint(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path) // SyncNone by default.
	if err != nil {
		t.Fatal(err)
	}

	// The main file right after Open holds both initial metadata copies and the root.
	mainAfterOpen, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// A commit under the checkpoint threshold must NOT touch the main file.
	if err := db.Put(testBucketName, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	mainAfterPut, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(mainAfterOpen, mainAfterPut) {
		t.Fatal("main file changed before checkpoint")
	}

	// Yet the value is readable, served from the overlay, not from main.
	if got, err := db.Get(testBucketName, []byte("k")); err != nil || !bytes.Equal(got, []byte("v")) {
		t.Fatalf("committed key not readable before checkpoint: got %q, err %v", got, err)
	}

	// Close must checkpoint: drain the overlay into main, then remove the WAL.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	mainAfterClose, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(mainAfterOpen, mainAfterClose) {
		t.Fatal("main file unchanged after close: the checkpoint must drain the overlay into main")
	}

	// Reopen with the WAL gone: the read now comes purely from main. Data must survive.
	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got, err := db.Get(testBucketName, []byte("k")); err != nil || !bytes.Equal(got, []byte("v")) {
		t.Fatalf("committed key lost after checkpoint + reopen: got %q, err %v", got, err)
	}
}

// TestRecoveryAppliesWALOverCheckpointedMainFile checks recovery when the main file has older committed data.
// Keys 0 to 9 have newer values in the WAL. Keys 10 to 99 exist only in the main file. Keys 100 to 149 exist only in the WAL.
func TestRecoveryAppliesWALOverCheckpointedMainFile(t *testing.T) {
	path := tempfile()
	wal := path + "-wal"
	defer os.RemoveAll(path)
	defer os.RemoveAll(wal)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	// Drive checkpoints by hand: a huge threshold stops any auto-checkpoint from
	// firing mid-test, so we control exactly what sits in main vs the WAL.
	db.wal.SetCheckpointThresholdBytes(1 << 30)

	valBase := bytes.Repeat([]byte("a"), 512) // ~512B/value forces a multi-level base tree
	valUpd := bytes.Repeat([]byte("b"), 512)  // same length, distinct content

	// Batch 1: keys 0..99, then a manual checkpoint drains them into the main file.
	for i := 0; i < 100; i++ {
		if err := db.Put(testBucketName, fmt.Appendf(nil, "key-%05d", i), valBase); err != nil {
			t.Fatalf("base put %d: %v", i, err)
		}
	}
	if err := db.checkpointWAL(); err != nil {
		t.Fatalf("manual checkpoint: %v", err)
	}
	// After the checkpoint the base lives in main and the WAL is reset. If the base
	// never split, the "non-empty base" is trivial — make sure the seam is real.
	if mustBucketRoot(t, db, testBucketName).IsLeaf() {
		t.Fatal("test setup: base tree did not split; the checkpointed base must be multi-level")
	}

	// Batch 2 (WAL/overlay only — main keeps the checkpointed base):
	//   overwrite keys 0..9 to valUpd, and add fresh keys 100..149 as valBase.
	for i := 0; i < 10; i++ {
		if err := db.Put(testBucketName, fmt.Appendf(nil, "key-%05d", i), valUpd); err != nil {
			t.Fatalf("overwrite put %d: %v", i, err)
		}
	}
	for i := 100; i < 150; i++ {
		if err := db.Put(testBucketName, fmt.Appendf(nil, "key-%05d", i), valBase); err != nil {
			t.Fatalf("post-checkpoint put %d: %v", i, err)
		}
	}

	// Capture the crash image BEFORE any clean close: main = checkpointed base,
	// WAL = the post-checkpoint delta (overwrites + new keys, never checkpointed).
	mainBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	walBytes, err := os.ReadFile(wal)
	if err != nil {
		t.Fatalf("expected a WAL holding the post-checkpoint delta: %v", err)
	}
	_ = db.Close()

	// Reconstruct the crash on a throwaway path: a NON-EMPTY main (the base) plus the
	// delta WAL. This is the seam — replay must land on top of the base, not a blank file.
	crash := tempfile()
	crashWal := crash + "-wal"
	defer os.RemoveAll(crash)
	defer os.RemoveAll(crashWal)
	if err := os.WriteFile(crash, mainBytes, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(crashWal, walBytes, 0600); err != nil {
		t.Fatal(err)
	}

	rec, err := openDB(crash)
	if err != nil {
		t.Fatalf("open crashed db: %v", err)
	}
	defer rec.Close()

	for i := 0; i < 150; i++ {
		k := fmt.Appendf(nil, "key-%05d", i)
		want := valBase
		if i < 10 {
			want = valUpd // overwritten in the WAL delta — replay must win over main
		}
		got, err := rec.Get(testBucketName, k)
		if err != nil {
			t.Fatalf("key %d missing after checkpoint+crash recovery: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			label := "base"
			if i < 10 {
				label = "overwritten"
			}
			t.Fatalf("key %d (%s) recovered wrong value: WAL delta did not merge over the checkpointed base", i, label)
		}
	}
}

// -----------------------------------------------------------------------------
// Basic persistence behavior.
//
// DB.Put and DB.Get access values through an existing named bucket. These tests
// check basic reads, writes, replacement, and persistence after reopen.
// -----------------------------------------------------------------------------

// TestPutGet: a value can be written and read back within one session.
func TestPutGet(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put(testBucketName, []byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	v, err := db.Get(testBucketName, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte("1")) {
		t.Fatalf("got %q, want %q", v, "1")
	}
}

// TestPutGet_Persists checks that a value survives Close and reopen.
func TestPutGet_Persists(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	v, err := db.Get(testBucketName, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte("1")) {
		t.Fatalf("after reopen: got %q, want %q", v, "1")
	}
}

// TestPut_Overwrite: writing the same key twice keeps the latest value.
func TestPut_Overwrite(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put(testBucketName, []byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("a"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	v, err := db.Get(testBucketName, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte("2")) {
		t.Fatalf("got %q, want %q", v, "2")
	}
}

// TestGet_Missing: reading a key that was never written returns an error.
func TestGet_Missing(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Get(testBucketName, []byte("nope")); err == nil {
		t.Fatal("expected an error for a missing key")
	}
}

func TestGet_WALValueIsOwnedByCaller(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{
		Synchronous:              SyncNone,
		CheckpointThresholdBytes: ^uint64(0),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	createBucket(t, db, testBucketName)
	if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	value, err := db.Get(testBucketName, []byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'

	got, err := db.Get(testBucketName, []byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("value")) {
		t.Fatalf("second Get: got %q, want value", got)
	}
}

func TestGet_ValueUsesSingleBuffer(t *testing.T) {
	for _, testCase := range []struct{ keyBytes, valueBytes int }{
		{3, 128}, {3, 64 << 10}, {3, 1 << 20}, {btree.MaxKeySize, 128},
	} {
		t.Run(fmt.Sprintf("key=%d/value=%d", testCase.keyBytes, testCase.valueBytes), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			db, err := Open(path, 0600, &Options{
				Synchronous:              SyncNone,
				CheckpointThresholdBytes: ^uint64(0),
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			createBucket(t, db, testBucketName)
			key := bytes.Repeat([]byte("k"), testCase.keyBytes)
			want := bytes.Repeat([]byte("v"), testCase.valueBytes)
			if err := db.Put(testBucketName, key, want); err != nil {
				t.Fatal(err)
			}
			for _, source := range []string{"WAL", "mapped"} {
				if source == "mapped" {
					if err := db.Close(); err != nil {
						t.Fatal(err)
					}
					reopened, err := Open(path, 0600, &Options{ReadOnly: true})
					if err != nil {
						t.Fatal(err)
					}
					db = reopened
				}
				t.Run(source, func(t *testing.T) {
					var value []byte
					allocs := testing.AllocsPerRun(10, func() {
						value, err = db.Get(testBucketName, key)
						if err != nil {
							t.Fatal(err)
						}
					})
					if allocs != 1 {
						t.Fatalf("Get allocations: got %g, want 1", allocs)
					}
					if !bytes.Equal(value, want) {
						t.Fatal("Get returned different bytes")
					}
					value[0] = 'X'
					again, err := db.Get(testBucketName, key)
					if err != nil || !bytes.Equal(again, want) {
						t.Fatalf("changing the Get result changed stored bytes: %v", err)
					}
				})
			}
		})
	}
}

// TestPutGet_MultipleKeys: several keys coexist and all survive a reopen.
func TestPutGet_MultipleKeys(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	pairs := map[string]string{"a": "1", "b": "2", "c": "3", "hello": "world"}

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range pairs {
		if err := db.Put(testBucketName, []byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for k, want := range pairs {
		got, err := db.Get(testBucketName, []byte(k))
		if err != nil {
			t.Fatalf("get %q: %v", k, err)
		}
		if !bytes.Equal(got, []byte(want)) {
			t.Fatalf("key %q: got %q, want %q", k, got, want)
		}
	}
}

// -----------------------------------------------------------------------------
// Open: creation and basic file lifecycle.
// -----------------------------------------------------------------------------

// TestOpen ensures that a database can be created and opened without error.
func TestOpen(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	} else if db == nil {
		t.Fatal("expected db")
	}

	if s := db.Path(); s != path {
		t.Fatalf("unexpected path: %s", s)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestOpen_ErrPathRequired checks the error from an empty path.
func TestOpen_ErrPathRequired(t *testing.T) {
	_, err := Open("", 0600, nil)
	if !errors.Is(err, ErrPathRequired) {
		t.Fatalf("Open with an empty path: got %v, want ErrPathRequired", err)
	}
}

// TestOpen_ErrNotExists ensures that opening in a non-existent directory errors.
func TestOpen_ErrNotExists(t *testing.T) {
	_, err := Open(filepath.Join(tempfile(), "bad-path"), 0600, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

// TestOpen_Reopen ensures a created database can be closed and reopened.
func TestOpen_Reopen(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = openDB(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	if s := db.Path(); s != path {
		t.Fatalf("unexpected path after reopen: %s", s)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultOptionsAreIndependent(t *testing.T) {
	const osPageSize = 4096
	first := defaultOptions(osPageSize)
	first.ReadOnly = true
	first.Synchronous = SyncNone
	first.CheckpointThresholdBytes = 1

	second := defaultOptions(osPageSize)
	if second.ReadOnly {
		t.Fatal("changing one default options value changed a later value")
	}
	if second.Synchronous != SyncFull {
		t.Fatalf("default synchronous mode: got %v, want SyncFull", second.Synchronous)
	}
	if second.CheckpointThresholdBytes != defaultCheckpointPageCount*osPageSize {
		t.Fatalf("default checkpoint threshold: got %d", second.CheckpointThresholdBytes)
	}
}

func TestOpen_NewDatabaseUsesOperatingSystemPageSize(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	pageSize := int64(os.Getpagesize())
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	if db.meta.PageSize() != pageSize {
		t.Fatalf("new database page size: got %d, want %d", db.meta.PageSize(), pageSize)
	}
	if got := fileSize(t, path); got != 4*pageSize {
		t.Fatalf("new database file size: got %d, want four %d-byte pages", got, pageSize)
	}
	data := make([]byte, pageSize)
	if _, err := db.file.ReadAt(data, int64(firstAllocationSegmentID)*pageSize); err != nil {
		t.Fatal(err)
	}
	segment, err := decodeAllocationSegment(data, pageSize, firstAllocationSegmentID)
	if err != nil {
		t.Fatalf("new database allocation page: %v", err)
	}
	for pageID := page.ID(0); pageID <= firstTreePageID; pageID++ {
		if !allocationBitIsSet(segment.bitmapBits(), pageID) {
			t.Fatalf("initial allocation page marks page %d free", pageID)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.meta.PageSize() != pageSize {
		t.Fatalf("reopened database page size: got %d, want stored size %d", reopened.meta.PageSize(), pageSize)
	}
}

func TestOpen_ExistingDatabaseUsesStoredPageSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	storedPageSize := int64(os.Getpagesize()) * 2
	writeEmptyDatabaseWithPageSize(t, path, storedPageSize)

	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := db.meta.PageSize(); got != storedPageSize {
		t.Fatalf("existing database page size: got %d, want stored size %d", got, storedPageSize)
	}
}

func TestOpen_InvalidSynchronousModeDoesNotCreateDatabase(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, &Options{Synchronous: SyncNone + 1})
	if db != nil {
		_ = db.Close()
	}
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("Open error: got %v, want ErrInvalid", err)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("invalid synchronous mode created the database, stat error %v", statErr)
	}
}

func TestOpen_UsesModeForDatabaseAndWAL(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	const mode os.FileMode = 0600
	db, err := Open(path, mode, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, filePath := range []string{path, path + "-wal"} {
		info, err := os.Stat(filePath)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != mode {
			t.Errorf("mode for %s: got %04o, want %04o", filePath, got, mode)
		}
	}
}

func TestOpen_NilOptionsCreatesWritableDatabase(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	createBucket(t, db, testBucketName)

	if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("database opened with nil options is not writable: %v", err)
	}
}

func TestOpen_SyncsParentDirectoryAtFirstWALSync(t *testing.T) {
	for _, test := range []struct {
		name     string
		options  *Options
		wantSync bool
	}{
		{name: "default", wantSync: true},
		{name: "full", options: &Options{Synchronous: SyncFull}, wantSync: true},
		{name: "normal", options: &Options{Synchronous: SyncNormal}, wantSync: true},
		{name: "none", options: &Options{Synchronous: SyncNone}},
		{name: "read-only", options: &Options{ReadOnly: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			parentInfo, err := os.Stat(filepath.Dir(path))
			if err != nil {
				t.Fatal(err)
			}
			// Open creates a database with a WAL commit, so that Open syncs the directory too.
			// Each open below finds an existing database.
			db, err := Open(path, 0600, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			// Reopen after Close removes the WAL. Its new name also needs a sync.
			for attempt := range 2 {
				var parent *os.File
				db, err := open(path, 0600, test.options, fileio.OSFS{SyncDirectory: func(dir *os.File) error {
					if parent != nil {
						t.Error("directory synced more than once")
					}
					parent = dir
					info, err := dir.Stat()
					if err != nil || !info.IsDir() || !os.SameFile(info, parentInfo) {
						t.Fatalf("sync target is not the database directory: %v", err)
					}
					for _, name := range []string{path, path + "-wal"} {
						if _, err := os.Stat(name); err != nil {
							t.Errorf("file missing before directory sync: %v", err)
						}
					}
					return fileio.Sync(dir)
				}})
				if err != nil {
					t.Fatal(err)
				}
				// A handle that does not write does not sync the directory.
				if parent != nil {
					t.Fatalf("open %d: Open synced the directory before a write", attempt)
				}
				if !db.options.ReadOnly {
					if err := db.Update(func(tx *Tx) error {
						_, err := tx.CreateBucket([]byte{'b', byte('0' + attempt)})
						return err
					}); err != nil {
						t.Fatal(err)
					}
					// SyncFull must sync the directory before the first commit returns success.
					if test.wantSync && db.options.Synchronous == SyncFull && parent == nil {
						t.Fatalf("open %d: the first SyncFull commit returned before the directory sync", attempt)
					}
				}
				// SyncNormal syncs the WAL, and so the directory, at the checkpoint in Close.
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				if (parent != nil) != test.wantSync {
					t.Fatalf("open %d: directory sync = %t, want %t", attempt, parent != nil, test.wantSync)
				}
				if parent != nil {
					if _, err := parent.Stat(); !errors.Is(err, os.ErrClosed) {
						t.Fatalf("directory was not closed: %v", err)
					}
				}
			}
		})
	}
}

func TestOpen_DirectorySyncFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	syncErr := errors.New("directory sync failed")
	var parent *os.File
	db, err = open(path, 0600, nil, fileio.OSFS{SyncDirectory: func(dir *os.File) error {
		parent = dir
		return syncErr
	}})
	if err != nil {
		t.Fatalf("Open synced the directory before a write: %v", err)
	}
	// The first commit syncs the WAL and then the directory. A failed sync must fail that commit.
	err = db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket(testBucketName)
		return err
	})
	if !errors.Is(err, syncErr) {
		t.Fatalf("first commit error: got %v, want %v", err, syncErr)
	}
	if _, err := parent.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("directory was not closed after sync failure: %v", err)
	}
	// A failed WAL sync stops the handle, like any failed WAL write.
	_ = db.Close()
	// Close must release the file lock so a new Open can succeed.
	db, err = Open(path, 0600, &Options{LockTimeout: time.Millisecond})
	if err != nil {
		t.Fatalf("retry Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// failFirstMainStatFS fails the first Stat of a main database file.
type failFirstMainStatFS struct {
	fileio.OSFS
	err    error
	failed bool
}

func (fsys *failFirstMainStatFS) OpenFile(name string, flag int, perm os.FileMode) (fileio.File, error) {
	file, err := fsys.OSFS.OpenFile(name, flag, perm)
	if err != nil || strings.HasSuffix(name, "-wal") {
		return file, err
	}
	return &failFirstStatFile{File: file, fsys: fsys}, nil
}

type failFirstStatFile struct {
	fileio.File
	fsys *failFirstMainStatFS
}

func (file *failFirstStatFile) Stat() (os.FileInfo, error) {
	if !file.fsys.failed {
		file.fsys.failed = true
		return nil, file.fsys.err
	}
	return file.File.Stat()
}

// A failed Stat says nothing about the file size.
// Open must fail, not write a new database over the old one.
// Blackout found this: Open read the failure as an empty file.
func TestOpen_StatFailureDoesNotReinitializeDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	statErr := errors.New("stat failed")
	db, err = open(path, 0600, nil, &failFirstMainStatFS{err: statErr})
	if err == nil {
		closeErr := db.Close()
		t.Fatalf("Open succeeded after the size check failed (close: %v)", closeErr)
	}
	if !errors.Is(err, statErr) {
		t.Fatalf("Open error: got %v, want %v", err, statErr)
	}

	db, err = Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	value, err := db.Get(testBucketName, []byte("key"))
	if err != nil || string(value) != "value" {
		t.Fatalf("Get after the failed Open: %q, %v", value, err)
	}
}

// failFirstMetaReadFS fails the first read of the main file at offset zero, where metadata page 0 starts.
type failFirstMetaReadFS struct {
	fileio.OSFS
	err    error
	failed bool
}

func (fsys *failFirstMetaReadFS) OpenFile(name string, flag int, perm os.FileMode) (fileio.File, error) {
	file, err := fsys.OSFS.OpenFile(name, flag, perm)
	if err != nil || strings.HasSuffix(name, "-wal") {
		return file, err
	}
	return &failFirstMetaReadFile{File: file, fsys: fsys}, nil
}

type failFirstMetaReadFile struct {
	fileio.File
	fsys *failFirstMetaReadFS
}

func (file *failFirstMetaReadFile) ReadAt(p []byte, offset int64) (int, error) {
	if offset == 0 && !file.fsys.failed {
		file.fsys.failed = true
		return 0, file.fsys.err
	}
	return file.File.ReadAt(p, offset)
}

// A failed read says nothing about a metadata page, so Open must fail and let the caller try again.
// Blackout found this: Open took an I/O error on page 0 for a damaged copy, looked for page 1 at the operating-system page size, and gave the WAL a wrong page size.
func TestOpen_MetadataReadFailureFailsOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	readErr := errors.New("read failed")
	db, err = open(path, 0600, nil, &failFirstMetaReadFS{err: readErr})
	if err == nil {
		closeErr := db.Close()
		t.Fatalf("Open succeeded after the read of metadata page 0 failed (close: %v)", closeErr)
	}
	if !errors.Is(err, readErr) {
		t.Fatalf("Open error: got %v, want %v", err, readErr)
	}
}

func TestOpen_RelaxedSyncModesDoNotSyncEachCommit(t *testing.T) {
	tests := []struct {
		name string
		mode Sync
	}{
		{name: "normal", mode: SyncNormal},
		{name: "none", mode: SyncNone},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			const checkpointBeyondTestWrite uint64 = 1 << 30
			db, err := Open(path, 0600, &Options{
				Synchronous:              test.mode,
				CheckpointThresholdBytes: checkpointBeyondTestWrite,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				db.wal.SetSyncFileForTesting(nil)
				_ = db.Close()
			}()
			createBucket(t, db, testBucketName)

			syncCalls := 0
			db.wal.SetSyncFileForTesting(func() error {
				syncCalls++
				return nil
			})
			if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			if syncCalls != 0 {
				t.Fatalf("commit sync calls: got %d, want 0", syncCalls)
			}
		})
	}
}

func TestOpen_CheckpointThresholdOptionTriggersCheckpoint(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, &Options{
		Synchronous:              SyncNone,
		CheckpointThresholdBytes: 1, // Every WAL transaction is larger than one byte.
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	createBucket(t, db, testBucketName)

	if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if got := fileSize(t, path+"-wal"); got != 0 {
		t.Fatalf("WAL size after threshold checkpoint: got %d, want 0", got)
	}
	stats, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.WALBytesWritten == 0 {
		t.Fatal("WAL bytes written is zero after committed updates")
	}
	// Open checkpoints the new database, and each of the two commits starts a checkpoint.
	if stats.CheckpointCount != 3 {
		t.Fatalf("checkpoint count: got %d, want 3", stats.CheckpointCount)
	}
}

// An automatic checkpoint syncs its WAL truncate, so the first commit of the next WAL generation need not sync its header.
// SyncFull checkpoints in the write batcher, and SyncNormal checkpoints in Update.
func TestIdenticalPutAfterSyncNoneCloseIsDurable(t *testing.T) {
	for _, mode := range []Sync{SyncFull, SyncNormal} {
		t.Run(map[Sync]string{SyncFull: "full", SyncNormal: "normal"}[mode], func(t *testing.T) {
			fsys := disk.New(1, disk.Config{KeepChange: disk.Ratio{Num: 0, Den: 1}})
			db, err := open(simulatedPath, 0o600, &Options{Synchronous: SyncFull}, fsys)
			if err != nil {
				t.Fatal(err)
			}
			simulatedPut(t, db, "key", "A")
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			db, err = open(simulatedPath, 0o600, &Options{Synchronous: SyncNone}, fsys)
			if err != nil {
				t.Fatal(err)
			}
			rootID, pageCount := db.meta.Root(), db.meta.LastPage()
			simulatedPut(t, db, "key", "B")
			if db.meta.Root() != rootID || db.meta.LastPage() != pageCount {
				t.Fatal("fixture changed the tree shape")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			db, err = open(simulatedPath, 0o600, &Options{Synchronous: mode}, fsys)
			if err != nil {
				t.Fatal(err)
			}
			simulatedPut(t, db, "key", "B")
			if mode == SyncNormal {
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				simulatedCrash(fsys, db)
			}
			fsys.Restart(true)
			if value, err := simulatedGet(t, fsys, mode, "key"); err != nil || value != "B" {
				t.Fatalf("durable identical Put lost its value: got %q, %v, want B", value, err)
			}
		})
	}
}

func TestIdenticalPutCommitDependsOnSyncMode(t *testing.T) {
	for _, mode := range []Sync{SyncFull, SyncNormal, SyncNone} {
		t.Run(map[Sync]string{SyncFull: "full", SyncNormal: "normal", SyncNone: "none"}[mode], func(t *testing.T) {
			fsys := disk.New(1, disk.Config{})
			db, err := open(simulatedPath, 0o600, &Options{Synchronous: mode}, fsys)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			simulatedPut(t, db, "key", "value")
			if err := db.checkpointWAL(); err != nil {
				t.Fatal(err)
			}
			before, err := db.Stats()
			if err != nil {
				t.Fatal(err)
			}
			simulatedPut(t, db, "key", "value")
			after, err := db.Stats()
			if err != nil {
				t.Fatal(err)
			}
			want := uint64(1)
			if mode == SyncNone {
				want = 0
			}
			if got := after.WALCommitCount - before.WALCommitCount; got != want {
				t.Fatalf("identical Put WAL commits: got %d, want %d", got, want)
			}
		})
	}
}

func TestUpdate_AutomaticCheckpointSyncsTheWALTruncate(t *testing.T) {
	for name, mode := range map[string]Sync{"full": SyncFull, "normal": SyncNormal} {
		t.Run(name, func(t *testing.T) {
			db, err := Open(filepath.Join(t.TempDir(), "database"), 0600, &Options{Synchronous: mode, CheckpointThresholdBytes: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			createBucket(t, db, testBucketName)
			// The write batcher returns the results before its checkpoint, but it holds this lock until the checkpoint ends.
			db.operationMu.Lock()
			syncs := 0
			db.wal.SetSyncFileForTesting(func() error { syncs++; return nil })
			err = db.wal.SyncTruncate()
			db.wal.SetSyncFileForTesting(nil)
			db.operationMu.Unlock()
			if err != nil || syncs != 0 {
				t.Fatalf("the truncate of the automatic checkpoint still waits for a sync: %v, %d syncs", err, syncs)
			}
		})
	}
}

// TestDB_Path ensures Path returns the database's file path.
func TestDB_Path(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if s := db.Path(); s != path {
		t.Fatalf("unexpected path: %s", s)
	}
}

func TestOpen_RecoversWALThroughSymlink(t *testing.T) {
	for _, writeThroughLink := range []bool{false, true} {
		for _, readOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("write-link=%t/read-only=%t", writeThroughLink, readOnly), func(t *testing.T) {
				dir := t.TempDir()
				if err := os.Mkdir(filepath.Join(dir, "data"), 0700); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(dir, "data", "database")
				link := filepath.Join(dir, "link")
				if err := os.Symlink(filepath.Join("data", "database"), link); err != nil {
					if errors.Is(err, os.ErrPermission) {
						t.Skipf("symbolic links are not permitted: %v", err)
					}
					t.Fatal(err)
				}
				writePath, readPath := path, link
				if writeThroughLink {
					writePath, readPath = link, path
				}
				db, err := Open(writePath, 0600, nil)
				if err != nil {
					t.Fatal(err)
				}
				createBucket(t, db, testBucketName)
				if err := db.Put(testBucketName, []byte("key"), []byte("old")); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				main, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				db, err = Open(writePath, 0600, &Options{CheckpointThresholdBytes: 1 << 30})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if !db.closed {
						_ = db.Close()
					}
				})
				if err := db.Put(testBucketName, []byte("key"), []byte("new")); err != nil {
					t.Fatal(err)
				}
				log, err := os.ReadFile(path + "-wal")
				if err != nil {
					t.Fatalf("committed log is not beside the database target: %v", err)
				}
				if db.Path() != writePath {
					t.Fatal("Path did not preserve the caller's path")
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				// Restore the pre-checkpoint main file with the committed log.
				if err := os.WriteFile(path, main, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path+"-wal", log, 0600); err != nil {
					t.Fatal(err)
				}
				recovered, err := Open(readPath, 0600, &Options{ReadOnly: readOnly})
				if err != nil {
					t.Fatal(err)
				}
				defer recovered.Close()
				if got, err := recovered.Get(testBucketName, []byte("key")); err != nil || string(got) != "new" {
					t.Fatalf("committed value after recovery: got %q, error %v", got, err)
				}
				if recovered.Path() != readPath {
					t.Fatal("Path did not preserve the caller's recovery path")
				}
				if _, err := os.Lstat(link + "-wal"); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("a separate log exists for the symbolic link: %v", err)
				}
			})
		}
	}
}

func TestDB_CloseAfterWorkingDirectoryChange(t *testing.T) {
	firstDir, secondDir := t.TempDir(), t.TempDir()
	const name = "database"
	t.Chdir(firstDir)
	first, err := Open(name, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !first.closed {
			_ = first.Close()
		}
	})

	t.Chdir(secondDir)
	second, err := Open(name, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !second.closed {
			_ = second.Close()
		}
	})
	createBucket(t, second, testBucketName)
	if err := second.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	secondLog := filepath.Join(secondDir, name+"-wal")
	wantLog, err := os.ReadFile(secondLog)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(firstDir, name+"-wal")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("first database log remains after Close: %v", err)
	}
	gotLog, err := os.ReadFile(secondLog)
	if err != nil {
		t.Fatalf("closing the first database removed the second database log: %v", err)
	}
	if !bytes.Equal(gotLog, wantLog) {
		t.Fatal("closing the first database changed the second database log")
	}
	if first.Path() != name || second.Path() != name {
		t.Fatal("Path did not preserve the caller's relative path")
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(filepath.Join(secondDir, name), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Get(testBucketName, []byte("key")); err != nil || string(got) != "value" {
		t.Fatalf("committed value after reopen: got %q, error %v", got, err)
	}
}

// -----------------------------------------------------------------------------
// Open and metadata robustness.
// -----------------------------------------------------------------------------

// TestOpen_ErrInvalid checks that opening a non-KVLite file returns ErrInvalid.
func TestOpen_ErrInvalid(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintln(f, "this is not a kvlite database"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := openDB(path); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected ErrInvalid, got: %v", err)
	}
}

// TestOpen_FileTooSmall checks that a file too small for metadata cannot open.
func TestOpen_FileTooSmall(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	if err := os.WriteFile(path, make([]byte, 16), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := openDB(path); err == nil {
		t.Fatal("expected error opening a too-small file")
	}
}

// TestOpen_ErrVersionMismatch checks that two unsupported metadata versions fail.
func TestOpen_ErrVersionMismatch(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	pageSize := db.meta.PageSize()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Both metadata copies use version bytes 4-7 within their page. Bump both
	// version fields so Open has no valid current-format copy.
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	buf[4] = 0xFF
	buf[pageSize+4] = 0xFF
	if err := os.WriteFile(path, buf, 0600); err != nil {
		t.Fatal(err)
	}

	if _, err := openDB(path); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("expected ErrVersionMismatch, got: %v", err)
	}
}

// TestOpen_ErrChecksum checks that corruption in both metadata copies fails.
func TestOpen_ErrChecksum(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	pageSize := db.meta.PageSize()
	if err := db.Put(testBucketName, []byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt the root field in both copies. Magic and version remain valid, so
	// only the checksums can reject the damage.
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	buf[24] ^= 0xFF
	buf[pageSize+24] ^= 0xFF
	if err := os.WriteFile(path, buf, 0600); err != nil {
		t.Fatal(err)
	}

	if _, err := openDB(path); !errors.Is(err, ErrChecksum) {
		t.Fatalf("expected ErrChecksum, got: %v", err)
	}
}

func readMetaCopy(t *testing.T, path string, pageSize int64, pageID page.ID) *page.Meta {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	data := make([]byte, page.MetaSize)
	if _, err := file.ReadAt(data, int64(pageID)*pageSize); err != nil {
		t.Fatal(err)
	}
	meta, err := page.DecodeMeta(data)
	if err != nil {
		t.Fatal(err)
	}
	return meta
}

func TestCheckpoint_PersistsCurrentMetadataToBothCopies(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	pageSize := db.meta.PageSize()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	meta0 := readMetaCopy(t, path, pageSize, page.Meta0ID)
	meta1 := readMetaCopy(t, path, pageSize, page.Meta1ID)
	if err := meta0.Validate(); err != nil {
		t.Fatalf("validate metadata page 0: %v", err)
	}
	if err := meta1.Validate(); err != nil {
		t.Fatalf("validate metadata page 1: %v", err)
	}
	if !bytes.Equal(page.EncodeMeta(meta0), page.EncodeMeta(meta1)) {
		t.Fatal("checkpoint stored different metadata copies")
	}
	if meta0.Generation() == 0 {
		t.Fatal("checkpoint did not advance the metadata generation")
	}
}

func TestOpen_RecoversFromCorruptMetadataPage1(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	pageSize := db.meta.PageSize()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	rootOffset := pageSize + 24
	rootByte := []byte{0}
	if _, err := file.ReadAt(rootByte, rootOffset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	rootByte[0] ^= 0xff
	if _, err := file.WriteAt(rootByte, rootOffset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openDB(path)
	if err != nil {
		t.Fatalf("open with corrupt metadata page 1: %v", err)
	}
	defer reopened.Close()
	value, err := reopened.Get(testBucketName, []byte("key"))
	if err != nil || !bytes.Equal(value, []byte("value")) {
		t.Fatalf("recovered value: got %q, error %v", value, err)
	}
}

func TestOpen_SelectsValidMetadataWithHighestGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	pageSize := db.meta.PageSize()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	newer := readMetaCopy(t, path, pageSize, page.Meta1ID)
	newer.AdvanceGeneration()
	newer.RefreshChecksum()
	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(page.EncodeMeta(newer), pageSize); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.meta.Generation(); got != newer.Generation() {
		t.Fatalf("selected metadata generation: got %d, want %d", got, newer.Generation())
	}
}

func TestOpen_RejectsDifferentMetadataAtSameGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	pageSize := db.meta.PageSize()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	conflicting := readMetaCopy(t, path, pageSize, page.Meta1ID)
	conflicting.SetRoot(conflicting.Root() + 1)
	conflicting.RefreshChecksum()
	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(page.EncodeMeta(conflicting), pageSize); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openDB(path)
	if reopened != nil {
		_ = reopened.Close()
	}
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("Open error: got %v, want ErrInvalid", err)
	}
}

func TestOpen_ErrChecksumForCorruptedBTreePage(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDBWithChecksums(path, true)
	if err != nil {
		t.Fatal(err)
	}
	pageSize := db.meta.PageSize()
	rootPageID := db.meta.Root()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	corruptOffset := int64(rootPageID)*pageSize + btree.NodeHeaderSize
	byteAtOffset := []byte{0}
	if _, err := file.ReadAt(byteAtOffset, corruptOffset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	byteAtOffset[0] ^= 0xff
	if _, err := file.WriteAt(byteAtOffset, corruptOffset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openDB(path)
	if reopened != nil {
		_ = reopened.Close()
	}
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("Open error: got %v, want ErrChecksum", err)
	}
}

// TestOpen_ReadPageSize_FromMeta1 checks that a valid second metadata page can
// recover the stored page size when the first metadata page is corrupt.
func TestOpen_ReadPageSize_FromMeta1(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	pageSize := int64(os.Getpagesize())

	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket(testBucketName)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(make([]byte, page.MetaSize), 0); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openDB(path)
	if err != nil {
		t.Fatalf("open with corrupt metadata page 0: %v", err)
	}
	defer reopened.Close()
	if got := reopened.meta.PageSize(); got != pageSize {
		t.Fatalf("recovered page size: got %d, want %d", got, pageSize)
	}
	value, err := reopened.Get(testBucketName, []byte("key"))
	if err != nil || !bytes.Equal(value, []byte("value")) {
		t.Fatalf("recovered value: got %q, error %v", value, err)
	}
}

func TestPageOwnershipValidOnNewAndReopenedDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.validatePageOwnership(); err != nil {
		t.Fatalf("check new database: %v", err)
	}
	if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.validatePageOwnership(); err != nil {
		t.Fatalf("check reopened database: %v", err)
	}
}

// TestDB_Open_ReadOnly: a read-only open allows reads, rejects writes, and does not
// mutate the files (no main-file write, no WAL created or deleted).
// (Concurrent read-only openers need file locking — deferred, see TestOpen_MultipleGoroutines.)
func TestDB_Open_ReadOnly(t *testing.T) {
	path := tempfile()
	wal := path + "-wal"
	defer os.RemoveAll(path)
	defer os.RemoveAll(wal)

	// Seed a database read-write, then close it (single file at rest).
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	mainBefore, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Open read-only.
	rdb, err := Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("read-only open of an existing db: %v", err)
	}

	// Reads work.
	if v, err := rdb.Get(testBucketName, []byte("a")); err != nil || !bytes.Equal(v, []byte("1")) {
		t.Fatalf("read-only Get: got %q, err %v", v, err)
	}

	// Writes are rejected up front, with the read-only error.
	if err := rdb.Put(testBucketName, []byte("b"), []byte("2")); !errors.Is(err, ErrDatabaseReadOnly) {
		t.Fatalf("read-only Put: expected ErrDatabaseReadOnly, got %v", err)
	}

	if err := rdb.Close(); err != nil {
		t.Fatalf("read-only Close: %v", err)
	}

	// A read-only session must not have changed the main file...
	mainAfter, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(mainBefore, mainAfter) {
		t.Fatal("read-only session modified the main file")
	}
	// ...and must not have left a WAL behind.
	if _, err := os.Stat(wal); !os.IsNotExist(err) {
		t.Fatalf("read-only session created a WAL, stat err=%v", err)
	}
}

// TestDB_Open_ReadOnly_NoCreate: a read-only open of a missing path must error and
// must not create the file.
func TestDB_Open_ReadOnly_NoCreate(t *testing.T) {
	path := tempfile() // does not exist yet
	defer os.RemoveAll(path)

	_, err := Open(path, 0600, &Options{ReadOnly: true})
	if err == nil {
		t.Fatal("read-only open of a missing path must error")
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("read-only open created the file, stat err=%v", statErr)
	}
}

func TestOpen_EnforcesProcessLockModes(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	firstReader, err := Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer firstReader.Close()
	secondReader, err := Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("second read-only Open: %v", err)
	}
	defer secondReader.Close()

	writer, err := Open(path, 0600, &Options{LockTimeout: time.Nanosecond})
	if writer != nil {
		defer writer.Close()
	}
	if !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("writable Open while read-only handles were open: got %v, want ErrDatabaseLocked", err)
	}

	if err := secondReader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := firstReader.Close(); err != nil {
		t.Fatal(err)
	}

	writer, err = Open(path, 0600, nil)
	if err != nil {
		t.Fatalf("writable Open after readers closed: %v", err)
	}
	defer writer.Close()

	secondWriter, err := Open(path, 0600, &Options{LockTimeout: time.Nanosecond})
	if secondWriter != nil {
		defer secondWriter.Close()
	}
	if !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("second writable Open: got %v, want ErrDatabaseLocked", err)
	}
	reader, err := Open(path, 0600, &Options{ReadOnly: true, LockTimeout: time.Nanosecond})
	if reader != nil {
		defer reader.Close()
	}
	if !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("read-only Open while a writable handle was open: got %v, want ErrDatabaseLocked", err)
	}
}

func TestOpen_DefaultWaitsForProcessLock(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	first, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	type openResult struct {
		db  *DB
		err error
	}
	result := make(chan openResult, 1)
	go func() {
		db, err := Open(path, 0600, nil)
		result <- openResult{db: db, err: err}
	}()

	select {
	case got := <-result:
		if got.db != nil {
			_ = got.db.Close()
		}
		t.Fatalf("Open returned while the first handle still held the lock: %v", got.err)
	case <-time.After(100 * time.Millisecond):
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatalf("Open after the first handle closed: %v", got.err)
		}
		if err := got.db.Close(); err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Open did not acquire the released process lock")
	}
}

func TestOpen_ProcessLockTimeout(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	first, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	const timeout = 75 * time.Millisecond
	started := time.Now()
	second, err := Open(path, 0600, &Options{LockTimeout: timeout})
	elapsed := time.Since(started)
	if second != nil {
		_ = second.Close()
		t.Fatal("Open acquired a process lock that another handle held")
	}
	if !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("Open after lock timeout: got %v, want ErrDatabaseLocked", err)
	}
	if elapsed < timeout {
		t.Fatalf("Open returned after %v, before the %v lock timeout", elapsed, timeout)
	}
}

func TestOpen_RejectsNegativeLockTimeout(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, &Options{LockTimeout: -time.Nanosecond})
	if db != nil {
		_ = db.Close()
	}
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("Open with a negative lock timeout: got %v, want ErrInvalid", err)
	}
}

// TestOpen_MetaInitWriteError: write errors during meta init must surface from Open.
func TestOpen_MetaInitWriteError(t *testing.T) {
	t.Skip("deferred (fault injection)")
}

// BenchmarkPut_Sequential measures writes with ascending keys and tree splits.
// BenchmarkMakeWALCommitData measures the change list without encoding or file writes.
func BenchmarkMakeWALCommitData(b *testing.B) {
	for _, count := range []int{1, 128, 1024} {
		b.Run(fmt.Sprintf("nodes=%d", count), func(b *testing.B) {
			dirty := make(map[page.ID]dirtyNode, count)
			for i := range count {
				node := btree.NewLeafNode(page.ID(i + 3))
				dirty[node.PageID()] = dirtyNode{original: node, final: node}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				nodes, meta := makeWALCommitData(dirty, nil, false)
				if len(nodes) != count || meta != nil || nodes[0].Final == nil {
					b.Fatal("invalid WAL change list")
				}
			}
		})
	}
}

func BenchmarkPut_Sequential(b *testing.B) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	value := []byte("some-benchmark-value-thats-a-realistic-size")

	i := 0
	for b.Loop() {
		key := fmt.Appendf(nil, "key-%08d", i)
		if err := db.Put(testBucketName, key, value); err != nil {
			b.Fatal(err)
		}
		i++
	}
}

// BenchmarkPut_OneOperationBucket measures the DB convenience path. Each Put
// opens one transaction and resolves the named bucket.
func BenchmarkPut_OneOperationBucket(b *testing.B) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	key := []byte("the-one-key")
	value := []byte("some-benchmark-value-thats-a-realistic-size")

	for b.Loop() {
		if err := db.Put(testBucketName, key, value); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPut_ReusedBucket measures writes after one bucket lookup in one
// transaction.
func BenchmarkPut_ReusedBucket(b *testing.B) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	key := []byte("the-one-key")
	value := []byte("some-benchmark-value-thats-a-realistic-size")

	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		for b.Loop() {
			if err := bucket.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
}

// BenchmarkGet_OneOperationBucket measures the direct DB convenience path.
func BenchmarkGet_OneOperationBucket(b *testing.B) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	key := []byte("the-one-key")
	value := []byte("some-benchmark-value-thats-a-realistic-size")
	if err := db.Put(testBucketName, key, value); err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		if _, err := db.Get(testBucketName, key); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGet_ReusedBucket measures reads after one bucket lookup in one
// transaction.
func BenchmarkGet_ReusedBucket(b *testing.B) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	key := []byte("the-one-key")
	value := []byte("some-benchmark-value-thats-a-realistic-size")
	if err := db.Put(testBucketName, key, value); err != nil {
		b.Fatal(err)
	}

	err = db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		for b.Loop() {
			if _, err := bucket.Get(key); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
}

// -----------------------------------------------------------------------------
// Managed transaction behavior.
//
// DB.Update and DB.View expose bucket handles for atomic writes and read-only
// access. Update commits all bucket changes only when its callback returns nil.
// -----------------------------------------------------------------------------

func TestUpdate_DoesNotStageWALBeforeCallbackReturns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte("key"), []byte("value")); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if got, err := db.Get(testBucketName, []byte("key")); err != nil || !bytes.Equal(got, []byte("value")) {
		t.Fatalf("committed value: got %q, err %v", got, err)
	}
}

func TestUpdate_OverwriteDoesNotCommitUnchangedMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put(testBucketName, []byte("key"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := db.checkpointWAL(); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("key"), []byte("new")); err != nil {
		t.Fatal(err)
	}

	stats := db.wal.Stats()
	if stats.CommittedRecordCount != 1 {
		t.Fatalf("overwrite committed records: got %d, want one data record", stats.CommittedRecordCount)
	}
	for _, pageID := range [...]page.ID{page.Meta0ID, page.Meta1ID} {
		if _, ok := db.wal.CommittedRecord(pageID); ok {
			t.Fatalf("overwrite committed unchanged metadata page %d", pageID)
		}
	}
}

func TestReadNode_DoesNotChangeMainFileOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.checkpointWAL(); err != nil {
		t.Fatal(err)
	}
	const offset int64 = 7
	if _, err := db.file.(*os.File).Seek(offset, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := db.readNode(db.meta.Root()); err != nil {
		t.Fatal(err)
	}
	got, err := db.file.(*os.File).Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if got != offset {
		t.Fatalf("main file offset after node read: got %d, want %d", got, offset)
	}
}

func TestReadValidMainMeta_DoesNotChangeMainFileOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const offset int64 = 7
	if _, err := db.file.(*os.File).Seek(offset, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.readValidMainMeta(); err != nil {
		t.Fatal(err)
	}
	got, err := db.file.(*os.File).Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if got != offset {
		t.Fatalf("main file offset after metadata read: got %d, want %d", got, offset)
	}
}

func TestCheckpoint_DoesNotChangeMainFileOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	const offset int64 = 7
	if _, err := db.file.(*os.File).Seek(offset, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if err := db.checkpointWAL(); err != nil {
		t.Fatal(err)
	}
	got, err := db.file.(*os.File).Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if got != offset {
		t.Fatalf("main file offset after checkpoint: got %d, want %d", got, offset)
	}
}

// TestTx_UpdateCommitsAtomically checks that one Update commits all bucket
// writes together and makes them visible inside the same transaction.
func TestTx_UpdateCommitsAtomically(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pairs := map[string]string{"alpha": "1", "bravo": "2", "charlie": "3"}

	if err := db.Update(func(tx *Tx) error {
		for k, v := range pairs {
			if err := mustBucket(t, tx, testBucketName).Put([]byte(k), []byte(v)); err != nil {
				return err
			}
			// read-your-writes: a value written earlier in THIS txn is visible now.
			if got, err := mustBucket(t, tx, testBucketName).Get([]byte(k)); err != nil || !bytes.Equal(got, []byte(v)) {
				t.Fatalf("read-your-writes failed for %q: got %q, err %v", k, got, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("Update returned an error on a clean commit: %v", err)
	}

	// After commit, a separate read txn sees every key.
	if err := db.View(func(tx *Tx) error {
		for k, want := range pairs {
			got, err := mustBucket(t, tx, testBucketName).Get([]byte(k))
			if err != nil || !bytes.Equal(got, []byte(want)) {
				t.Fatalf("committed key %q not visible after Update: got %q, err %v", k, got, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestTx_UpdateRollsBackOnError: the atomicity guarantee. An Update whose fn returns
// an error must leave NO trace — every write in that txn is discarded, while data
// committed by an EARLIER successful txn is untouched. The assertion runs on the
// SAME open handle (no reopen), so it also catches the in-memory-rollback trap:
// discarding the WAL buffer is not enough if db.rootNode was mutated in place.
func TestTx_UpdateRollsBackOnError(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// A prior committed txn that rollback must NOT touch.
	if err := db.Update(func(tx *Tx) error {
		return mustBucket(t, tx, testBucketName).Put([]byte("keep"), []byte("me"))
	}); err != nil {
		t.Fatal(err)
	}

	errBoom := errors.New("boom")
	got := db.Update(func(tx *Tx) error {
		if err := mustBucket(t, tx, testBucketName).Put([]byte("doomed-a"), []byte("x")); err != nil {
			return err
		}
		if err := mustBucket(t, tx, testBucketName).Put([]byte("doomed-b"), []byte("y")); err != nil {
			return err
		}
		return errBoom // abort AFTER writing -> everything above must vanish
	})
	if !errors.Is(got, errBoom) {
		t.Fatalf("Update must return fn's error verbatim, got %v", got)
	}

	if err := db.View(func(tx *Tx) error {
		for _, k := range []string{"doomed-a", "doomed-b"} {
			if _, err := mustBucket(t, tx, testBucketName).Get([]byte(k)); !errors.Is(err, ErrKeyNotFound) {
				t.Fatalf("rolled-back key %q leaked (in-memory tree left dirty?): err %v", k, err)
			}
		}
		if v, err := mustBucket(t, tx, testBucketName).Get([]byte("keep")); err != nil || !bytes.Equal(v, []byte("me")) {
			t.Fatalf("rollback clobbered a previously committed key: got %q, err %v", v, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_UpdateRollsBackOnPanic(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	panicValue := "boom"
	var recovered any
	var panickedBucket *Bucket
	func() {
		defer func() {
			recovered = recover()
		}()

		_ = db.Update(func(tx *Tx) error {
			panickedBucket = mustBucket(t, tx, testBucketName)
			if err := panickedBucket.Put([]byte("doomed"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			panic(panicValue)
		})
	}()

	if recovered != panicValue {
		t.Fatalf("Update did not propagate the callback panic: got %v", recovered)
	}
	if err := panickedBucket.Put([]byte("late"), []byte("value")); !errors.Is(err, ErrTxClosed) {
		t.Fatalf("Put through panicked transaction: expected ErrTxClosed, got %v", err)
	}
	if _, err := db.Get(testBucketName, []byte("doomed")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("panicked Update changed the database: expected ErrKeyNotFound, got %v", err)
	}
}

// TestTx_ViewIsReadOnly: a write attempted inside db.View fails with ErrTxNotWritable
// and mutates nothing.
func TestTx_ViewIsReadOnly(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.View(func(tx *Tx) error {
		if tx.Writable() {
			t.Fatal("a View txn must not be writable")
		}
		return mustBucket(t, tx, testBucketName).Put([]byte("nope"), []byte("nope"))
	})
	if !errors.Is(err, ErrTxNotWritable) {
		t.Fatalf("a write inside View must fail with ErrTxNotWritable, got %v", err)
	}

	// Nothing was written.
	if err := db.View(func(tx *Tx) error {
		if _, err := mustBucket(t, tx, testBucketName).Get([]byte("nope")); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("View leaked a write: err %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_GetAfterViewReturnsTxClosed(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	var savedBucket *Bucket
	if err := db.View(func(tx *Tx) error {
		savedBucket = mustBucket(t, tx, testBucketName)
		value, err := savedBucket.Get([]byte("key"))
		if err != nil {
			return err
		}
		if !bytes.Equal(value, []byte("value")) {
			t.Fatalf("Get returned %q, expected %q", value, "value")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := savedBucket.Get([]byte("key")); !errors.Is(err, ErrTxClosed) {
		t.Fatalf("Get through closed transaction: expected ErrTxClosed, got %v", err)
	}
}

func TestBucketReadsAfterViewStopAtClosedTransaction(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("bucket"))
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte("key"), []byte("value")); err != nil {
			return err
		}
		_, err = bucket.CreateBucket([]byte("child"))
		return err
	}); err != nil {
		t.Fatal(err)
	}

	var savedTx *Tx
	var savedBucket *Bucket
	if err := db.View(func(tx *Tx) error {
		savedTx = tx
		var err error
		savedBucket, err = tx.Bucket([]byte("bucket"))
		if err != nil {
			return err
		}
		if value := mustBucketValue(t, savedBucket, []byte("key")); !bytes.Equal(value, []byte("value")) {
			t.Fatalf("Get returned %q, expected %q", value, "value")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if bucket, err := savedTx.Bucket([]byte("bucket")); bucket != nil || !errors.Is(err, ErrTxClosed) {
		t.Fatalf("closed transaction lookup: expected ErrTxClosed, got bucket %v and error %v", bucket, err)
	}
	if value, err := savedBucket.Get([]byte("key")); value != nil || !errors.Is(err, ErrTxClosed) {
		t.Fatalf("closed bucket read: expected ErrTxClosed, got value %q and error %v", value, err)
	}
	if child, err := savedBucket.Bucket([]byte("child")); child != nil || !errors.Is(err, ErrTxClosed) {
		t.Fatalf("closed bucket lookup: expected ErrTxClosed, got bucket %v and error %v", child, err)
	}
}

// TestTx_ViewCannotCreateBucket checks that a read-only transaction cannot add a bucket.
// It also checks that a later write cannot commit a bucket from the rejected call.
func TestTx_ViewCannotCreateBucket(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// A CreateBucket inside a View must fail and change nothing.
	err = db.View(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("phantom"))
		return err
	})
	if !errors.Is(err, ErrTxNotWritable) {
		t.Fatalf("CreateBucket inside View must fail with ErrTxNotWritable, got %v", err)
	}

	// A later, legitimate write must not carry a leaked phantom bucket with it.
	if err := db.Update(func(tx *Tx) error {
		return mustBucket(t, tx, testBucketName).Put([]byte("real"), []byte("value"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		if b, err := tx.Bucket([]byte("phantom")); b != nil || !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("phantom bucket lookup: expected ErrBucketNotFound, got bucket %v and error %v", b, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucketPutAfterUpdateReturnsTxClosed(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var saved *Bucket
	if err := db.Update(func(tx *Tx) error {
		var err error
		saved, err = tx.CreateBucket([]byte("b"))
		return err
	}); err != nil {
		t.Fatal(err)
	}

	if err := saved.Put([]byte("late"), []byte("value")); !errors.Is(err, ErrTxClosed) {
		t.Errorf("Put through a closed bucket: expected ErrTxClosed, got %v", err)
	}

	if err := db.Update(func(tx *Tx) error {
		return mustBucket(t, tx, testBucketName).Put([]byte("trigger"), []byte("value"))
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error {
		b := mustBucket(t, tx, []byte("b"))
		if got, err := b.Get([]byte("late")); got != nil || !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("closed bucket write lookup: expected ErrKeyNotFound, got value %q and error %v", got, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTreePutEntry_DoesNotPublishPrivateRoot(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	originalRoot := db.rootNode
	originalMetaRoot := db.meta.Root()
	value := bytes.Repeat([]byte("v"), int(db.meta.PageSize()/2))
	committedBefore := db.wal.Stats().CommittedRecordCount

	err = db.Update(func(tx *Tx) error {
		root := tx.rootNode
		newRoot, err := tx.tree.PutEntry(root, btree.NewEntry(0, []byte("a"), value))
		if err != nil {
			return err
		}
		newRoot, err = tx.tree.PutEntry(newRoot, btree.NewEntry(0, []byte("b"), value))
		if err != nil {
			return err
		}
		if newRoot.IsLeaf() {
			t.Fatal("second entry did not split the root")
		}
		if newRoot.PageID() == root.PageID() {
			t.Fatalf("private root page ID after split: got old page %d, want a new page", root.PageID())
		}
		if tx.rootNode != root {
			t.Fatal("tree engine published its private root through the transaction")
		}
		if db.rootNode != originalRoot {
			t.Fatal("transaction changed the shared database root before commit")
		}
		if db.meta.Root() != originalMetaRoot {
			t.Fatalf("transaction changed meta root before commit: got %d, want %d", db.meta.Root(), originalMetaRoot)
		}
		if stats := db.wal.Stats(); stats.CommittedRecordCount != committedBefore {
			t.Fatalf("WAL records published before commit: got %d, want %d",
				stats.CommittedRecordCount, committedBefore)
		}
		return errDiscardTx
	})
	if !errors.Is(err, errDiscardTx) {
		t.Fatalf("Update: got %v, want %v", err, errDiscardTx)
	}
	if db.rootNode != originalRoot {
		t.Fatal("discarded transaction changed the database root")
	}
	if db.meta.Root() != originalMetaRoot {
		t.Fatalf("discarded transaction changed meta root: got %d, want %d", db.meta.Root(), originalMetaRoot)
	}
}

// TestBucketPutErrorLeavesDataIntact verifies that a rejected bucket write does
// not damage data committed before it.
func TestBucketPutErrorLeavesDataIntact(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		return mustBucket(t, tx, testBucketName).Put([]byte("keep"), []byte("me"))
	}); err != nil {
		t.Fatal(err)
	}

	got := db.Update(func(tx *Tx) error {
		return mustBucket(t, tx, testBucketName).Put(nil, []byte("x"))
	})
	if !errors.Is(got, ErrKeyRequired) {
		t.Fatalf("expected ErrKeyRequired from a bad bucket Put, got %v", got)
	}

	if v, err := db.Get(testBucketName, []byte("keep")); err != nil || !bytes.Equal(v, []byte("me")) {
		t.Fatalf("committed key lost after a failed bucket Put: got %q, err %v", v, err)
	}
}

func TestEmptyKeysReturnErrKeyRequired(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put(testBucketName, nil, []byte("value")); !errors.Is(err, ErrKeyRequired) {
		t.Fatalf("DB.Put: expected ErrKeyRequired, got %v", err)
	}
	if _, err := db.Get(testBucketName, nil); !errors.Is(err, ErrKeyRequired) {
		t.Fatalf("DB.Get: expected ErrKeyRequired, got %v", err)
	}

	err = db.Update(func(tx *Tx) error {
		if err := mustBucket(t, tx, testBucketName).Put(nil, []byte("value")); !errors.Is(err, ErrKeyRequired) {
			t.Fatalf("Bucket.Put: expected ErrKeyRequired, got %v", err)
		}
		if _, err := mustBucket(t, tx, testBucketName).Get(nil); !errors.Is(err, ErrKeyRequired) {
			t.Fatalf("Bucket.Get: expected ErrKeyRequired, got %v", err)
		}

		bucket, err := tx.CreateBucket([]byte("bucket"))
		if err != nil {
			return err
		}
		if err := bucket.Put(nil, []byte("value")); !errors.Is(err, ErrKeyRequired) {
			t.Fatalf("Bucket.Put: expected ErrKeyRequired, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCreateBucketRequiresName(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *Tx) error {
		if _, err := tx.CreateBucket(nil); !errors.Is(err, ErrBucketNameRequired) {
			t.Fatalf("Tx.CreateBucket: expected ErrBucketNameRequired, got %v", err)
		}
		if _, err := tx.Bucket(nil); !errors.Is(err, ErrBucketNameRequired) {
			t.Fatalf("Tx.Bucket: expected ErrBucketNameRequired, got %v", err)
		}

		parent, err := tx.CreateBucket([]byte("parent"))
		if err != nil {
			return err
		}
		if _, err := parent.Bucket(nil); !errors.Is(err, ErrBucketNameRequired) {
			t.Fatalf("Bucket.Bucket: expected ErrBucketNameRequired, got %v", err)
		}
		if _, err := parent.CreateBucket(nil); !errors.Is(err, ErrBucketNameRequired) {
			t.Fatalf("Bucket.CreateBucket: expected ErrBucketNameRequired, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestNestedBucketLookupContracts(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *Tx) error {
		parent, err := tx.CreateBucket([]byte("parent"))
		if err != nil {
			return err
		}
		child, err := parent.CreateBucket([]byte("child"))
		if err != nil {
			return err
		}
		if err := parent.Put([]byte("value"), []byte("plain")); err != nil {
			return err
		}
		if got, err := parent.Bucket([]byte("child")); err != nil || got != child {
			t.Fatalf("Bucket did not return the cached child: %v", err)
		}
		if got, err := parent.Bucket([]byte("missing")); got != nil || !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("Bucket: expected ErrBucketNotFound, got bucket %v and error %v", got, err)
		}
		if got, err := parent.Bucket([]byte("value")); got != nil || !errors.Is(err, ErrIncompatibleValue) {
			t.Fatalf("Bucket: expected ErrIncompatibleValue, got bucket %v and error %v", got, err)
		}
		if got, err := tx.Bucket([]byte("missing")); got != nil || !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("Tx.Bucket: expected ErrBucketNotFound, got bucket %v and error %v", got, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBucketGetReturnsLookupErrors(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *Tx) error {
		parent, err := tx.CreateBucket([]byte("parent"))
		if err != nil {
			return err
		}
		if _, err := parent.CreateBucket([]byte("child")); err != nil {
			return err
		}

		if value, err := parent.Get([]byte("missing")); value != nil || !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("missing value: expected ErrKeyNotFound, got value %q and error %v", value, err)
		}
		if value, err := parent.Get([]byte("child")); value != nil || !errors.Is(err, ErrIncompatibleValue) {
			t.Fatalf("nested bucket value: expected ErrIncompatibleValue, got value %q and error %v", value, err)
		}
		if value, err := parent.Get(nil); value != nil || !errors.Is(err, ErrKeyRequired) {
			t.Fatalf("empty key: expected ErrKeyRequired, got value %q and error %v", value, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteBucketVisitsMappedDescendants(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		parent, err := tx.CreateBucket([]byte("parent"))
		if err != nil {
			return err
		}
		child, err := parent.CreateBucket([]byte("child"))
		if err != nil {
			return err
		}
		for index := range 200 {
			if err := child.Put(fmt.Appendf(nil, "key-%03d", index), make([]byte, 200)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.DeleteBucket([]byte("parent")); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		_, err := tx.Bucket([]byte("parent"))
		return err
	}); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("deleted bucket: got %v, want ErrBucketNotFound", err)
	}
	if err := db.validatePageOwnership(); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteBucketTopLevelInvalidatesOldHandles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *Tx) error {
		old, err := tx.CreateBucket([]byte("users"))
		if err != nil {
			return err
		}
		if err := old.Put([]byte("old"), []byte("value")); err != nil {
			return err
		}
		cursor, err := old.Cursor()
		if err != nil {
			return err
		}
		if err := tx.DeleteBucket([]byte("users")); err != nil {
			return err
		}
		if _, err := tx.Bucket([]byte("users")); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("deleted bucket lookup: got %v, want ErrBucketNotFound", err)
		}
		if _, err := old.Get([]byte("old")); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("old handle read: got %v, want ErrBucketNotFound", err)
		}
		if err := old.Put([]byte("key"), []byte("value")); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("old handle write: got %v, want ErrBucketNotFound", err)
		}
		if _, err := old.Cursor(); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("old handle cursor: got %v, want ErrBucketNotFound", err)
		}
		if _, _, err := cursor.First(); !errors.Is(err, ErrCursorInvalidated) {
			t.Fatalf("old cursor: got %v, want ErrCursorInvalidated", err)
		}
		replacement, err := tx.CreateBucket([]byte("users"))
		if err != nil {
			return err
		}
		return replacement.Put([]byte("new"), []byte("value"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get([]byte("users"), []byte("new")); err != nil || !bytes.Equal(got, []byte("value")) {
		t.Fatalf("replacement value: got %q, error %v", got, err)
	}
	if _, err := db.Get([]byte("users"), []byte("old")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("old value: got %v, want ErrKeyNotFound", err)
	}
	if err := db.DeleteBucket([]byte("users")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("users"), []byte("new")); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("removed bucket: got %v, want ErrBucketNotFound", err)
	}
}

func TestDeleteBucketNestedReclaimsPagesAndRollsBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone, CheckpointThresholdBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	value := bytes.Repeat([]byte("v"), 400)
	err = db.Update(func(tx *Tx) error {
		parent, err := tx.CreateBucket([]byte("parent"))
		if err != nil {
			return err
		}
		if err := parent.Put([]byte("keep"), []byte("value")); err != nil {
			return err
		}
		child, err := parent.CreateBucket([]byte("child"))
		if err != nil {
			return err
		}
		grandchild, err := child.CreateBucket([]byte("grandchild"))
		if err != nil {
			return err
		}
		for index := range 100 {
			key := fmt.Appendf(nil, "key-%03d", index)
			if err := child.Put(key, value); err != nil {
				return err
			}
			if err := grandchild.Put(key, value); err != nil {
				return err
			}
		}
		_, err = tx.CreateBucket([]byte("anchor"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	before, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}

	rollback := errors.New("roll back bucket deletion")
	err = db.Update(func(tx *Tx) error {
		parent, err := tx.Bucket([]byte("parent"))
		if err != nil {
			return err
		}
		if err := parent.DeleteBucket([]byte("child")); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("rollback: got %v, want callback error", err)
	}
	afterRollback, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if afterRollback.AllocatedPageCount != before.AllocatedPageCount || afterRollback.PagesRetired != before.PagesRetired {
		t.Fatalf("rollback changed allocation: before %+v, after %+v", before, afterRollback)
	}
	if err := db.View(func(tx *Tx) error {
		parent, err := tx.Bucket([]byte("parent"))
		if err != nil {
			return err
		}
		child, err := parent.Bucket([]byte("child"))
		if err != nil {
			return err
		}
		grandchild, err := child.Bucket([]byte("grandchild"))
		if err != nil {
			return err
		}
		_, err = grandchild.Get([]byte("key-099"))
		return err
	}); err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(tx *Tx) error {
		parent, err := tx.Bucket([]byte("parent"))
		if err != nil {
			return err
		}
		child, err := parent.Bucket([]byte("child"))
		if err != nil {
			return err
		}
		grandchild, err := child.Bucket([]byte("grandchild"))
		if err != nil {
			return err
		}
		cursor, err := parent.Cursor()
		if err != nil {
			return err
		}
		if err := parent.DeleteBucket([]byte("child")); err != nil {
			return err
		}
		if _, err := grandchild.Get([]byte("key-099")); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("old descendant handle: got %v, want ErrBucketNotFound", err)
		}
		if err := grandchild.ScanRange(nil, nil, func([]byte, []byte) error { return nil }); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("old descendant scan: got %v, want ErrBucketNotFound", err)
		}
		if _, err := grandchild.CreateBucket([]byte("new")); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("old descendant create: got %v, want ErrBucketNotFound", err)
		}
		if _, _, err := cursor.First(); !errors.Is(err, ErrCursorInvalidated) {
			t.Fatalf("parent cursor: got %v, want ErrCursorInvalidated", err)
		}
		if _, err := parent.Bucket([]byte("child")); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("deleted child lookup: got %v, want ErrBucketNotFound", err)
		}
		got, err := parent.Get([]byte("keep"))
		if err != nil || !bytes.Equal(got, []byte("value")) {
			t.Fatalf("parent value: got %q, error %v", got, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	afterDelete, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if afterDelete.PagesRetired <= before.PagesRetired || afterDelete.ReusablePageCount == 0 {
		t.Fatalf("bucket pages were not retired: before %+v, after %+v", before, afterDelete)
	}
	if err := db.validatePageOwnership(); err != nil {
		t.Fatalf("page ownership after delete: %v", err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("replacement"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	afterReuse, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if afterReuse.PagesReused <= afterDelete.PagesReused {
		t.Fatalf("replacement did not reuse a page: before %+v, after %+v", afterDelete, afterReuse)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.validatePageOwnership(); err != nil {
		t.Fatalf("page ownership after reopen: %v", err)
	}
	if err := reopened.View(func(tx *Tx) error {
		parent, err := tx.Bucket([]byte("parent"))
		if err != nil {
			return err
		}
		_, err = parent.Bucket([]byte("child"))
		if !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("deleted child after reopen: got %v, want ErrBucketNotFound", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteBucketErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteBucket(nil); !errors.Is(err, ErrBucketNameRequired) {
		t.Fatalf("empty name: got %v, want ErrBucketNameRequired", err)
	}
	if err := db.DeleteBucket([]byte("missing")); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("missing bucket: got %v, want ErrBucketNotFound", err)
	}
	if err := db.Update(func(tx *Tx) error {
		parent, err := tx.CreateBucket([]byte("parent"))
		if err != nil {
			return err
		}
		if err := parent.Put([]byte("plain"), []byte("value")); err != nil {
			return err
		}
		if err := parent.DeleteBucket([]byte("plain")); !errors.Is(err, ErrIncompatibleValue) {
			t.Fatalf("plain value: got %v, want ErrIncompatibleValue", err)
		}
		if err := parent.DeleteBucket([]byte("missing")); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("missing child: got %v, want ErrBucketNotFound", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		if err := tx.DeleteBucket([]byte("parent")); !errors.Is(err, ErrTxNotWritable) {
			t.Fatalf("read-only transaction: got %v, want ErrTxNotWritable", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	readOnly, err := Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	if err := readOnly.DeleteBucket([]byte("parent")); !errors.Is(err, ErrDatabaseReadOnly) {
		t.Fatalf("read-only database: got %v, want ErrDatabaseReadOnly", err)
	}
}

func TestDeleteBucketRepairsNestedParentRoot(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "database"), 0600, &Options{Synchronous: SyncNone, CheckpointThresholdBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const count = 200
	name := func(index int) []byte {
		return fmt.Appendf(nil, "child-%03d-%s", index, strings.Repeat("x", 100))
	}
	err = db.Update(func(tx *Tx) error {
		parent, err := tx.CreateBucket([]byte("parent"))
		if err != nil {
			return err
		}
		for index := range count {
			if _, err := parent.CreateBucket(name(index)); err != nil {
				return err
			}
		}
		if parent.rootNode.IsLeaf() {
			t.Fatal("parent tree did not split")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *Tx) error {
		parent, err := tx.Bucket([]byte("parent"))
		if err != nil {
			return err
		}
		for index := range count - 1 {
			if err := parent.DeleteBucket(name(index)); err != nil {
				return err
			}
		}
		if !parent.rootNode.IsLeaf() {
			t.Fatal("parent root did not collapse")
		}
		_, err = parent.Bucket(name(count - 1))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.validatePageOwnership(); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		parent, err := tx.Bucket([]byte("parent"))
		if err != nil {
			return err
		}
		_, err = parent.Bucket(name(count - 1))
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteBucketRepairsCatalogRoot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone, CheckpointThresholdBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	name := func(index int) []byte {
		return fmt.Appendf(nil, "bucket-%03d-%s", index, strings.Repeat("x", 100))
	}
	const count = 200
	if err := db.Update(func(tx *Tx) error {
		for index := range count {
			if _, err := tx.CreateBucket(name(index)); err != nil {
				return err
			}
		}
		if tx.rootNode.IsLeaf() {
			t.Fatal("catalog tree did not split")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		for index := range count - 1 {
			if err := tx.DeleteBucket(name(index)); err != nil {
				return err
			}
		}
		if !tx.rootNode.IsLeaf() || tx.rootNode.PageID() != tx.meta.Root() {
			t.Fatal("catalog root did not collapse into metadata")
		}
		_, err := tx.Bucket(name(count - 1))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.validatePageOwnership(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.View(func(tx *Tx) error {
		_, err := tx.Bucket(name(count - 1))
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteBucketWALFailureKeepsTreeAndAllocation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncFull})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.wal.SetSyncFileForTesting(nil)
		if err := db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	})
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("data"))
		if err != nil {
			return err
		}
		for index := range 100 {
			if err := bucket.Put(fmt.Appendf(nil, "key-%03d", index), bytes.Repeat([]byte("v"), 300)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("WAL sync failed")
	db.wal.SetSyncFileForTesting(func() error { return wantErr })
	if err := db.DeleteBucket([]byte("data")); !errors.Is(err, wantErr) {
		t.Fatalf("failed delete: got %v, want %v", err, wantErr)
	}
	db.wal.SetSyncFileForTesting(nil)
	after, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if after.AllocatedPageCount != before.AllocatedPageCount || after.PagesRetired != before.PagesRetired {
		t.Fatalf("failed commit changed allocation: before %+v, after %+v", before, after)
	}
	db = reopenAfterWALWriteFailure(t, db, path, &Options{Synchronous: SyncFull})
	if value, err := db.Get([]byte("data"), []byte("key-099")); err != nil || len(value) != 300 {
		t.Fatalf("value after failed delete: length %d, error %v", len(value), err)
	}
}

// TestBucket_PutGetRoundTrip checks reads in the write transaction and after commit.
func TestBucket_PutGetRoundTrip(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("users"))
		if err != nil {
			return err
		}
		if err := b.Put([]byte("name"), []byte("alice")); err != nil {
			return err
		}
		if got := mustBucketValue(t, b, []byte("name")); !bytes.Equal(got, []byte("alice")) {
			t.Fatalf("read-your-writes in bucket failed: got %q", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	if err := db.View(func(tx *Tx) error {
		b := mustBucket(t, tx, []byte("users"))
		if got := mustBucketValue(t, b, []byte("name")); !bytes.Equal(got, []byte("alice")) {
			t.Fatalf("committed bucket value not visible: got %q", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestBucket_IsolatesSameKey checks that two buckets can store different values under the same key.
func TestBucket_IsolatesSameKey(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		a, err := tx.CreateBucket([]byte("A"))
		if err != nil {
			return err
		}
		b, err := tx.CreateBucket([]byte("B"))
		if err != nil {
			return err
		}
		if err := a.Put([]byte("k"), []byte("from-A")); err != nil {
			return err
		}
		return b.Put([]byte("k"), []byte("from-B"))
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	if err := db.View(func(tx *Tx) error {
		a := mustBucket(t, tx, []byte("A"))
		if got := mustBucketValue(t, a, []byte("k")); !bytes.Equal(got, []byte("from-A")) {
			t.Fatalf("bucket A leaked/collided: got %q, want from-A", got)
		}
		b := mustBucket(t, tx, []byte("B"))
		if got := mustBucketValue(t, b, []byte("k")); !bytes.Equal(got, []byte("from-B")) {
			t.Fatalf("bucket B leaked/collided: got %q, want from-B", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestBucket_MissingAndDoubleCreateErrors: opening an absent bucket returns
// ErrBucketNotFound; creating the same bucket twice returns ErrBucketExists.
func TestBucket_MissingAndDoubleCreateErrors(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.View(func(tx *Tx) error {
		if b, err := tx.Bucket([]byte("ghost")); b != nil || !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("missing bucket lookup: expected ErrBucketNotFound, got bucket %v and error %v", b, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.CreateBucket([]byte("dup")); err != nil {
			return err
		}
		if _, err := tx.CreateBucket([]byte("dup")); !errors.Is(err, ErrBucketExists) {
			t.Fatalf("second CreateBucket must return ErrBucketExists, got %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestBucket_SurvivesReopen: a bucket and its contents persist across close + reopen.
// Forces the new bucket's root page to be really materialized + persisted, and the
// name->pgid entry to survive, and reopen to descend from the stored pgid.
func TestBucket_SurvivesReopen(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("cfg"))
		if err != nil {
			return err
		}
		return b.Put([]byte("theme"), []byte("dark"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *Tx) error {
		b := mustBucket(t, tx, []byte("cfg"))
		if got := mustBucketValue(t, b, []byte("theme")); !bytes.Equal(got, []byte("dark")) {
			t.Fatalf("bucket value lost across reopen: got %q", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestBucket_SurvivesOwnSplit checks that a bucket keeps all values when its
// root changes from a leaf into a branch. A second small bucket checks
// that page allocation for the large bucket does not change another bucket.
func TestBucket_SurvivesOwnSplit(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}

	const n = 100
	val := bytes.Repeat([]byte("x"), 512) // 100 * ~525B ≈ 50 KB -> the bucket's tree splits
	if err := db.Update(func(tx *Tx) error {
		big, err := tx.CreateBucket([]byte("big"))
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := big.Put(fmt.Appendf(nil, "key-%05d", i), val); err != nil {
				return err
			}
		}
		small, err := tx.CreateBucket([]byte("small"))
		if err != nil {
			return err
		}
		return small.Put([]byte("only"), []byte("one"))
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	// The replacement root and its children must be readable in this session.
	if err := db.View(func(tx *Tx) error {
		big := mustBucket(t, tx, []byte("big"))
		for i := 0; i < n; i++ {
			if got := mustBucketValue(t, big, fmt.Appendf(nil, "key-%05d", i)); !bytes.Equal(got, val) {
				t.Fatalf("key %d unreadable after in-bucket split (in-session): got %d bytes", i, len(got))
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen must resolve the recorded replacement root page.
	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *Tx) error {
		big := mustBucket(t, tx, []byte("big"))
		if big == nil {
			t.Fatal("bucket \"big\" lost across reopen")
		}
		for i := 0; i < n; i++ {
			if got := mustBucketValue(t, big, fmt.Appendf(nil, "key-%05d", i)); !bytes.Equal(got, val) {
				t.Fatalf("key %d lost after reopening the split bucket", i)
			}
		}
		small := mustBucket(t, tx, []byte("small"))
		if got := mustBucketValue(t, small, []byte("only")); !bytes.Equal(got, []byte("one")) {
			t.Fatalf("sibling bucket clobbered by the big bucket's growth: got %q", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestBucket_ManyBucketsSurviveRootCatalogSplit checks a multi-level stable
// catalog root and a large bucket tree at the same time. Every bucket and value
// must remain readable in the transaction and after reopen.
func TestBucket_ManyBucketsSurviveRootCatalogSplit(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}

	const nBuckets = 1000 // ~1000 name->pgid entries overflow one page -> catalog splits
	const growTarget = 500
	const nBig = 100
	bigVal := bytes.Repeat([]byte("x"), 512) // 100*~525B -> the grown bucket's tree splits

	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < nBuckets; i++ {
			b, err := tx.CreateBucket(fmt.Appendf(nil, "bkt-%05d", i))
			if err != nil {
				return fmt.Errorf("create bucket %d: %w", i, err)
			}
			if err := b.Put([]byte("k"), fmt.Appendf(nil, "v-%05d", i)); err != nil {
				return err
			}
		}
		big := mustBucket(t, tx, fmt.Appendf(nil, "bkt-%05d", growTarget))
		if big == nil {
			t.Fatalf("bucket %d already lost mid-txn: catalog split dropped it", growTarget)
		}
		for j := 0; j < nBig; j++ {
			if err := big.Put(fmt.Appendf(nil, "big-%05d", j), bigVal); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	verify := func(t *testing.T, db *DB, when string) {
		t.Helper()
		if err := db.View(func(tx *Tx) error {
			for i := 0; i < nBuckets; i++ {
				b := mustBucket(t, tx, fmt.Appendf(nil, "bkt-%05d", i))
				if b == nil {
					t.Fatalf("%s: bucket %d lost — moved catalog root not written back", when, i)
				}
				if got := mustBucketValue(t, b, []byte("k")); !bytes.Equal(got, fmt.Appendf(nil, "v-%05d", i)) {
					t.Fatalf("%s: bucket %d value wrong: got %q", when, i, got)
				}
			}
			big := mustBucket(t, tx, fmt.Appendf(nil, "bkt-%05d", growTarget))
			for j := 0; j < nBig; j++ {
				if got := mustBucketValue(t, big, fmt.Appendf(nil, "big-%05d", j)); !bytes.Equal(got, bigVal) {
					t.Fatalf("%s: grown bucket key %d lost: got %d bytes", when, j, len(got))
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	verify(t, db, "in-session")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	verify(t, db, "after reopen")
}

// TestBucket_NestedInSplitParent checks child lookup after the parent tree splits.
// It checks the child before and after reopen.
func TestBucket_NestedInSplitParent(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}

	const n = 200
	val := bytes.Repeat([]byte("x"), 256) // fills the parent bucket past one page -> it splits
	if err := db.Update(func(tx *Tx) error {
		parent, err := tx.CreateBucket([]byte("parent"))
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := parent.Put(fmt.Appendf(nil, "pk-%05d", i), val); err != nil {
				return err
			}
		}
		if parent.rootNode.IsLeaf() {
			t.Fatal("test setup: parent bucket tree did not split")
		}
		child, err := parent.CreateBucket([]byte("child"))
		if err != nil {
			return fmt.Errorf("create nested child in a split parent: %w", err)
		}
		if err := child.Put([]byte("k"), []byte("childval")); err != nil {
			return err
		}
		// read-your-writes: the child must resolve through the branchy parent.
		c := mustNestedBucket(t, parent, []byte("child"))
		if c == nil {
			t.Fatal("nested child bucket not found through a split parent (non-descending lookup)")
		}
		if got := mustBucketValue(t, c, []byte("k")); !bytes.Equal(got, []byte("childval")) {
			t.Fatalf("nested child value wrong in-session: got %q", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// It must survive a reopen: parent resolves, and the child resolves under it.
	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *Tx) error {
		parent := mustBucket(t, tx, []byte("parent"))
		if parent == nil {
			t.Fatal("parent bucket lost across reopen")
		}
		c := mustNestedBucket(t, parent, []byte("child"))
		if c == nil {
			t.Fatal("nested child not resolved after reopen")
		}
		if got := mustBucketValue(t, c, []byte("k")); !bytes.Equal(got, []byte("childval")) {
			t.Fatalf("nested child value lost across reopen: got %q", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestBucket_Nested checks that values and nested buckets can share one parent.
// A nested bucket name does not act as a plain value. The child survives reopen.
func TestBucket_Nested(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		parent, err := tx.CreateBucket([]byte("parent"))
		if err != nil {
			return err
		}
		if err := parent.Put([]byte("pk"), []byte("pv")); err != nil { // a plain key alongside the sub-bucket
			return err
		}
		child, err := parent.CreateBucket([]byte("child"))
		if err != nil {
			return err
		}
		if err := child.Put([]byte("k"), []byte("childval")); err != nil {
			return err
		}
		if got := mustBucketValue(t, child, []byte("k")); !bytes.Equal(got, []byte("childval")) {
			t.Fatalf("nested read-your-writes failed: got %q", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	verify := func(t *testing.T, db *DB, when string) {
		t.Helper()
		if err := db.View(func(tx *Tx) error {
			p := mustBucket(t, tx, []byte("parent"))
			if p == nil {
				t.Fatalf("%s: parent bucket missing", when)
			}
			if got := mustBucketValue(t, p, []byte("pk")); !bytes.Equal(got, []byte("pv")) {
				t.Fatalf("%s: parent's plain key lost: got %q", when, got)
			}
			c := mustNestedBucket(t, p, []byte("child"))
			if c == nil {
				t.Fatalf("%s: child bucket not resolved", when)
			}
			if got := mustBucketValue(t, c, []byte("k")); !bytes.Equal(got, []byte("childval")) {
				t.Fatalf("%s: nested value lost: got %q", when, got)
			}
			// isolation: the child's key is NOT a plain key of the parent
			if got, err := p.Get([]byte("k")); got != nil || !errors.Is(err, ErrKeyNotFound) {
				t.Fatalf("%s: child key lookup in parent: expected ErrKeyNotFound, got value %q and error %v", when, got, err)
			}
			// A sub-bucket key is not a value.
			if got, err := p.Get([]byte("child")); got != nil || !errors.Is(err, ErrIncompatibleValue) {
				t.Fatalf("%s: nested bucket value lookup: expected ErrIncompatibleValue, got value %q and error %v", when, got, err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	verify(t, db, "in-session")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	verify(t, db, "after reopen")
}

// TestSplit_LargeValuesGetOwnNode checks that a split keeps one large value in each leaf.
// Each value fits in one page, but two values do not fit together.
func TestSplit_LargeValuesGetOwnNode(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}

	page := int(db.meta.PageSize())
	// Each value is ~0.6 of a page: one entry per node fits, two never do.
	vlen := page * 6 / 10
	const n = 6
	keys := make([][]byte, n)
	vals := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Appendf(nil, "key-%03d", i)
		vals[i] = bytes.Repeat([]byte{byte('a' + i)}, vlen)
		if err := db.Put(testBucketName, keys[i], vals[i]); err != nil {
			t.Fatalf("put %d (value %d bytes, page %d): %v", i, vlen, page, err)
		}
	}

	// Every node must fit one page and hold at least one entry (no empty leaf).
	walkNodes(t, db, mustBucketRoot(t, db, testBucketName), func(nd *btree.Node) {
		if sz := nd.EncodedSize(); sz > page {
			t.Fatalf("node serializes to %d bytes > one %d-byte page", sz, page)
		}
		if nd.EntryCount() == 0 {
			t.Fatalf("empty node in the tree (split produced a node with no entries)")
		}
	})

	verify := func(t *testing.T, db *DB, when string) {
		t.Helper()
		for i := 0; i < n; i++ {
			got, err := db.Get(testBucketName, keys[i])
			if err != nil {
				t.Fatalf("%s: get %d: %v", when, i, err)
			}
			if !bytes.Equal(got, vals[i]) {
				t.Fatalf("%s: value %d wrong: got %d bytes want %d", when, i, len(got), len(vals[i]))
			}
		}
	}
	verify(t, db, "in-session")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	verify(t, db, "after reopen")
}

// TestBucket_HandleSurvivesCatalogSplit checks that a live bucket handle remains
// usable while the catalog root changes from a leaf into a branch.
func TestBucket_HandleSurvivesCatalogSplit(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}

	const nCatalog = 1200 // enough name->pgid entries to split the catalog tree
	const nBig = 100
	bigVal := bytes.Repeat([]byte("x"), 512) // grows the held bucket past one page

	if err := db.Update(func(tx *Tx) error {
		// Create and HOLD the handle while the catalog is still one leaf.
		// The name sorts last, so a catalog split moves it to a new node.
		held, err := tx.CreateBucket([]byte("zzz-bucket"))
		if err != nil {
			return err
		}
		if err := held.Put([]byte("seed"), []byte("seed")); err != nil {
			return err
		}
		// Split the catalog with many smaller-sorting buckets.
		for i := 0; i < nCatalog; i++ {
			if _, err := tx.CreateBucket(fmt.Appendf(nil, "bkt-%05d", i)); err != nil {
				return fmt.Errorf("create catalog bucket %d: %w", i, err)
			}
		}
		// Now split the held bucket's own root.
		for j := 0; j < nBig; j++ {
			if err := held.Put(fmt.Appendf(nil, "big-%05d", j), bigVal); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	verify := func(t *testing.T, db *DB, when string) {
		t.Helper()
		if err := db.View(func(tx *Tx) error {
			b := mustBucket(t, tx, []byte("zzz-bucket"))
			if b == nil {
				t.Fatalf("%s: held bucket lost", when)
			}
			for j := 0; j < nBig; j++ {
				k := fmt.Appendf(nil, "big-%05d", j)
				if got := mustBucketValue(t, b, k); !bytes.Equal(got, bigVal) {
					t.Fatalf("%s: key %q lost through a held bucket handle: got %d bytes", when, k, len(got))
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	verify(t, db, "in-session")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	verify(t, db, "after reopen")
}

// TestSplit_LargeInsertKeepsEveryLeafWithinPage catches a split that leaves an
// oversized sibling after one large entry is added to a nearly full leaf.
func TestSplit_LargeInsertKeepsEveryLeafWithinPage(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}

	page := int(db.meta.PageSize())
	smallVal := bytes.Repeat([]byte("s"), 100)
	smallEntry := 16 + 6 + len(smallVal) // One leaf descriptor, the key, and the value.
	// Fill one leaf to ~0.9 of a page so it stays a single leaf before the big insert.
	nSmall := (page * 9 / 10) / smallEntry
	keys := make([][]byte, 0, nSmall+1)
	for i := 0; i < nSmall; i++ {
		k := fmt.Appendf(nil, "k%05d", i) // 6 bytes, all sort before the big key
		if err := db.Put(testBucketName, k, smallVal); err != nil {
			t.Fatalf("small put %d: %v", i, err)
		}
		keys = append(keys, k)
	}
	if !mustBucketRoot(t, db, testBucketName).IsLeaf() {
		t.Fatalf("setup: root split before the big insert (nSmall=%d too high)", nSmall)
	}

	// One medium value, sorts last, and forces the root leaf to split.
	bigKey := []byte("zzz-big")
	bigVal := bytes.Repeat([]byte("B"), page*85/100)
	if err := db.Put(testBucketName, bigKey, bigVal); err != nil {
		t.Fatalf("big put (the one a single-split loop rejects): %v", err)
	}
	keys = append(keys, bigKey)

	// The big insert must grow the tree, and every resulting node must fit.
	leaves := 0
	walkNodes(t, db, mustBucketRoot(t, db, testBucketName), func(nd *btree.Node) {
		if sz := nd.EncodedSize(); sz > page {
			t.Fatalf("node serializes to %d bytes > one %d-byte page", sz, page)
		}
		if nd.IsLeaf() {
			leaves++
		}
	})

	if leaves < 2 {
		t.Fatalf("expected the root to split into at least two leaves, got %d", leaves)
	}

	verify := func(t *testing.T, db *DB, when string) {
		t.Helper()
		for _, k := range keys {
			want := smallVal
			if bytes.Equal(k, bigKey) {
				want = bigVal
			}
			got, err := db.Get(testBucketName, k)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("%s: key %q wrong: err=%v got %d bytes want %d", when, k, err, len(got), len(want))
			}
		}
	}
	verify(t, db, "in-session")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	verify(t, db, "after reopen")
}

// TestCreateBucket_ParentRootSplitUpdatesCatalog catches nested bucket creation
// that does not write the parent bucket's replacement root into the catalog.
func TestCreateBucket_ParentRootSplitUpdatesCatalog(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *Tx) error {
		if _, err := tx.CreateBucket([]byte("p")); err != nil {
			return err
		}
		p := mustBucket(t, tx, []byte("p"))
		rootPageID := p.rootNode.PageID()

		pageSize := int(db.meta.PageSize())
		childName := bytes.Repeat([]byte("c"), 300) // The large name makes the bucket entry exceed the remaining space.
		fillVal := bytes.Repeat([]byte("x"), 200)

		// Pack p's single leaf as full as possible without overflowing it. Each fill
		// entry is ~220 bytes, so the leftover slack ends up smaller than the ~320-byte
		// nested bucket entry below, which guarantees that the insert overflows.
		var fillKeys [][]byte
		for i := 0; ; i++ {
			k := fmt.Appendf(nil, "fill-%06d", i)
			entrySize := 16 + len(k) + len(fillVal)
			if p.rootNode.EncodedSize()+entrySize > pageSize {
				break
			}
			if err := p.Put(k, fillVal); err != nil {
				return err
			}
			fillKeys = append(fillKeys, k)
		}
		if !p.rootNode.IsLeaf() {
			t.Fatal("setup error: p split during packing; it should still be one leaf")
		}

		if _, err := p.CreateBucket(childName); err != nil {
			return err
		}

		if p.rootNode.IsLeaf() {
			t.Fatal("nested bucket creation did not split the full parent root")
		}
		if got := p.rootNode.PageID(); got == rootPageID {
			t.Fatalf("parent root page ID after split: got old page %d, want a new page", got)
		}
		entry, found, err := tx.tree.FindEntryRef(tx.rootNode, []byte("p"), true)
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("catalog lost parent bucket after its root split")
		}
		catalogRoot, err := page.DecodeID(entry.Value())
		if err != nil {
			return err
		}
		if got, want := catalogRoot, p.rootNode.PageID(); got != want {
			t.Fatalf("catalog root page ID: got %d, want %d", got, want)
		}

		// Load a fresh handle through the updated catalog pointer.
		p2, err := tx.loadBucket(tx.rootNode, []byte("p"), nil)
		if err != nil {
			return err
		}
		if p2 == nil {
			t.Fatal("bucket \"p\" lost from catalog after its root split")
		}
		for _, k := range fillKeys {
			if got := mustBucketValue(t, p2, k); !bytes.Equal(got, fillVal) {
				t.Fatalf("key %q lost after stable parent split", k)
			}
		}
		child := mustNestedBucket(t, p2, childName)
		if child == nil {
			t.Fatal("child bucket pointer lost: it landed in the orphaned right half")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestWAL_BytesSinceCheckpointTracksWALSize checks the count after repeated writes to one page.
//
// BytesSinceCheckpoint starts a checkpoint at the set limit. Only the final page image enters the WAL.
// After this commit, the count must equal the WAL file size.
func TestWAL_BytesSinceCheckpointTracksWALSize(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Overwrite one key 100 times in one transaction. Only the final leaf page enters the WAL.
	err = db.Update(func(tx *Tx) error {
		for i := 0; i < 100; i++ {
			if err := mustBucket(t, tx, testBucketName).Put([]byte("k"), []byte("v")); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	walSize := fileSize(t, path+"-wal")
	if db.wal.Stats().BytesSinceCheckpoint != uint64(walSize) {
		t.Fatalf("bytesSinceCheckpoint=%d, want WAL file size %d (counter must track real WAL growth, not collectRecord calls)",
			db.wal.Stats().BytesSinceCheckpoint, walSize)
	}
}

// TestBucket_SharedHandleSeesWrites checks that two lookups use the same bucket handle.
//
// The first handle sees a root split. The second lookup must return that same handle.
func TestBucket_SharedHandleSeesWrites(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *Tx) error {
		if _, err := tx.CreateBucket([]byte("b")); err != nil {
			return err
		}
		h1 := mustBucket(t, tx, []byte("b"))
		h2 := mustBucket(t, tx, []byte("b")) // same cached handle as h1

		val := bytes.Repeat([]byte("x"), 200)
		for i := 0; i < 500; i++ {
			if err := h1.Put(fmt.Appendf(nil, "key-%08d", i), val); err != nil {
				return err
			}
		}
		for i := 0; i < 500; i++ {
			if got := mustBucketValue(t, h2, fmt.Appendf(nil, "key-%08d", i)); got == nil {
				t.Fatalf("second handle cannot see key %d written through the first handle", i)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBucket_NestedHandlesSurviveSiblingChanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *Tx) error {
		parent, err := tx.CreateBucket([]byte("parent"))
		if err != nil {
			return err
		}
		first, err := parent.CreateBucket([]byte("first"))
		if err != nil {
			return err
		}
		if got := mustNestedBucket(t, parent, []byte("first")); got != first {
			t.Fatal("first lookup returned a different handle")
		}
		second, err := parent.CreateBucket([]byte("second"))
		if err != nil {
			return err
		}
		if got := mustNestedBucket(t, parent, []byte("first")); got != first {
			t.Fatal("second child changed the first handle")
		}
		if got := mustNestedBucket(t, parent, []byte("second")); got != second {
			t.Fatal("second lookup returned a different handle")
		}
		if err := parent.DeleteBucket([]byte("first")); err != nil {
			return err
		}
		if _, err := first.Get([]byte("key")); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("deleted handle: got %v, want ErrBucketNotFound", err)
		}
		replacement, err := parent.CreateBucket([]byte("first"))
		if err != nil {
			return err
		}
		if got := mustNestedBucket(t, parent, []byte("first")); got != replacement || got == first {
			t.Fatal("recreated child returned an old handle")
		}
		if got := mustNestedBucket(t, parent, []byte("second")); got != second {
			t.Fatal("recreated child changed the second handle")
		}
		return replacement.Put([]byte("key"), []byte("value"))
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error {
		parent := mustBucket(t, tx, []byte("parent"))
		first := mustNestedBucket(t, parent, []byte("first"))
		if got := mustNestedBucket(t, parent, []byte("first")); got != first {
			t.Fatal("read transaction returned a different first handle")
		}
		second := mustNestedBucket(t, parent, []byte("second"))
		if got := mustNestedBucket(t, parent, []byte("second")); got != second {
			t.Fatal("read transaction returned a different second handle")
		}
		if got := mustBucketValue(t, first, []byte("key")); !bytes.Equal(got, []byte("value")) {
			t.Fatalf("recreated child value: got %q, want value", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestGet_NilValueIsFoundNotMissing checks that a stored empty value differs from a missing key.
func TestGet_NilValueIsFoundNotMissing(t *testing.T) {
	path := tempfile()
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put(testBucketName, []byte("k"), nil); err != nil {
		t.Fatalf("put nil value: %v", err)
	}
	v, err := db.Get(testBucketName, []byte("k"))
	if errors.Is(err, ErrKeyNotFound) {
		t.Fatal("key with a nil value read back as ErrKeyNotFound")
	}
	if err != nil {
		t.Fatalf("get nil value: %v", err)
	}
	// A found key never yields a nil value; an empty stored value comes back as a
	// zero-length slice.
	if v == nil {
		t.Fatal("found key returned a nil value")
	}
	if len(v) != 0 {
		t.Fatalf("expected zero-length value, got %q", v)
	}
}

func TestGet_EmptyValueRoundTrips(t *testing.T) {
	path := tempfile()
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put(testBucketName, []byte("k"), []byte{}); err != nil {
		t.Fatalf("put empty value: %v", err)
	}
	v, err := db.Get(testBucketName, []byte("k"))
	if err != nil {
		t.Fatalf("get empty value: %v", err)
	}
	if v == nil || len(v) != 0 {
		t.Fatalf("expected zero-length non-nil value, got %v", v)
	}
}

func TestGet_MissingStillErrKeyNotFound(t *testing.T) {
	path := tempfile()
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Get(testBucketName, []byte("absent")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound for an absent key, got %v", err)
	}
}

func TestTreeFindEntryRef_DistinguishesEmptyValueFromMissing(t *testing.T) {
	path := tempfile()
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put(testBucketName, []byte("present"), nil); err != nil {
		t.Fatalf("put empty value: %v", err)
	}

	err = db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		if err := bucket.loadRootNode(); err != nil {
			return err
		}
		root := bucket.rootNode
		entry, found, err := tx.tree.FindEntryRef(root, []byte("present"), true)
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("stored entry reported missing")
		}
		if entry.Value() == nil || len(entry.Value()) != 0 {
			t.Fatalf("stored empty value: got %v, want a non-nil empty slice", entry.Value())
		}

		_, found, err = tx.tree.FindEntryRef(root, []byte("missing"), true)
		if err != nil {
			return err
		}
		if found {
			t.Fatal("missing entry reported present")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("find tree entry: %v", err)
	}
}

// An empty value must also survive a checkpoint + reopen, not just an in-memory
// round-trip. This guards the encode/decode path, where a zero-length value could
// re-emerge as nil.
func TestGet_EmptyValueSurvivesReopen(t *testing.T) {
	path := tempfile()
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("k"), []byte{}); err != nil {
		t.Fatalf("put empty value: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := openDB(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	v, err := db2.Get(testBucketName, []byte("k"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if v == nil || len(v) != 0 {
		t.Fatalf("expected zero-length non-nil value after reopen, got %v", v)
	}
}

// A bucket Put must not overwrite a nested bucket entry inside its parent.
func TestBucketPut_RefusesOverwritingNestedBucket(t *testing.T) {
	path := tempfile()
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *Tx) error {
		parent, err := tx.CreateBucket([]byte("parent"))
		if err != nil {
			return err
		}
		if _, err := parent.CreateBucket([]byte("child")); err != nil {
			return err
		}
		// Overwriting the sub-bucket "child" with a raw value must be refused.
		if err := parent.Put([]byte("child"), []byte("raw")); !errors.Is(err, ErrIncompatibleValue) {
			t.Fatalf("bucket.Put over a nested bucket: expected ErrIncompatibleValue, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// The nested bucket must still resolve.
	if err := db.View(func(tx *Tx) error {
		parent := mustBucket(t, tx, []byte("parent"))
		if parent == nil {
			t.Fatal("parent bucket missing")
		}
		child := mustNestedBucket(t, parent, []byte("child"))
		if child == nil {
			t.Fatal("nested bucket was destroyed by the refused Put")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDBPut_OverwritesBucketValue(t *testing.T) {
	path := tempfile()
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put(testBucketName, []byte("k"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("k"), []byte("v2")); err != nil {
		t.Fatalf("overwrite plain key: %v", err)
	}
	v, err := db.Get(testBucketName, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte("v2")) {
		t.Fatalf("got %q want %q", v, "v2")
	}
}

func TestBucketCreateBucket_RefusesOverwritingNestedValue(t *testing.T) {
	path := tempfile()
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *Tx) error {
		parent, err := tx.CreateBucket([]byte("parent"))
		if err != nil {
			return err
		}
		if err := parent.Put([]byte("k"), []byte("plain-value")); err != nil {
			return err
		}
		if _, err := parent.CreateBucket([]byte("k")); !errors.Is(err, ErrIncompatibleValue) {
			t.Fatalf("nested CreateBucket over a plain key: expected ErrIncompatibleValue, got %v", err)
		}
		if got := mustBucketValue(t, parent, []byte("k")); !bytes.Equal(got, []byte("plain-value")) {
			t.Fatalf("value corrupted by the refused nested CreateBucket: got %q", got)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestWAL_OverlayRecordsCarryCommittedTxid checks the transaction ID on each committed WAL record.
func TestWAL_OverlayRecordsCarryCommittedTxid(t *testing.T) {
	path := tempfile()
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.checkpointWAL(); err != nil {
		t.Fatal(err)
	}

	if err := db.Put(testBucketName, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	wantTxid := db.wal.Stats().NextTxID - 1 // the txid the commit above was just stamped with
	records := db.wal.CommittedRecords()
	if len(records) == 0 {
		t.Fatal("expected at least one overlay record after a commit")
	}
	for pgid, record := range records {
		if record.Header.TxID != wantTxid {
			t.Fatalf("overlay record for pgid %d has txid %d, want %d (the committing transaction's txid)",
				pgid, record.Header.TxID, wantTxid)
		}
	}
}

// TestPut_EntryBeyondOnePage checks that a value can use continuation pages.
func TestPut_EntryBeyondOnePage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	value := bytes.Repeat([]byte("x"), int(db.meta.PageSize()))
	if err := db.Put(testBucketName, []byte("k"), value); err != nil {
		t.Fatal(err)
	}
	got, err := db.Get(testBucketName, []byte("k"))
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("large value: length=%d, error=%v", len(got), err)
	}
}

func TestPut_KeyTooLargeDoesNotChangeDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	before := page.EncodeMeta(db.meta)
	key := bytes.Repeat([]byte("k"), MaxKeySize+1)
	if err := db.Put(testBucketName, key, []byte("value")); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("Put: %v", err)
	}
	if !bytes.Equal(before, page.EncodeMeta(db.meta)) {
		t.Fatal("rejected Put changed metadata")
	}
	if err := db.View(func(tx *Tx) error {
		cursor, err := mustBucket(t, tx, testBucketName).Cursor()
		if err != nil {
			return err
		}
		key, _, err := cursor.First()
		if key != nil {
			t.Fatal("rejected Put added an entry")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

// TestCreateBucket_OwnsNameAfterCallerMutatesBuffer checks that the bucket does not keep the caller's mutable name slice.
func TestCreateBucket_OwnsNameAfterCallerMutatesBuffer(t *testing.T) {
	path := tempfile()
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const n = 100
	val := bytes.Repeat([]byte("x"), 512) // enough to split the bucket's own tree

	name := []byte("aaaa")
	err = db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket(name)
		if err != nil {
			return err
		}

		// Simulate the caller reusing its name buffer for something else, the way a
		// scratch key-building buffer would be reused across iterations.
		copy(name, "bbbb")

		for i := 0; i < n; i++ {
			if err := b.Put(fmt.Appendf(nil, "key-%05d", i), val); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	if err := db.View(func(tx *Tx) error {
		if bucket, err := tx.Bucket([]byte("bbbb")); bucket != nil || !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("phantom bucket lookup: expected ErrBucketNotFound, got bucket %v and error %v", bucket, err)
		}
		aaaa := mustBucket(t, tx, []byte("aaaa"))
		if aaaa == nil {
			t.Fatal("bucket \"aaaa\" lost after the caller changed its name buffer")
		}
		for i := 0; i < n; i++ {
			k := fmt.Appendf(nil, "key-%05d", i)
			if got := mustBucketValue(t, aaaa, k); !bytes.Equal(got, val) {
				t.Fatalf("key %q lost after the bucket root split", k)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucketCreateBucket_OwnsNameAfterCallerMutatesBuffer(t *testing.T) {
	path := tempfile()
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const n = 100
	val := bytes.Repeat([]byte("x"), 512) // enough to split the nested bucket's own tree

	name := []byte("aaaa")
	err = db.Update(func(tx *Tx) error {
		parent, err := tx.CreateBucket([]byte("parent"))
		if err != nil {
			return err
		}
		child, err := parent.CreateBucket(name)
		if err != nil {
			return err
		}

		copy(name, "bbbb")

		for i := 0; i < n; i++ {
			if err := child.Put(fmt.Appendf(nil, "key-%05d", i), val); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	if err := db.View(func(tx *Tx) error {
		parent := mustBucket(t, tx, []byte("parent"))
		if parent == nil {
			t.Fatal("parent bucket missing")
		}
		if b, err := parent.Bucket([]byte("bbbb")); b != nil || !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("phantom nested bucket lookup: expected ErrBucketNotFound, got bucket %v and error %v", b, err)
		}
		aaaa := mustNestedBucket(t, parent, []byte("aaaa"))
		if aaaa == nil {
			t.Fatal("nested bucket \"aaaa\" lost after the caller changed its name buffer")
		}
		for i := 0; i < n; i++ {
			k := fmt.Appendf(nil, "key-%05d", i)
			if got := mustBucketValue(t, aaaa, k); !bytes.Equal(got, val) {
				t.Fatalf("key %q lost after the nested bucket root split", k)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestUpdate_FailedCheckpointDoesNotLeakOverlayData checks that a checkpoint failure does not undo a committed WAL write.
func TestUpdate_FailedCheckpointDoesNotLeakOverlayData(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	// Large default threshold: this first Put must NOT auto-checkpoint, so its
	// content lives only in wal.overlay (not yet on the main file) — otherwise
	// this test's own setup would need the main file to be healthy too.
	db, err := Open(path, 0644, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	createBucket(t, db, testBucketName)

	if err := db.Put(testBucketName, []byte("k"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if db.wal.Stats().CommittedRecordCount == 0 {
		t.Fatal("test setup: expected the first put to sit in wal.overlay, not be checkpointed yet")
	}

	// Arrange for exactly the NEXT commit's growth to cross the checkpoint
	// threshold, so it (and only it) attempts a checkpoint.
	db.wal.SetCheckpointThresholdBytes(db.wal.Stats().BytesSinceCheckpoint + 1)

	// Simulate the main file failing right when the checkpoint tries to write to
	// it. wal.file (used for the WAL write itself) stays open, so the write
	// succeeds and wal.overlay gets updated before the checkpoint step fails.
	if err := db.file.Close(); err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(tx *Tx) error {
		return mustBucket(t, tx, testBucketName).Put([]byte("k"), []byte("new"))
	})
	if err != nil {
		t.Fatalf("durable WAL commit returned a checkpoint error: %v", err)
	}

	v, err := db.Get(testBucketName, []byte("k"))
	if err != nil {
		t.Fatalf("get after committed update: %v", err)
	}
	if !bytes.Equal(v, []byte("new")) {
		t.Fatalf("committed update returned %q, want %q", v, "new")
	}
}

func TestPut_FailedCheckpointDoesNotChangeReadableValue(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0644, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	createBucket(t, db, testBucketName)

	if err := db.Put(testBucketName, []byte("k"), []byte("old")); err != nil {
		t.Fatal(err)
	}

	db.wal.SetCheckpointThresholdBytes(db.wal.Stats().BytesSinceCheckpoint + 1)
	if err := db.file.Close(); err != nil {
		t.Fatal(err)
	}

	if err := db.Put(testBucketName, []byte("k"), []byte("new")); err != nil {
		t.Fatalf("durable WAL commit returned a checkpoint error: %v", err)
	}

	got, err := db.Get(testBucketName, []byte("k"))
	if err != nil {
		t.Fatalf("get after committed Put: %v", err)
	}
	if !bytes.Equal(got, []byte("new")) {
		t.Fatalf("committed Put returned %q, want %q", got, "new")
	}

	if err := db.unmapMainFile(); err != nil {
		t.Fatal(err)
	}
	if err := db.wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, 0644, &Options{LockTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	got, err = reopened.Get(testBucketName, []byte("k"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if !bytes.Equal(got, []byte("new")) {
		t.Fatalf("committed Put recovered value %q after reopen, want %q", got, "new")
	}
}

func TestAudit_TwoWritableHandlesDoNotLoseCommittedData(t *testing.T) {
	t.Skip("deferred until kvlite implements multiple-writer coordination")

	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	first, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := first.Put(testBucketName, []byte("first"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := second.Put(testBucketName, []byte("second"), []byte("two")); err != nil {
		t.Fatal(err)
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	_ = second.Close()
	_ = second.file.Close()

	reopened, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	for key, want := range map[string]string{"first": "one", "second": "two"} {
		got, err := reopened.Get(testBucketName, []byte(key))
		if err != nil || !bytes.Equal(got, []byte(want)) {
			t.Fatalf("committed key %q was lost: got %q, err %v", key, got, err)
		}
	}
}

func TestAudit_GoexitRollsBackUpdate(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = db.Update(func(tx *Tx) error {
			if err := mustBucket(t, tx, testBucketName).Put([]byte("doomed"), []byte("value")); err != nil {
				panic(err)
			}
			runtime.Goexit()
			return nil
		})
	}()
	<-done

	if _, err := db.Get(testBucketName, []byte("doomed")); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("unfinished Update changed the database: expected ErrKeyNotFound, got %v", err)
	}
	if err := db.Put(testBucketName, []byte("keep"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Get(testBucketName, []byte("doomed")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("unfinished Update was persisted: expected ErrKeyNotFound, got %v", err)
	}
}

func TestAudit_MidWALChecksumFailureStopsRecovery(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	mainBefore, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("first"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("second"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	walBytes, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	_ = db.wal.Close()
	_ = db.file.Close()

	if len(walBytes) < wal.HeaderSize {
		t.Fatalf("WAL is too small: %d bytes", len(walBytes))
	}
	const fileHeaderSize = 8
	contentSize := int(binary.LittleEndian.Uint32(walBytes[fileHeaderSize+17 : fileHeaderSize+21]))
	checksumOffset := fileHeaderSize + wal.HeaderSize + contentSize
	if checksumOffset >= len(walBytes)-1 {
		t.Fatalf("first record has no later WAL data: checksum offset %d, WAL size %d", checksumOffset, len(walBytes))
	}
	walBytes[checksumOffset] ^= 0xff

	crashPath := tempfile()
	defer os.RemoveAll(crashPath)
	defer os.RemoveAll(crashPath + "-wal")
	if err := os.WriteFile(crashPath, mainBefore, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(crashPath+"-wal", walBytes, 0600); err != nil {
		t.Fatal(err)
	}

	recovered, err := openDB(crashPath)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	for _, key := range []string{"first", "second"} {
		if _, err := recovered.Get(testBucketName, []byte(key)); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("replayed key %q after damaged WAL record: %v", key, err)
		}
	}
}

func TestAudit_FinalWALChecksumFailureDiscardsTransaction(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	mainBefore, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	walBytes, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	_ = db.wal.Close()
	_ = db.file.Close()

	if len(walBytes) == 0 {
		t.Fatal("WAL is empty")
	}
	// The last byte belongs to the final record checksum. XOR with 0xff flips every bit.
	walBytes[len(walBytes)-1] ^= 0xff

	crashPath := tempfile()
	defer os.RemoveAll(crashPath)
	defer os.RemoveAll(crashPath + "-wal")
	if err := os.WriteFile(crashPath, mainBefore, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(crashPath+"-wal", walBytes, 0600); err != nil {
		t.Fatal(err)
	}

	recovered, err := openDB(crashPath)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if _, err := recovered.Get(testBucketName, []byte("key")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("replayed transaction with damaged commit marker: %v", err)
	}
}

func TestAudit_GetAfterCloseReturnsDatabaseNotOpen(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Get(testBucketName, []byte("key")); !errors.Is(err, ErrDatabaseNotOpen) {
		t.Fatalf("Get after Close: expected ErrDatabaseNotOpen, got %v", err)
	}
}

func TestUpdate_SerializesConcurrentCallbacks(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const updateCount = 16
	start := make(chan struct{})
	errorsByUpdate := make(chan error, updateCount)
	var ready sync.WaitGroup
	var done sync.WaitGroup
	var active atomic.Int64
	var maximumActive atomic.Int64
	ready.Add(updateCount)
	done.Add(updateCount)

	for range updateCount {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			errorsByUpdate <- db.Update(func(*Tx) error {
				current := active.Add(1)
				for {
					maximum := maximumActive.Load()
					if current <= maximum || maximumActive.CompareAndSwap(maximum, current) {
						break
					}
				}
				time.Sleep(10 * time.Millisecond)
				active.Add(-1)
				return nil
			})
		}()
	}

	ready.Wait()
	close(start)
	done.Wait()
	close(errorsByUpdate)
	for err := range errorsByUpdate {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := maximumActive.Load(); got != 1 {
		t.Fatalf("maximum concurrent update callbacks: got %d, want 1", got)
	}
}

func TestView_AllowsConcurrentCallbacks(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseCallbacks := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseCallbacks()
	errorsByView := make(chan error, 2)
	firstEntered := make(chan struct{})
	secondEntered := make(chan struct{})

	go func() {
		errorsByView <- db.View(func(*Tx) error {
			close(firstEntered)
			<-release
			return nil
		})
	}()
	<-firstEntered

	go func() {
		errorsByView <- db.View(func(*Tx) error {
			close(secondEntered)
			<-release
			return nil
		})
	}()

	select {
	case <-secondEntered:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("second View callback did not enter while the first callback was active")
	}
	releaseCallbacks()
	for range 2 {
		if err := <-errorsByView; err != nil {
			t.Fatal(err)
		}
	}
}

func TestGet_AllowsConcurrentReads(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	key := []byte("key")
	want := []byte("value")
	if err := db.Put(testBucketName, key, want); err != nil {
		t.Fatal(err)
	}

	const readerCount = 16
	const readsPerReader = 100
	start := make(chan struct{})
	errorsByReader := make(chan error, readerCount)
	var readers sync.WaitGroup
	readers.Add(readerCount)
	for range readerCount {
		go func() {
			defer readers.Done()
			<-start
			for range readsPerReader {
				got, err := db.Get(testBucketName, key)
				if err != nil {
					errorsByReader <- err
					return
				}
				if !bytes.Equal(got, want) {
					errorsByReader <- fmt.Errorf("value: got %q, want %q", got, want)
					return
				}
			}
		}()
	}
	close(start)
	readers.Wait()
	close(errorsByReader)
	for err := range errorsByReader {
		t.Fatal(err)
	}
}

func TestUpdate_WaitsForActiveView(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	releaseView := make(chan struct{})
	viewEntered := make(chan struct{})
	viewDone := make(chan error, 1)
	go func() {
		viewDone <- db.View(func(*Tx) error {
			close(viewEntered)
			<-releaseView
			return nil
		})
	}()
	<-viewEntered

	updateEntered := make(chan struct{})
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- db.Update(func(*Tx) error {
			close(updateEntered)
			return nil
		})
	}()
	select {
	case <-updateEntered:
		close(releaseView)
		t.Fatal("Update callback entered while a View callback was active")
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseView)
	if err := <-viewDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-updateEntered:
	case <-time.After(time.Second):
		t.Fatal("Update callback did not enter after the View callback finished")
	}
	if err := <-updateDone; err != nil {
		t.Fatal(err)
	}
}

func TestUpdate_GroupsConcurrentDurableWrites(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.wal.SetSyncFileForTesting(nil)
		_ = db.Close()
	}()
	createBucket(t, db, testBucketName)
	if err := db.checkpointWAL(); err != nil {
		t.Fatal(err)
	}

	var syncCalls atomic.Int64
	// The first sync waits, as a slow storage sync does, so the other writes wait in the request queue.
	// A sync without a delay lets the scheduler decide how many writes each batch gets.
	syncing := make(chan struct{})
	release := make(chan struct{})
	db.wal.SetSyncFileForTesting(func() error {
		if syncCalls.Add(1) == 1 {
			close(syncing)
			<-release
		}
		return nil
	})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()

	const writeCount = 32
	errorsByWrite := make(chan error, writeCount)
	put := func(index int) {
		key := fmt.Appendf(nil, "key-%02d", index)
		errorsByWrite <- db.Put(testBucketName, key, []byte("value"))
	}
	go put(0)
	<-syncing
	for index := 1; index < writeCount; index++ {
		go put(index)
	}

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(db.writeRequests) < writeCount-1 {
		select {
		case <-deadline.C:
			t.Fatalf("request queue during the first sync: got %d writes, want %d", len(db.writeRequests), writeCount-1)
		default:
			runtime.Gosched()
		}
	}
	close(release)
	for range writeCount {
		if err := <-errorsByWrite; err != nil {
			t.Fatal(err)
		}
	}
	db.wal.SetSyncFileForTesting(nil)

	// The first write syncs the header of the new WAL generation and its own commit. The other writes share one sync.
	if got, want := syncCalls.Load(), int64(3); got != want {
		t.Fatalf("WAL sync calls for %d concurrent writes: got %d, want %d", writeCount, got, want)
	}
	records, err := db.wal.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	transactionIDs := make(map[wal.TxID]struct{})
	commitMarkers := 0
	for _, record := range records {
		transactionIDs[record.Header.TxID] = struct{}{}
		if wal.IsCommitMarker(&record) {
			commitMarkers++
		}
	}
	// This checkpoint does not sync its WAL truncate, so the first commit after it also syncs the header of the new WAL generation.
	if got, want := len(transactionIDs), int(syncCalls.Load())-1; got != want {
		t.Fatalf("WAL transactions: got %d, want one for each of %d syncs after the header sync", got, want)
	}
	if commitMarkers != len(transactionIDs) {
		t.Fatalf("WAL commit markers: got %d, want %d", commitMarkers, len(transactionIDs))
	}
	for index := range writeCount {
		key := fmt.Appendf(nil, "key-%02d", index)
		if value, err := db.Get(testBucketName, key); err != nil || !bytes.Equal(value, []byte("value")) {
			t.Fatalf("read %q after grouped commit: value=%q error=%v", key, value, err)
		}
	}
}

func TestUpdate_GroupsWriteThatArrivesDuringCallback(t *testing.T) {
	for _, test := range []struct {
		name      string
		limit     int
		wantSyncs int64
	}{
		{name: "one", limit: 1, wantSyncs: 2},
		{name: "two", limit: 2, wantSyncs: 1},
		{name: "default", wantSyncs: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := tempfile()
			defer os.RemoveAll(path)
			defer os.RemoveAll(path + "-wal")

			db, err := Open(path, 0600, &Options{MaxWriteBatchSize: test.limit})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				db.wal.SetSyncFileForTesting(nil)
				_ = db.Close()
			}()
			createBucket(t, db, testBucketName)

			var syncCalls atomic.Int64
			db.wal.SetSyncFileForTesting(func() error {
				syncCalls.Add(1)
				return nil
			})

			entered := make(chan struct{})
			release := make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			firstDone := make(chan error, 1)
			go func() {
				firstDone <- db.Update(func(tx *Tx) error {
					bucket, err := tx.Bucket(testBucketName)
					if err != nil {
						return err
					}
					if err := bucket.Put([]byte("first"), []byte("value")); err != nil {
						return err
					}
					close(entered)
					<-release
					return nil
				})
			}()
			<-entered

			secondDone := make(chan error, 1)
			go func() {
				secondDone <- db.Update(func(tx *Tx) error {
					bucket, err := tx.Bucket(testBucketName)
					if err != nil {
						return err
					}
					if value, err := bucket.Get([]byte("first")); err != nil || !bytes.Equal(value, []byte("value")) {
						return fmt.Errorf("second callback read first write: value=%q error=%v", value, err)
					}
					return bucket.Put([]byte("second"), []byte("value"))
				})
			}()

			deadline := time.NewTimer(time.Second)
			defer deadline.Stop()
			for len(db.writeRequests) == 0 {
				select {
				case <-deadline.C:
					t.Fatal("second write did not enter the request queue")
				default:
					runtime.Gosched()
				}
			}
			close(release)
			if err := <-firstDone; err != nil {
				t.Fatal(err)
			}
			if err := <-secondDone; err != nil {
				t.Fatal(err)
			}
			if got := syncCalls.Load(); got != test.wantSyncs {
				t.Fatalf("WAL sync calls: got %d, want %d", got, test.wantSyncs)
			}
		})
	}
}

func TestUpdate_DurableGoexitDoesNotStopWriteBatcher(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	createBucket(t, db, testBucketName)

	updateDone := make(chan struct{})
	go func() {
		defer close(updateDone)
		_ = db.Update(func(tx *Tx) error {
			if err := mustBucket(t, tx, testBucketName).Put([]byte("doomed"), []byte("value")); err != nil {
				return err
			}
			runtime.Goexit()
			return nil
		})
	}()
	<-updateDone

	select {
	case <-db.writeBatcherDone:
		t.Fatal("runtime.Goexit in an Update callback stopped the write batcher")
	default:
	}
	if _, err := db.Get(testBucketName, []byte("doomed")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Goexit update changed the database: got %v, want ErrKeyNotFound", err)
	}
}

func TestExecuteWriteCallback_UsesAtMostTwoAllocations(t *testing.T) {
	tx := &Tx{}
	allocations := testing.AllocsPerRun(100, func() {
		tx.closed = false
		_ = executeWriteCallback(func(*Tx) error { return nil }, tx)
	})
	if allocations > 2 {
		t.Fatalf("execute write callback allocations: got %v, want at most 2", allocations)
	}
}

func TestWriteBatch_LaterCallbackSeesEarlierWrite(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	createBucket(t, db, testBucketName)

	requests := []*writeRequest{
		{
			transaction: func(tx *Tx) error {
				return mustBucket(t, tx, testBucketName).Put([]byte("first"), []byte("one"))
			},
			result: make(chan writeResult, 1),
		},
		{
			transaction: func(tx *Tx) error {
				bucket := mustBucket(t, tx, testBucketName)
				value, err := bucket.Get([]byte("first"))
				if err != nil {
					return err
				}
				if !bytes.Equal(value, []byte("one")) {
					t.Fatalf("earlier batch value: got %q, want %q", value, "one")
				}
				return bucket.Put([]byte("second"), []byte("two"))
			},
			result: make(chan writeResult, 1),
		},
	}

	db.operationMu.Lock()
	db.executeWriteBatch(requests)
	db.operationMu.Unlock()
	for _, request := range requests {
		if result := <-request.result; result.err != nil || result.panicked || result.goexited {
			t.Fatalf("write result: %+v", result)
		}
	}
}

func TestWriteBatch_RetirementReusesPrivateAllocationPage(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "database"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	original := db.allocation.segments[0].data
	wantOriginal := bytes.Clone(original)
	var private *allocationSegment
	request := &writeRequest{
		transaction: func(tx *Tx) error {
			name := []byte("temporary")
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
			if err := tx.DeleteBucket(name); err != nil {
				return err
			}
			private = tx.allocation.dirtySegments[0]
			return nil
		},
		result: make(chan writeResult, 1),
	}
	db.operationMu.Lock()
	db.executeWriteBatch([]*writeRequest{request})
	db.operationMu.Unlock()
	if result := <-request.result; result.err != nil || result.panicked || result.goexited {
		t.Fatalf("write result: %+v", result)
	}
	if private == nil || &private.data[0] == &original[0] {
		t.Fatal("callback did not make a private allocation page")
	}
	if &db.allocation.segments[0].data[0] != &private.data[0] {
		t.Fatal("batch preparation copied the private allocation page again")
	}
	if !bytes.Equal(original, wantOriginal) {
		t.Fatal("batch preparation changed the original allocation page")
	}
	if db.allocation.allocated(4) {
		t.Fatal("retired bucket page is still allocated")
	}
}

func TestWriteBatch_CallbackErrorDiscardsOnlyItsChanges(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	createBucket(t, db, testBucketName)

	wantErr := errors.New("discard callback")
	requests := []*writeRequest{
		{
			transaction: func(tx *Tx) error {
				return mustBucket(t, tx, testBucketName).Put([]byte("kept-first"), []byte("one"))
			},
			result: make(chan writeResult, 1),
		},
		{
			transaction: func(tx *Tx) error {
				if err := mustBucket(t, tx, testBucketName).Put([]byte("discarded"), []byte("two")); err != nil {
					return err
				}
				return wantErr
			},
			result: make(chan writeResult, 1),
		},
		{
			transaction: func(tx *Tx) error {
				return mustBucket(t, tx, testBucketName).Put([]byte("kept-last"), []byte("three"))
			},
			result: make(chan writeResult, 1),
		},
	}

	db.operationMu.Lock()
	db.executeWriteBatch(requests)
	db.operationMu.Unlock()
	if result := <-requests[0].result; result.err != nil {
		t.Fatalf("first callback: %v", result.err)
	}
	if result := <-requests[1].result; !errors.Is(result.err, wantErr) {
		t.Fatalf("failed callback: got %v, want %v", result.err, wantErr)
	}
	if result := <-requests[2].result; result.err != nil {
		t.Fatalf("last callback: %v", result.err)
	}

	for _, key := range [][]byte{[]byte("kept-first"), []byte("kept-last")} {
		if _, err := db.Get(testBucketName, key); err != nil {
			t.Fatalf("read kept key %q: %v", key, err)
		}
	}
	if _, err := db.Get(testBucketName, []byte("discarded")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("discarded callback value: got %v, want ErrKeyNotFound", err)
	}
}

func TestWriteBatch_WALFailurePublishesNoCallback(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.wal.SetSyncFileForTesting(nil)
		_ = db.Close()
	}()
	createBucket(t, db, testBucketName)
	if err := db.checkpointWAL(); err != nil {
		t.Fatal(err)
	}

	wantErr := errors.New("batch sync failed")
	db.wal.SetSyncFileForTesting(func() error { return wantErr })
	requests := []*writeRequest{
		{
			transaction: func(tx *Tx) error {
				return mustBucket(t, tx, testBucketName).Put([]byte("first"), []byte("one"))
			},
			result: make(chan writeResult, 1),
		},
		{
			transaction: func(tx *Tx) error {
				return mustBucket(t, tx, testBucketName).Put([]byte("second"), []byte("two"))
			},
			result: make(chan writeResult, 1),
		},
	}

	db.operationMu.Lock()
	db.executeWriteBatch(requests)
	db.operationMu.Unlock()
	for _, request := range requests {
		if result := <-request.result; !errors.Is(result.err, wantErr) {
			t.Fatalf("write error: got %v, want %v", result.err, wantErr)
		}
	}
	db = reopenAfterWALWriteFailure(t, db, path, nil)
	for _, key := range [][]byte{[]byte("first"), []byte("second")} {
		if _, err := db.Get(testBucketName, key); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("value %q after failed batch: got %v, want ErrKeyNotFound", key, err)
		}
	}
}

func TestWriteBatch_WALFailureReplacesErrorAfterPendingWrite(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.wal.SetSyncFileForTesting(nil)
		_ = db.Close()
	}()
	createBucket(t, db, testBucketName)
	if err := db.checkpointWAL(); err != nil {
		t.Fatal(err)
	}

	wantErr := errors.New("batch sync failed")
	errKeyExists := errors.New("key exists")
	db.wal.SetSyncFileForTesting(func() error { return wantErr })
	requests := []*writeRequest{
		{
			transaction: func(tx *Tx) error {
				return mustBucket(t, tx, testBucketName).Put([]byte("first"), []byte("one"))
			},
			result: make(chan writeResult, 1),
		},
		{
			// This callback sees the pending write from the first callback and fails because of it.
			transaction: func(tx *Tx) error {
				if _, err := mustBucket(t, tx, testBucketName).Get([]byte("first")); err == nil {
					return errKeyExists
				}
				return nil
			},
			result: make(chan writeResult, 1),
		},
	}

	db.operationMu.Lock()
	db.executeWriteBatch(requests)
	db.operationMu.Unlock()
	if result := <-requests[0].result; !errors.Is(result.err, wantErr) {
		t.Fatalf("write error: got %v, want %v", result.err, wantErr)
	}
	result := <-requests[1].result
	db = reopenAfterWALWriteFailure(t, db, path, nil)
	if _, err := db.Get(testBucketName, []byte("first")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("value after failed batch: got %v, want ErrKeyNotFound", err)
	}
	if !errors.Is(result.err, wantErr) || errors.Is(result.err, errKeyExists) {
		t.Fatalf("callback error after failed batch: got %v, want only %v", result.err, wantErr)
	}
}

func TestWriteBatch_WALFailureKeepsResultsWithoutPendingDependency(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.wal.SetSyncFileForTesting(nil)
		_ = db.Close()
	}()
	createBucket(t, db, testBucketName)
	if err := db.checkpointWAL(); err != nil {
		t.Fatal(err)
	}

	wantErr := errors.New("batch sync failed")
	errOwn := errors.New("callback failed before pending writes")
	db.wal.SetSyncFileForTesting(func() error { return wantErr })
	requests := []*writeRequest{
		{
			// This callback runs before any pending write, so it sees only committed state.
			transaction: func(tx *Tx) error {
				return errOwn
			},
			result: make(chan writeResult, 1),
		},
		{
			transaction: func(tx *Tx) error {
				return mustBucket(t, tx, testBucketName).Put([]byte("first"), []byte("one"))
			},
			result: make(chan writeResult, 1),
		},
		{
			transaction: func(tx *Tx) error {
				panic("callback panic")
			},
			result: make(chan writeResult, 1),
		},
	}

	db.operationMu.Lock()
	db.executeWriteBatch(requests)
	db.operationMu.Unlock()
	if result := <-requests[0].result; !errors.Is(result.err, errOwn) || errors.Is(result.err, wantErr) {
		t.Fatalf("callback before pending writes: got %v, want only %v", result.err, errOwn)
	}
	if result := <-requests[1].result; !errors.Is(result.err, wantErr) {
		t.Fatalf("write error: got %v, want %v", result.err, wantErr)
	}
	if result := <-requests[2].result; !result.panicked || result.panicValue != "callback panic" || result.err != nil {
		t.Fatalf("panic after pending write: got %+v, want the unchanged panic", result)
	}
}

// failFirstRemoveFS fails the first Remove.
// With removeAnyway, that Remove also removes the name, as an unlink that fails after its directory change does.
type failFirstRemoveFS struct {
	fileio.OSFS
	err          error
	removeAnyway bool
	failed       bool
}

func (fsys *failFirstRemoveFS) Remove(name string) error {
	if !fsys.failed {
		fsys.failed = true
		if fsys.removeAnyway {
			if err := fsys.OSFS.Remove(name); err != nil {
				return err
			}
		}
		return fsys.err
	}
	return fsys.OSFS.Remove(name)
}

// Close documents that the caller can retry after an error.
// Blackout found that after a failed WAL remove, every retry failed with "file already closed".
func TestClose_RetryAfterWALRemoveFailure(t *testing.T) {
	for _, removeAnyway := range []bool{false, true} {
		t.Run(fmt.Sprintf("removed=%t", removeAnyway), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			removeErr := errors.New("remove failed")
			db, err := open(path, 0600, nil, &failFirstRemoveFS{err: removeErr, removeAnyway: removeAnyway})
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Update(func(tx *Tx) error {
				bucket, err := tx.CreateBucket(testBucketName)
				if err != nil {
					return err
				}
				return bucket.Put([]byte("key"), []byte("value"))
			}); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); !errors.Is(err, removeErr) {
				t.Fatalf("first Close: got %v, want %v", err, removeErr)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("Close retry: %v", err)
			}
			if _, err := os.Stat(path + "-wal"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("WAL after Close retry: %v", err)
			}
			db, err = Open(path, 0600, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if value, err := db.Get(testBucketName, []byte("key")); err != nil || string(value) != "value" {
				t.Fatalf("Get after reopen: %q, %v", value, err)
			}
		})
	}
}

func TestClose_WaitsForActiveUpdate(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	createBucket(t, db, testBucketName)

	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- db.Update(func(tx *Tx) error {
			close(callbackStarted)
			<-releaseCallback
			bucket, err := tx.Bucket(testBucketName)
			if err != nil {
				return err
			}
			return bucket.Put([]byte("key"), []byte("value"))
		})
	}()
	<-callbackStarted

	closeDone := make(chan error, 1)
	go func() { closeDone <- db.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before the active Update ended: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseCallback)
	if err := <-updateDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if value, err := reopened.Get(testBucketName, []byte("key")); err != nil || !bytes.Equal(value, []byte("value")) {
		t.Fatalf("value after concurrent Close: value=%q error=%v", value, err)
	}
}

func TestClose_WithConcurrentGets(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	const readers = 16
	ready := make(chan error, readers)
	done := make(chan error, readers)
	for range readers {
		go func() {
			value, err := db.Get(testBucketName, []byte("key"))
			if err == nil && !bytes.Equal(value, []byte("value")) {
				err = fmt.Errorf("Get returned %q", value)
			}
			ready <- err
			if err != nil {
				done <- err
				return
			}
			for {
				_, err := db.Get(testBucketName, []byte("key"))
				if errors.Is(err, ErrDatabaseNotOpen) {
					done <- nil
					return
				}
				if err != nil {
					done <- err
					return
				}
			}
		}()
	}
	for range readers {
		if err := <-ready; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for range readers {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestView_DoesNotCreatePrivateNodeMaps(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.View(func(tx *Tx) error {
		if tx.store.nodes != nil || tx.store.dirty != nil {
			t.Fatal("read transaction created private node maps")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestAudit_RejectedTxPutDoesNotChangeReadableState(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	large := bytes.Repeat([]byte("x"), MaxKeySize+1)
	err = db.Update(func(tx *Tx) error {
		if err := mustBucket(t, tx, testBucketName).Put(large, nil); !errors.Is(err, ErrKeyTooLarge) {
			t.Fatalf("expected ErrKeyTooLarge, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("callback returned nil, but Update returned %v", err)
	}

	if _, err := db.Get(testBucketName, []byte("rejected")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("rejected Put changed readable state: expected ErrKeyNotFound, got %v", err)
	}
}

func TestAudit_CommittedWALSurvivesCheckpointFailure(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("key"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := db.checkpointWAL(); err != nil {
		t.Fatal(err)
	}
	db.wal.SetCheckpointThresholdBytes(1)
	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		if err := db.file.Close(); err != nil {
			return err
		}
		return bucket.Put([]byte("key"), []byte("new"))
	})
	if err != nil {
		t.Fatalf("committed WAL update returned a checkpoint error: %v", err)
	}
	if got, err := db.Get(testBucketName, []byte("key")); err != nil || !bytes.Equal(got, []byte("new")) {
		t.Fatalf("committed overlay value: got %q, err %v", got, err)
	}
	if err := db.unmapMainFile(); err != nil {
		t.Fatal(err)
	}
	if err := db.wal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, 0600, &Options{Synchronous: SyncNone, LockTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Get(testBucketName, []byte("key")); err != nil || !bytes.Equal(got, []byte("new")) {
		t.Fatalf("recovered committed value: got %q, err %v", got, err)
	}
}

func TestAudit_AutomaticCheckpointUsesSelectedSyncMode(t *testing.T) {
	tests := []struct {
		name      string
		mode      Sync
		syncCalls int
	}{
		// SyncNormal syncs the WAL before the checkpoint copies it, and it syncs the WAL truncate after the checkpoint.
		{name: "normal", mode: SyncNormal, syncCalls: 2},
		{name: "none", mode: SyncNone, syncCalls: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			db, err := Open(path, 0600, &Options{Synchronous: test.mode})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				db.wal.SetSyncFileForTesting(nil)
				_ = db.Close()
			}()
			createBucket(t, db, testBucketName)

			// One byte makes every non-empty WAL append cross the checkpoint threshold.
			db.wal.SetCheckpointThresholdBytes(1)
			syncCalls := 0
			db.wal.SetSyncFileForTesting(func() error {
				syncCalls++
				return nil
			})

			if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			if got := fileSize(t, path+"-wal"); got != 0 {
				t.Fatalf("WAL size after automatic checkpoint: got %d, want 0", got)
			}
			if syncCalls != test.syncCalls {
				t.Fatalf("automatic checkpoint WAL sync calls: got %d, want %d", syncCalls, test.syncCalls)
			}
		})
	}
}

func TestAudit_CloseAfterFullCommitDoesNotResyncWAL(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if !db.closed {
			db.wal.SetSyncFileForTesting(nil)
			_ = db.Close()
		}
	}()
	createBucket(t, db, testBucketName)

	syncCalls := 0
	db.wal.SetSyncFileForTesting(func() error {
		syncCalls++
		return nil
	})

	if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if syncCalls != 1 {
		t.Fatalf("full commit and close WAL sync calls: got %d, want 1", syncCalls)
	}
}

func TestAudit_CloseDoesNotSyncWALInSyncNoneMode(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if !db.closed {
			db.wal.SetSyncFileForTesting(nil)
			_ = db.Close()
		}
	}()

	syncCalls := 0
	db.wal.SetSyncFileForTesting(func() error {
		syncCalls++
		return nil
	})

	if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if syncCalls != 0 {
		t.Fatalf("SyncNone commit and close WAL sync calls: got %d, want 0", syncCalls)
	}
}

func TestAudit_CloseSyncsWALInSyncNormalMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{
		Synchronous:              SyncNormal,
		CheckpointThresholdBytes: 1 << 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if !db.closed {
			db.wal.SetSyncFileForTesting(nil)
			_ = db.Close()
		}
	}()
	createBucket(t, db, testBucketName)

	syncCalls := 0
	db.wal.SetSyncFileForTesting(func() error {
		syncCalls++
		return nil
	})
	if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if syncCalls != 0 {
		t.Fatalf("commit sync calls: got %d, want 0", syncCalls)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if syncCalls != 1 {
		t.Fatalf("commit and close WAL sync calls: got %d, want 1", syncCalls)
	}
}

func TestAudit_FailedSyncRestoresPriorWALSyncState(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if !db.closed {
			db.wal.SetSyncFileForTesting(nil)
			_ = db.Close()
		}
	}()
	createBucket(t, db, testBucketName)

	if err := db.Put(testBucketName, []byte("stable"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	syncErr := errors.New("injected WAL sync failure")
	syncCalls := 0
	db.wal.SetSyncFileForTesting(func() error {
		syncCalls++
		// The first hooked sync belongs to the failed commit, and the second to its rollback. Any later call is redundant.
		if syncCalls == 1 {
			return syncErr
		}
		return nil
	})

	if err := db.Put(testBucketName, []byte("failed"), []byte("value")); !errors.Is(err, syncErr) {
		t.Fatalf("Put error: got %v, want %v", err, syncErr)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if syncCalls != 2 {
		t.Fatalf("failed commit, rollback, and close WAL sync calls: got %d, want 2", syncCalls)
	}
}

func TestAudit_WALSyncFailureRollsBackBeforePublication(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	createBucket(t, db, testBucketName)

	if err := db.Put(testBucketName, []byte("key"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := db.checkpointWAL(); err != nil {
		t.Fatal(err)
	}

	walBefore, err := db.wal.FileForTesting().Stat()
	if err != nil {
		t.Fatal(err)
	}
	db.wal.SetCheckpointThresholdBytes(1)
	syncErr := errors.New("injected WAL sync failure")
	syncCalls := 0
	publishedBeforeSync := false
	db.wal.SetSyncFileForTesting(func() error {
		syncCalls++
		stats := db.wal.Stats()
		publishedBeforeSync = stats.CommittedRecordCount != 0
		return syncErr
	})

	err = db.Put(testBucketName, []byte("key"), []byte("new"))
	db.wal.SetSyncFileForTesting(nil)
	if !errors.Is(err, syncErr) {
		t.Fatalf("Put error: got %v, want %v", err, syncErr)
	}
	// The failed commit syncs, and its rollback syncs the truncate.
	if syncCalls != 2 {
		t.Fatalf("WAL sync calls: got %d, want 2", syncCalls)
	}
	if publishedBeforeSync {
		t.Fatal("transaction was published to the overlay before WAL sync succeeded")
	}
	stats := db.wal.Stats()
	if stats.CommittedRecordCount != 0 {
		t.Fatalf("failed transaction was published: committed records=%d",
			stats.CommittedRecordCount)
	}
	if stats.BytesSinceCheckpoint != 0 {
		t.Fatalf("failed transaction byte count: got %d, want 0", stats.BytesSinceCheckpoint)
	}
	walAfter, err := db.wal.FileForTesting().Stat()
	if err != nil {
		t.Fatal(err)
	}
	if walAfter.Size() != walBefore.Size() {
		t.Fatalf("failed transaction WAL size: got %d, want %d", walAfter.Size(), walBefore.Size())
	}
	db = reopenAfterWALWriteFailure(t, db, path, &Options{Synchronous: SyncNormal})
	if got, err := db.Get(testBucketName, []byte("key")); err != nil || !bytes.Equal(got, []byte("old")) {
		t.Fatalf("value after WAL sync failure: got %q, err %v", got, err)
	}
}

func TestUpdate_WALWriteFailureStopsDatabaseUntilReopen(t *testing.T) {
	for _, test := range []struct {
		name        string
		synchronous Sync
	}{
		// SyncFull commits through the write batch and fails in the WAL sync.
		{name: "sync failure", synchronous: SyncFull},
		// SyncNormal commits directly and fails in the WAL write.
		{name: "write failure", synchronous: SyncNormal},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			db, err := Open(path, 0600, &Options{Synchronous: test.synchronous})
			if err != nil {
				t.Fatal(err)
			}
			createBucket(t, db, testBucketName)
			if err := db.Put(testBucketName, []byte("saved"), []byte("value")); err != nil {
				t.Fatal(err)
			}

			injected := errors.New("injected WAL sync failure")
			var restore func()
			if test.synchronous == SyncFull {
				db.wal.SetSyncFileForTesting(func() error { return injected })
				restore = func() { db.wal.SetSyncFileForTesting(nil) }
			} else {
				writable := db.wal.FileForTesting()
				readOnly, err := os.Open(path + "-wal")
				if err != nil {
					t.Fatal(err)
				}
				db.wal.ReplaceFileForTesting(readOnly)
				restore = func() {
					db.wal.ReplaceFileForTesting(writable)
					_ = readOnly.Close()
				}
			}

			err = db.Put(testBucketName, []byte("failed"), []byte("value"))
			// The storage works again. The handle must still stop, because the failed commit can be on disk.
			restore()
			if !errors.Is(err, ErrWALWriteFailed) {
				t.Fatalf("failed commit: got %v, want ErrWALWriteFailed", err)
			}
			if test.synchronous == SyncFull && !errors.Is(err, injected) {
				t.Fatalf("failed commit: got %v, want the sync error %v", err, injected)
			}

			callbackRan := false
			err = db.Update(func(tx *Tx) error {
				callbackRan = true
				return nil
			})
			if !errors.Is(err, ErrWALWriteFailed) || callbackRan {
				t.Fatalf("Update after WAL failure: callback ran=%v, error=%v, want ErrWALWriteFailed without the callback", callbackRan, err)
			}
			if err := db.View(func(tx *Tx) error { return nil }); !errors.Is(err, ErrWALWriteFailed) {
				t.Fatalf("View after WAL failure: got %v, want ErrWALWriteFailed", err)
			}
			if _, err := db.Get(testBucketName, []byte("saved")); !errors.Is(err, ErrWALWriteFailed) {
				t.Fatalf("Get after WAL failure: got %v, want ErrWALWriteFailed", err)
			}
			if err := db.Backup(filepath.Join(t.TempDir(), "backup")); !errors.Is(err, ErrWALWriteFailed) {
				t.Fatalf("Backup after WAL failure: got %v, want ErrWALWriteFailed", err)
			}
			if _, err := db.Stats(); err != nil {
				t.Fatalf("Stats after WAL failure: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("Close after WAL failure: %v", err)
			}

			reopened, err := Open(path, 0600, &Options{Synchronous: test.synchronous})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if got, err := reopened.Get(testBucketName, []byte("saved")); err != nil || !bytes.Equal(got, []byte("value")) {
				t.Fatalf("saved value after reopen: got %q, error %v", got, err)
			}
			if _, err := reopened.Get(testBucketName, []byte("failed")); !errors.Is(err, ErrKeyNotFound) {
				t.Fatalf("failed value after reopen: got %v, want ErrKeyNotFound", err)
			}
			if err := reopened.Put(testBucketName, []byte("after"), []byte("value")); err != nil {
				t.Fatalf("Put after reopen: %v", err)
			}
		})
	}
}

// reopenAfterWALWriteFailure checks that db stopped after a failed WAL append. It closes db and opens path again with options.
func reopenAfterWALWriteFailure(t *testing.T, db *DB, path string, options *Options) *DB {
	t.Helper()
	if err := db.View(func(*Tx) error { return nil }); !errors.Is(err, ErrWALWriteFailed) {
		t.Fatalf("View after WAL write failure: got %v, want ErrWALWriteFailed", err)
	}
	db.wal.SetSyncFileForTesting(nil)
	if err := db.Close(); err != nil {
		t.Fatalf("close after WAL write failure: %v", err)
	}
	reopened, err := Open(path, 0600, options)
	if err != nil {
		t.Fatalf("reopen after WAL write failure: %v", err)
	}
	return reopened
}

func TestAudit_CheckpointFailureRetriesOnNextCommit(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("baseline"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.checkpointWAL(); err != nil {
		t.Fatal(err)
	}
	db.wal.SetCheckpointThresholdBytes(1)
	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		if err := db.file.Close(); err != nil {
			return err
		}
		return bucket.Put([]byte("first"), []byte("one"))
	})
	if err != nil {
		t.Fatalf("first committed WAL update returned a checkpoint error: %v", err)
	}
	stats := db.wal.Stats()
	if stats.CommittedRecordCount == 0 || stats.BytesSinceCheckpoint == 0 {
		t.Fatalf("failed checkpoint did not retain committed state: overlay=%d, bytes=%d",
			stats.CommittedRecordCount, stats.BytesSinceCheckpoint)
	}
	walBeforeRetry, err := db.wal.FileForTesting().Stat()
	if err != nil {
		t.Fatal(err)
	}
	if walBeforeRetry.Size() == 0 {
		t.Fatal("failed checkpoint did not retain the committed WAL")
	}

	db.file, err = os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("second"), []byte("two")); err != nil {
		t.Fatalf("second commit did not retry the checkpoint: %v", err)
	}
	stats = db.wal.Stats()
	if stats.CommittedRecordCount != 0 || stats.BytesSinceCheckpoint != 0 {
		t.Fatalf("checkpoint retry did not drain committed state: overlay=%d, bytes=%d",
			stats.CommittedRecordCount, stats.BytesSinceCheckpoint)
	}
	walAfterRetry, err := db.wal.FileForTesting().Stat()
	if err != nil {
		t.Fatal(err)
	}
	if walAfterRetry.Size() != 0 {
		t.Fatalf("checkpoint retry did not truncate WAL: size=%d", walAfterRetry.Size())
	}
	for key, want := range map[string]string{"first": "one", "second": "two"} {
		if got, err := db.Get(testBucketName, []byte(key)); err != nil || !bytes.Equal(got, []byte(want)) {
			t.Fatalf("value %q after checkpoint retry: got %q, err %v", key, got, err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for key, want := range map[string]string{"first": "one", "second": "two"} {
		if got, err := reopened.Get(testBucketName, []byte(key)); err != nil || !bytes.Equal(got, []byte(want)) {
			t.Fatalf("reopened value %q after checkpoint retry: got %q, err %v", key, got, err)
		}
	}
}

func TestAudit_ValidWALRecoversDamagedMainMeta(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.unmapMainFile(); err != nil {
		t.Fatal(err)
	}
	_ = db.wal.Close()
	_ = db.file.Close()

	mainBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mainBytes[24] ^= 0xff
	mainBytes[db.meta.PageSize()+24] ^= 0xff
	if err := os.WriteFile(path, mainBytes, 0600); err != nil {
		t.Fatal(err)
	}

	recovered, err := Open(path, 0600, &Options{Synchronous: SyncNone, LockTimeout: time.Second})
	if err != nil {
		t.Fatalf("valid WAL did not recover damaged main metadata: %v", err)
	}
	defer recovered.Close()
	got, err := recovered.Get(testBucketName, []byte("key"))
	if err != nil || !bytes.Equal(got, []byte("value")) {
		t.Fatalf("recovered value: got %q, err %v", got, err)
	}
}

func TestAudit_MainNodeLookupCannotCrossPageBoundary(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("k")
	if err := db.Put(testBucketName, key, []byte("v")); err != nil {
		t.Fatal(err)
	}
	pageSize := db.meta.PageSize()
	bucketPageID := mustBucketRoot(t, db, testBucketName).PageID()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Truncate(info.Size() + pageSize); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}

	pageData := make([]byte, pageSize)
	pageOffset := int64(bucketPageID) * pageSize
	if _, err := file.ReadAt(pageData, pageOffset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	const leafDescriptorValueSizeFieldOffset = 12
	valueSizeOffset := btree.NodeHeaderSize + leafDescriptorValueSizeFieldOffset
	binary.LittleEndian.PutUint32(pageData[valueSizeOffset:valueSizeOffset+4], uint32(pageSize))

	if _, err := file.WriteAt(pageData, pageOffset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Get(testBucketName, key); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Get error: got %v, want ErrInvalid for a node value that crossed its page boundary", err)
	}
}

func TestAudit_CommitMarkerMustMatchRecordTransaction(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("key"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	firstWAL, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.checkpointWAL(); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("key"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	secondWAL, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.unmapMainFile(); err != nil {
		t.Fatal(err)
	}
	_ = db.wal.Close()
	_ = db.file.Close()

	commitMarkerSize := wal.HeaderSize + wal.ChecksumSize
	if len(firstWAL) <= commitMarkerSize || len(secondWAL) <= commitMarkerSize {
		t.Fatal("WAL transaction is too small")
	}
	firstMarker := firstWAL[len(firstWAL)-commitMarkerSize:]
	if firstMarker[0] != byte(wal.RecordTypeCommit) {
		t.Fatal("first WAL does not end with a commit marker")
	}
	secondRecords := secondWAL[:len(secondWAL)-commitMarkerSize]
	mixedWAL := append(bytes.Clone(secondRecords), firstMarker...)
	if err := os.WriteFile(path+"-wal", mixedWAL, 0600); err != nil {
		t.Fatal(err)
	}

	recovered, err := Open(path, 0600, &Options{Synchronous: SyncNone, LockTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	got, err := recovered.Get(testBucketName, []byte("key"))
	if err != nil || !bytes.Equal(got, []byte("old")) {
		t.Fatalf("mismatched commit marker committed another transaction: got %q, err %v", got, err)
	}
}

func TestAudit_RecoveryFailureKeepsCommittedWAL(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.wal.FileForTesting().(*os.File).Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	records, err := db.wal.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	if records == nil {
		t.Fatal("committed WAL has no records")
	}
	committed := committedWALRecords(records)

	db.wal.ClearCommittedRecordsForTesting()
	if err := db.file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.ingestWalRecords(committed); err == nil {
		t.Fatal("expected WAL replay to fail on the closed main file")
	}
	_ = db.closeFiles()

	if _, err := os.Stat(path + "-wal"); err != nil {
		t.Fatalf("failed recovery removed the committed WAL: %v", err)
	}
}

func TestDeleteBatchAcrossLeaves(t *testing.T) {
	path := filepath.Join(t.TempDir(), "batch.db")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	name := []byte("batch")
	keys := make([][]byte, 600)
	value := bytes.Repeat([]byte("v"), 80)
	for index := range keys {
		keys[index] = fmt.Appendf(nil, "key-%04d", index)
	}
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket(name)
		if err != nil {
			return err
		}
		for _, key := range keys {
			if err := bucket.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	order := rand.New(rand.NewSource(42)).Perm(len(keys))
	for first := 0; first < len(order); first += 75 {
		batch := make([][]byte, 0, 78)
		for _, index := range order[first : first+75] {
			batch = append(batch, keys[index])
		}
		batch = append(batch, batch[0], []byte("missing"), batch[0])
		if first/75%2 == 0 {
			slices.SortFunc(batch, bytes.Compare)
		}
		if err := db.Update(func(tx *Tx) error {
			bucket, err := tx.Bucket(name)
			if err != nil {
				return err
			}
			return bucket.DeleteBatch(batch)
		}); err != nil {
			t.Fatalf("batch %d: %v", first/75, err)
		}
		if err := db.View(func(tx *Tx) error {
			bucket, err := tx.Bucket(name)
			if err != nil {
				return err
			}
			for _, index := range order[:first+75] {
				if _, err := bucket.Get(keys[index]); !errors.Is(err, ErrKeyNotFound) {
					return fmt.Errorf("deleted key %q: %w", keys[index], err)
				}
			}
			for _, index := range order[first+75:] {
				if got, err := bucket.Get(keys[index]); err != nil || !bytes.Equal(got, value) {
					return fmt.Errorf("remaining key %q: value %q, error %v", keys[index], got, err)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(name)
		if err != nil {
			return err
		}
		for _, key := range keys {
			if _, err := bucket.Get(key); !errors.Is(err, ErrKeyNotFound) {
				return fmt.Errorf("reopened deleted key %q: %v", key, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteBatchRejectsBucketAndRollsBack(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "batch.db"), 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("parent"))
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte("a-plain"), []byte("value")); err != nil {
			return err
		}
		for index := range 200 {
			if err := bucket.Put(fmt.Appendf(nil, "middle-%03d", index), []byte("value")); err != nil {
				return err
			}
		}
		_, err = bucket.CreateBucket([]byte("z-child"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket([]byte("parent"))
		if err != nil {
			return err
		}
		return bucket.DeleteBatch([][]byte{[]byte("a-plain"), []byte("z-child")})
	})
	if !errors.Is(err, ErrIncompatibleValue) {
		t.Fatalf("delete nested bucket: %v", err)
	}
	if err := db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket([]byte("parent"))
		if err != nil {
			return err
		}
		if value, err := bucket.Get([]byte("a-plain")); err != nil || string(value) != "value" {
			return fmt.Errorf("rollback plain key: %q, %v", value, err)
		}
		_, err = bucket.Bucket([]byte("z-child"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

// TestPut_ContinuedTree covers leaf and branch writes through both commit paths.
func TestPut_ContinuedTree(t *testing.T) {
	for _, mode := range []Sync{SyncNone, SyncFull} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "continued.db")
			options := &Options{Synchronous: mode, CheckpointThresholdBytes: 1 << 30}
			db, err := Open(path, 0600, options)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { db.Close() }()
			keys := make([][]byte, 20)
			values := make([][]byte, len(keys))
			// The last value spans hundreds of continuation pages.
			for i := range keys {
				keys[i] = bytes.Repeat([]byte{byte('a' + i)}, MaxKeySize)
				values[i] = bytes.Repeat([]byte{byte(i)}, 3*int(db.meta.PageSize())+i)
			}
			values[len(values)-1] = bytes.Repeat([]byte("v"), 520*int(db.meta.PageSize()))
			if err := db.Update(func(tx *Tx) error {
				bucket, err := tx.CreateBucket(testBucketName)
				if err != nil {
					return err
				}
				for i := range keys {
					if err := bucket.Put(keys[i], values[i]); err != nil {
						return err
					}
					got, err := bucket.Get(keys[i])
					if err != nil || !bytes.Equal(got, values[i]) {
						t.Fatalf("transaction read %d: %v", i, err)
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			check := func() {
				t.Helper()
				for i := range keys {
					got, err := db.Get(testBucketName, keys[i])
					if err != nil || !bytes.Equal(got, values[i]) {
						t.Fatalf("Get %d: length=%d, error=%v", i, len(got), err)
					}
				}
				if err := db.View(func(tx *Tx) error {
					bucket := mustBucket(t, tx, testBucketName)
					cursor, err := bucket.Cursor()
					if err != nil {
						return err
					}
					index := 0
					for key, value, err := cursor.First(); key != nil || err != nil; key, value, err = cursor.Next() {
						if err != nil {
							return err
						}
						if index >= len(keys) || !bytes.Equal(key, keys[index]) || !bytes.Equal(value, values[index]) {
							t.Fatalf("cursor entry %d differs", index)
						}
						index++
					}
					if index != len(keys) {
						t.Fatalf("cursor count: %d", index)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			check()
			oldBranches := make(map[page.ID][]page.ID)
			walkNodes(t, db, mustBucketRoot(t, db, testBucketName), func(node *btree.Node) {
				if !node.IsLeaf() {
					oldBranches[node.PageID()] = slices.Clone(node.OverflowPages())
				}
			})
			// Another leaf split grows a committed branch with large separator keys.
			key := bytes.Repeat([]byte("z"), MaxKeySize)
			value := []byte("last")
			if err := db.Put(testBucketName, key, value); err != nil {
				t.Fatal(err)
			}
			keys, values = append(keys, key), append(values, value)
			grew := false
			walkNodes(t, db, mustBucketRoot(t, db, testBucketName), func(node *btree.Node) {
				previous, found := oldBranches[node.PageID()]
				if !found {
					return
				}
				ids := node.OverflowPages()
				kept := min(len(ids), len(previous))
				if !slices.Equal(ids[:kept], previous[:kept]) {
					t.Fatal("branch update replaced existing continuation IDs")
				}
				grew = grew || len(ids) > len(previous)
				delete(oldBranches, node.PageID())
			})
			if !grew || len(oldBranches) != 0 {
				t.Fatalf("branch growth=%t, missing original branches=%d", grew, len(oldBranches))
			}
			check()
			leaves, branches := 0, 0
			walkNodes(t, db, mustBucketRoot(t, db, testBucketName), func(node *btree.Node) {
				if len(node.OverflowPages()) == 0 {
					t.Fatal("large node has no continuation pages")
				}
				if node.IsLeaf() {
					leaves++
				} else {
					branches++
					if len(node.Children) < 2 {
						t.Fatal("branch has fewer than two children")
					}
				}
			})
			if leaves != len(keys) || branches < 2 {
				t.Fatalf("tree shape: %d leaves, %d branches", leaves, branches)
			}
			// Rebuild encoded WAL records. Cached decoded nodes must not hide read errors.
			records, err := db.wal.ReadRecords()
			if err != nil {
				t.Fatal(err)
			}
			committed := committedWALRecords(records)
			db.wal.ClearCommittedRecordsForTesting()
			if err := db.loadCommittedIntoOverlay(committed); err != nil {
				t.Fatal(err)
			}
			check()
			if err := db.checkpointWAL(); err != nil {
				t.Fatal(err)
			}
			check()
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = Open(path, 0600, options)
			if err != nil {
				t.Fatal(err)
			}
			check()
		})
	}
}

// TestPut_ContinuationPageOwnership checks growth, splits, replacement, and reuse.
func TestPut_ContinuationPageOwnership(t *testing.T) {
	for _, mode := range []Sync{SyncNone, SyncFull} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			db, err := Open(filepath.Join(t.TempDir(), "database"), 0600, &Options{Synchronous: mode, CheckpointThresholdBytes: 1 << 30})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := db.Update(func(tx *Tx) error {
				_, err := tx.CreateBucket(testBucketName)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			small := []byte("small")
			large := bytes.Repeat([]byte("v"), 3*int(db.meta.PageSize()))
			put := func(key string, value []byte) {
				t.Helper()
				if err := db.Put(testBucketName, []byte(key), value); err != nil {
					t.Fatal(err)
				}
				got, err := db.Get(testBucketName, []byte(key))
				if err != nil || !bytes.Equal(got, value) {
					t.Fatalf("Get %s: %v", key, err)
				}
			}
			findLeaf := func(key string) *btree.Node {
				t.Helper()
				node := mustBucketRoot(t, db, testBucketName)
				for !node.IsLeaf() {
					_, _, child, err := btree.LookupNode(node, []byte(key))
					if err != nil {
						t.Fatal(err)
					}
					node, err = db.readNode(child)
					if err != nil {
						t.Fatal(err)
					}
				}
				return node
			}
			checkReleased := func(ids []page.ID) {
				t.Helper()
				for _, id := range ids {
					if db.allocation.allocated(id) {
						t.Fatalf("old continuation %d is still allocated", id)
					}
					if _, ok := db.wal.Lookup(id); ok {
						t.Fatalf("old continuation %d is still in the WAL view", id)
					}
				}
			}
			put("a", small)
			head := findLeaf("a").PageID()
			put("a", large)
			first := findLeaf("a")
			if first.PageID() != head || len(first.OverflowPages()) == 0 {
				t.Fatal("growth changed first page or omitted continuation pages")
			}
			old := slices.Clone(first.OverflowPages())
			if err := db.checkpointWAL(); err != nil {
				t.Fatal(err)
			}
			last := db.meta.LastPage()
			reusedBefore, retiredBefore := db.stats.PagesReused, db.stats.PagesRetired
			replacement := bytes.Repeat([]byte("r"), len(large))
			put("a", replacement)
			if !slices.Equal(findLeaf("a").OverflowPages(), old) {
				t.Fatal("same-size replacement changed continuation IDs")
			}
			if db.meta.LastPage() != last || db.stats.PagesReused != reusedBefore || db.stats.PagesRetired != retiredBefore {
				t.Fatal("same-size replacement changed allocation state")
			}
			if !slices.Equal(first.OverflowPages(), old) {
				t.Fatal("replacement changed the original node's IDs")
			}
			put("b", large)
			if !slices.Equal(findLeaf("a").OverflowPages(), old) {
				t.Fatal("split replaced the left node's continuation IDs")
			}
			first = findLeaf("a")
			if first.PageID() != head {
				t.Fatal("split changed the left page ID")
			}
			old = slices.Clone(first.OverflowPages())
			put("a", small)
			first = findLeaf("a")
			if first.PageID() != head || len(first.OverflowPages()) != 0 {
				t.Fatal("shrink kept continuation pages or changed first page")
			}
			checkReleased(old)
			second := findLeaf("b")
			secondHead := second.PageID()
			secondOld := slices.Clone(second.OverflowPages())
			// Keep existing IDs and append reused low IDs. The list must have a gap.
			larger := bytes.Repeat([]byte("w"), 6*int(db.meta.PageSize()))
			put("b", larger)
			second = findLeaf("b")
			if second.PageID() != secondHead {
				t.Fatal("replacement changed first page")
			}
			ids := second.OverflowPages()
			if len(ids) <= len(secondOld) || !slices.Equal(ids[:len(secondOld)], secondOld) {
				t.Fatal("growth replaced existing continuation IDs")
			}
			reused, gap := false, false
			for i, id := range ids {
				if slices.Contains(old, id) {
					reused = true
				}
				if i > 0 && id != ids[i-1]+1 {
					gap = true
				}
				if !db.allocation.allocated(id) {
					t.Fatalf("new continuation %d is free", id)
				}
			}
			if !reused || !gap {
				t.Fatalf("reuse=%t, non-consecutive IDs=%t", reused, gap)
			}
			old = slices.Clone(ids)
			put("b", bytes.Repeat([]byte("s"), 2*int(db.meta.PageSize())))
			kept := slices.Clone(findLeaf("b").OverflowPages())
			if len(kept) == 0 || len(kept) >= len(old) || !slices.Equal(kept, old[:len(kept)]) {
				t.Fatal("shrink replaced the kept continuation IDs")
			}
			checkReleased(old[len(kept):])
			old = kept
			put("b", small)
			checkReleased(old)
			if err := db.checkpointWAL(); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"a", "b"} {
				value, err := db.Get(testBucketName, []byte(key))
				if err != nil || !bytes.Equal(value, small) {
					t.Fatalf("checkpoint read %s: %v", key, err)
				}
			}
		})
	}
}

// TestPut_ContinuedCommitFailure keeps the old data and allocation state on failure.
func TestPut_ContinuedCommitFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	options := &Options{Synchronous: SyncFull, CheckpointThresholdBytes: 1 << 30}
	db, err := Open(path, 0600, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket(testBucketName)
		if err != nil {
			return err
		}
		return bucket.Put([]byte("key"), []byte("small"))
	}); err != nil {
		t.Fatal(err)
	}
	small := []byte("small")
	large := bytes.Repeat([]byte("v"), 3*int(db.meta.PageSize()))
	replacement := bytes.Repeat([]byte("r"), len(large))
	larger := bytes.Repeat([]byte("g"), 5*int(db.meta.PageSize()))
	shorter := bytes.Repeat([]byte("s"), 2*int(db.meta.PageSize()))
	for _, transition := range []struct{ before, after []byte }{{small, large}, {large, replacement}, {large, larger}, {large, shorter}, {large, small}} {
		if err := db.Put(testBucketName, []byte("key"), transition.before); err != nil {
			t.Fatal(err)
		}
		if err := db.checkpointWAL(); err != nil {
			t.Fatal(err)
		}
		root := mustBucketRoot(t, db, testBucketName)
		original, err := db.readNode(root.PageID())
		if err != nil {
			t.Fatal(err)
		}
		ids := slices.Clone(original.OverflowPages())
		meta := page.EncodeMeta(db.meta)
		db.wal.SetSyncFileForTesting(func() error { return errDiscardTx })
		err = db.Put(testBucketName, []byte("key"), transition.after)
		db.wal.SetSyncFileForTesting(nil)
		if !errors.Is(err, errDiscardTx) {
			t.Fatalf("commit error: %v", err)
		}
		current, err := db.readNode(root.PageID())
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(ids, current.OverflowPages()) || !bytes.Equal(meta, page.EncodeMeta(db.meta)) {
			t.Fatal("failed commit changed page ownership or metadata")
		}
		for _, id := range ids {
			if !db.allocation.allocated(id) {
				t.Fatalf("failed commit freed continuation %d", id)
			}
		}
		db = reopenAfterWALWriteFailure(t, db, path, options)
		got, err := db.Get(testBucketName, []byte("key"))
		if err != nil || !bytes.Equal(got, transition.before) {
			t.Fatalf("failed commit changed readable bytes: %v", err)
		}
	}
}

// TestPut_ContinuedWriteBatch commits successful callbacks and drops a failed one.
func TestPut_ContinuedWriteBatch(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "database"), 0600, &Options{Synchronous: SyncFull, CheckpointThresholdBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	first := bytes.Repeat([]byte("a"), 3*int(db.meta.PageSize()))
	final := bytes.Repeat([]byte("b"), 4*int(db.meta.PageSize()))
	callbacks := []func(*Tx) error{
		func(tx *Tx) error {
			bucket, err := tx.CreateBucket(testBucketName)
			if err != nil {
				return err
			}
			return bucket.Put([]byte("a"), first)
		},
		func(tx *Tx) error {
			bucket := mustBucket(t, tx, testBucketName)
			if err := bucket.Put([]byte("a"), final); err != nil {
				return err
			}
			if err := bucket.Put([]byte("discarded"), final); err != nil {
				return err
			}
			return errDiscardTx
		},
		func(tx *Tx) error {
			bucket := mustBucket(t, tx, testBucketName)
			got, err := bucket.Get([]byte("a"))
			if err != nil || !bytes.Equal(got, first) {
				t.Fatalf("failed callback changed batch state: %v", err)
			}
			if err := bucket.Put([]byte("a"), final); err != nil {
				return err
			}
			return bucket.Put([]byte("b"), final)
		},
	}
	requests := make([]*writeRequest, len(callbacks))
	for i, callback := range callbacks {
		requests[i] = &writeRequest{transaction: callback, result: make(chan writeResult, 1)}
	}
	syncs := 0
	db.wal.SetSyncFileForTesting(func() error { syncs++; return nil })
	db.operationMu.Lock()
	db.executeWriteBatch(requests)
	db.operationMu.Unlock()
	db.wal.SetSyncFileForTesting(nil)
	// The first commit of a WAL generation also syncs its header, unless a sync followed the truncate before it.
	if syncs != 2 {
		t.Fatalf("batch sync count: %d", syncs)
	}
	for i, request := range requests {
		result := <-request.result
		if i == 1 {
			if !errors.Is(result.err, errDiscardTx) {
				t.Fatalf("failed callback error: %v", result.err)
			}
		} else if result.err != nil {
			t.Fatal(result.err)
		}
	}
	for _, key := range []string{"a", "b"} {
		got, err := db.Get(testBucketName, []byte(key))
		if err != nil || !bytes.Equal(got, final) {
			t.Fatalf("batch Get %s: %v", key, err)
		}
	}
	if _, err := db.Get(testBucketName, []byte("discarded")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("failed callback key: %v", err)
	}
}

// TestReadNode_ContinuedWALRecordUsesCommittedNode checks that a write reads a continued WAL leaf without a new decode.
// Failed updates between commits must not change the value that later writes read.
func TestReadNode_ContinuedWALRecordUsesCommittedNode(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "database"), 0600, &Options{Synchronous: SyncNone, CheckpointThresholdBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	size := 3 * int(db.meta.PageSize())
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket(testBucketName)
		if err != nil {
			return err
		}
		return bucket.Put([]byte("a"), bytes.Repeat([]byte{0}, size))
	}); err != nil {
		t.Fatal(err)
	}
	for round := 1; round <= 4; round++ {
		err := db.Update(func(tx *Tx) error {
			if err := mustBucket(t, tx, testBucketName).Put([]byte("a"), bytes.Repeat([]byte{0xff}, size)); err != nil {
				return err
			}
			return errDiscardTx
		})
		if !errors.Is(err, errDiscardTx) {
			t.Fatalf("round %d: failed update: %v", round, err)
		}
		if err := db.Update(func(tx *Tx) error {
			bucket := mustBucket(t, tx, testBucketName)
			got, err := bucket.Get([]byte("a"))
			if want := bytes.Repeat([]byte{byte(round - 1)}, size); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("round %d: write transaction read %d bytes starting %v, error %v; want the previous commit", round, len(got), got[:min(len(got), 1)], err)
			}
			return bucket.Put([]byte("a"), bytes.Repeat([]byte{byte(round)}, size))
		}); err != nil {
			t.Fatal(err)
		}
	}

	// A changed continued leaf can move to new pages, so read its page ID after the last commit.
	var leafPageID page.ID
	if err := db.View(func(tx *Tx) error {
		leafPageID = mustBucket(t, tx, testBucketName).rootPageID
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	record, ok := db.wal.Lookup(leafPageID)
	if !ok || record.Node != nil || record.Payload == nil {
		t.Fatalf("leaf %d WAL record: found %t, decoded %t; want an encoded record", leafPageID, ok, record.Node != nil)
	}
	var node *btree.Node
	allocations := testing.AllocsPerRun(10, func() {
		node, err = db.readNode(leafPageID)
		if err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("allocations for each continued WAL leaf read: got %.0f, want 0", allocations)
	}
	if got := node.EntryAt(0).Value(); !bytes.Equal(got, bytes.Repeat([]byte{4}, size)) {
		t.Fatalf("continued WAL leaf value: got %d bytes starting %v, want the last commit", len(got), got[:min(len(got), 1)])
	}
}

// TestBucketGet_ReadTransactionSkipsContinuedKey checks that a read-only Get does not copy a key that does not fit in one page.
// Get uses only the value, so the only allocation is the copy of the continued value.
func TestBucketGet_ReadTransactionSkipsContinuedKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	key := bytes.Repeat([]byte("k"), MaxKeySize)
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket(testBucketName)
		if err != nil {
			return err
		}
		return bucket.Put(key, []byte("value"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// A reopened read-only handle reads the mapped main file.
	db, err = Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *Tx) error {
		bucket := mustBucket(t, tx, testBucketName)
		allocations := testing.AllocsPerRun(10, func() {
			value, err := bucket.Get(key)
			if err != nil || string(value) != "value" {
				panic(fmt.Sprintf("Get: value %q, error %v", value, err))
			}
		})
		if allocations != 1 {
			t.Fatalf("allocations for each Get: got %.0f, want 1 for the value copy", allocations)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestDelete_ContinuedTree releases removed leaves and branches and repairs the root.
func TestDelete_ContinuedTree(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%t", batch), func(t *testing.T) {
			db, err := Open(filepath.Join(t.TempDir(), "database"), 0600, &Options{Synchronous: SyncNone, CheckpointThresholdBytes: 1 << 30})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			keys := make([][]byte, 12)
			value := bytes.Repeat([]byte("v"), 3*int(db.meta.PageSize()))
			if err := db.Update(func(tx *Tx) error {
				bucket, err := tx.CreateBucket(testBucketName)
				if err != nil {
					return err
				}
				for i := range keys {
					keys[i] = bytes.Repeat([]byte{byte('a' + i)}, MaxKeySize)
					if err := bucket.Put(keys[i], value); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := db.checkpointWAL(); err != nil {
				t.Fatal(err)
			}
			remaining := make([]bool, len(keys))
			for i := range remaining {
				remaining[i] = true
			}
			order := []int{0, 11, 5, 6, 1, 10, 2, 9, 3, 8, 4, 7}
			for offset := 0; offset < len(order); {
				count := 1
				if batch {
					count = 2
				}
				deleted := make([][]byte, count)
				for i := range deleted {
					deleted[i] = keys[order[offset+i]]
					remaining[order[offset+i]] = false
				}
				if err := db.Update(func(tx *Tx) error {
					bucket := mustBucket(t, tx, testBucketName)
					if batch {
						return bucket.DeleteBatch(deleted)
					}
					return bucket.Delete(deleted[0])
				}); err != nil {
					t.Fatal(err)
				}
				if err := db.validatePageOwnership(); err != nil {
					t.Fatal(err)
				}
				for i, key := range keys {
					got, err := db.Get(testBucketName, key)
					if remaining[i] {
						if err != nil || !bytes.Equal(got, value) {
							t.Fatalf("remaining key %d: %v", i, err)
						}
					} else if !errors.Is(err, ErrKeyNotFound) {
						t.Fatalf("deleted key %d: %v", i, err)
					}
				}
				offset += count
			}
			root := mustBucketRoot(t, db, testBucketName)
			if !root.IsLeaf() || root.EntryCount() != 0 || len(root.OverflowPages()) != 0 {
				t.Fatal("empty bucket did not become a normal leaf")
			}
			if err := db.checkpointWAL(); err != nil {
				t.Fatal(err)
			}
			if err := db.validatePageOwnership(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestDeleteBucket_ContinuedPages checks mapped, WAL, and private node ownership.
func TestDeleteBucket_ContinuedPages(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		t.Run(fmt.Sprintf("checkpoint=%t", checkpoint), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			db, err := Open(path, 0600, &Options{Synchronous: SyncFull, CheckpointThresholdBytes: 1 << 30})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { db.Close() }()
			value := bytes.Repeat([]byte("v"), 3*int(db.meta.PageSize()))
			if err := db.Update(func(tx *Tx) error {
				parent, err := tx.CreateBucket(testBucketName)
				if err != nil {
					return err
				}
				child, err := parent.CreateBucket([]byte("child"))
				if err != nil {
					return err
				}
				grandchild, err := child.CreateBucket(bytes.Repeat([]byte("z"), MaxKeySize))
				if err != nil {
					return err
				}
				for i := 0; i < 8; i++ {
					key := bytes.Repeat([]byte{byte('a' + i)}, MaxKeySize)
					if err := child.Put(key, value); err != nil {
						return err
					}
					if err := grandchild.Put(key, value); err != nil {
						return err
					}
				}
				_, err = tx.CreateBucket([]byte("keep"))
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if checkpoint {
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db, err = Open(path, 0600, &Options{Synchronous: SyncFull, CheckpointThresholdBytes: 1 << 30})
				if err != nil {
					t.Fatal(err)
				}
			}
			before := db.allocation.allocatedPageCount()
			// A failed callback must keep every page owned by the nested tree.
			err = db.Update(func(tx *Tx) error {
				parent := mustBucket(t, tx, testBucketName)
				if err := parent.DeleteBucket([]byte("child")); err != nil {
					return err
				}
				return errDiscardTx
			})
			if !errors.Is(err, errDiscardTx) {
				t.Fatalf("rollback: %v", err)
			}
			if db.allocation.allocatedPageCount() != before {
				t.Fatal("rollback changed allocation")
			}
			if err := db.validatePageOwnership(); err != nil {
				t.Fatal(err)
			}
			db.wal.SetSyncFileForTesting(func() error { return errDiscardTx })
			err = db.Update(func(tx *Tx) error { return mustBucket(t, tx, testBucketName).DeleteBucket([]byte("child")) })
			db.wal.SetSyncFileForTesting(nil)
			if !errors.Is(err, errDiscardTx) {
				t.Fatalf("failed sync: %v", err)
			}
			if db.allocation.allocatedPageCount() != before {
				t.Fatal("failed sync changed allocation")
			}
			if err := db.validatePageOwnership(); err != nil {
				t.Fatal(err)
			}
			db = reopenAfterWALWriteFailure(t, db, path, &Options{Synchronous: SyncFull, CheckpointThresholdBytes: 1 << 30})
			if db.allocation.allocatedPageCount() != before {
				t.Fatal("reopen after failed sync changed allocation")
			}
			if err := db.Update(func(tx *Tx) error {
				parent := mustBucket(t, tx, testBucketName)
				child, err := parent.Bucket([]byte("child"))
				if err != nil {
					return err
				}
				// Visit a changed private leaf before the whole nested bucket is removed.
				if err := child.Put(bytes.Repeat([]byte("a"), MaxKeySize), []byte("changed")); err != nil {
					return err
				}
				return parent.DeleteBucket([]byte("child"))
			}); err != nil {
				t.Fatal(err)
			}
			if db.allocation.allocatedPageCount() >= before {
				t.Fatal("bucket deletion did not release pages")
			}
			if err := db.validatePageOwnership(); err != nil {
				t.Fatal(err)
			}
			if err := db.checkpointWAL(); err != nil {
				t.Fatal(err)
			}
			if err := db.validatePageOwnership(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestRecovery_ContinuedDeletesAndReuse replays file snapshots before checkpoint.
// It checks replay and commit-marker handling, not physical power-loss behavior.
func TestRecovery_ContinuedDeletesAndReuse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone, CheckpointThresholdBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	keys := make([][]byte, 8)
	value := bytes.Repeat([]byte("v"), 3*int(db.meta.PageSize()))
	if err := db.Update(func(tx *Tx) error {
		keep, err := tx.CreateBucket(testBucketName)
		if err != nil {
			return err
		}
		remove, err := tx.CreateBucket([]byte("remove"))
		if err != nil {
			return err
		}
		for i := range keys {
			keys[i] = bytes.Repeat([]byte{byte('a' + i)}, MaxKeySize)
			if err := keep.Put(keys[i], value); err != nil {
				return err
			}
			if err := remove.Put(keys[i], value); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.checkpointWAL(); err != nil {
		t.Fatal(err)
	}
	base, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		if err := mustBucket(t, tx, testBucketName).DeleteBatch(keys[1:7]); err != nil {
			return err
		}
		return tx.DeleteBucket([]byte("remove"))
	}); err != nil {
		t.Fatal(err)
	}
	reusedBefore := db.stats.PagesReused
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("replacement"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte("key"), value)
	}); err != nil {
		t.Fatal(err)
	}
	if db.stats.PagesReused <= reusedBefore {
		t.Fatal("replacement did not reuse freed pages")
	}
	log, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	// Keep the WAL after writing main-file pages to test interrupted cleanup.
	if err := db.wal.Close(); err != nil {
		t.Fatal(err)
	}
	readOnlyWAL, err := os.Open(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	db.wal.ReplaceFileForTesting(readOnlyWAL)
	if err := db.checkpointWAL(); err == nil {
		t.Fatal("expected WAL cleanup error")
	}
	afterCheckpoint, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	retainedLog, err := os.ReadFile(path + "-wal")
	if err != nil || !bytes.Equal(retainedLog, log) {
		t.Fatalf("checkpoint changed the retained WAL: %v", err)
	}
	partial := bytes.Clone(base)
	boundary := len(afterCheckpoint) / int(db.meta.PageSize()) / 2 * int(db.meta.PageSize())
	copy(partial[:boundary], afterCheckpoint[:boundary])
	for state, main := range [][]byte{base, partial, afterCheckpoint} {
		for _, readOnly := range []bool{false, true} {
			for _, committed := range []bool{false, true} {
				if state != 0 && !committed {
					continue
				}
				t.Run(fmt.Sprintf("main=%d/readOnly=%t/committed=%t", state, readOnly, committed), func(t *testing.T) {
					crash := filepath.Join(t.TempDir(), "database")
					replay := log
					if !committed {
						replay = log[:len(log)-wal.HeaderSize-wal.ChecksumSize]
					}
					if err := os.WriteFile(crash, main, 0600); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(crash+"-wal", replay, 0600); err != nil {
						t.Fatal(err)
					}
					recovered, err := Open(crash, 0600, &Options{ReadOnly: readOnly, Synchronous: SyncNone})
					if err != nil {
						t.Fatal(err)
					}
					defer recovered.Close()
					for i, key := range keys {
						got, err := recovered.Get(testBucketName, key)
						if i == 0 || i == 7 {
							if err != nil || !bytes.Equal(got, value) {
								t.Fatalf("surviving key %d: %v", i, err)
							}
						} else if !errors.Is(err, ErrKeyNotFound) {
							t.Fatalf("deleted key %d: %v", i, err)
						}
					}
					if _, err := recovered.Get([]byte("remove"), keys[0]); !errors.Is(err, ErrBucketNotFound) {
						t.Fatalf("deleted bucket: %v", err)
					}
					got, err := recovered.Get([]byte("replacement"), []byte("key"))
					if committed {
						if err != nil || !bytes.Equal(got, value) {
							t.Fatalf("replacement: %v", err)
						}
					} else if !errors.Is(err, ErrBucketNotFound) {
						t.Fatalf("uncommitted replacement: %v", err)
					}
					if err := recovered.validatePageOwnership(); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

// TestDeleteBucket_ContinuedReadFailure keeps the bucket when a descendant is damaged.
func TestDeleteBucket_ContinuedReadFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	options := &Options{Synchronous: SyncNone, CheckpointThresholdBytes: 1 << 30, DataPageChecksums: true}
	db, err := Open(path, 0600, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			db.Close()
		}
	}()
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket(testBucketName)
		if err != nil {
			return err
		}
		for i := 0; i < 3; i++ {
			if err := bucket.Put(fmt.Appendf(nil, "key-%d", i), bytes.Repeat([]byte("v"), 3*int(tx.meta.PageSize()))); err != nil {
				return err
			}
		}
		_, err = tx.CreateBucket([]byte("keep"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var tail page.ID
	walkNodes(t, db, mustBucketRoot(t, db, testBucketName), func(node *btree.Node) {
		if node.IsLeaf() {
			ids := node.OverflowPages()
			tail = ids[len(ids)-1]
		}
	})
	offset := int64(tail)*db.meta.PageSize() + btree.ContinuationHeaderSize
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, 0600, options)
	if err != nil {
		t.Fatal(err)
	}
	var original [1]byte
	if _, err := db.file.ReadAt(original[:], offset); err != nil {
		t.Fatal(err)
	}
	damaged := []byte{original[0] ^ 1}
	if _, err := db.file.WriteAt(damaged, offset); err != nil {
		t.Fatal(err)
	}
	allocated, retired := db.allocation.allocatedPageCount(), db.stats.PagesRetired
	if err := db.Update(func(tx *Tx) error {
		if err := tx.DeleteBucket(testBucketName); !errors.Is(err, ErrChecksum) {
			t.Fatalf("damaged descendant: %v", err)
		}
		if _, err := tx.Bucket(testBucketName); err != nil {
			t.Fatalf("failed deletion removed the name: %v", err)
		}
		return mustBucket(t, tx, []byte("keep")).Put([]byte("after-error"), []byte("ok"))
	}); err != nil {
		t.Fatal(err)
	}
	if db.allocation.allocatedPageCount() != allocated || db.stats.PagesRetired != retired {
		t.Fatal("read failure retired pages")
	}
	if _, err := db.file.WriteAt(original[:], offset); err != nil {
		t.Fatal(err)
	}
	if err := db.validatePageOwnership(); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteBucket(testBucketName); err != nil {
		t.Fatal(err)
	}
	if err := db.validatePageOwnership(); err != nil {
		t.Fatal(err)
	}
}

// TestPut_MaxValueSize checks the declared limit through write, checkpoint, and reopen.
// Run it separately with KVLITE_TEST_MAX_VALUE=1. It uses several GiB of memory.
func TestPut_MaxValueSize(t *testing.T) {
	if os.Getenv("KVLITE_TEST_MAX_VALUE") != "1" {
		t.Skip("set KVLITE_TEST_MAX_VALUE=1 for the maximum-value test")
	}
	if strconv.IntSize < 64 {
		t.Skip("the maximum-value test needs a 64-bit address space")
	}
	path := filepath.Join(t.TempDir(), "maximum.db")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			db.Close()
		}
	}()
	if err := db.Update(func(tx *Tx) error { _, err := tx.CreateBucket(testBucketName); return err }); err != nil {
		t.Fatal(err)
	}
	key := bytes.Repeat([]byte("k"), MaxKeySize)
	value := make([]byte, MaxValueSize+1)
	before := page.EncodeMeta(db.meta)
	if err := db.Put(testBucketName, key, value); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("value above limit: %v", err)
	}
	if !bytes.Equal(before, page.EncodeMeta(db.meta)) {
		t.Fatal("rejected value changed metadata")
	}
	value = value[:MaxValueSize]
	// Nonzero markers cross page and ID-array boundaries. The final byte stays zero.
	for at := 0; at < len(value); at += 1 << 20 {
		value[at] = byte(at>>20) + 1
	}
	value[len(value)-2] = 0x7f
	table := crc32.MakeTable(crc32.Castagnoli)
	expected := crc32.Checksum(value, table)
	if err := db.Put(testBucketName, key, value); err != nil {
		t.Fatal(err)
	}
	value = nil
	check := func() {
		t.Helper()
		got, err := db.Get(testBucketName, key)
		if err != nil || len(got) != MaxValueSize {
			t.Fatalf("maximum read: length=%d, error=%v", len(got), err)
		}
		if crc32.Checksum(got, table) != expected {
			t.Fatal("maximum value bytes differ")
		}
	}
	check()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	check()
}

func TestDelete_CompactPrivateLeafRollback(t *testing.T) {
	for _, memory := range []bool{false, true} {
		name := "file"
		if memory {
			name = "memory"
		}
		t.Run(name, func(t *testing.T) {
			var db *DB
			if memory {
				db = OpenMemory()
			} else {
				var err error
				db, err = Open(filepath.Join(t.TempDir(), "db"), 0600, &Options{Synchronous: SyncNone})
				if err != nil {
					t.Fatal(err)
				}
			}
			defer db.Close()
			if err := db.Update(func(tx *Tx) error {
				bucket, err := tx.CreateBucket(testBucketName)
				if err != nil {
					return err
				}
				for _, key := range []string{"a", "b", "c", "d"} {
					if err := bucket.Put([]byte(key), []byte("value-"+key)); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			change := func(tx *Tx) error {
				bucket := mustBucket(t, tx, testBucketName)
				if err := bucket.Delete([]byte("b")); err != nil {
					return err
				}
				if err := bucket.Put([]byte("e"), []byte("new")); err != nil {
					return err
				}
				return bucket.DeleteBatch([][]byte{[]byte("d"), []byte("a"), []byte("a"), []byte("missing")})
			}
			if err := db.Update(func(tx *Tx) error {
				if err := change(tx); err != nil {
					return err
				}
				return errDiscardTx
			}); !errors.Is(err, errDiscardTx) {
				t.Fatalf("rollback: %v", err)
			}
			for _, key := range []string{"a", "b", "c", "d"} {
				got, err := db.Get(testBucketName, []byte(key))
				if err != nil || string(got) != "value-"+key {
					t.Fatalf("rollback changed %s: %q, %v", key, got, err)
				}
			}
			if _, err := db.Get(testBucketName, []byte("e")); !errors.Is(err, ErrKeyNotFound) {
				t.Fatalf("rollback published e: %v", err)
			}
			if err := db.Update(change); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"a", "b", "d"} {
				if _, err := db.Get(testBucketName, []byte(key)); !errors.Is(err, ErrKeyNotFound) {
					t.Fatalf("committed delete retained %s: %v", key, err)
				}
			}
			for key, expected := range map[string]string{"c": "value-c", "e": "new"} {
				if got, err := db.Get(testBucketName, []byte(key)); err != nil || string(got) != expected {
					t.Fatalf("retained %s: %q, %v", key, got, err)
				}
			}
		})
	}
}

func TestDeleteLeafEntries_PreservesWriteBatchImages(t *testing.T) {
	meta := page.NewMeta(4096)
	root := btree.NewLeafNode(meta.Root())
	db := &DB{meta: meta, rootNode: root}
	base := btree.NewLeafNode(10)
	for _, key := range []string{"a", "b", "c", "d", "e", "f"} {
		if err := base.InsertEntry(btree.NewEntry(0, []byte(key), []byte(key))); err != nil {
			t.Fatal(err)
		}
	}
	before := btree.EncodeWALNode(base)
	first := newWriteTx(db, meta, root, nil, nil)
	middle := first.store.DeleteLeafEntries(base, 1, []int{3}, 0)
	if got := first.store.DeleteLeafEntries(middle, 2, nil, 0); got != middle {
		t.Fatal("later delete replaced the private node")
	}
	retained := btree.EncodeWALNode(middle)
	failed := newWriteTx(db, meta, root, first.store.dirty, nil)
	failed.store.DeleteLeafEntries(middle, 0, nil, 0)
	if !bytes.Equal(btree.EncodeWALNode(middle), retained) || !bytes.Equal(btree.EncodeWALNode(base), before) {
		t.Fatal("later callback changed an earlier node image")
	}
	last := newWriteTx(db, meta, root, first.store.dirty, nil)
	final := last.store.DeleteLeafEntries(middle, 1, nil, 0)
	nodes, _ := makeWALCommitData(last.store.dirty, meta, false)
	if len(nodes) != 1 || nodes[0].Original != base || nodes[0].Final != final || final.EntryCount() != 2 {
		t.Fatal("delete did not preserve the original and final WAL images")
	}
	if string(final.EntryAt(0).Key()) != "a" || string(final.EntryAt(1).Key()) != "f" {
		t.Fatal("delete kept the wrong callback entries")
	}
}
