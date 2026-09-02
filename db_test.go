package kvlite

// Some tests here are adapted from etcd-io/bbolt
// (https://github.com/etcd-io/bbolt), MIT License, Copyright (c) 2013 Ben Johnson.
//
// The suite started from a small Put/Get path and now covers the database file,
// metadata, WAL, transactions, buckets, locking, and recovery behavior.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

var testBucketName = []byte("test-data")
var errDiscardTx = errors.New("discard transaction")

func TestEncodeWALRecords_DoesNotCalculateDatabasePageChecksum(t *testing.T) {
	meta := page.NewMeta(4096)
	node := btree.NewLeafNode(1)
	if err := node.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("value"))); err != nil {
		t.Fatal(err)
	}

	records := encodeWALRecords(map[page.ID]*btree.Node{node.PageID(): node}, meta, false)
	if len(records) != 1 {
		t.Fatalf("WAL records: got %d, want 1", len(records))
	}
	if got := binary.LittleEndian.Uint32(records[0].PageContent[12:btree.NodeHeaderSize]); got != 0 {
		t.Fatalf("WAL node page checksum: got %x, want zero", got)
	}
}

func TestEncodeWALRecords_ReplicatesChangedMetadata(t *testing.T) {
	meta := page.NewMeta(4096)
	records := encodeWALRecords(nil, meta, true)
	if len(records) != 2 {
		t.Fatalf("metadata WAL records: got %d, want 2", len(records))
	}

	for index, pageID := range [...]page.ID{page.Meta0ID, page.Meta1ID} {
		record := records[index]
		if record.Header.Type != wal.RecordTypeMeta || record.Header.PageID != pageID {
			t.Fatalf("metadata WAL record %d header: got %+v, want page %d metadata", index, record.Header, pageID)
		}
		decoded, err := page.DecodeMeta(record.PageContent)
		if err != nil {
			t.Fatal(err)
		}
		if err := decoded.Validate(); err != nil {
			t.Fatalf("validate metadata WAL record %d: %v", index, err)
		}
		if got := decoded.Generation(); got != 1 {
			t.Fatalf("metadata WAL record %d generation: got %d, want 1", index, got)
		}
	}
	if !bytes.Equal(records[0].PageContent, records[1].PageContent) {
		t.Fatal("metadata WAL records contain different copies")
	}
}

// tempfile returns a temporary file path for a database.
func tempfile() string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("kvlite-%d.db", time.Now().UnixNano()))
}

// openDB opens a database for tests in NORMAL sync mode, so the suite is not
// dominated by a per-commit fsync (FULL, the production default, costs ~one macOS
// F_FULLFSYNC per Put). NORMAL is functionally identical here — it only defers the
// fsync to the checkpoint — so every test that does not specifically assert
// per-commit durability uses this.
func openDB(path string) (*DB, error) {
	db, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
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
	encodedMeta := page.EncodeMeta(meta)
	for _, pageID := range [...]page.ID{page.Meta0ID, page.Meta1ID} {
		if _, err := file.WriteAt(encodedMeta, int64(pageID)*pageSize); err != nil {
			t.Fatal(err)
		}
	}
	root := btree.NewLeafNode(meta.Root())
	if err := btree.WriteNode(io.NewOffsetWriter(file, int64(root.PageID())*pageSize), root, pageSize, true); err != nil {
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

func TestViewOneBucketLookupUsesAtMostSixAllocations(t *testing.T) {
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
	if allocations > 6 {
		t.Fatalf("one-bucket View allocations: got %v, want at most 6", allocations)
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
		entry, found, err := tx.findTreeEntry(tx.rootNode, testBucketName)
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
	db, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
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

	rejectedKey := []byte("too-large")
	rejectedValue := bytes.Repeat([]byte("v"), int(db.meta.PageSize()))
	err = db.Update(func(tx *Tx) error {
		bucket := mustBucket(t, tx, testBucketName)
		beforeMeta := page.EncodeMeta(tx.meta)

		if err := bucket.Put(rejectedKey, rejectedValue); !errors.Is(err, ErrEntryTooLargeForPage) {
			t.Fatalf("oversized Put: got %v, want ErrEntryTooLargeForPage", err)
		}
		if afterMeta := page.EncodeMeta(tx.meta); !bytes.Equal(afterMeta, beforeMeta) {
			t.Fatal("rejected Put changed transaction metadata")
		}

		return bucket.Put([]byte("good"), []byte("value"))
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := db.Get(testBucketName, rejectedKey); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("rejected key lookup: got %v, want ErrKeyNotFound", err)
	}
	if got, err := db.Get(testBucketName, []byte("good")); err != nil || !bytes.Equal(got, []byte("value")) {
		t.Fatalf("valid write after rejected Put: got %q, err %v", got, err)
	}
}

// -----------------------------------------------------------------------------
// RUNG 2 — Efficient lookup + no forever-growth: a single sorted node.  <-- NEXT
//
// Same Put/Get contract, smarter guts. These two are hook-free and run now. (The
// "is it ACTUALLY binary search?" test needs a probe counter in your Get — see chat.)
// -----------------------------------------------------------------------------

// TestPut_Overwrite_BoundedGrowth: overwriting ONE key 1000x must not grow the file
// without bound. RED against an append-log (each Put appends a record); GREEN once a
// real structure updates the key in place.
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
		t.Fatalf("file grew unboundedly overwriting one key 1000x: %d -> %d bytes "+
			"(append-log symptom — Rung 2 wants in-place update)", small, big)
	}
}

// TestPutGet_Stress: many keys inserted in a non-sorted order, half overwritten, all
// must read back correctly after a reopen. Algorithm-blind (passes for linear OR binary
// search) — the correctness backstop that catches off-by-ones, bad inserts, and wrong
// overwrite handling.
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

// -----------------------------------------------------------------------------
// RUNG 3a — Real fixed-size pages: correct padding + offsets.  <-- NEXT
//
// No behavior change; the LAYOUT becomes page-based. Every node is padded to a whole
// NODE_SIZE page and written at a page offset. (Splitting big nodes into one-page nodes
// so they stay <= a page is Rung 3b.)
// -----------------------------------------------------------------------------

// TestFile_SinglePage: a small bucket uses one catalog leaf and one data leaf.
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

	// Pages 0 and 1 are metadata, page 2 is the bucket catalog, and page 3 is the data leaf.
	if size := fileSize(t, path); size != 4*pageBytes {
		t.Fatalf("a small bucket should occupy exactly four %d-byte pages (two metadata + catalog + data leaf), got %d bytes "+
			"(nodes must be padded to page boundaries)", pageBytes, size)
	}
}

// -----------------------------------------------------------------------------
// RUNG 3b — The split: a full node splits, and the tree grows up.  <-- NEXT
//
// When a node's serialized size exceeds one page, it splits in two and a separator
// is pushed to its parent; splitting the root mints a new branch root (tree grows up).
// -----------------------------------------------------------------------------

// TestSplit_TreeGrows: insert enough to overflow a single leaf, forcing a split. The
// root must stop being a leaf — a new branch root is born (the tree grows up). RED
// while everything still lives in one oversized node.
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
		t.Fatal("root is still a single leaf after ~38 KB of entries — a full node must " +
			"split and grow a branch root (Rung 3b)")
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

// -----------------------------------------------------------------------------
// RUNG 3b — split staircase.  Build in THIS order; each test lights up the next
// capability. They all fail today with "not implemented" (the overflow insert
// bails), which IS your to-do list.
//
//   1. TestSplit_RootBecomesBranch   — the first split: root leaf -> branch root.
//   2. TestSplit_EveryNodeFitsInOnePage — the invariant the split exists to keep.
//   3. TestSplit_PropagatesToParent  — a child leaf splits and pushes a separator
//                                       up into the already-existing branch root.
//   4. TestSplit_TreeGrows / _SurvivesReopen (above) then carry you home.
// -----------------------------------------------------------------------------

// walkNodes visits every node of the on-disk tree rooted at root, reading
// each child by its pgid. It doubles as a reachability check: a bad child pgid
// (mis-wired separator/split) makes readNode fail and the walk t.Fatal.
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

// TestSplit_RootBecomesBranch: the first milestone. Insert ~1.5 pages of data so a
// single leaf overflows once. The root must stop being a leaf, the new branch root
// must point at both halves, and BOTH the smallest and largest keys must survive
// (a split that drops a half fails here).
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

// TestSplit_EveryNodeFitsInOnePage: the invariant the whole rung exists to keep.
// After many inserts, walk the on-disk tree and assert no node serializes past one
// page. A "split" that doesn't actually drop each piece below a page fails here.
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

// -----------------------------------------------------------------------------
// RUNG 4c — redo WAL (eager / write-through to start).  <-- NEXT
//
// Commit = append changed pages to the WAL (convention: main + "-wal") + fsync = the
// commit point. Main file stays current (eager) so reads are unchanged. A clean Close
// checkpoints the WAL into the main file and deletes it (single file at rest). A crash
// (unclean shutdown, WAL left behind) is recovered by replaying the WAL on open.
// -----------------------------------------------------------------------------

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

	if err := db.checkpointWAL(); err != nil {
		t.Fatalf("checkpoint returned an error after the main file was durable: %v", err)
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
//
// RED today: recovery has no commit concept. It either errors on the partial record
// (io.ReadFull fails -> Open fails) or blindly applies T2's leading records without
// its meta -> wrong state. GREEN needs (1) a commit marker ending each transaction,
// (2) buffering a txn's dirty pages and appending its frames + commit marker as a unit,
// (3) replay that scans to the LAST valid commit marker and drops anything after it.
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
//
// This will not compile until you add the trigger + the checkpoint it drives:
//   - wal.bytesSinceCheckpoint: an in-memory counter, bumped by largeBuf.Len() in
//     persistCollectedRecords, reset in checkpoint(). No syscall.
//   - wal.checkpointThresholdBytes: the knob below (rename freely — adjust the test).
//   - checkpoint(): fsync(main) -> truncate(WAL) -> reset the counter.
//   - after each commit: if bytesSinceCheckpoint > checkpointThresholdBytes { checkpoint() }
//   - Close: checkpoint (or fsync(main)) BEFORE wal.delete().
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

// TestMode2_CommitDefersMainWrite_CheckpointDrains: the defining property of Mode 2
// (SQLite WAL mode). A commit writes only the WAL; the main file is untouched until a
// checkpoint. Reads still see committed data, served from the in-memory overlay, not
// the main file. A checkpoint (here, Close) drains the overlay into the main file.
func TestMode2_CommitDefersMainWrite_CheckpointDrains(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path) // NORMAL by default
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
		t.Fatal("main file changed on a plain commit: Mode 2 must defer main writes to a checkpoint")
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

// TestMode2_RecoversAfterCheckpointThenCrash: the Mode 2 recovery seam that every
// other crash test skips. All of TestWAL_Recovers* replay a WAL onto a *pristine*
// (pre-write) main file. Real Mode 2 crashes onto a main file that a PRIOR checkpoint
// already filled: main holds the checkpointed base, the WAL holds only the delta
// committed since. Recovery must replay that delta ON TOP OF the non-empty base.
//
// It also pins last-write-wins across the checkpoint boundary: a key checkpointed
// into main with an OLD value, then re-Put with a NEW value that lives only in the
// WAL, must recover to the NEW value — the replayed WAL page must win over the page
// already sitting in main. Get it wrong and recovery serves the stale checkpointed
// value (or silently keeps both).
//
// The three post-recovery buckets prove the union is correct:
//   - keys 0..9    : checkpointed as valBase, then overwritten to valUpd (WAL only)  -> valUpd
//   - keys 10..99  : checkpointed as valBase, never touched again (main base only)   -> valBase
//   - keys 100..149: committed only after the checkpoint (WAL only)                  -> valBase
func TestMode2_RecoversAfterCheckpointThenCrash(t *testing.T) {
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

// TestPutGet_Persists is the rung-1 goal: a value survives Close + reopen.
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
// Open: creation & basic lifecycle  (already GREEN)
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

// TestOpen_ErrPathRequired ensures that opening with a blank path returns an error.
func TestOpen_ErrPathRequired(t *testing.T) {
	_, err := Open("", 0600, nil)
	if err == nil {
		t.Fatalf("expected error")
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
	first := defaultOptions()
	first.ReadOnly = true
	first.Synchronous = SyncNormal
	first.CheckpointThresholdBytes = 1

	second := defaultOptions()
	if second.ReadOnly {
		t.Fatal("changing one default options value changed a later value")
	}
	if second.Synchronous != SyncFull {
		t.Fatalf("default synchronous mode: got %v, want SyncFull", second.Synchronous)
	}
	if second.CheckpointThresholdBytes != defaultCheckpointPageCount*uint64(os.Getpagesize()) {
		t.Fatalf("default checkpoint threshold: got %d", second.CheckpointThresholdBytes)
	}
}

func TestOpen_NewDatabaseUsesOperatingSystemPageSize(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	pageSize := int64(os.Getpagesize())
	db, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
	if err != nil {
		t.Fatal(err)
	}
	if db.meta.PageSize() != pageSize {
		t.Fatalf("new database page size: got %d, want %d", db.meta.PageSize(), pageSize)
	}
	if got := fileSize(t, path); got != 3*pageSize {
		t.Fatalf("new database file size: got %d, want three %d-byte pages", got, pageSize)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
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

	db, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
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

	db, err := Open(path, 0600, &Options{Synchronous: SyncNormal + 1})
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
	db, err := Open(path, mode, &Options{Synchronous: SyncNormal})
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

func TestOpen_SynchronousNormalDefersWALSync(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	// The threshold is larger than this test transaction, so NORMAL mode must
	// leave the WAL unsynced until a later checkpoint or close.
	const checkpointBeyondTestWrite uint64 = 1 << 30
	db, err := Open(path, 0600, &Options{
		Synchronous:              SyncNormal,
		CheckpointThresholdBytes: checkpointBeyondTestWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
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
		t.Fatalf("NORMAL commit sync calls: got %d, want 0", syncCalls)
	}
}

func TestOpen_CheckpointThresholdOptionTriggersCheckpoint(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, &Options{
		Synchronous:              SyncNormal,
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

	db, err := openDB(path)
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

	db, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
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

// TestOpen_Size: a fresh DB lays out a fixed initial set of pages; reopen + a small
// write must not balloon the file. TODO(Rung 3-4): needs page size + layout + writes.
func TestOpen_Size(t *testing.T) {
	t.Skip("deferred: needs page size + initial layout")
}

// TestOpen_Check: fresh and reopened DBs pass an integrity check.
// TODO(hardening): needs tx.Check().
func TestOpen_Check(t *testing.T) {
	t.Skip("deferred: needs tx.Check() integrity checker")
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

// -----------------------------------------------------------------------------
// FSYNC DECISION BENCHMARK
//
// Decide whether re-enabling fsync (the commented-out db.file.Sync() calls in
// persistNode/persistMeta/applyRecordToDatabase, and any Sync() added to
// wal.persistRecord) is acceptable: run this now (fsync off) to get a baseline,
// enable fsync, run again, and compare ns/op. Every Put currently does 2 WAL
// record writes (node + meta) and 2 main-file writes even on the non-split path,
// so each Sync() call you add multiplies the number of syncs per Put — the
// benchmark should make that cost visible up front, before deciding where to sync.
// -----------------------------------------------------------------------------

// BenchmarkPut_Sequential: repeated Put with ascending keys — the common case,
// triggers node splits as the tree grows.
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Appendf(nil, "key-%08d", i)
		if err := db.Put(testBucketName, key, value); err != nil {
			b.Fatal(err)
		}
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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

	b.ResetTimer()
	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		for i := 0; i < b.N; i++ {
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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

	b.ResetTimer()
	err = db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		for i := 0; i < b.N; i++ {
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
	if _, err := db.file.Seek(offset, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := db.readNode(db.meta.Root()); err != nil {
		t.Fatal(err)
	}
	got, err := db.file.Seek(0, io.SeekCurrent)
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
	if _, err := db.file.Seek(offset, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.readValidMainMeta(); err != nil {
		t.Fatal(err)
	}
	got, err := db.file.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if got != offset {
		t.Fatalf("main file offset after metadata read: got %d, want %d", got, offset)
	}
}

func TestPersistNode_DoesNotChangeMainFileOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const offset int64 = 7
	if _, err := db.file.Seek(offset, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if err := db.persistNode(db.rootNode); err != nil {
		t.Fatal(err)
	}
	got, err := db.file.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if got != offset {
		t.Fatalf("main file offset after node write: got %d, want %d", got, offset)
	}
}

func TestPersistMeta_DoesNotChangeMainFileOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const offset int64 = 7
	if _, err := db.file.Seek(offset, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if err := db.persistMeta(); err != nil {
		t.Fatal(err)
	}
	got, err := db.file.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if got != offset {
		t.Fatalf("main file offset after metadata write: got %d, want %d", got, offset)
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
	if _, err := db.file.Seek(offset, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if err := db.checkpointWAL(); err != nil {
		t.Fatal(err)
	}
	got, err := db.file.Seek(0, io.SeekCurrent)
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

// TestTx_ViewCannotCreateBucket: CreateBucket is a write. A View transaction must
// reject it with ErrTxNotWritable, exactly like Bucket.Put does. Before the fix,
// CreateBucket changed shared metadata and pending WAL state. The next write could
// then commit a bucket that came from a read-only transaction.
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

func TestPutTreeEntry_DoesNotPublishPrivateRoot(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
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
		newRoot, err := tx.putTreeEntry(root, btree.NewEntry(0, []byte("a"), value))
		if err != nil {
			return err
		}
		newRoot, err = tx.putTreeEntry(newRoot, btree.NewEntry(0, []byte("b"), value))
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

// -----------------------------------------------------------------------------
// RUNG 5b — Buckets: many named keyspaces in one file.  <-- BUILD THIS
//
// A bucket is your SAME tree, started at a different root pgid. The root bucket's
// root is meta.root; a named bucket's root is a (BucketLeafFlag-flagged) value that
// lives as an entry in a parent bucket's tree. So CreateBucket("users") = insert
// name->newRootPgid into the root tree (flagged) AND materialize an empty leaf at
// newRootPgid; bucket.Put/Get then descend FROM that pgid, not db.rootNode.
//
// The bucket surface these tests use:
//   - (*Tx).CreateBucket(name []byte) (*Bucket, error)   // ErrBucketExists if present
//   - (*Tx).Bucket(name []byte) (*Bucket, error)
//   - (*Bucket).Put(key, value []byte) error
//   - (*Bucket).Get(key []byte) ([]byte, error)
//
// The load-bearing gap these drive out: descend must take the bucket's root as a
// parameter. Until then every bucket shares one tree and TestBucket_IsolatesSameKey
// fails.
// -----------------------------------------------------------------------------

// TestBucket_PutGetRoundTrip: create a bucket, write into it, read it back — both
// read-your-writes inside the txn and from a separate read txn after commit.
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

// TestBucket_IsolatesSameKey: THE bucket test. The SAME key in two different buckets
// holds two different values — they must not collide. This is impossible unless
// Put/Get descend from each bucket's own root pgid. RED while everything still lives
// in the one db.rootNode tree.
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

// TestBucket_NestedInSplitParent: a nested bucket must resolve even when the PARENT
// bucket's OWN tree has split (its root became a branch). Bucket.Bucket() and the
// duplicate check in Bucket.CreateBucket look the child up with bucket.rootNode.get(name)
// — a single-node lookup that ERRORS on a branch root (ErrNotLeafNode) instead of
// descending — while Tx.Bucket() correctly uses the descending _get. So a child bucket
// in a large parent is wrongly reported missing (and CreateBucket's guard then silently
// overwrites). Here the parent is filled past one page BEFORE the child is created, then
// the child is read back both in-session and after a reopen.
//
// RED today: parent.Bucket("child") returns nil once the parent root is a branch. GREEN
// once the nested lookup descends the parent's tree (share Tx.Bucket's _get path).
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

// TestBucket_Nested: a bucket inside a bucket — the recursion the whole flag design
// exists for. A sub-bucket is a BucketLeafFlag entry in its parent's tree, so the same
// machinery nests with no new tree type. Drives (*Bucket).CreateBucket, which doesn't
// exist yet. Checks: the child round-trips through parent.Bucket(...).Get; a plain key
// and a sub-bucket key coexist in the same parent without colliding; Get on a sub-bucket
// key returns nil (it's not a value); and all of it survives a reopen.
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

// -----------------------------------------------------------------------------
// Spring-cleaning Phase 1 — bug fixes (TDD: these are RED before the fix).
// -----------------------------------------------------------------------------

// TestSplit_LargeValuesGetOwnNode: a value whose encoded size reaches half a page
// must still split cleanly. Node.split cuts at the first entry that reaches
// pageSize/2, so if the FIRST entry already exceeds that limit the separator index
// is 0 — the left node becomes EMPTY and the right node keeps the whole (over-page)
// content. The split loop in _put then advances to the parent and never re-checks
// that oversized right node. Result: two ~0.6-page values (which fit fine at one
// entry per node) are rejected with "record page content exceeds page size", or a
// corrupt empty leaf is wired under a branch separator.
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
	smallEntry := 12 + 6 + len(smallVal) // flags(4)+keylen(4)+key(6)+vallen(4)+val
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

// -----------------------------------------------------------------------------
// PHASE 1 — spring-cleaning regression tests.
// -----------------------------------------------------------------------------

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
			entrySize := 4 + 4 + len(k) + 4 + len(fillVal)
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
		entry, found, err := tx.findTreeEntry(tx.rootNode, []byte("p"))
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

// TestWAL_BytesSinceCheckpointTracksWALSize arms Phase-1 bug #3.
//
// bytesSinceCheckpoint gates the checkpoint. The old code added a record's size
// every time it staged a page. Rewriting one page many times inflated the counter
// and started checkpoints too early. The counter must equal the bytes actually
// appended to the WAL since the last checkpoint. A fresh DB starts with an empty
// WAL, so after one commit the counter must equal the WAL file size on disk.
func TestWAL_BytesSinceCheckpointTracksWALSize(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Overwrite one key 100 times in a single transaction. Only the final leaf page
	// enters the WAL. The old per-call counter would report about 100 times that.
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

// TestBucket_SharedHandleSeesWrites fixes Phase-1 bug #1.
//
// tx.Bucket(name) used to build a fresh handle on every call, each caching its own
// root snapshot, so a write through one handle that split the bucket was invisible
// to a second handle for the same bucket. Tx and Bucket now cache one handle per
// name (per transaction, and per parent bucket for nested names), so every call
// with the same name returns the same *Bucket object.
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

// -----------------------------------------------------------------------------
// Bug 1 — an empty or nil value must round-trip, and must not read back as a
// missing key. "Missing" is signalled by ErrKeyNotFound, not by a nil value.
// -----------------------------------------------------------------------------

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

func TestFindTreeEntry_DistinguishesEmptyValueFromMissing(t *testing.T) {
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
		entry, found, err := tx.findTreeEntry(root, []byte("present"))
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("stored entry reported missing")
		}
		if entry.Value() == nil || len(entry.Value()) != 0 {
			t.Fatalf("stored empty value: got %v, want a non-nil empty slice", entry.Value())
		}

		_, found, err = tx.findTreeEntry(root, []byte("missing"))
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

// -----------------------------------------------------------------------------
// Bug 4 — a committed-but-not-yet-checkpointed WAL record must carry the txid it
// was actually committed under. persistCollectedRecords stamped the txid onto a
// range-loop copy of each record before encoding it, but copied the ORIGINAL
// (pre-stamp) record into wal.overlay, so every overlay record carried a stale
// txid. Nothing reads that field today, but it exists for future crash-recovery
// ordering, so it must be correct now rather than silently wrong.
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// Bug 5 — a single key/value pair that alone overflows a page cannot be split
// (both halves of a split must be non-empty), so it used to fail with the
// internal ErrNodeNotSaturated leaking straight out of Put. That error describes
// a split precondition, not something a caller can act on. It must now surface as
// a clear, documented error instead.
// -----------------------------------------------------------------------------

func TestPut_EntryTooLargeForPageReturnsClearError(t *testing.T) {
	path := tempfile()
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	huge := bytes.Repeat([]byte("x"), int(db.meta.PageSize()))
	if err := db.Put(testBucketName, []byte("k"), huge); !errors.Is(err, ErrEntryTooLargeForPage) {
		t.Fatalf("expected ErrEntryTooLargeForPage, got %v", err)
	}
}

func TestPut_EntryTooLargeForPageDoesNotChangeDatabase(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	huge := bytes.Repeat([]byte("x"), int(db.meta.PageSize()))
	if err := db.Put(testBucketName, []byte("large"), huge); !errors.Is(err, ErrEntryTooLargeForPage) {
		t.Fatalf("expected ErrEntryTooLargeForPage, got %v", err)
	}

	if _, err := db.Get(testBucketName, []byte("large")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("failed Put changed the database: expected ErrKeyNotFound, got %v", err)
	}
}

// -----------------------------------------------------------------------------
// Bug 6 — a *Bucket must own a private copy of its name. CreateBucket and Bucket
// must not retain the caller's mutable buffer for the stored catalog entry or the
// transaction bucket cache.
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// Bug 7 — a required WAL sync is the commit point. A checkpoint failure after
// that sync must keep the committed WAL and overlay state and return success.
// -----------------------------------------------------------------------------

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

	if err := db.wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, 0644, nil)
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

func TestAudit_MidWALChecksumFailureIsNotTreatedAsTornTail(t *testing.T) {
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
	contentSize := int(binary.LittleEndian.Uint32(walBytes[17:21]))
	checksumOffset := wal.HeaderSize + contentSize
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
	if err == nil {
		_ = recovered.Close()
		t.Fatal("Open accepted a checksum failure before later WAL records")
	}
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("expected ErrChecksum, got %v", err)
	}
}

func TestAudit_FinalWALChecksumFailureReturnsChecksumError(t *testing.T) {
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
	if err == nil {
		_ = recovered.Close()
		t.Fatal("Open accepted a checksum failure on the final WAL record")
	}
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("expected ErrChecksum, got %v", err)
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
	db.wal.SetSyncFileForTesting(func() error {
		syncCalls.Add(1)
		return nil
	})

	const writeCount = 32
	start := make(chan struct{})
	errorsByWrite := make(chan error, writeCount)
	var ready sync.WaitGroup
	var done sync.WaitGroup
	ready.Add(writeCount)
	done.Add(writeCount)
	for index := range writeCount {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			key := []byte(fmt.Sprintf("key-%02d", index))
			errorsByWrite <- db.Put(testBucketName, key, []byte("value"))
		}()
	}

	ready.Wait()
	close(start)
	done.Wait()
	close(errorsByWrite)
	for err := range errorsByWrite {
		if err != nil {
			t.Fatal(err)
		}
	}
	db.wal.SetSyncFileForTesting(nil)

	if got := syncCalls.Load(); got > 4 {
		t.Fatalf("WAL sync calls for %d concurrent writes: got %d, want at most 4", writeCount, got)
	}
	walBytes, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	reader := bytes.NewReader(walBytes)
	transactionIDs := make(map[wal.TxID]struct{})
	commitMarkers := 0
	for {
		record, err := wal.DecodeRecord(reader, db.meta.PageSize())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		transactionIDs[record.Header.TxID] = struct{}{}
		if wal.IsCommitMarker(record) {
			commitMarkers++
		}
	}
	if got, want := len(transactionIDs), int(syncCalls.Load()); got != want {
		t.Fatalf("WAL transactions: got %d, want one for each of %d syncs", got, want)
	}
	if commitMarkers != len(transactionIDs) {
		t.Fatalf("WAL commit markers: got %d, want %d", commitMarkers, len(transactionIDs))
	}
	for index := range writeCount {
		key := []byte(fmt.Sprintf("key-%02d", index))
		if value, err := db.Get(testBucketName, key); err != nil || !bytes.Equal(value, []byte("value")) {
			t.Fatalf("read %q after grouped commit: value=%q error=%v", key, value, err)
		}
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
	db.wal.SetSyncFileForTesting(nil)
	for _, key := range [][]byte{[]byte("first"), []byte("second")} {
		if _, err := db.Get(testBucketName, key); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("value %q after failed batch: got %v, want ErrKeyNotFound", key, err)
		}
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

	large := bytes.Repeat([]byte("x"), int(db.meta.PageSize()))
	err = db.Update(func(tx *Tx) error {
		if err := mustBucket(t, tx, testBucketName).Put([]byte("rejected"), large); !errors.Is(err, ErrEntryTooLargeForPage) {
			t.Fatalf("expected ErrEntryTooLargeForPage, got %v", err)
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
		t.Fatalf("durable WAL commit returned a checkpoint error: %v", err)
	}
	if got, err := db.Get(testBucketName, []byte("key")); err != nil || !bytes.Equal(got, []byte("new")) {
		t.Fatalf("committed overlay value: got %q, err %v", got, err)
	}
	if err := db.wal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Get(testBucketName, []byte("key")); err != nil || !bytes.Equal(got, []byte("new")) {
		t.Fatalf("recovered committed value: got %q, err %v", got, err)
	}
}

func TestAudit_AutomaticCheckpointSyncsWALOnce(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.wal.SetSyncFileForTesting(nil)
		_ = db.Close()
	}()

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
	db.wal.SetSyncFileForTesting(nil)
	if syncCalls != 1 {
		t.Fatalf("automatic checkpoint WAL sync calls: got %d, want 1", syncCalls)
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

func TestAudit_CloseSyncsUnsyncedNormalWAL(t *testing.T) {
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
	if syncCalls != 1 {
		t.Fatalf("normal commit and close WAL sync calls: got %d, want 1", syncCalls)
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
		// The first hooked sync belongs to the failed commit. Any later call is redundant.
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
	if syncCalls != 1 {
		t.Fatalf("failed commit and close WAL sync calls: got %d, want 1", syncCalls)
	}
}

func TestAudit_WALSyncFailureRollsBackBeforePublication(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

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
	if syncCalls != 1 {
		t.Fatalf("WAL sync calls: got %d, want 1", syncCalls)
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
	if got, err := db.Get(testBucketName, []byte("key")); err != nil || !bytes.Equal(got, []byte("old")) {
		t.Fatalf("value after WAL sync failure: got %q, err %v", got, err)
	}
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
		t.Fatalf("first durable WAL commit returned a checkpoint error: %v", err)
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

	recovered, err := openDB(path)
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
	const (
		entryCountBytes    = 4 // The entry count uses one uint32 value.
		entryFlagsBytes    = 4 // Each entry stores flags in one uint32 value.
		encodedLengthBytes = 4 // Each key or value length uses one uint32 value.
	)
	valueLengthOffset := btree.NodeHeaderSize + entryCountBytes + entryFlagsBytes + encodedLengthBytes + len(key)
	binary.LittleEndian.PutUint32(pageData[valueLengthOffset:valueLengthOffset+encodedLengthBytes], uint32(pageSize))

	checksumTable := crc32.MakeTable(crc32.Castagnoli)
	checksum := crc32.Update(0, checksumTable, pageData[:12])
	checksum = crc32.Update(checksum, checksumTable, pageData[btree.NodeHeaderSize:])
	binary.LittleEndian.PutUint32(pageData[12:btree.NodeHeaderSize], checksum)
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

	recovered, err := openDB(path)
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
	if _, err := db.wal.FileForTesting().Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	records, err := db.wal.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	if records == nil {
		t.Fatal("committed WAL has no records")
	}

	db.wal.ClearCommittedRecordsForTesting()
	if err := db.file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.ingestWalRecords(records); err == nil {
		t.Fatal("expected WAL replay to fail on the closed main file")
	}
	_ = db.closeFiles()

	if _, err := os.Stat(path + "-wal"); err != nil {
		t.Fatalf("failed recovery removed the committed WAL: %v", err)
	}
}
