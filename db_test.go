package kvlite

// Some tests here are adapted from etcd-io/bbolt
// (https://github.com/etcd-io/bbolt), MIT License, Copyright (c) 2013 Ben Johnson.
//
// We build DFS: the thinnest working database first, then deepen. So the LIVE
// target right now is "Rung 1 — walking skeleton" (Put/Get that survives reopen).
// The bbolt-flavored open/meta tests are PARKED (skipped) until Rung 4, because
// they demand a meta page before we even have a working store — that's the
// breadth-first trap we're avoiding.

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// tempfile returns a temporary file path for a database.
func tempfile() string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("kvlite-%d.db", time.Now().UnixNano()))
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

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("a"), []byte("first")); err != nil {
		t.Fatal(err)
	}
	small := fileSize(t, path)

	for i := 0; i < 1000; i++ {
		if err := db.Put([]byte("a"), []byte("value")); err != nil {
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

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}

	const n = 500
	want := make(map[string]string, n)

	// Insert in descending order (deliberately not sorted-ascending).
	for i := n - 1; i >= 0; i-- {
		k := fmt.Sprintf("key-%04d", i)
		v := fmt.Sprintf("val-%d", i)
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
		want[k] = v
	}
	// Overwrite the even-numbered keys.
	for i := 0; i < n; i += 2 {
		k := fmt.Sprintf("key-%04d", i)
		v := fmt.Sprintf("val-%d-updated", i)
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
		want[k] = v
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for k, wantV := range want {
		got, err := db.Get([]byte(k))
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

// TestFile_SinglePage: a small database (fits in one node) is stored as exactly one
// padded page. RED against variable-length writes; GREEN once nodes are padded to
// NODE_SIZE.
func TestFile_SinglePage(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		if err := db.Put([]byte(kv[0]), []byte(kv[1])); err != nil {
			t.Fatal(err)
		}
	}
	pageBytes := db.meta.pageSize
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Decision B: page 0 is the meta page and the single leaf is page 1 -> two pages.
	if size := fileSize(t, path); size != 2*pageBytes {
		t.Fatalf("a small DB should occupy exactly two %d-byte pages (meta + one leaf), got %d bytes "+
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

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 150 x 256B ≈ 38 KB — blows past one page at any OS page size (4 KB or 16 KB).
	val := bytes.Repeat([]byte("x"), 256)
	for i := 0; i < 150; i++ {
		if err := db.Put(fmt.Appendf(nil, "key-%05d", i), val); err != nil {
			t.Fatal(err)
		}
	}

	if db.rootNode.IsLeaf {
		t.Fatal("root is still a single leaf after ~38 KB of entries — a full node must " +
			"split and grow a branch root (Rung 3b)")
	}
	if got, err := db.Get([]byte("key-00042")); err != nil || !bytes.Equal(got, val) {
		t.Fatalf("key-00042 unreadable after split: err=%v", err)
	}
}

// TestSplit_SurvivesReopen: after splits, close + reopen must locate the (now
// multi-node) root and descend to read every key back.
func TestSplit_SurvivesReopen(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}

	const n = 150
	val := bytes.Repeat([]byte("y"), 256)
	for i := 0; i < n; i++ {
		if err := db.Put(fmt.Appendf(nil, "key-%05d", i), val); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < n; i++ {
		got, err := db.Get(fmt.Appendf(nil, "key-%05d", i))
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

// walkNodes visits every node of the on-disk tree rooted at db.rootNode, reading
// each child by its pgid. It doubles as a reachability check: a bad child pgid
// (mis-wired separator/split) makes readNode fail and the walk t.Fatal.
func walkNodes(t *testing.T, db *DB, visit func(n *Node)) {
	t.Helper()
	var rec func(n *Node)
	rec = func(n *Node) {
		visit(n)
		if n.IsLeaf {
			return
		}
		for _, childPgid := range n.Children {
			child := db.readNode(childPgid)
			if child == nil {
				t.Fatalf("walk: could not read child pgid %d (broken split wiring?)", childPgid)
			}
			rec(child)
		}
	}
	rec(db.rootNode)
}

// TestSplit_RootBecomesBranch: the first milestone. Insert ~1.5 pages of data so a
// single leaf overflows once. The root must stop being a leaf, the new branch root
// must point at both halves, and BOTH the smallest and largest keys must survive
// (a split that drops a half fails here).
func TestSplit_RootBecomesBranch(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pageBytes := int(db.meta.pageSize)
	val := bytes.Repeat([]byte("x"), 256)
	// Size the count off the page size so this forces ~one split on any page size.
	entryBytes := 4 + len("key-00000") + 4 + len(val)
	n := pageBytes/entryBytes + pageBytes/entryBytes/2 // ~1.5 pages

	for i := 0; i < n; i++ {
		if err := db.Put(fmt.Appendf(nil, "key-%05d", i), val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	if db.rootNode.IsLeaf {
		t.Fatal("root is still a leaf after overflowing a page — it must split into a branch root")
	}
	if len(db.rootNode.Children) < 2 {
		t.Fatalf("branch root must point at >= 2 children, got %d", len(db.rootNode.Children))
	}
	for _, i := range []int{0, n - 1} { // first and last: neither half may be lost
		k := fmt.Appendf(nil, "key-%05d", i)
		if got, err := db.Get(k); err != nil || !bytes.Equal(got, val) {
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

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	val := bytes.Repeat([]byte("z"), 256)
	for i := 0; i < 300; i++ {
		if err := db.Put(fmt.Appendf(nil, "key-%05d", i), val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	pageBytes := int(db.meta.pageSize)
	walkNodes(t, db, func(nd *Node) {
		if sz := nd.serializedSize(); sz > pageBytes {
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

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const n = 400
	val := bytes.Repeat([]byte("w"), 256)
	for i := 0; i < n; i++ {
		if err := db.Put(fmt.Appendf(nil, "key-%05d", i), val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	if db.rootNode.IsLeaf {
		t.Fatal("root must be a branch after 400 entries")
	}
	leaves := 0
	walkNodes(t, db, func(nd *Node) {
		if nd.IsLeaf {
			leaves++
		}
	})
	if leaves < 3 {
		t.Fatalf("expected >= 3 leaves (a child-leaf split must add one, propagating a separator up), got %d", leaves)
	}
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "key-%05d", i)
		if got, err := db.Get(k); err != nil || !bytes.Equal(got, val) {
			t.Fatalf("key %q unreadable after propagating splits: err=%v", k, err)
		}
	}
}

// -----------------------------------------------------------------------------
// RUNG 1 — Walking skeleton: a file-backed KV that survives reopen.  <-- BUILD THIS
//
// The dumbest thing that is a real database. []byte API (db.Put/db.Get) — no
// buckets, no transactions, no pages, no tree. This API is deliberately throwaway
// and will evolve toward bbolt's later. The whole goal: a value written before
// Close is still there after reopening the same file.
//
// These won't compile until you add `Put` and `Get` to *DB — that undefined-method
// error IS your rung-1 to-do list:
//   func (db *DB) Put(key, value []byte) error
//   func (db *DB) Get(key []byte) ([]byte, error)   // missing key -> an error
// -----------------------------------------------------------------------------

// TestPutGet: a value can be written and read back within one session.
func TestPutGet(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	v, err := db.Get([]byte("a"))
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

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	v, err := db.Get([]byte("a"))
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

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("a"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	v, err := db.Get([]byte("a"))
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

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Get([]byte("nope")); err == nil {
		t.Fatal("expected an error for a missing key")
	}
}

// TestPutGet_MultipleKeys: several keys coexist and all survive a reopen.
func TestPutGet_MultipleKeys(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	pairs := map[string]string{"a": "1", "b": "2", "c": "3", "hello": "world"}

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range pairs {
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for k, want := range pairs {
		got, err := db.Get([]byte(k))
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

	db, err := Open(path, 0600, nil)
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

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, 0600, nil)
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

// TestDB_Path ensures Path returns the database's file path.
func TestDB_Path(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if s := db.Path(); s != path {
		t.Fatalf("unexpected path: %s", s)
	}
}

// -----------------------------------------------------------------------------
// PARKED until Rung 4 (robustness). These demand a meta page / locking / tx and
// are premature for the walking skeleton — deliberately skipped so the only red
// signal is Rung 1. Un-skip each when its wall actually bites.
// -----------------------------------------------------------------------------

// TestOpen_ErrInvalid: opening a non-kvlite file must return ErrInvalid.
// (Needs a meta page to validate against — Rung 4.)
func TestOpen_ErrInvalid(t *testing.T) {
	t.Skip("deferred to Rung 4: needs the meta page")

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

	if _, err := Open(path, 0600, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected ErrInvalid, got: %v", err)
	}
}

// TestOpen_FileTooSmall: opening a file too small to hold the meta pages errors.
// (Needs meta/page validation — Rung 4.)
func TestOpen_FileTooSmall(t *testing.T) {
	t.Skip("deferred to Rung 4: needs meta/page validation")

	path := tempfile()
	defer os.RemoveAll(path)

	if err := os.WriteFile(path, make([]byte, 16), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, 0600, nil); err == nil {
		t.Fatal("expected error opening a too-small file")
	}
}

// TestOpen_ErrVersionMismatch: meta with a different format version must fail with
// ErrVersionMismatch. TODO(Rung 4/meta): create a valid DB, flip `version` in both
// meta pages, reopen, assert errors.Is(err, ErrVersionMismatch).
func TestOpen_ErrVersionMismatch(t *testing.T) {
	t.Skip("deferred to Rung 4: needs meta-page layout")
}

// TestOpen_ErrChecksum: a corrupted meta checksum must fail with ErrChecksum.
// TODO(Rung 4/meta): corrupt a meta field in both pages so the seal mismatches.
func TestOpen_ErrChecksum(t *testing.T) {
	t.Skip("deferred to Rung 4: needs meta-page checksum")
}

// TestOpen_ReadPageSize_FromMeta1: if meta page 0 is corrupt, recover page size + DB
// from meta page 1. TODO(Rung 4/meta): needs dual meta pages + page-size detection.
func TestOpen_ReadPageSize_FromMeta1(t *testing.T) {
	t.Skip("deferred to Rung 4: needs dual meta pages")
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

// TestDB_Open_ReadOnly: ReadOnly allows reads, rejects writes, permits concurrent
// read-only openers. TODO(Rung 4+): needs locking + read-only tx semantics.
func TestDB_Open_ReadOnly(t *testing.T) {
	t.Skip("deferred: needs file locking + read-only tx semantics")
}

// TestDB_Open_ReadOnly_NoCreate: read-only open of a missing path must error, never create.
func TestDB_Open_ReadOnly_NoCreate(t *testing.T) {
	t.Skip("deferred: needs read-only open semantics")
}

// TestOpen_MultipleGoroutines: concurrent opens/closes must be safe (exclusive lock).
func TestOpen_MultipleGoroutines(t *testing.T) {
	t.Skip("deferred: needs file locking")
}

// TestOpen_MetaInitWriteError: write errors during meta init must surface from Open.
func TestOpen_MetaInitWriteError(t *testing.T) {
	t.Skip("deferred (fault injection)")
}
