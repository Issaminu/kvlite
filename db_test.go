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
			child, err := db.readNode(childPgid)
			if err != nil || child == nil {
				t.Fatalf("walk: could not read child pgid %d (broken split wiring?): %v", childPgid, err)
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

// TestSplit_Cascades: force the tree to depth 3 — enough leaves that the ROOT BRANCH
// itself overflows a page and must split, minting a new root above two BRANCH children.
// This is the first test that exercises a BRANCH split (not just a leaf split). Large,
// same-size keys bloat the separators so the root branch fills fast.
func TestSplit_Cascades(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}

	pageBytes := int(db.meta.pageSize)
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
		if err := db.Put(mkKey(i), val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	// Depth >= 3: root is a branch AND at least one of its children is ALSO a branch
	// (which only happens once the root branch itself has split).
	if db.rootNode.IsLeaf {
		t.Fatal("root must be a branch")
	}
	child, err := db.readNode(db.rootNode.Children[0])
	if err != nil || child == nil {
		t.Fatalf("could not read root's first child: %v", err)
	}
	if child.IsLeaf {
		t.Fatalf("tree only reached depth 2 — %d fat keys should overflow the root branch and force a BRANCH split (depth 3)", n)
	}

	// Every key must still route correctly through the branch-split tree.
	for i := 0; i < n; i++ {
		if got, err := db.Get(mkKey(i)); err != nil || !bytes.Equal(got, val) {
			t.Fatalf("key %d unreadable after a branch split (mis-wired separator?): err=%v", i, err)
		}
	}

	// And survive a reopen.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < n; i++ {
		if got, err := db.Get(mkKey(i)); err != nil || !bytes.Equal(got, val) {
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

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
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
	db, err = Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if v, err := db.Get([]byte("a")); err != nil || !bytes.Equal(v, []byte("1")) {
		t.Fatalf("key a lost after checkpoint+reopen: got %q, err %v", v, err)
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
	db, err := Open(path, 0600, nil)
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
	db, err = Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
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
	rec, err := Open(crash, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Close()
	if v, err := rec.Get([]byte("a")); err != nil || !bytes.Equal(v, []byte("1")) {
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
	db, err := Open(path, 0600, nil)
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
	db, err = Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	const n = 100
	val := bytes.Repeat([]byte("x"), 512) // 100 * ~525B ≈ 50 KB, well past any page
	for i := 0; i < n; i++ {
		if err := db.Put(fmt.Appendf(nil, "key-%05d", i), val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if db.rootNode.IsLeaf {
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

	// Recovery must rebuild the whole multi-level tree — including the moved root.
	rec, err := Open(crash, 0600, nil)
	if err != nil {
		t.Fatalf("open crashed db: %v", err)
	}
	defer rec.Close()
	if rec.rootNode.IsLeaf {
		t.Fatal("recovered root is a leaf — the moved root pointer was not replayed")
	}
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "key-%05d", i)
		if got, err := rec.Get(k); err != nil || !bytes.Equal(got, val) {
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
	db, err := Open(path, 0600, nil)
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
	db, err = Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	walAfterT1, err := os.ReadFile(wal)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("b"), []byte("2")); err != nil {
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
		return Open(crash, 0600, nil)
	}

	// Positive control: the FULL WAL (T2's commit marker intact) recovers both keys.
	// If this fails the harness itself is wrong, not the torn-tail logic.
	t.Run("full-wal-recovers-both", func(t *testing.T) {
		rec, err := recover(t, walAfterT2)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer rec.Close()
		if v, err := rec.Get([]byte("a")); err != nil || !bytes.Equal(v, []byte("1")) {
			t.Fatalf("a: got %q err %v, want \"1\"", v, err)
		}
		if v, err := rec.Get([]byte("b")); err != nil || !bytes.Equal(v, []byte("2")) {
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
			if v, err := rec.Get([]byte("a")); err != nil || !bytes.Equal(v, []byte("1")) {
				t.Fatalf("committed T1 lost: a = %q, err %v", v, err)
			}
			if _, err := rec.Get([]byte("b")); !errors.Is(err, ErrKeyNotFound) {
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

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Force frequent checkpoints so we can prove the WAL is bounded.
	db.wal.checkpointThresholdBytes = 8 * 1024 // 8 KiB

	const n = 400
	val := bytes.Repeat([]byte("x"), 2048) // ~2 KiB per value: the WAL grows fast
	want := make(map[string]string, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%05d", i)
		if err := db.Put([]byte(k), val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		want[k] = string(val)
	}

	// We wrote ~800 KiB, far past the 8 KiB threshold. If the checkpoint fires and
	// resets the WAL, the WAL at rest holds only the records since the last one.
	// Without a checkpoint it grows with the data (RED).
	if got := fileSize(t, wal); got > 128*1024 {
		t.Fatalf("WAL was not checkpointed: %d bytes at rest after writing ~%d KiB "+
			"(expected it to reset near the %d-byte threshold)", got, n*len(val)/1024, db.wal.checkpointThresholdBytes)
	}

	// All data readable before the close.
	for k, wantV := range want {
		if got, err := db.Get([]byte(k)); err != nil || string(got) != wantV {
			t.Fatalf("get %q before close: got %q, err %v", k, got, err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: the checkpoint must have moved everything into the main file, so a
	// clean close + reopen keeps all data even though the WAL is gone at rest.
	db, err = Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for k, wantV := range want {
		if got, err := db.Get([]byte(k)); err != nil || string(got) != wantV {
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

	db, err := Open(path, 0600, nil) // NORMAL by default
	if err != nil {
		t.Fatal(err)
	}

	// The main file right after Open holds the initial meta + root.
	mainAfterOpen, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// A commit under the checkpoint threshold must NOT touch the main file.
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
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
	if got, err := db.Get([]byte("k")); err != nil || !bytes.Equal(got, []byte("v")) {
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
	db, err = Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got, err := db.Get([]byte("k")); err != nil || !bytes.Equal(got, []byte("v")) {
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

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Drive checkpoints by hand: a huge threshold stops any auto-checkpoint from
	// firing mid-test, so we control exactly what sits in main vs the WAL.
	db.wal.checkpointThresholdBytes = 1 << 30

	valBase := bytes.Repeat([]byte("a"), 512) // ~512B/value forces a multi-level base tree
	valUpd := bytes.Repeat([]byte("b"), 512)  // same length, distinct content

	// Batch 1: keys 0..99, then a manual checkpoint drains them into the main file.
	for i := 0; i < 100; i++ {
		if err := db.Put(fmt.Appendf(nil, "key-%05d", i), valBase); err != nil {
			t.Fatalf("base put %d: %v", i, err)
		}
	}
	if err := db.wal.checkpoint(); err != nil {
		t.Fatalf("manual checkpoint: %v", err)
	}
	// After the checkpoint the base lives in main and the WAL is reset. If the base
	// never split, the "non-empty base" is trivial — make sure the seam is real.
	if db.rootNode.IsLeaf {
		t.Fatal("test setup: base tree did not split; the checkpointed base must be multi-level")
	}

	// Batch 2 (WAL/overlay only — main keeps the checkpointed base):
	//   overwrite keys 0..9 to valUpd, and add fresh keys 100..149 as valBase.
	for i := 0; i < 10; i++ {
		if err := db.Put(fmt.Appendf(nil, "key-%05d", i), valUpd); err != nil {
			t.Fatalf("overwrite put %d: %v", i, err)
		}
	}
	for i := 100; i < 150; i++ {
		if err := db.Put(fmt.Appendf(nil, "key-%05d", i), valBase); err != nil {
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

	rec, err := Open(crash, 0600, nil)
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
		got, err := rec.Get(k)
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
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Meta lives at offset 0: magic(0-3), version(4-7), pageSize(8-15), pgid(16-23),
	// root(24-31). Bump the version field, leaving the magic intact.
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	buf[4] = 0xFF // version was 1 -> now unsupported
	if err := os.WriteFile(path, buf, 0600); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path, 0600, nil); !errors.Is(err, ErrVersionNotSupported) {
		t.Fatalf("expected ErrVersionNotSupported, got: %v", err)
	}
}

// TestOpen_ErrChecksum: a corrupted meta checksum must fail with ErrChecksum.
// TODO(Rung 4/meta): corrupt a meta field in both pages so the seal mismatches.
func TestOpen_ErrChecksum(t *testing.T) {
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

	// Corrupt a meta DATA field (root pgid, uint64 at offset 24). magic + version stay
	// valid, so ONLY a checksum can catch this — that's what forces the checksum to exist.
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	buf[24] ^= 0xFF
	if err := os.WriteFile(path, buf, 0600); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path, 0600, nil); !errors.Is(err, ErrChecksum) {
		t.Fatalf("expected ErrChecksum, got: %v", err)
	}
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

// TestDB_Open_ReadOnly: a read-only open allows reads, rejects writes, and does not
// mutate the files (no main-file write, no WAL created or deleted).
// (Concurrent read-only openers need file locking — deferred, see TestOpen_MultipleGoroutines.)
func TestDB_Open_ReadOnly(t *testing.T) {
	path := tempfile()
	wal := path + "-wal"
	defer os.RemoveAll(path)
	defer os.RemoveAll(wal)

	// Seed a database read-write, then close it (single file at rest).
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
	if v, err := rdb.Get([]byte("a")); err != nil || !bytes.Equal(v, []byte("1")) {
		t.Fatalf("read-only Get: got %q, err %v", v, err)
	}

	// Writes are rejected up front, with the read-only error.
	if err := rdb.Put([]byte("b"), []byte("2")); !errors.Is(err, ErrDatabaseReadOnly) {
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

// TestOpen_MultipleGoroutines: concurrent opens/closes must be safe (exclusive lock).
func TestOpen_MultipleGoroutines(t *testing.T) {
	t.Skip("deferred: needs file locking")
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

	db, err := Open(path, 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	value := []byte("some-benchmark-value-thats-a-realistic-size")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Appendf(nil, "key-%08d", i)
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPut_SingleKeyOverwrite: repeated Put on ONE key — no splits, isolates
// the per-Put sync cost from split-induced extra writes.
func BenchmarkPut_SingleKeyOverwrite(b *testing.B) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := Open(path, 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	key := []byte("the-one-key")
	value := []byte("some-benchmark-value-thats-a-realistic-size")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}
}
