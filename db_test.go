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

// openDB opens a database for tests in NORMAL sync mode, so the suite is not
// dominated by a per-commit fsync (FULL, the production default, costs ~one macOS
// F_FULLFSYNC per Put). NORMAL is functionally identical here — it only defers the
// fsync to the checkpoint — so every test that does not specifically assert
// per-commit durability uses this.
func openDB(path string) (*DB, error) {
	return Open(path, 0600, &Options{synchronous: SYNCHRONOUS_NORMAL})
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

	db, err := openDB(path)
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

	db, err = openDB(path)
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

	db, err := openDB(path)
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

	db, err := openDB(path)
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

	db, err := openDB(path)
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

	db, err = openDB(path)
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

	db, err := openDB(path)
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

	db, err := openDB(path)
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

	db, err := openDB(path)
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

	db, err := openDB(path)
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
	db, err = openDB(path)
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

	db, err := openDB(path)
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
	db, err = openDB(path)
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
	rec, err := openDB(crash)
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
	rec, err := openDB(crash)
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

	db, err := openDB(path)
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
	db, err = openDB(path)
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

	db, err := openDB(path) // NORMAL by default
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
	db, err = openDB(path)
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

	db, err := openDB(path)
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

	db, err := openDB(path)
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

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
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

	db, err := openDB(path)
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

	db, err := openDB(path)
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

	db, err := openDB(path)
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

	db, err = openDB(path)
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

	if _, err := openDB(path); !errors.Is(err, ErrInvalid) {
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
	if _, err := openDB(path); err == nil {
		t.Fatal("expected error opening a too-small file")
	}
}

// TestOpen_ErrVersionMismatch: meta with a different format version must fail with
// ErrVersionMismatch. TODO(Rung 4/meta): create a valid DB, flip `version` in both
// meta pages, reopen, assert errors.Is(err, ErrVersionMismatch).
func TestOpen_ErrVersionMismatch(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := openDB(path)
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

	if _, err := openDB(path); !errors.Is(err, ErrVersionNotSupported) {
		t.Fatalf("expected ErrVersionNotSupported, got: %v", err)
	}
}

// TestOpen_ErrChecksum: a corrupted meta checksum must fail with ErrChecksum.
// TODO(Rung 4/meta): corrupt a meta field in both pages so the seal mismatches.
func TestOpen_ErrChecksum(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := openDB(path)
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

	if _, err := openDB(path); !errors.Is(err, ErrChecksum) {
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
	db, err := openDB(path)
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

	db, err := openDB(path)
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

	db, err := openDB(path)
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

// -----------------------------------------------------------------------------
// RUNG 5a — Transactions: db.Update / db.View closures.  <-- BUILD THIS
//
// bbolt's transactional core, minus buckets for now (5b adds those). A write txn
// is a closure: db.Update(fn). Its writes commit as ONE atomic unit iff fn returns
// nil; if fn returns an error (or would panic), the WHOLE txn rolls back and NONE
// of its writes survive. db.View is the read-only twin: it never commits, and a
// write inside it fails with ErrTxNotWritable.
//
// This is the payoff of the WAL you already built: `collectedRecords` is the txn's
// write buffer, `persistCollectedRecords` (frames + commit marker) IS Commit, and
// `readNode` already consults `collectedRecords` first (read-your-writes). The work:
//   - a *Tx type; db.Update/db.View(fn func(*Tx) error) error
//   - (*Tx).Put/Get/Writable — the ops the closure calls (throwaway single-tree
//     surface; 5b re-homes them onto *Bucket)
//   - MOVE the WAL flush out of db.Put (one-txn-per-Put) UP into tx.Commit, so many
//     Puts commit as one unit. db.Put becomes a one-shot db.Update wrapper.
//   - Rollback: discard the txn's buffered records AND undo its in-memory tree
//     mutations (the trap — db.Put mutates db.rootNode in place today, so "don't
//     persist" is not enough; the in-memory tree is already dirty).
// -----------------------------------------------------------------------------

// TestTx_UpdateCommitsAtomically: many Puts in one Update land together on a nil
// return, and are read-your-writes visible INSIDE the same txn before commit.
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
			if err := tx.Put([]byte(k), []byte(v)); err != nil {
				return err
			}
			// read-your-writes: a value written earlier in THIS txn is visible now.
			if got, err := tx.Get([]byte(k)); err != nil || !bytes.Equal(got, []byte(v)) {
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
			got, err := tx.Get([]byte(k))
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
		return tx.Put([]byte("keep"), []byte("me"))
	}); err != nil {
		t.Fatal(err)
	}

	errBoom := errors.New("boom")
	got := db.Update(func(tx *Tx) error {
		if err := tx.Put([]byte("doomed-a"), []byte("x")); err != nil {
			return err
		}
		if err := tx.Put([]byte("doomed-b"), []byte("y")); err != nil {
			return err
		}
		return errBoom // abort AFTER writing -> everything above must vanish
	})
	if !errors.Is(got, errBoom) {
		t.Fatalf("Update must return fn's error verbatim, got %v", got)
	}

	if err := db.View(func(tx *Tx) error {
		for _, k := range []string{"doomed-a", "doomed-b"} {
			if _, err := tx.Get([]byte(k)); !errors.Is(err, ErrKeyNotFound) {
				t.Fatalf("rolled-back key %q leaked (in-memory tree left dirty?): err %v", k, err)
			}
		}
		if v, err := tx.Get([]byte("keep")); err != nil || !bytes.Equal(v, []byte("me")) {
			t.Fatalf("rollback clobbered a previously committed key: got %q, err %v", v, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
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
		return tx.Put([]byte("nope"), []byte("nope"))
	})
	if !errors.Is(err, ErrTxNotWritable) {
		t.Fatalf("a write inside View must fail with ErrTxNotWritable, got %v", err)
	}

	// Nothing was written.
	if err := db.View(func(tx *Tx) error {
		if _, err := tx.Get([]byte("nope")); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("View leaked a write: err %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestTx_PutSurvivesRootSplit: a splitting tx.Put on the DEFAULT tree must promote the
// new branch root, exactly like db.Put does. Tx.Put currently discards _put's returned
// root, so a root split inside an Update loses every key that moved to the far side of
// the new root — and persists a stale meta.root. Enough 256B values to split the default
// tree more than once; every key must read back after the (nil) commit.
//
// RED today: Tx.Put is `_, err := tx.db._put(...)`. GREEN once it captures the returned
// root into db.rootNode / db.meta.root when the root actually moved (as db.Put does).
func TestTx_PutSurvivesRootSplit(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const n = 200
	val := bytes.Repeat([]byte("x"), 256)
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < n; i++ {
			if err := tx.Put(fmt.Appendf(nil, "key-%05d", i), val); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	// A split must actually have happened — otherwise a pass proves nothing.
	if db.rootNode.IsLeaf {
		t.Fatal("test setup: default tree did not split; raise n or the value size")
	}

	// Every key the Update committed must be readable.
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "key-%05d", i)
		got, err := db.Get(k)
		if err != nil || !bytes.Equal(got, val) {
			t.Fatalf("key %q lost after a tx.Put root split (dropped new root): err=%v", k, err)
		}
	}
}

// TestTx_PutErrorLeavesRootIntact: a tx.Put that fails validation must NOT touch the
// tree. _put returns (nil, err) on its guard paths (empty/oversized key, etc.), so a
// root-capture that runs unconditionally sets db.rootNode = nil and dereferences a nil
// node. The failed Put must return the error, leave db.rootNode non-nil, and leave a
// previously committed key readable.
//
// RED if tx.Put writes db.rootNode/meta.root before checking that the root actually
// moved. GREEN once the capture is guarded (only when _put returns a new, non-nil root).
func TestTx_PutErrorLeavesRootIntact(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Seed one committed key the failing txn must not disturb.
	if err := db.Update(func(tx *Tx) error {
		return tx.Put([]byte("keep"), []byte("me"))
	}); err != nil {
		t.Fatal(err)
	}

	// A tx.Put with an empty key fails _put's guard -> _put returns (nil, err).
	got := db.Update(func(tx *Tx) error {
		return tx.Put(nil, []byte("x")) // empty key -> ErrKeyEmpty from _put
	})
	if !errors.Is(got, ErrKeyEmpty) {
		t.Fatalf("expected ErrKeyEmpty from a bad tx.Put, got %v", got)
	}

	// The tree must be intact: root still set, seeded key still readable.
	if db.rootNode == nil {
		t.Fatal("tx.Put error nilled out db.rootNode")
	}
	if v, err := db.Get([]byte("keep")); err != nil || !bytes.Equal(v, []byte("me")) {
		t.Fatalf("committed key lost after a failed tx.Put: got %q, err %v", v, err)
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
// The bbolt-faithful surface these tests assume ([]byte names — the string API is a
// later convenience rung):
//   - (*Tx).CreateBucket(name []byte) (*Bucket, error)   // ErrBucketExists if present
//   - (*Tx).Bucket(name []byte) *Bucket                  // nil if missing (no error)
//   - (*Bucket).Put(key, value []byte) error
//   - (*Bucket).Get(key []byte) []byte                   // nil if missing (bbolt style)
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
		if got := b.Get([]byte("name")); !bytes.Equal(got, []byte("alice")) {
			t.Fatalf("read-your-writes in bucket failed: got %q", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	if err := db.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("users"))
		if b == nil {
			t.Fatal("bucket \"users\" not found after commit")
		}
		if got := b.Get([]byte("name")); !bytes.Equal(got, []byte("alice")) {
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
		if got := tx.Bucket([]byte("A")).Get([]byte("k")); !bytes.Equal(got, []byte("from-A")) {
			t.Fatalf("bucket A leaked/collided: got %q, want from-A", got)
		}
		if got := tx.Bucket([]byte("B")).Get([]byte("k")); !bytes.Equal(got, []byte("from-B")) {
			t.Fatalf("bucket B leaked/collided: got %q, want from-B", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestBucket_MissingIsNilAndDoubleCreateErrors: opening an absent bucket returns nil
// (not an auto-created one); creating the same bucket twice returns ErrBucketExists.
func TestBucket_MissingIsNilAndDoubleCreateErrors(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.View(func(tx *Tx) error {
		if b := tx.Bucket([]byte("ghost")); b != nil {
			t.Fatal("Bucket() on a missing name must return nil, not auto-create")
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
		b := tx.Bucket([]byte("cfg"))
		if b == nil {
			t.Fatal("bucket did not survive reopen")
		}
		if got := b.Get([]byte("theme")); !bytes.Equal(got, []byte("dark")) {
			t.Fatalf("bucket value lost across reopen: got %q", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestBucket_SurvivesOwnSplit: the parked trap, now armed. Enough data into ONE
// bucket to split ITS tree, so that bucket's own root pgid moves. The new root pgid
// must be written back into the parent (root-tree) entry for that bucket — otherwise
// the name still points at the pre-split root and reopen loses everything past it. A
// second, tiny bucket rules out cross-bucket clobber while the big one grows and
// allocates pages. Every key must read back, both in-session and after a reopen.
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

	// Readable in the same session (the moved root must be tracked in-memory too).
	if err := db.View(func(tx *Tx) error {
		big := tx.Bucket([]byte("big"))
		for i := 0; i < n; i++ {
			if got := big.Get(fmt.Appendf(nil, "key-%05d", i)); !bytes.Equal(got, val) {
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

	// The real test: reopen resolves "big" -> its NEW (post-split) root pgid.
	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *Tx) error {
		big := tx.Bucket([]byte("big"))
		if big == nil {
			t.Fatal("bucket \"big\" lost across reopen")
		}
		for i := 0; i < n; i++ {
			if got := big.Get(fmt.Appendf(nil, "key-%05d", i)); !bytes.Equal(got, val) {
				t.Fatalf("key %d lost after reopen: the moved bucket root was not written back to its parent entry", i)
			}
		}
		if got := tx.Bucket([]byte("small")).Get([]byte("only")); !bytes.Equal(got, []byte("one")) {
			t.Fatalf("sibling bucket clobbered by the big bucket's growth: got %q", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestBucket_ManyBucketsSurviveRootCatalogSplit: the lurking writeback bug, armed.
// Every other bucket test keeps the root (catalog) tree in a single leaf. Here we
// create enough buckets that the CATALOG tree itself splits — so its root pgid moves.
// CreateBucket does `_, err := _put(rootNode, ...)`, discarding the new catalog root,
// exactly like bucket.Put discarded a moved bucket root. If that return isn't captured
// into meta.root / db.rootNode, buckets vanish once the catalog splits.
//
// Then we grow ONE bucket past a page so ITS tree splits too — its writeback _put lands
// in the now-multi-level catalog, exercising bucket.Put's writeback under a splitting
// parent (not just CreateBucket's). Everything must read back, in-session and reopened.
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
		big := tx.Bucket(fmt.Appendf(nil, "bkt-%05d", growTarget))
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
				b := tx.Bucket(fmt.Appendf(nil, "bkt-%05d", i))
				if b == nil {
					t.Fatalf("%s: bucket %d lost — moved catalog root not written back", when, i)
				}
				if got := b.Get([]byte("k")); !bytes.Equal(got, fmt.Appendf(nil, "v-%05d", i)) {
					t.Fatalf("%s: bucket %d value wrong: got %q", when, i, got)
				}
			}
			big := tx.Bucket(fmt.Appendf(nil, "bkt-%05d", growTarget))
			for j := 0; j < nBig; j++ {
				if got := big.Get(fmt.Appendf(nil, "big-%05d", j)); !bytes.Equal(got, bigVal) {
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
		if parent.rootNode.IsLeaf {
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
		c, err := parent.Bucket([]byte("child"))
		if err != nil {
			return fmt.Errorf("lookup nested child: %w", err)
		}
		if c == nil {
			t.Fatal("nested child bucket not found through a split parent (non-descending lookup)")
		}
		if got := c.Get([]byte("k")); !bytes.Equal(got, []byte("childval")) {
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
		parent := tx.Bucket([]byte("parent"))
		if parent == nil {
			t.Fatal("parent bucket lost across reopen")
		}
		c, err := parent.Bucket([]byte("child"))
		if err != nil || c == nil {
			t.Fatalf("nested child not resolved after reopen: err=%v", err)
		}
		if got := c.Get([]byte("k")); !bytes.Equal(got, []byte("childval")) {
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
		if got := child.Get([]byte("k")); !bytes.Equal(got, []byte("childval")) {
			t.Fatalf("nested read-your-writes failed: got %q", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	verify := func(t *testing.T, db *DB, when string) {
		t.Helper()
		if err := db.View(func(tx *Tx) error {
			p := tx.Bucket([]byte("parent"))
			if p == nil {
				t.Fatalf("%s: parent bucket missing", when)
			}
			if got := p.Get([]byte("pk")); !bytes.Equal(got, []byte("pv")) {
				t.Fatalf("%s: parent's plain key lost: got %q", when, got)
			}
			c, err := p.Bucket([]byte("child"))
			if err != nil || c == nil {
				t.Fatalf("%s: child bucket not resolved: err %v", when, err)
			}
			if got := c.Get([]byte("k")); !bytes.Equal(got, []byte("childval")) {
				t.Fatalf("%s: nested value lost: got %q", when, got)
			}
			// isolation: the child's key is NOT a plain key of the parent
			if got := p.Get([]byte("k")); got != nil {
				t.Fatalf("%s: child key leaked into parent as a plain value: %q", when, got)
			}
			// a sub-bucket key is not a value: Get must return nil, not the raw header
			if got := p.Get([]byte("child")); got != nil {
				t.Fatalf("%s: Get on a sub-bucket key must return nil, got %q", when, got)
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

	page := int(db.meta.pageSize)
	// Each value is ~0.6 of a page: one entry per node fits, two never do.
	vlen := page * 6 / 10
	const n = 6
	keys := make([][]byte, n)
	vals := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Appendf(nil, "key-%03d", i)
		vals[i] = bytes.Repeat([]byte{byte('a' + i)}, vlen)
		if err := db.Put(keys[i], vals[i]); err != nil {
			t.Fatalf("put %d (value %d bytes, page %d): %v", i, vlen, page, err)
		}
	}

	// Every node must fit one page and hold at least one entry (no empty leaf).
	walkNodes(t, db, func(nd *Node) {
		if sz := nd.serializedSize(); sz > page {
			t.Fatalf("node serializes to %d bytes > one %d-byte page", sz, page)
		}
		if len(nd.entries) == 0 {
			t.Fatalf("empty node in the tree (split produced a node with no entries)")
		}
	})

	verify := func(t *testing.T, db *DB, when string) {
		t.Helper()
		for i := 0; i < n; i++ {
			got, err := db.Get(keys[i])
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

// TestBucket_HandleSurvivesCatalogSplit: a live bucket handle held across a catalog
// (top-level root) split must still write back to the RIGHT place. A Bucket caches
// its parent as a raw *Node taken at creation time. When later CreateBucket calls
// split the catalog, db.rootNode becomes a new object and the cached parent goes
// stale. The bucket name "zzz-bucket" sorts last, so the split moves its catalog
// entry OFF the original (in-place) left node onto a new right node — the stale
// cached parent no longer holds it. When the held bucket then splits its own tree,
// Bucket.Put writes the new root pgid into the stale parent node, not the real
// catalog entry. The catalog keeps pointing at the pre-split bucket root, so every
// key added after the bucket split is lost.
//
// RED today: the post-split keys vanish. GREEN once the writeback resolves the
// parent from the current tree instead of a stale cached node.
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
		// Now split the HELD bucket's own tree via its (now stale) parent handle.
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
			b := tx.Bucket([]byte("zzz-bucket"))
			if b == nil {
				t.Fatalf("%s: held bucket lost", when)
			}
			for j := 0; j < nBig; j++ {
				k := fmt.Appendf(nil, "big-%05d", j)
				if got := b.Get(k); !bytes.Equal(got, bigVal) {
					t.Fatalf("%s: key %q lost (writeback landed on a stale parent node): got %d bytes", when, k, len(got))
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

// TestSplit_OneLeafSplitsIntoThree: a single Put can create a node that ONE balanced
// split still leaves over a page. Fill one leaf with small entries to just under a
// page, then insert a single medium value (~0.85 page, sorts last). The overflowing
// node is now ~1.5 pages: cutting it at half a page leaves ~half the small entries
// PLUS the medium value on the right — still over a page. The split loop must keep
// splitting that right sibling until every node fits; if it only splits once and
// climbs, the oversized right node is rejected at persist ("record page content
// exceeds page size") and the Put fails.
func TestSplit_OneLeafSplitsIntoThree(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)
	defer os.RemoveAll(path + "-wal")

	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}

	page := int(db.meta.pageSize)
	smallVal := bytes.Repeat([]byte("s"), 100)
	smallEntry := 12 + 6 + len(smallVal) // flags(4)+keylen(4)+key(6)+vallen(4)+val
	// Fill one leaf to ~0.9 of a page so it stays a single leaf before the big insert.
	nSmall := (page * 9 / 10) / smallEntry
	keys := make([][]byte, 0, nSmall+1)
	for i := 0; i < nSmall; i++ {
		k := fmt.Appendf(nil, "k%05d", i) // 6 bytes, all sort before the big key
		if err := db.Put(k, smallVal); err != nil {
			t.Fatalf("small put %d: %v", i, err)
		}
		keys = append(keys, k)
	}
	if !db.rootNode.IsLeaf {
		t.Fatalf("setup: root split before the big insert (nSmall=%d too high)", nSmall)
	}

	// One medium value, sorts last, ~0.85 page: legal (< one page) but big enough that
	// half the leaf plus this value still overflows a page after a single split.
	bigKey := []byte("zzz-big")
	bigVal := bytes.Repeat([]byte("B"), page*85/100)
	if err := db.Put(bigKey, bigVal); err != nil {
		t.Fatalf("big put (the one a single-split loop rejects): %v", err)
	}
	keys = append(keys, bigKey)

	// The big insert must have grown the tree to at least three leaves.
	leaves := 0
	walkNodes(t, db, func(nd *Node) {
		if sz := nd.serializedSize(); sz > page {
			t.Fatalf("node serializes to %d bytes > one %d-byte page", sz, page)
		}
		if nd.IsLeaf {
			leaves++
		}
	})
	if leaves < 3 {
		t.Fatalf("expected >= 3 leaves (one leaf must split into three), got %d", leaves)
	}

	verify := func(t *testing.T, db *DB, when string) {
		t.Helper()
		for _, k := range keys {
			want := smallVal
			if bytes.Equal(k, bigKey) {
				want = bigVal
			}
			got, err := db.Get(k)
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
