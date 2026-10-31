package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/Issaminu/kvlite/internal/page"
)

type memoryTreeStore struct {
	pageSize    int64
	nextID      page.ID
	nodes       map[page.ID]*MutableNode
	dirty       map[page.ID]*MutableNode
	freed       []page.ID
	allocateErr error
}

func (store *memoryTreeStore) ReadNode(id page.ID) (Node, error) {
	node := store.nodes[id]
	if node == nil {
		return Node{}, ErrKeyNotFound
	}
	return node.ReadOnly(), nil
}

func (store *memoryTreeStore) PageSize() int64 {
	return store.pageSize
}

func (store *memoryTreeStore) AllocatePage() (page.ID, error) {
	if store.allocateErr != nil {
		return 0, store.allocateErr
	}
	pageID := store.nextID
	store.nextID++
	return pageID, nil
}

func (store *memoryTreeStore) FreePage(pageID page.ID) error {
	store.freed = append(store.freed, pageID)
	delete(store.nodes, pageID)
	delete(store.dirty, pageID)
	return nil
}

func (store *memoryTreeStore) StageNode(node *MutableNode) {
	if store.nodes == nil {
		store.nodes = make(map[page.ID]*MutableNode)
	}
	if store.dirty == nil {
		store.dirty = make(map[page.ID]*MutableNode)
	}
	store.nodes[node.PageID()] = node
	store.dirty[node.PageID()] = node
}

func TestTreeFindEntryRef_ReturnsLeafValue(t *testing.T) {
	store := &memoryTreeStore{pageSize: 128, nextID: 2, nodes: make(map[page.ID]*MutableNode)}
	tree := newTestTree(store)
	root := NewLeafNode(1)
	if err := root.InsertEntry(NewEntry(0, []byte("key"), []byte("value"))); err != nil {
		t.Fatal(err)
	}

	entry, found, err := tree.FindEntryRef(root, []byte("key"), true)
	if err != nil {
		t.Fatal(err)
	}
	if !found || !bytes.Equal(entry.Value(), []byte("value")) {
		t.Fatalf("find reference: found=%t value=%q", found, entry.Value())
	}
}

func TestTreeAppendSplitKeepsCompletedLeavesFull(t *testing.T) {
	store := &memoryTreeStore{pageSize: 256, nextID: 3, nodes: make(map[page.ID]*MutableNode)}
	tree := newTestTree(store)
	root := NewLeafNode(2)
	store.StageNode(root)
	value := bytes.Repeat([]byte("v"), 8)
	for index := range 200 {
		var err error
		root, err = tree.PutEntry(root, NewEntry(0, fmt.Appendf(nil, "k%04d", index), value))
		if err != nil {
			t.Fatal(err)
		}
	}
	rightmost := root
	for !rightmost.IsLeaf() {
		rightmost = store.nodes[rightmost.Children[len(rightmost.Children)-1]]
	}
	nextSize := NewEntry(0, []byte("k0200"), value).EncodedSize(true)
	for _, node := range store.nodes {
		if node.IsLeaf() && node != rightmost && node.EncodedSize()+nextSize <= int(store.pageSize) {
			t.Fatalf("completed leaf %d leaves room for another entry", node.PageID())
		}
	}
	assertTreeSeparators(t, store, root)
	for index := range 200 {
		entry, found, err := tree.FindEntryRef(root, fmt.Appendf(nil, "k%04d", index), true)
		if err != nil || !found || !bytes.Equal(entry.Value(), value) {
			t.Fatalf("find %d: found=%v error=%v", index, found, err)
		}
	}
}

func TestTreeAppendSplitRequiresNewGlobalMaximum(t *testing.T) {
	leaf := func(id page.ID, keys string) *MutableNode {
		node := NewLeafNode(id)
		for _, key := range []byte(keys) {
			if err := node.InsertEntry(NewEntry(0, []byte{key}, bytes.Repeat([]byte("v"), 8))); err != nil {
				t.Fatal(err)
			}
		}
		return node
	}
	t.Run("local maximum", func(t *testing.T) {
		left, right := leaf(2, "abcd"), leaf(3, "z")
		root := NewRootNode(1, left.PageID(), right.PageID(), []byte("z"))
		store := &memoryTreeStore{pageSize: 128, nextID: 4, nodes: map[page.ID]*MutableNode{1: root, 2: left, 3: right}}
		got, err := newTestTree(store).PutEntry(root, NewEntry(0, []byte("e"), bytes.Repeat([]byte("v"), 8)))
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Children) != 3 || store.nodes[got.Children[0]].EntryCount() != 2 || store.nodes[got.Children[1]].EntryCount() != 3 {
			t.Fatal("a non-rightmost leaf used an append split")
		}
		assertTreeSeparators(t, store, got)
	})
	t.Run("replacement", func(t *testing.T) {
		root := leaf(2, "abcdefgh")
		before := EncodeNode(root, testNodePageSize)
		store := &memoryTreeStore{pageSize: 256, nextID: 3, nodes: map[page.ID]*MutableNode{2: root}}
		got, err := newTestTree(store).PutEntry(root, NewEntry(0, []byte("h"), bytes.Repeat([]byte("r"), 48)))
		if err != nil {
			t.Fatal(err)
		}
		if got.IsLeaf() || store.nodes[got.Children[0]].EntryCount() != 5 || store.nodes[got.Children[1]].EntryCount() != 3 {
			t.Fatal("a replacement used an append split")
		}
		if !bytes.Equal(before, EncodeNode(root, testNodePageSize)) {
			t.Fatal("replacement split changed committed input")
		}
		assertTreeSeparators(t, store, got)
	})
}

func TestTreePutEntryReturnsAllocationError(t *testing.T) {
	allocationErr := errors.New("no page available")
	store := &memoryTreeStore{pageSize: 64, nextID: 3, nodes: make(map[page.ID]*MutableNode)}
	tree := newTestTree(store)
	root := NewLeafNode(2)
	store.StageNode(root)

	root, err := tree.PutEntry(root, NewEntry(0, []byte("a"), bytes.Repeat([]byte("v"), 16)))
	if err != nil {
		t.Fatal(err)
	}
	store.allocateErr = allocationErr
	if _, err := tree.PutEntry(root, NewEntry(0, []byte("b"), bytes.Repeat([]byte("v"), 16))); !errors.Is(err, allocationErr) {
		t.Fatalf("PutEntry error: got %v, want allocation error", err)
	}
}

// TestTreePutEntry_AcceptsKeyBeyondOnePage checks that large branch separators
// remain valid without changing the tree's page-size policy.
func TestTreePutEntry_AcceptsKeyBeyondOnePage(t *testing.T) {
	const pageSize int64 = 128
	store := &memoryTreeStore{pageSize: pageSize, nextID: 3, nodes: make(map[page.ID]*MutableNode)}
	tree := newTestTree(store)
	root := NewLeafNode(2)
	store.StageNode(root)
	for i := 0; i < 12; i++ {
		key := bytes.Repeat([]byte{byte('a' + i)}, 256)
		var err error
		root, err = tree.PutEntry(root, NewEntry(0, key, []byte("value")))
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 12; i++ {
		key := bytes.Repeat([]byte{byte('a' + i)}, 256)
		entry, found, err := tree.FindEntryRef(root, key, true)
		if err != nil || !found || !bytes.Equal(entry.Value(), []byte("value")) {
			t.Fatalf("key %d: found=%t, error=%v", i, found, err)
		}
	}
	for _, node := range store.nodes {
		if node.EntryCount() == 0 {
			t.Fatal("split produced an empty node")
		}
		if !node.IsLeaf() && len(node.Children) < 2 {
			t.Fatal("split produced a one-child branch")
		}
	}
}

func TestTree_PutAndFindWithoutDatabase(t *testing.T) {
	const (
		pageSize   int64   = 128 // This small page size forces several tree levels.
		rootPageID page.ID = 2   // Pages zero and one are reserved for metadata.
		firstNewID page.ID = 3   // New tree pages start after the root page.
		entryCount         = 100 // This many entries cannot fit in one 128-byte page.
	)

	store := &memoryTreeStore{
		pageSize: pageSize,
		nextID:   firstNewID,
		nodes:    make(map[page.ID]*MutableNode),
	}
	tree := newTestTree(store)
	root := NewLeafNode(rootPageID)
	store.StageNode(root)

	for index := 0; index < entryCount; index++ {
		key := fmt.Appendf(nil, "key-%03d", index)
		value := fmt.Appendf(nil, "value-%03d", index)
		var err error
		root, err = tree.PutEntry(root, NewEntry(0, key, value))
		if err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
	}
	if root.IsLeaf() {
		t.Fatal("tree did not create a branch root")
	}

	for index := 0; index < entryCount; index++ {
		key := fmt.Appendf(nil, "key-%03d", index)
		want := fmt.Appendf(nil, "value-%03d", index)
		entry, found, err := tree.FindEntryRef(root, key, true)
		if err != nil {
			t.Fatalf("find %q: %v", key, err)
		}
		if !found || !bytes.Equal(entry.Value(), want) {
			t.Fatalf("find %q: found=%t value=%q, want %q", key, found, entry.Value(), want)
		}
	}
}

// TestTreePutEntry_RootSplitReturnsNewRoot catches a split that overwrites the
// old root instead of returning a new branch root above it.
func TestTreePutEntry_RootSplitReturnsNewRoot(t *testing.T) {
	const (
		pageSize   int64   = 128
		rootPageID page.ID = 1
	)
	store := &memoryTreeStore{
		pageSize: pageSize,
		nextID:   2,
		nodes:    make(map[page.ID]*MutableNode),
	}
	tree := newTestTree(store)
	root := NewLeafNode(rootPageID)
	store.StageNode(root)

	inserted := 0
	for root.IsLeaf() {
		index := inserted
		key := fmt.Appendf(nil, "key-%02d", index)
		value := bytes.Repeat([]byte("v"), 40)
		var err error
		root, err = tree.PutEntry(root, NewEntry(0, key, value))
		if err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
		inserted++
	}

	if got := root.PageID(); got == rootPageID {
		t.Fatalf("root page ID after split: got old page %d, want a new page", got)
	}
	if root.IsLeaf() {
		t.Fatal("root remains a leaf after split")
	}
	if got := root.Children[0]; got != rootPageID {
		t.Fatalf("left child page ID: got %d, want old root page %d", got, rootPageID)
	}
	for index := 0; index < inserted; index++ {
		key := fmt.Appendf(nil, "key-%02d", index)
		entry, found, err := tree.FindEntryRef(root, key, true)
		if err != nil {
			t.Fatalf("find %q: %v", key, err)
		}
		if !found || !bytes.Equal(entry.Value(), bytes.Repeat([]byte("v"), 40)) {
			t.Fatalf("find %q after root split: found=%t value=%q", key, found, entry.Value())
		}
	}
}

func TestTreePutEntry_DoesNotChangeStoredInputNodes(t *testing.T) {
	left := NewLeafNode(2)
	if err := left.InsertEntry(NewEntry(0, []byte("a"), []byte("old"))); err != nil {
		t.Fatal(err)
	}
	right := NewLeafNode(3)
	if err := right.InsertEntry(NewEntry(0, []byte("z"), []byte("right"))); err != nil {
		t.Fatal(err)
	}
	root := &MutableNode{
		header:   newNodeHeader(NodeTypeBranch, 1),
		entries:  []Entry{{key: []byte("m")}},
		Children: []page.ID{left.PageID(), right.PageID()},
	}
	store := &memoryTreeStore{
		pageSize: 128,
		nextID:   4,
		nodes:    map[page.ID]*MutableNode{1: root, 2: left, 3: right},
	}
	tree := newTestTree(store)
	beforeRoot := EncodeNode(root, testNodePageSize)
	beforeLeft := EncodeNode(left, testNodePageSize)

	if _, err := tree.PutEntry(root, NewEntry(0, []byte("a"), []byte("changed"))); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(EncodeNode(root, testNodePageSize), beforeRoot) {
		t.Fatal("write changed the input root")
	}
	if !bytes.Equal(EncodeNode(left, testNodePageSize), beforeLeft) {
		t.Fatal("write changed the stored input leaf")
	}
}

func TestTreePutEntry_ClonesOnlyChangedLeafWithoutSplit(t *testing.T) {
	left := NewLeafNode(2)
	if err := left.InsertEntry(NewEntry(0, []byte("a"), []byte("old"))); err != nil {
		t.Fatal(err)
	}
	right := NewLeafNode(3)
	if err := right.InsertEntry(NewEntry(0, []byte("z"), []byte("right"))); err != nil {
		t.Fatal(err)
	}
	root := &MutableNode{
		header:   newNodeHeader(NodeTypeBranch, 1),
		entries:  []Entry{{key: []byte("m")}},
		Children: []page.ID{left.PageID(), right.PageID()},
	}
	store := &memoryTreeStore{
		pageSize: 4096,
		nextID:   4,
		nodes:    map[page.ID]*MutableNode{1: root, 2: left, 3: right},
		dirty:    make(map[page.ID]*MutableNode),
	}
	tree := newTestTree(store)

	if _, err := tree.PutEntry(root, NewEntry(0, []byte("a"), []byte("changed"))); err != nil {
		t.Fatal(err)
	}

	if len(store.dirty) != 1 {
		t.Fatalf("dirty node count: got %d, want 1", len(store.dirty))
	}
	if _, ok := store.dirty[left.PageID()]; !ok {
		t.Fatalf("dirty nodes do not contain changed leaf %d", left.PageID())
	}
	if _, ok := store.dirty[root.PageID()]; ok {
		t.Fatalf("unchanged root %d is dirty", root.PageID())
	}
}

type countingPutStore struct {
	*memoryTreeStore
	writes int
}

func TestTreePutIdenticalEntryKeepsNodesClean(t *testing.T) {
	for _, size := range []int{0, 128, 65536} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			store := &countingPutStore{memoryTreeStore: &memoryTreeStore{pageSize: 4096, nodes: make(map[page.ID]*MutableNode)}}
			node := NewLeafNode(2)
			value := bytes.Repeat([]byte("v"), size)
			if err := node.InsertEntry(NewEntry(0, []byte("key"), value)); err != nil {
				t.Fatal(err)
			}
			store.nodes[node.PageID()] = node
			tree := newTestTree(store)
			tree.SkipIdenticalPuts = true
			replacement := bytes.Clone(value)
			got, err := tree.PutEntry(node, NewEntry(0, []byte("key"), replacement))
			if err != nil {
				t.Fatal(err)
			}
			if got != node || store.writes != 0 || len(store.dirty) != 0 {
				t.Fatalf("identical entry changed nodes: writes=%d dirty=%d", store.writes, len(store.dirty))
			}
			if len(replacement) != 0 {
				replacement[0] = 'x'
				if node.entries[0].value[0] != 'v' {
					t.Fatal("identical replacement borrowed caller bytes")
				}
			}
		})
	}
}

func TestTreePutIdenticalPrivateEntrySkipsWrite(t *testing.T) {
	store := &countingPutStore{memoryTreeStore: &memoryTreeStore{pageSize: 4096, nodes: make(map[page.ID]*MutableNode)}}
	committed := NewLeafNode(2)
	if err := committed.InsertEntry(NewEntry(0, []byte("key"), []byte("old"))); err != nil {
		t.Fatal(err)
	}
	store.nodes[committed.PageID()] = committed
	tree := newTestTree(store)
	tree.SkipIdenticalPuts = true
	private, err := tree.PutEntry(committed, NewEntry(0, []byte("key"), []byte("new")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tree.PutEntry(private, NewEntry(0, []byte("key"), []byte("new"))); err != nil {
		t.Fatal(err)
	}
	if store.writes != 1 || string(committed.entries[0].value) != "old" {
		t.Fatalf("private replacement: writes=%d committed=%q", store.writes, committed.entries[0].value)
	}
}

func TestTreePutIdenticalValueStillChangesFlags(t *testing.T) {
	store := &countingPutStore{memoryTreeStore: &memoryTreeStore{pageSize: 4096, nodes: make(map[page.ID]*MutableNode)}}
	node := NewLeafNode(2)
	if err := node.InsertEntry(NewEntry(0, []byte("key"), []byte("value"))); err != nil {
		t.Fatal(err)
	}
	store.nodes[node.PageID()] = node
	tree := newTestTree(store)
	tree.SkipIdenticalPuts = true
	private, err := tree.PutEntry(node, NewEntry(2, []byte("key"), []byte("value")))
	if err != nil {
		t.Fatal(err)
	}
	if store.writes != 1 || private.entries[0].flags != 2 || node.entries[0].flags != 0 {
		t.Fatal("flag replacement was lost or changed committed state")
	}
	if _, err := tree.PutEntry(private, NewEntry(BucketLeafFlag, []byte("key"), []byte("value"))); !errors.Is(err, ErrIncompatibleValue) {
		t.Fatalf("bucket conflict: got %v", err)
	}
	if store.writes != 1 {
		t.Fatal("bucket conflict changed the node")
	}
}

func TestTreePutIdenticalEntryRewritesByDefault(t *testing.T) {
	store := &countingPutStore{memoryTreeStore: &memoryTreeStore{pageSize: 4096, nodes: make(map[page.ID]*MutableNode)}}
	node := NewLeafNode(2)
	if err := node.InsertEntry(NewEntry(0, []byte("key"), []byte("value"))); err != nil {
		t.Fatal(err)
	}
	store.nodes[node.PageID()] = node
	tree := newTestTree(store)
	if _, err := tree.PutEntry(node, NewEntry(0, []byte("key"), []byte("value"))); err != nil {
		t.Fatal(err)
	}
	if store.writes != 1 || len(store.dirty) != 1 {
		t.Fatalf("default identical Put stayed clean: writes=%d dirty=%d", store.writes, len(store.dirty))
	}
}

func TestTreeDeleteEntryRemovesExistingKey(t *testing.T) {
	store := &memoryTreeStore{pageSize: 256, nextID: 3, nodes: make(map[page.ID]*MutableNode)}
	tree := newTestTree(store)
	root := NewLeafNode(2)
	store.StageNode(root)
	for _, key := range []string{"a", "b", "c"} {
		var err error
		root, err = tree.PutEntry(root, NewEntry(0, []byte(key), []byte("value")))
		if err != nil {
			t.Fatal(err)
		}
	}

	root, found, err := tree.DeleteEntry(root, []byte("b"))
	if err != nil || !found {
		t.Fatalf("delete: found %t, error %v", found, err)
	}
	if _, found, err := tree.FindEntryRef(root, []byte("b"), true); err != nil || found {
		t.Fatalf("find deleted key: found %t, error %v", found, err)
	}
}

func TestTreeDeleteEntryMissingKeyDoesNotChangeTree(t *testing.T) {
	store := &memoryTreeStore{pageSize: 256, nextID: 3, nodes: make(map[page.ID]*MutableNode)}
	tree := newTestTree(store)
	root := NewLeafNode(2)
	store.StageNode(root)
	store.dirty = nil

	gotRoot, found, err := tree.DeleteEntry(root, []byte("missing"))
	if err != nil || found {
		t.Fatalf("delete: found %t, error %v", found, err)
	}
	if gotRoot != root {
		t.Fatal("missing delete replaced the root")
	}
	if len(store.dirty) != 0 {
		t.Fatal("missing delete staged a node")
	}
}

func TestTreeDeleteEntryValidatesKeyAndRejectsBucket(t *testing.T) {
	store := &memoryTreeStore{pageSize: 256, nextID: 3, nodes: make(map[page.ID]*MutableNode)}
	tree := newTestTree(store)
	root := NewLeafNode(2)
	store.StageNode(root)
	if err := root.InsertEntry(NewEntry(BucketLeafFlag, []byte("nested"), page.EncodeID(9))); err != nil {
		t.Fatal(err)
	}

	if _, _, err := tree.DeleteEntry(root, nil); !errors.Is(err, ErrKeyRequired) {
		t.Fatalf("empty key: got %v, want ErrKeyRequired", err)
	}
	largeKey := bytes.Repeat([]byte("k"), MaxKeySize+1)
	if _, _, err := tree.DeleteEntry(root, largeKey); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("large key: got %v, want ErrKeyTooLarge", err)
	}
	if _, _, err := tree.DeleteEntry(root, []byte("nested")); !errors.Is(err, ErrIncompatibleValue) {
		t.Fatalf("bucket key: got %v, want ErrIncompatibleValue", err)
	}
}

func TestTreeDeleteBucketEntryRejectsPlainValue(t *testing.T) {
	store := &memoryTreeStore{pageSize: 256, nextID: 3, nodes: make(map[page.ID]*MutableNode)}
	tree := newTestTree(store)
	root := NewLeafNode(2)
	store.StageNode(root)
	if err := root.InsertEntry(NewEntry(0, []byte("plain"), []byte("value"))); err != nil {
		t.Fatal(err)
	}
	if err := root.InsertEntry(NewEntry(BucketLeafFlag, []byte("nested"), page.EncodeID(9))); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tree.DeleteBucketEntry(root, []byte("plain")); !errors.Is(err, ErrIncompatibleValue) {
		t.Fatalf("plain value: got %v, want ErrIncompatibleValue", err)
	}
	root, found, err := tree.DeleteBucketEntry(root, []byte("nested"))
	if err != nil || !found {
		t.Fatalf("bucket delete: found %t, error %v", found, err)
	}
	if _, found, err := tree.FindEntryRef(root, []byte("nested"), true); err != nil || found {
		t.Fatalf("deleted bucket entry: found %t, error %v", found, err)
	}
}

func TestTreeDeleteEntryDoesNotDirtyUnchangedParent(t *testing.T) {
	store := &memoryTreeStore{pageSize: 256, nextID: 5, nodes: make(map[page.ID]*MutableNode)}
	tree := newTestTree(store)
	left := NewLeafNode(3)
	right := NewLeafNode(4)
	for _, key := range []string{"a", "b", "c", "d"} {
		if err := left.InsertEntry(NewEntry(0, []byte(key), bytes.Repeat([]byte("l"), 32))); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{"m", "n", "o", "p"} {
		if err := right.InsertEntry(NewEntry(0, []byte(key), bytes.Repeat([]byte("r"), 32))); err != nil {
			t.Fatal(err)
		}
	}
	root := NewRootNode(2, left.PageID(), right.PageID(), []byte("m"))
	store.StageNode(root)
	store.StageNode(left)
	store.StageNode(right)
	store.dirty = nil

	gotRoot, found, err := tree.DeleteEntry(root, []byte("o"))
	if err != nil || !found {
		t.Fatalf("delete: found %t, error %v", found, err)
	}
	if gotRoot != root {
		t.Fatal("delete replaced an unchanged root")
	}
	if _, dirty := store.dirty[root.PageID()]; dirty {
		t.Fatal("delete staged an unchanged parent")
	}
	if got := len(store.dirty); got != 1 {
		t.Fatalf("dirty node count: got %d, want 1", got)
	}
}

// TestTreeDeleteEntryDoesNotMergeCommittedParentInPlace checks a parent merge.
// Removing an empty first leaf forces repair of its under-full parent.
// The merge must change a private parent, not its committed image.
func TestTreeDeleteEntryDoesNotMergeCommittedParentInPlace(t *testing.T) {
	leaf := func(pageID page.ID, keys ...string) *MutableNode {
		node := NewLeafNode(pageID)
		for _, key := range keys {
			if err := node.InsertEntry(NewEntry(0, []byte(key), bytes.Repeat([]byte("v"), 16))); err != nil {
				t.Fatal(err)
			}
		}
		return node
	}
	branch := func(pageID page.ID, separators []string, children ...page.ID) *MutableNode {
		node := &MutableNode{header: newNodeHeader(NodeTypeBranch, pageID), Children: children}
		for _, separator := range separators {
			node.entries = append(node.entries, Entry{key: []byte(separator)})
		}
		return node
	}
	firstKeys := []string{"a", "b", "c", "d"}

	firstKeys = []string{"a"}

	nodes := []*MutableNode{
		leaf(10, firstKeys...), leaf(11, "e", "f", "g", "h"),
		leaf(12, "i", "j", "k", "l"), leaf(13, "m", "n", "o", "p"), leaf(14, "q", "r", "s", "t"),
		branch(4, []string{"e"}, 10, 11),
		branch(5, []string{"m", "q"}, 12, 13, 14),
		branch(2, []string{"i"}, 4, 5),
	}
	store := &memoryTreeStore{pageSize: 256, nextID: 20, nodes: make(map[page.ID]*MutableNode)}
	committed := make(map[*MutableNode][]byte)
	for _, node := range nodes {
		store.nodes[node.PageID()] = node
		committed[node] = EncodeNode(node, testNodePageSize)
	}
	parent := store.nodes[4]
	if size := int64(parent.EncodedSize()); size >= store.pageSize/4 {
		t.Fatalf("test setup: parent uses %d bytes, want an under-full parent below %d bytes", size, store.pageSize/4)
	}

	root, found, err := newTestTree(store).DeleteEntry(store.nodes[2], []byte("a"))
	if err != nil || !found {
		t.Fatalf("delete: found %t, error %v", found, err)
	}
	for node, before := range committed {
		if !bytes.Equal(EncodeNode(node, testNodePageSize), before) {
			t.Errorf("delete changed committed node %d in place", node.PageID())
		}
	}
	if _, dirty := store.dirty[4]; !dirty {
		t.Fatal("the parent merge did not stage a private parent")
	}
	if root.PageID() != 4 || !slices.Contains(store.freed, page.ID(5)) {
		t.Fatalf("the parent merge did not collapse the root: root=%d, freed=%v", root.PageID(), store.freed)
	}
}

func TestTreeDeleteEntryMergesAndCollapsesRoot(t *testing.T) {
	store := &memoryTreeStore{pageSize: 128, nextID: 3, nodes: make(map[page.ID]*MutableNode)}
	tree := newTestTree(store)
	root := NewLeafNode(2)
	store.StageNode(root)

	for index := range 40 {
		var err error
		root, err = tree.PutEntry(root, NewEntry(0, fmt.Appendf(nil, "key-%02d", index), bytes.Repeat([]byte("v"), 16)))
		if err != nil {
			t.Fatal(err)
		}
	}
	for index := range 39 {
		key := fmt.Appendf(nil, "key-%02d", index)
		var found bool
		var err error
		root, found, err = tree.DeleteEntry(root, key)
		if err != nil || !found {
			t.Fatalf("delete %q: found %t, error %v", key, found, err)
		}
	}
	if !root.IsLeaf() {
		t.Fatalf("root after merge: got branch page %d", root.PageID())
	}
	if len(store.freed) == 0 {
		t.Fatal("delete did not retire merged pages")
	}
}

func TestTreeDeleteEmptyLeafKeepsOtherLeavesClean(t *testing.T) {
	for _, count := range []int{2, 3} {
		for removed := range count {
			t.Run(fmt.Sprintf("children=%d/remove=%d", count, removed), func(t *testing.T) {
				store := &memoryTreeStore{pageSize: 128, nodes: make(map[page.ID]*MutableNode)}
				root := &MutableNode{header: newNodeHeader(NodeTypeBranch, 2)}
				leaves := make([]*MutableNode, count)
				for i := range count {
					leaf := NewLeafNode(page.ID(i + 3))
					key := []byte{byte('a' + i)}
					if err := leaf.InsertEntry(NewEntry(0, key, bytes.Repeat([]byte("v"), 65536))); err != nil {
						t.Fatal(err)
					}
					leaves[i] = leaf
					store.nodes[leaf.PageID()] = leaf
					root.Children = append(root.Children, leaf.PageID())
					if i != 0 {
						root.entries = append(root.entries, NewEntry(0, key, nil))
					}
				}
				store.nodes[root.PageID()] = root
				gotRoot, found, err := newTestTree(store).DeleteEntry(root, []byte{byte('a' + removed)})
				if err != nil || !found {
					t.Fatalf("delete: found=%t error=%v", found, err)
				}
				for i, leaf := range leaves {
					if i == removed {
						if !slices.Contains(store.freed, leaf.PageID()) {
							t.Fatal("empty leaf was not freed")
						}
						continue
					}
					if _, dirty := store.dirty[leaf.PageID()]; dirty {
						t.Fatalf("unchanged leaf %d is dirty", leaf.PageID())
					}
					entry, found, err := newTestTree(store).FindEntryRef(gotRoot, []byte{byte('a' + i)}, true)
					if err != nil || !found || !bytes.Equal(entry.Value(), leaf.entries[0].value) {
						t.Fatalf("retained leaf %d: found=%t error=%v", leaf.PageID(), found, err)
					}
				}
				assertTreeSeparators(t, store, gotRoot)
			})
		}
	}
}

func TestTreeDeleteEmptyFirstLeafUpdatesAncestor(t *testing.T) {
	store := &memoryTreeStore{pageSize: 128, nodes: make(map[page.ID]*MutableNode)}
	branch := func(id page.ID, keys string, firstLeaf page.ID) *MutableNode {
		node := &MutableNode{header: newNodeHeader(NodeTypeBranch, id)}
		for i := range len(keys) {
			leaf := NewLeafNode(firstLeaf + page.ID(i))
			if err := leaf.InsertEntry(NewEntry(0, []byte{keys[i]}, bytes.Repeat([]byte("v"), 65536))); err != nil {
				t.Fatal(err)
			}
			store.nodes[leaf.PageID()] = leaf
			node.Children = append(node.Children, leaf.PageID())
			if i != 0 {
				node.entries = append(node.entries, NewEntry(0, []byte{keys[i]}, nil))
			}
		}
		store.nodes[id] = node
		return node
	}
	left := branch(3, "abc", 10)
	right := branch(4, "mno", 20)
	left = NewRootNode(5, left.PageID(), branch(6, "ghi", 30).PageID(), []byte("g"))
	right = NewRootNode(7, right.PageID(), branch(8, "stu", 40).PageID(), []byte("s"))
	store.nodes[left.PageID()] = left
	store.nodes[right.PageID()] = right
	root := NewRootNode(2, left.PageID(), right.PageID(), []byte("m"))
	store.nodes[root.PageID()] = root
	gotRoot, found, err := newTestTree(store).DeleteEntry(root, []byte("m"))
	if err != nil || !found {
		t.Fatalf("delete: found=%t error=%v", found, err)
	}
	wantSeparator := "n"

	wantSeparator = "m"
	if gotRoot != root {
		t.Fatal("removing the first leaf copied an unchanged ancestor")
	}
	if _, dirty := store.dirty[root.PageID()]; dirty {
		t.Fatal("removing the first leaf staged an unchanged ancestor")
	}

	if string(gotRoot.entries[0].key) != wantSeparator {
		t.Fatalf("ancestor separator: got %q, want %q", gotRoot.entries[0].key, wantSeparator)
	}
	if string(root.entries[0].key) != "m" {
		t.Fatal("delete changed the committed ancestor in place")
	}
	for _, id := range []page.ID{10, 11, 12, 21, 22, 30, 31, 32, 40, 41, 42} {
		if _, dirty := store.dirty[id]; dirty {
			t.Fatalf("unchanged leaf %d is dirty", id)
		}
	}
	assertTreeSeparators(t, store, gotRoot)
}

func TestTreeDeleteEntryRandomOrderKeepsExactSeparators(t *testing.T) {
	store := &memoryTreeStore{pageSize: 192, nextID: 3, nodes: make(map[page.ID]*MutableNode)}
	tree := newTestTree(store)
	root := NewLeafNode(2)
	store.StageNode(root)

	const keyCount = 200
	for index := range keyCount {
		var err error
		root, err = tree.PutEntry(root, NewEntry(0, fmt.Appendf(nil, "key-%03d", index), bytes.Repeat([]byte("v"), 24)))
		if err != nil {
			t.Fatal(err)
		}
	}

	deleted := make([]bool, keyCount)
	for step := range keyCount {
		index := (step * 73) % keyCount
		key := fmt.Appendf(nil, "key-%03d", index)
		var found bool
		var err error
		root, found, err = tree.DeleteEntry(root, key)
		if err != nil || !found {
			t.Fatalf("delete %q: found %t, error %v", key, found, err)
		}
		deleted[index] = true
		assertTreeSeparators(t, store, root)
		for candidate := range keyCount {
			_, found, err := tree.FindEntryRef(root, fmt.Appendf(nil, "key-%03d", candidate), true)
			if err != nil {
				t.Fatal(err)
			}
			if found == deleted[candidate] {
				t.Fatalf("find key %d after step %d: found %t, deleted %t", candidate, step, found, deleted[candidate])
			}
		}
	}
}

func TestDeleteRepairDoesNotMergePastPageSize(t *testing.T) {
	const pageSize int64 = 256
	left := NewLeafNode(3)
	right := NewLeafNode(4)
	if err := left.InsertEntry(NewEntry(0, []byte("a"), bytes.Repeat([]byte("l"), 210))); err != nil {
		t.Fatal(err)
	}
	if err := right.InsertEntry(NewEntry(0, []byte("z"), []byte("r"))); err != nil {
		t.Fatal(err)
	}
	parent := NewRootNode(2, left.PageID(), right.PageID(), []byte("z"))

	if got := mergedChildrenSize(parent, 0, left, right); got <= pageSize {
		t.Fatalf("merged size: got %d, want more than %d", got, pageSize)
	}
	if !redistributeChildren(parent, 0, left, right, pageSize, pageSize/4) {
		t.Fatal("redistribution failed for two valid sibling pages")
	}
	if int64(left.EncodedSize()) > pageSize || int64(right.EncodedSize()) > pageSize {
		t.Fatalf("redistribution made an oversized page: left %d, right %d", left.EncodedSize(), right.EncodedSize())
	}
}

func TestDeleteRepairRedistributesByEncodedBytes(t *testing.T) {
	const pageSize int64 = 256
	left := NewLeafNode(3)
	right := NewLeafNode(4)
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		if err := left.InsertEntry(NewEntry(0, []byte(key), bytes.Repeat([]byte("l"), 25))); err != nil {
			t.Fatal(err)
		}
	}
	if err := right.InsertEntry(NewEntry(0, []byte("z"), bytes.Repeat([]byte("r"), 25))); err != nil {
		t.Fatal(err)
	}
	parent := NewRootNode(2, left.PageID(), right.PageID(), []byte("z"))

	if !redistributeChildren(parent, 0, left, right, pageSize, pageSize/4) {
		t.Fatal("redistribution failed")
	}
	if right.EntryCount() < 3 {
		t.Fatalf("right entry count: got %d, want at least 3", right.EntryCount())
	}
	if int64(left.EncodedSize()) < pageSize/4 || int64(right.EncodedSize()) < pageSize/4 {
		t.Fatalf("redistribution left an avoidable underfull page: left %d, right %d", left.EncodedSize(), right.EncodedSize())
	}
	if got, want := parent.EntryAt(0).Key(), right.EntryAt(0).Key(); !bytes.Equal(got, want) {
		t.Fatalf("separator: got %q, want %q", got, want)
	}
}

func TestDeleteRepairSelectsSiblingThatRepairsUnderfullPage(t *testing.T) {
	const pageSize int64 = 256
	left := NewLeafNode(3)
	node := NewLeafNode(4)
	right := NewLeafNode(5)
	if err := left.InsertEntry(NewEntry(0, []byte("a"), bytes.Repeat([]byte("l"), 205))); err != nil {
		t.Fatal(err)
	}
	if err := node.InsertEntry(NewEntry(0, []byte("m"), []byte("n"))); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"z1", "z2", "z3", "z4", "z5"} {
		if err := right.InsertEntry(NewEntry(0, []byte(key), bytes.Repeat([]byte("r"), 30))); err != nil {
			t.Fatal(err)
		}
	}
	parent := &MutableNode{
		header:   newNodeHeader(NodeTypeBranch, 2),
		entries:  []Entry{{key: []byte("m")}, {key: []byte("z1")}},
		Children: []page.ID{left.PageID(), node.PageID(), right.PageID()},
	}

	useLeft, ok := chooseSiblingRedistribution(parent, 1, left, node, right, pageSize, pageSize/4)
	if !ok {
		t.Fatal("no redistribution was selected")
	}
	if useLeft {
		t.Fatal("selected the left sibling that leaves the page underfull")
	}
}

func TestMergedBranchSizeMatchesEncodedResult(t *testing.T) {
	leftLeaf := NewLeafNode(30)
	rightLeaf := NewLeafNode(31)
	otherLeftLeaf := NewLeafNode(32)
	otherRightLeaf := NewLeafNode(33)
	left := NewRootNode(20, leftLeaf.PageID(), rightLeaf.PageID(), []byte("b"))
	right := NewRootNode(21, otherLeftLeaf.PageID(), otherRightLeaf.PageID(), []byte("z"))
	parent := NewRootNode(10, left.PageID(), right.PageID(), []byte("m"))

	want := mergedChildrenSize(parent, 0, left, right)
	parentCopy := parent.Clone()
	leftCopy := left.Clone()
	rightCopy := right.Clone()
	mergeChildren(parentCopy, 0, leftCopy, rightCopy)
	if got := int64(leftCopy.EncodedSize()); got != want {
		t.Fatalf("merged branch size: got %d, want %d", got, want)
	}
}

func assertTreeSeparators(t *testing.T, store *memoryTreeStore, node *MutableNode) []byte {
	t.Helper()
	if node.IsLeaf() {
		if len(node.entries) == 0 {
			return nil
		}
		return slices.Clone(node.entries[0].key)
	}

	minimum := assertTreeSeparators(t, store, store.nodes[node.Children[0]])
	for index := 1; index < len(node.Children); index++ {
		childMinimum := assertTreeSeparators(t, store, store.nodes[node.Children[index]])

		left := store.nodes[node.Children[index-1]]
		for !left.IsLeaf() {
			left = store.nodes[left.Children[len(left.Children)-1]]
		}
		separator := node.entries[index-1].key
		if bytes.Compare(left.entries[len(left.entries)-1].key, separator) >= 0 || bytes.Compare(separator, childMinimum) > 0 {
			t.Fatalf("branch %d separator %d does not divide its child ranges", node.PageID(), index-1)
		}

	}
	return minimum
}

func TestLeafSeparator(t *testing.T) {
	for _, pair := range [][2][]byte{
		{[]byte("account/apple"), []byte("account/banana")},
		{{0}, {0, 0}},
		{{0, 0xff}, {1, 0}},
		{{1, 0}, {1, 0, 0}},
		{{1, 2, 3}, {1, 3}},
		{bytes.Repeat([]byte{1}, 4096), append(bytes.Repeat([]byte{1}, 4096), 0)},
	} {
		left, right := pair[0], pair[1]
		separator := leafSeparator(left, right)
		if bytes.Compare(left, separator) >= 0 || bytes.Compare(separator, right) > 0 {
			t.Fatalf("%x does not divide %x and %x", separator, left, right)
		}
		if len(separator) > 1 && bytes.Compare(left, separator[:len(separator)-1]) < 0 {
			t.Fatalf("separator %x has an unnecessary byte", separator)
		}

	}
}

func TestAppendSplitSeparator(t *testing.T) {
	store := &memoryTreeStore{pageSize: 256, nextID: 3, nodes: make(map[page.ID]*MutableNode)}
	tree := newTestTree(store)
	root := NewLeafNode(2)
	store.StageNode(root)
	keys := [][]byte{
		append([]byte{'a'}, bytes.Repeat([]byte{'x'}, 64)...),
		append([]byte{'b'}, bytes.Repeat([]byte{'x'}, 64)...),
		append([]byte{'c'}, bytes.Repeat([]byte{'x'}, 64)...),
	}
	for _, key := range keys {
		var err error
		root, err = tree.PutEntry(root, NewEntry(0, key, bytes.Repeat([]byte{'v'}, 16)))
		if err != nil {
			t.Fatal(err)
		}
	}
	if root.IsLeaf() || store.nodes[root.Children[0]].EntryCount() != 2 || store.nodes[root.Children[1]].EntryCount() != 1 {
		t.Fatal("fixture did not use the append split")
	}
	want := keys[2]

	want = []byte{'c'}

	if !bytes.Equal(root.entries[0].key, want) {
		t.Fatalf("append separator: got %q, want %q", root.entries[0].key, want)
	}
	assertTreeSeparators(t, store, root)
}

func TestSeparatorBoundsAndGapInserts(t *testing.T) {
	for _, pageSize := range []int64{128, 512, 4096} {
		t.Run(fmt.Sprint(pageSize), func(t *testing.T) {
			store := &memoryTreeStore{pageSize: pageSize, nextID: 3, nodes: make(map[page.ID]*MutableNode)}
			tree := newTestTree(store)
			root := NewLeafNode(2)
			store.StageNode(root)
			reference := make(map[string][]byte)
			keys := separatorTestKeys("mixed", 512)
			random := rand.New(rand.NewSource(718))
			put := func(key []byte) {
				t.Helper()
				var err error
				root, err = tree.PutEntry(root, NewEntry(0, key, key))
				if err != nil {
					t.Fatal(err)
				}
				reference[string(key)] = slices.Clone(key)
				store.dirty = nil
			}
			for _, i := range random.Perm(len(keys)) {
				put(keys[i])
			}
			assertTreeSeparators(t, store, root)

			// Insert the artificial boundaries and keys beside them. This tests
			// the gaps that exact minimum separators did not expose before.
			var boundaries [][]byte
			for _, node := range store.nodes {
				if !node.IsLeaf() {
					for _, entry := range node.entries {
						boundaries = append(boundaries, slices.Clone(entry.key))
					}
				}
			}
			for _, key := range boundaries {
				put(key)
				put(append(slices.Clone(key), 0))
				put(append(slices.Clone(key), 0xff))
			}
			checkSeparatorTree(t, tree, store, root, reference)
			for step := range 1800 {
				key := keys[random.Intn(len(keys))]
				if random.Intn(3) == 0 {
					put(key)
				} else {
					var found bool
					var err error
					root, found, err = tree.DeleteEntry(root, key)
					_, want := reference[string(key)]
					if err != nil || found != want {
						t.Fatalf("delete %x: found %t, want %t, error %v", key, found, want, err)
					}
					delete(reference, string(key))
					store.dirty = nil
				}
				if step%32 == 0 {
					checkSeparatorTree(t, tree, store, root, reference)
				}
			}
			checkSeparatorTree(t, tree, store, root, reference)
		})
	}
}

func checkSeparatorTree(t *testing.T, tree *testTree, store *memoryTreeStore, root *MutableNode, reference map[string][]byte) {
	t.Helper()
	assertTreeSeparators(t, store, root)
	keys := make([]string, 0, len(reference))
	for key := range reference {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	cursor := tree.Cursor(root)
	found, err := cursor.First()
	for i, key := range keys {
		_, actualKey, actualValue := cursor.Entry()
		if err != nil || !found || !bytes.Equal(actualKey, []byte(key)) || !bytes.Equal(actualValue, reference[key]) {
			t.Fatalf("scan entry %d: found %t, error %v", i, found, err)
		}
		found, err = cursor.Next()
	}
	if err != nil || found {
		t.Fatalf("scan end: found %t, error %v", found, err)
	}
	for _, key := range keys {
		entry, found, err := tree.FindEntryRef(root, []byte(key), true)
		if err != nil || !found || !bytes.Equal(entry.value, reference[key]) {
			t.Fatalf("lookup %x: found %t, error %v", key, found, err)
		}
		found, err = cursor.Seek([]byte(key))
		_, actualKey, _ := cursor.Entry()
		if err != nil || !found || !bytes.Equal(actualKey, []byte(key)) {
			t.Fatalf("seek %x: found %t, error %v", key, found, err)
		}
	}
}

func separatorTestKeys(kind string, count int) [][]byte {
	keys := make([][]byte, count)
	for i := range keys {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i)*3+1)
		switch kind {
		case "long":
			key = append(key, bytes.Repeat([]byte{byte(i)}, 120)...)
		case "prefix":
			key = append(bytes.Repeat([]byte("account/"), 15), key...)
		case "mixed":
			key = append([]byte{0, byte(i % 4)}, key...)
			key = append(key, bytes.Repeat([]byte{byte(i)}, i%70)...)
		}
		keys[i] = key
	}
	return keys
}

func TestFenceKeepsParentCleanAfterMinimumDelete(t *testing.T) {
	store := &memoryTreeStore{pageSize: 512, nextID: 5, nodes: make(map[page.ID]*MutableNode)}
	tree := newTestTree(store)
	left, right := NewLeafNode(2), NewLeafNode(3)
	for i := range 4 {
		for _, item := range []struct {
			node *MutableNode
			side string
		}{{left, "a"}, {right, "b"}} {
			key := fmt.Appendf(nil, "account/%s/%03d", item.side, i)
			if err := item.node.InsertEntry(NewEntry(0, key, key)); err != nil {
				t.Fatal(err)
			}
		}
	}
	separator := leafSeparator(left.entries[len(left.entries)-1].key, right.entries[0].key)
	root := NewRootNode(4, left.PageID(), right.PageID(), separator)
	store.StageNode(left)
	store.StageNode(right)
	store.StageNode(root)
	store.dirty = nil
	oldRoot := root
	oldSeparator := slices.Clone(root.entries[0].key)
	var err error
	root, found, err := tree.DeleteEntry(root, []byte("account/b/000"))
	if err != nil || !found {
		t.Fatalf("delete minimum: found=%t, error=%v", found, err)
	}
	assertTreeSeparators(t, store, root)

	if root != oldRoot || !bytes.Equal(root.entries[0].key, oldSeparator) {
		t.Fatal("a minimum-only delete changed the saved parent fence")
	}
	if _, dirty := store.dirty[root.PageID()]; dirty {
		t.Fatal("a minimum-only delete made the parent dirty")
	}

}

func TestFenceDeletesAndRefillsOldGaps(t *testing.T) {
	for _, pageSize := range []int64{128, 512} {
		t.Run(fmt.Sprint(pageSize), func(t *testing.T) {
			store := &memoryTreeStore{pageSize: pageSize, nextID: 3, nodes: make(map[page.ID]*MutableNode)}
			tree := newTestTree(store)
			root := NewLeafNode(2)
			store.StageNode(root)
			reference := make(map[string][]byte)
			keys := separatorTestKeys("mixed", 1024)
			put := func(key []byte) {
				t.Helper()
				var err error
				root, err = tree.PutEntry(root, NewEntry(0, key, key))
				if err != nil {
					t.Fatal(err)
				}
				reference[string(key)] = slices.Clone(key)
				store.dirty = nil
			}
			remove := func(key []byte) {
				t.Helper()
				var found bool
				var err error
				root, found, err = tree.DeleteEntry(root, key)
				_, want := reference[string(key)]
				if err != nil || found != want {
					t.Fatalf("delete %x: found=%t, want=%t, error=%v", key, found, want, err)
				}
				delete(reference, string(key))
				store.dirty = nil
			}
			for _, index := range rand.New(rand.NewSource(519)).Perm(len(keys)) {
				put(keys[index])
			}
			if root.IsLeaf() || store.nodes[root.Children[0]].IsLeaf() {
				t.Fatal("fixture did not force at least three tree levels")
			}
			var oldFences, rightMinima [][]byte
			for _, node := range store.nodes {
				if node.IsLeaf() {
					continue
				}
				for i, child := range node.Children[1:] {
					oldFences = append(oldFences, slices.Clone(node.entries[i].key))
					minimum, err := tree.subtreeMinimum(store.nodes[child])
					if err != nil {
						t.Fatal(err)
					}
					rightMinima = append(rightMinima, slices.Clone(minimum))
				}
			}
			for i, key := range rightMinima {
				remove(key)
				if i%32 == 0 {
					checkSeparatorTree(t, tree, store, root, reference)
				}
			}
			// Remove most keys from each range. This forces sibling moves,
			// first-child removal, branch merging, and ancestor repair.
			for i, key := range keys {
				if i%4 != 1 {
					remove(key)
				}
				if i%32 == 0 {
					checkSeparatorTree(t, tree, store, root, reference)
				}
			}
			for _, key := range keys {
				put(key)
			}
			for _, key := range oldFences {
				put(key)
				put(append(slices.Clone(key), 0))
				put(append(slices.Clone(key), 0xff))
			}
			checkSeparatorTree(t, tree, store, root, reference)
			remaining := make([]string, 0, len(reference))
			for key := range reference {
				remaining = append(remaining, key)
			}
			slices.Sort(remaining)
			for i := len(remaining) - 1; i >= 0; i-- {
				remove([]byte(remaining[i]))
				if i%32 == 0 {
					checkSeparatorTree(t, tree, store, root, reference)
				}
			}
			if !root.IsLeaf() || root.EntryCount() != 0 {
				t.Fatal("deleting all keys did not collapse to an empty leaf root")
			}
			for _, key := range oldFences {
				put(key)
			}
			checkSeparatorTree(t, tree, store, root, reference)
		})
	}
}

// subtreeMinimum finds the first key below node. The search follows the first
// child until it reaches a leaf. The returned key belongs to that leaf and
// remains valid while the leaf remains valid. An empty leaf returns nil.
func (tree *testTree) subtreeMinimum(node *MutableNode) ([]byte, error) {
	for !node.IsLeaf() {
		var err error
		node, err = tree.readChild(node, 0)
		if err != nil {
			return nil, err
		}
	}
	return node.firstKey(), nil
}

// testTree translates decoded fixtures to the production page-ID interface.
// Assertions can inspect fixture nodes without adding representation choices to Tree.
type testTree struct {
	*Tree
	fixtures interface{ seedNode(*MutableNode) }
}

func newTestTree(store interface {
	Store
	seedNode(*MutableNode)
}) *testTree {
	return &testTree{Tree: NewTree(store), fixtures: store}
}

func (tree *testTree) register(root *MutableNode) {
	if _, err := tree.store.ReadNode(root.PageID()); err != nil {
		tree.fixtures.seedNode(root)
	}
}
func (tree *testTree) node(id page.ID, err error) (*MutableNode, error) {
	if err != nil {
		return nil, err
	}
	node, err := tree.store.ReadNode(id)
	if err != nil {
		return nil, err
	}
	return node.decode(tree.store.PageSize())
}
func (tree *testTree) PutEntry(root *MutableNode, entry Entry) (*MutableNode, error) {
	tree.register(root)
	return tree.node(tree.Tree.PutEntry(root.PageID(), entry))
}
func (tree *testTree) FindEntryRef(root *MutableNode, key []byte, includeKey bool) (Entry, bool, error) {
	tree.register(root)
	return tree.Tree.FindEntryRef(root.PageID(), key, includeKey)
}
func (tree *testTree) FindEntryRefFromPage(id page.ID, key []byte, includeKey bool) (Entry, bool, error) {
	return tree.Tree.FindEntryRef(id, key, includeKey)
}
func (tree *testTree) DeleteEntry(root *MutableNode, key []byte) (*MutableNode, bool, error) {
	tree.register(root)
	id, found, err := tree.Tree.DeleteEntry(root.PageID(), key)
	if err != nil {
		return nil, found, err
	}
	node, err := tree.node(id, nil)
	return node, found, err
}
func (tree *testTree) DeleteBucketEntry(root *MutableNode, key []byte) (*MutableNode, bool, error) {
	tree.register(root)
	id, found, err := tree.Tree.DeleteBucketEntry(root.PageID(), key)
	if err != nil {
		return nil, found, err
	}
	node, err := tree.node(id, nil)
	return node, found, err
}
func (tree *testTree) DeleteEntries(root *MutableNode, keys [][]byte) (*MutableNode, bool, error) {
	tree.register(root)
	id, found, err := tree.Tree.DeleteEntries(root.PageID(), keys)
	if err != nil {
		return nil, found, err
	}
	node, err := tree.node(id, nil)
	return node, found, err
}
func (tree *testTree) Cursor(root *MutableNode) *Cursor {
	tree.register(root)
	cursor := NewCursor(tree.store, root.PageID())
	return &cursor
}
func (tree *testTree) readChild(parent *MutableNode, index int) (*MutableNode, error) {
	node, err := tree.Tree.readChild(parent.ReadOnly(), index)
	if err != nil {
		return nil, err
	}
	return node.decode(tree.store.PageSize())
}
func (store *memoryTreeStore) EditNode(id page.ID) (*MutableNode, error) {
	node := store.nodes[id]
	if node == nil {
		return nil, ErrInvalid
	}
	if dirty, ok := store.dirty[id]; ok {
		return dirty, nil
	}
	private := node.Clone()
	store.StageNode(private)
	return private, nil
}

func (store *countingPutStore) EditNode(id page.ID) (*MutableNode, error) {
	store.writes++
	return store.memoryTreeStore.EditNode(id)
}

func (*memoryTreeStore) Writable() bool { return true }

func (store *memoryTreeStore) seedNode(node *MutableNode) {
	if store.nodes == nil {
		store.nodes = make(map[page.ID]*MutableNode)
	}
	store.nodes[node.PageID()] = node
}
