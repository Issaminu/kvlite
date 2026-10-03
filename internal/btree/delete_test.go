package btree

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/Issaminu/kvlite/internal/page"
)

func TestTreeDeleteEntryRemovesExistingKey(t *testing.T) {
	store := &memoryTreeStore{pageSize: 256, nextID: 3, nodes: make(map[page.ID]*Node)}
	tree := NewTree(store)
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
	if _, found, err := tree.FindEntryRef(root, []byte("b")); err != nil || found {
		t.Fatalf("find deleted key: found %t, error %v", found, err)
	}
}

func TestTreeDeleteEntryMissingKeyDoesNotChangeTree(t *testing.T) {
	store := &memoryTreeStore{pageSize: 256, nextID: 3, nodes: make(map[page.ID]*Node)}
	tree := NewTree(store)
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
	store := &memoryTreeStore{pageSize: 256, nextID: 3, nodes: make(map[page.ID]*Node)}
	tree := NewTree(store)
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
	store := &memoryTreeStore{pageSize: 256, nextID: 3, nodes: make(map[page.ID]*Node)}
	tree := NewTree(store)
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
	if _, found, err := tree.FindEntryRef(root, []byte("nested")); err != nil || found {
		t.Fatalf("deleted bucket entry: found %t, error %v", found, err)
	}
}

func TestTreeDeleteEntryDoesNotDirtyUnchangedParent(t *testing.T) {
	store := &memoryTreeStore{pageSize: 256, nextID: 5, nodes: make(map[page.ID]*Node)}
	tree := NewTree(store)
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
	root := NewRootNode(2, left, right, []byte("m"))
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

func TestTreeDeleteEntryMergesAndCollapsesRoot(t *testing.T) {
	store := &memoryTreeStore{pageSize: 128, nextID: 3, nodes: make(map[page.ID]*Node)}
	tree := NewTree(store)
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

func TestTreeDeleteEntryRandomOrderKeepsExactSeparators(t *testing.T) {
	store := &memoryTreeStore{pageSize: 192, nextID: 3, nodes: make(map[page.ID]*Node)}
	tree := NewTree(store)
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
			_, found, err := tree.FindEntryRef(root, fmt.Appendf(nil, "key-%03d", candidate))
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
	parent := NewRootNode(2, left, right, []byte("z"))

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
	parent := NewRootNode(2, left, right, []byte("z"))

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
	parent := &Node{
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
	left := NewRootNode(20, leftLeaf, rightLeaf, []byte("b"))
	right := NewRootNode(21, otherLeftLeaf, otherRightLeaf, []byte("z"))
	parent := NewRootNode(10, left, right, []byte("m"))

	want := mergedChildrenSize(parent, 0, left, right)
	parentCopy := parent.Clone()
	leftCopy := left.Clone()
	rightCopy := right.Clone()
	mergeChildren(parentCopy, 0, leftCopy, rightCopy)
	if got := int64(leftCopy.EncodedSize()); got != want {
		t.Fatalf("merged branch size: got %d, want %d", got, want)
	}
}

func assertTreeSeparators(t *testing.T, store *memoryTreeStore, node *Node) []byte {
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
		if !bytes.Equal(node.entries[index-1].key, childMinimum) {
			t.Fatalf("branch %d separator %d: got %q, want %q", node.PageID(), index-1, node.entries[index-1].key, childMinimum)
		}
	}
	return minimum
}
