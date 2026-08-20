package btree

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/Issaminu/kvlite/internal/page"
)

func TestNodeCodec_UsesFixedLeafLayout(t *testing.T) {
	node := NewLeafNode(page.ID(1))
	if err := node.InsertEntry(NewEntry(0, []byte("k"), []byte("v"))); err != nil {
		t.Fatal(err)
	}

	// Layout: leaf marker, one entry, zero flags, one-byte key, and one-byte value.
	want := []byte{
		1,
		1, 0, 0, 0,
		0, 0, 0, 0,
		1, 0, 0, 0, 'k',
		1, 0, 0, 0, 'v',
	}
	encoded := EncodeNode(node)
	if !bytes.Equal(encoded, want) {
		t.Fatalf("encode leaf: got %x, want %x", encoded, want)
	}

	decoded, err := DecodeNode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	entry, found, err := decoded.FindEntry([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("decoded node does not contain key k")
	}
	if entry.Flags() != 0 || !bytes.Equal(entry.Key(), []byte("k")) || !bytes.Equal(entry.Value(), []byte("v")) {
		t.Fatalf("decoded leaf entry: flags=%d key=%q value=%q", entry.Flags(), entry.Key(), entry.Value())
	}

	for size := 0; size < len(encoded); size++ {
		if _, err := DecodeNode(encoded[:size]); err == nil {
			t.Errorf("decode %d-byte leaf prefix: expected an error", size)
		}
	}
}

func TestNodeCodec_UsesFixedBranchLayout(t *testing.T) {
	node := &Node{
		entries:  []Entry{{key: []byte("m")}},
		Children: []page.ID{2, 3},
	}
	// Layout: branch marker, one separator, zero flags, one-byte key, and two child page IDs.
	want := []byte{
		0,
		1, 0, 0, 0,
		0, 0, 0, 0,
		1, 0, 0, 0, 'm',
		2, 0, 0, 0, 0, 0, 0, 0,
		3, 0, 0, 0, 0, 0, 0, 0,
	}

	encoded := EncodeNode(node)
	if !bytes.Equal(encoded, want) {
		t.Fatalf("encode branch: got %x, want %x", encoded, want)
	}

	decoded, err := DecodeNode(encoded)
	if err != nil {
		t.Fatalf("decode branch: %v", err)
	}
	if decoded.IsLeaf || len(decoded.entries) != 1 || len(decoded.Children) != 2 {
		t.Fatalf("decode branch shape: %+v", decoded)
	}
	if !bytes.Equal(decoded.entries[0].key, []byte("m")) || decoded.Children[0] != 2 || decoded.Children[1] != 3 {
		t.Fatalf("decode branch data: %+v", decoded)
	}
}

func TestNodeEncodedSize_MatchesEncodingWithoutAllocating(t *testing.T) {
	nodes := []*Node{
		{
			IsLeaf: true,
			entries: []Entry{
				{key: []byte("alpha"), value: []byte("one")},
				{key: []byte("beta"), value: []byte{}},
			},
		},
		{
			entries: []Entry{
				{key: []byte("middle")},
			},
			Children: []page.ID{2, 3},
		},
	}

	for index, node := range nodes {
		if got, want := node.EncodedSize(), len(EncodeNode(node)); got != want {
			t.Fatalf("node %d encoded size: got %d, want %d", index, got, want)
		}
		if got := testing.AllocsPerRun(100, func() { _ = node.EncodedSize() }); got != 0 {
			t.Fatalf("node %d encoded size allocations: got %v, want 0", index, got)
		}
	}
}

func TestNodeFindEntryRef_ReturnsStoredValue(t *testing.T) {
	node := NewLeafNode(1)
	if err := node.InsertEntry(NewEntry(0, []byte("key"), []byte("value"))); err != nil {
		t.Fatal(err)
	}

	entry, found, err := node.FindEntryRef([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("key not found")
	}

	entry.Value()[0] = 'V'
	if got := node.entries[0].value; !bytes.Equal(got, []byte("Value")) {
		t.Fatalf("stored value after reference change: got %q, want %q", got, "Value")
	}
}

func TestNodeClone_DoesNotShareEntriesOrChildren(t *testing.T) {
	original := &Node{
		entries:  []Entry{{key: []byte("middle")}},
		Children: []page.ID{2, 3},
		pgid:     1,
	}

	clone := original.Clone()
	clone.entries[0].key[0] = 'M'
	clone.Children[0] = 20

	if got := original.entries[0].key; !bytes.Equal(got, []byte("middle")) {
		t.Fatalf("original key changed through clone: got %q", got)
	}
	if got := original.Children[0]; got != 2 {
		t.Fatalf("original child changed through clone: got %d, want 2", got)
	}
}

// oneByteWriter accepts one byte from each Write call without returning an
// error. It verifies that code using io.Writer handles valid partial writes.
type oneByteWriter struct {
	bytes.Buffer
}

func (w *oneByteWriter) Write(data []byte) (int, error) {
	if len(data) > 1 {
		data = data[:1]
	}
	return w.Buffer.Write(data)
}

func TestWriteNode_CompletesPartialWrites(t *testing.T) {
	pageSize := int64(os.Getpagesize())
	node := NewLeafNode(1)
	if err := node.InsertEntry(NewEntry(0, []byte("key"), []byte("value"))); err != nil {
		t.Fatal(err)
	}

	writer := new(oneByteWriter)
	if err := WriteNode(writer, node, pageSize, true); err != nil {
		t.Fatal(err)
	}
	if writer.Len() != int(pageSize) {
		t.Fatalf("encoded node size: got %d bytes, want one %d-byte page", writer.Len(), pageSize)
	}
}

func TestNodeSplit_DoesNotRequireDatabase(t *testing.T) {
	const (
		pageSize  int64   = 64 // Two 33-byte entries exceed this page size.
		valueSize         = 20 // A leaf entry uses 12 fixed bytes, a one-byte key, and this value.
		leftPgid  page.ID = 1  // Page 1 is the first node page after the metadata page.
		rightPgid page.ID = 2  // Page 2 is the next page allocated for the split.
	)

	node := &Node{
		IsLeaf: true,
		pgid:   leftPgid,
		entries: []Entry{
			{key: []byte("a"), value: make([]byte, valueSize)},
			{key: []byte("b"), value: make([]byte, valueSize)},
		},
	}

	rightNode, separator, err := node.Split(rightPgid, pageSize)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(separator, []byte("b")) {
		t.Fatalf("separator: got %q, want %q", separator, "b")
	}
	if len(node.entries) != 1 || !bytes.Equal(node.entries[0].key, []byte("a")) {
		t.Fatalf("left entries: got %q, want [a]", node.entries)
	}
	if rightNode.pgid != rightPgid {
		t.Fatalf("right page ID: got %d, want %d", rightNode.pgid, rightPgid)
	}
	if !rightNode.IsLeaf || len(rightNode.entries) != 1 || !bytes.Equal(rightNode.entries[0].key, []byte("b")) {
		t.Fatalf("right entries: got %q, want [b]", rightNode.entries)
	}
}

type memoryTreeStore struct {
	pageSize int64
	nextID   page.ID
	nodes    map[page.ID]*Node
	dirty    map[page.ID]*Node
}

func (store *memoryTreeStore) PageSize() int64 {
	return store.pageSize
}

func (store *memoryTreeStore) ReadNode(pageID page.ID) (*Node, error) {
	return store.nodes[pageID], nil
}

func (store *memoryTreeStore) AllocatePage() page.ID {
	pageID := store.nextID
	store.nextID++
	return pageID
}

func (store *memoryTreeStore) WritableNode(node *Node) *Node {
	if dirty, ok := store.dirty[node.PageID()]; ok {
		return dirty
	}
	private := node.Clone()
	store.StageNode(private)
	return private
}

func (store *memoryTreeStore) StageNode(node *Node) {
	if store.dirty == nil {
		store.dirty = make(map[page.ID]*Node)
	}
	store.nodes[node.PageID()] = node
	store.dirty[node.PageID()] = node
}

func TestTree_PutAndFindWithoutDatabase(t *testing.T) {
	const (
		pageSize   int64   = 128 // This small page size forces several tree levels.
		rootPageID page.ID = 1   // Page zero is reserved for metadata.
		firstNewID page.ID = 2   // New tree pages start after the root page.
		entryCount         = 100 // This many entries cannot fit in one 128-byte page.
	)

	store := &memoryTreeStore{
		pageSize: pageSize,
		nextID:   firstNewID,
		nodes:    make(map[page.ID]*Node),
	}
	tree := NewTree(store)
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
	if root.IsLeaf {
		t.Fatal("tree did not create a branch root")
	}

	for index := 0; index < entryCount; index++ {
		key := fmt.Appendf(nil, "key-%03d", index)
		want := fmt.Appendf(nil, "value-%03d", index)
		entry, found, err := tree.FindEntry(root, key)
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
		nodes:    make(map[page.ID]*Node),
	}
	tree := NewTree(store)
	root := NewLeafNode(rootPageID)
	store.StageNode(root)

	for index := 0; root.IsLeaf; index++ {
		key := fmt.Appendf(nil, "key-%02d", index)
		value := bytes.Repeat([]byte("v"), 40)
		var err error
		root, err = tree.PutEntry(root, NewEntry(0, key, value))
		if err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
	}

	if got := root.PageID(); got == rootPageID {
		t.Fatalf("root page ID after split: got old page %d, want a new page", got)
	}
	if root.IsLeaf {
		t.Fatal("root remains a leaf after split")
	}
	if got := root.Children[0]; got != rootPageID {
		t.Fatalf("left child page ID: got %d, want old root page %d", got, rootPageID)
	}
	for index := 0; index < 3; index++ {
		key := fmt.Appendf(nil, "key-%02d", index)
		entry, found, err := tree.FindEntry(root, key)
		if err != nil {
			t.Fatalf("find %q: %v", key, err)
		}
		if !found || !bytes.Equal(entry.Value(), bytes.Repeat([]byte("v"), 40)) {
			t.Fatalf("find %q after root split: found=%t value=%q", key, found, entry.Value())
		}
	}
}

func TestTreeFindEntryRef_ReturnsLeafValue(t *testing.T) {
	store := &memoryTreeStore{pageSize: 128, nextID: 2, nodes: make(map[page.ID]*Node)}
	tree := NewTree(store)
	root := NewLeafNode(1)
	if err := root.InsertEntry(NewEntry(0, []byte("key"), []byte("value"))); err != nil {
		t.Fatal(err)
	}

	entry, found, err := tree.FindEntryRef(root, []byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if !found || !bytes.Equal(entry.Value(), []byte("value")) {
		t.Fatalf("find reference: found=%t value=%q", found, entry.Value())
	}
}

// TestNodeFindEntryRef_DoesNotChangeStoredEmptyValue catches a read that
// normalizes the value by changing the stored node.
func TestNodeFindEntryRef_DoesNotChangeStoredEmptyValue(t *testing.T) {
	node := NewLeafNode(1)
	node.entries = append(node.entries, Entry{key: []byte("key")})

	entry, found, err := node.FindEntryRef([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if !found || entry.Value() == nil || len(entry.Value()) != 0 {
		t.Fatalf("empty value lookup: found=%t value=%v", found, entry.Value())
	}
	if node.entries[0].value != nil {
		t.Fatal("FindEntryRef changed the stored node")
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
	root := &Node{
		entries:  []Entry{{key: []byte("m")}},
		Children: []page.ID{left.PageID(), right.PageID()},
		pgid:     1,
	}
	store := &memoryTreeStore{
		pageSize: 128,
		nextID:   4,
		nodes:    map[page.ID]*Node{1: root, 2: left, 3: right},
	}
	tree := NewTree(store)
	beforeRoot := EncodeNode(root)
	beforeLeft := EncodeNode(left)

	if _, err := tree.PutEntry(root, NewEntry(0, []byte("a"), []byte("changed"))); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(EncodeNode(root), beforeRoot) {
		t.Fatal("write changed the input root")
	}
	if !bytes.Equal(EncodeNode(left), beforeLeft) {
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
	root := &Node{
		entries:  []Entry{{key: []byte("m")}},
		Children: []page.ID{left.PageID(), right.PageID()},
		pgid:     1,
	}
	store := &memoryTreeStore{
		pageSize: 4096,
		nextID:   4,
		nodes:    map[page.ID]*Node{1: root, 2: left, 3: right},
		dirty:    make(map[page.ID]*Node),
	}
	tree := NewTree(store)

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
