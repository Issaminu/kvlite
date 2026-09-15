package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"slices"
	"testing"

	"github.com/Issaminu/kvlite/internal/page"
)

const testNodePageSize int64 = 4096

func TestNodeCodec_UsesProtectedNodeHeader(t *testing.T) {
	const pageSize int64 = 64
	node := NewLeafNode(page.ID(7))
	if err := node.InsertEntry(NewEntry(0, []byte("k"), []byte("v"))); err != nil {
		t.Fatal(err)
	}

	var pageData bytes.Buffer
	if err := WriteNode(&pageData, node, pageSize, true); err != nil {
		t.Fatal(err)
	}
	encoded := pageData.Bytes()
	if len(encoded) != int(pageSize) {
		t.Fatalf("encoded page size: got %d, want %d", len(encoded), pageSize)
	}
	if got, want := binary.LittleEndian.Uint16(encoded[0:2]), nodeFormatVersion; got != want {
		t.Fatalf("node format version: got %d, want %d", got, want)
	}
	if got, want := binary.LittleEndian.Uint16(encoded[2:4]), uint16(1); got != want {
		t.Fatalf("node type: got %d, want leaf type %d", got, want)
	}
	if got, want := page.ID(binary.LittleEndian.Uint64(encoded[4:12])), node.PageID(); got != want {
		t.Fatalf("node page ID: got %d, want %d", got, want)
	}
	if got, want := binary.LittleEndian.Uint32(encoded[12:16]), uint32(1); got != want {
		t.Fatalf("entry count: got %d, want %d", got, want)
	}

	hash := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	_, _ = hash.Write(encoded[:nodeChecksumOffset])
	_, _ = hash.Write(encoded[NodeHeaderSize:])
	if got, want := binary.LittleEndian.Uint32(encoded[nodeChecksumOffset:NodeHeaderSize]), hash.Sum32(); got != want {
		t.Fatalf("node checksum: got %x, want %x", got, want)
	}
}

func TestEncodeWALNode_LeavesDatabasePageChecksumUnset(t *testing.T) {
	node := NewLeafNode(page.ID(7))
	if err := node.InsertEntry(NewEntry(0, []byte("key"), []byte("value"))); err != nil {
		t.Fatal(err)
	}

	encoded := EncodeWALNode(node)
	if got := binary.LittleEndian.Uint32(encoded[nodeChecksumOffset:NodeHeaderSize]); got != 0 {
		t.Fatalf("WAL node page checksum: got %x, want zero", got)
	}
	decoded, err := DecodeWALNode(encoded, node.PageID())
	if err != nil {
		t.Fatal(err)
	}
	if decoded.PageID() != node.PageID() || decoded.EntryCount() != 1 {
		t.Fatalf("decoded WAL node: got page=%d entries=%d", decoded.PageID(), decoded.EntryCount())
	}
}

func TestAppendEncodedWALNode_PreservesPrefix(t *testing.T) {
	node := NewLeafNode(7)
	if err := node.InsertEntry(NewEntry(0, []byte("key"), []byte("value"))); err != nil {
		t.Fatal(err)
	}
	prefix := []byte("prefix")
	got := AppendEncodedWALNode(slices.Clone(prefix), node)
	if !bytes.Equal(got[:len(prefix)], prefix) {
		t.Fatalf("prefix: got %q, want %q", got[:len(prefix)], prefix)
	}
	if want := EncodeWALNode(node); !bytes.Equal(got[len(prefix):], want) {
		t.Fatal("appended WAL node differs from standalone encoding")
	}
}

func TestDecodeNode_RejectsCorruptedPageBeforePayloadDecode(t *testing.T) {
	node := NewLeafNode(page.ID(7))
	if err := node.InsertEntry(NewEntry(0, []byte("key"), []byte("value"))); err != nil {
		t.Fatal(err)
	}

	encoded := EncodeNode(node, testNodePageSize)
	encoded[len(encoded)-1] ^= 0xff

	if _, err := DecodeNode(encoded, node.PageID(), testNodePageSize); !errors.Is(err, page.ErrChecksum) {
		t.Fatalf("DecodeNode error: got %v, want ErrChecksum", err)
	}
}

func TestDecodeNode_RejectsCorruptedPagePaddingBeforePayloadDecode(t *testing.T) {
	const pageSize int64 = 64
	node := NewLeafNode(page.ID(7))
	if err := node.InsertEntry(NewEntry(0, []byte("k"), []byte("v"))); err != nil {
		t.Fatal(err)
	}

	var encoded bytes.Buffer
	if err := WriteNode(&encoded, node, pageSize, true); err != nil {
		t.Fatal(err)
	}
	data := encoded.Bytes()
	data[len(data)-1] ^= 0xff

	if _, err := DecodeNode(data, node.PageID(), pageSize); !errors.Is(err, page.ErrChecksum) {
		t.Fatalf("DecodeNode error: got %v, want ErrChecksum", err)
	}
}

func TestDecodeNode_RejectsHeaderForWrongPage(t *testing.T) {
	node := NewLeafNode(page.ID(7))
	encoded := EncodeNode(node, testNodePageSize)

	if _, err := DecodeNode(encoded, page.ID(8), testNodePageSize); !errors.Is(err, page.ErrInvalid) {
		t.Fatalf("DecodeNode error: got %v, want ErrInvalid", err)
	}
}

func TestDecodeNode_RejectsInvalidProtectedNodeHeader(t *testing.T) {
	node := NewLeafNode(page.ID(7))

	testCases := []struct {
		name    string
		change  func([]byte)
		wantErr error
	}{
		{
			name: "unsupported node format version",
			change: func(data []byte) {
				binary.LittleEndian.PutUint16(data[0:2], nodeFormatVersion+1)
				resealNodeForTest(data, testNodePageSize)
			},
			wantErr: page.ErrVersionMismatch,
		},
		{
			name: "unknown node type",
			change: func(data []byte) {
				binary.LittleEndian.PutUint16(data[2:4], 3)
				resealNodeForTest(data, testNodePageSize)
			},
			wantErr: page.ErrInvalid,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			encoded := EncodeNode(node, testNodePageSize)
			testCase.change(encoded)
			if _, err := DecodeNode(encoded, node.PageID(), testNodePageSize); !errors.Is(err, testCase.wantErr) {
				t.Fatalf("DecodeNode error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func resealNodeForTest(data []byte, pageSize int64) {
	table := crc32.MakeTable(crc32.Castagnoli)
	checksum := crc32.Update(0, table, data[:nodeChecksumOffset])
	checksum = crc32.Update(checksum, table, data[NodeHeaderSize:])
	checksum = crc32.Update(checksum, table, make([]byte, pageSize-int64(len(data))))
	binary.LittleEndian.PutUint32(data[nodeChecksumOffset:NodeHeaderSize], checksum)
}

func TestNodeCodec_UsesFixedLeafPayloadLayout(t *testing.T) {
	node := NewLeafNode(page.ID(1))
	if err := node.InsertEntry(NewEntry(0, []byte("k"), []byte("v"))); err != nil {
		t.Fatal(err)
	}

	// Payload layout: one 16-byte descriptor, then the key and value.
	want := []byte{
		0, 0, 0, 0,
		36, 0, 0, 0,
		1, 0, 0, 0,
		1, 0, 0, 0,
		'k', 'v',
	}
	encoded := EncodeNode(node, testNodePageSize)
	if !bytes.Equal(encoded[NodeHeaderSize:], want) {
		t.Fatalf("encode leaf payload: got %x, want %x", encoded[NodeHeaderSize:], want)
	}

	decoded, err := DecodeNode(encoded, node.PageID(), testNodePageSize)
	if err != nil {
		t.Fatal(err)
	}
	entry, found, err := decoded.FindEntryRef([]byte("k"))
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
		if _, err := DecodeNode(encoded[:size], node.PageID(), testNodePageSize); err == nil {
			t.Errorf("decode %d-byte leaf prefix: expected an error", size)
		}
	}
}

func TestNodeCodec_UsesFixedBranchPayloadLayout(t *testing.T) {
	node := &Node{
		header:   newNodeHeader(NodeTypeBranch, 0),
		entries:  []Entry{{key: []byte("m")}},
		Children: []page.ID{2, 3},
	}
	// Payload layout: one 8-byte descriptor, two child page IDs, then the key.
	want := []byte{
		44, 0, 0, 0,
		1, 0, 0, 0,
		2, 0, 0, 0, 0, 0, 0, 0,
		3, 0, 0, 0, 0, 0, 0, 0,
		'm',
	}

	encoded := EncodeNode(node, testNodePageSize)
	if !bytes.Equal(encoded[NodeHeaderSize:], want) {
		t.Fatalf("encode branch payload: got %x, want %x", encoded[NodeHeaderSize:], want)
	}

	decoded, err := DecodeNode(encoded, node.PageID(), testNodePageSize)
	if err != nil {
		t.Fatalf("decode branch: %v", err)
	}
	if decoded.IsLeaf() || len(decoded.entries) != 1 || len(decoded.Children) != 2 {
		t.Fatalf("decode branch shape: %+v", decoded)
	}
	if !bytes.Equal(decoded.entries[0].key, []byte("m")) || decoded.Children[0] != 2 || decoded.Children[1] != 3 {
		t.Fatalf("decode branch data: %+v", decoded)
	}
}

func TestDecodeNode_AllocatesOnlyNodeHeaderAndEntryList(t *testing.T) {
	node := &Node{
		header: newNodeHeader(NodeTypeLeaf, 0),
		entries: []Entry{
			{key: []byte("alpha"), value: bytes.Repeat([]byte("a"), 128)},
			{key: []byte("beta"), value: bytes.Repeat([]byte("b"), 128)},
		},
	}
	encoded := EncodeNode(node, testNodePageSize)

	var decoded *Node
	var decodeErr error
	if got := testing.AllocsPerRun(100, func() {
		decoded, decodeErr = DecodeNode(encoded, node.PageID(), testNodePageSize)
	}); got != 3 {
		t.Fatalf("DecodeNode allocations: got %v, want 3", got)
	}
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if decoded == nil || len(decoded.entries) != 2 {
		t.Fatalf("decoded entries: got %+v", decoded)
	}
}

func TestDecodeNode_EntrySlicesCannotGrowIntoEncodedData(t *testing.T) {
	node := &Node{
		header: newNodeHeader(NodeTypeLeaf, 0),
		entries: []Entry{
			{key: []byte("alpha"), value: []byte("one")},
			{key: []byte("beta"), value: []byte("two")},
		},
	}
	encoded := EncodeNode(node, testNodePageSize)
	encodedBeforeAppend := bytes.Clone(encoded)

	decoded, err := DecodeNode(encoded, node.PageID(), testNodePageSize)
	if err != nil {
		t.Fatal(err)
	}
	_ = append(decoded.entries[0].value, 'x')

	if !bytes.Equal(encoded, encodedBeforeAppend) {
		t.Fatal("appending to a decoded value changed the encoded node")
	}
}

func TestDecodeNode_RejectsEntryCountLargerThanInputCanContain(t *testing.T) {
	node := NewLeafNode(1)
	data := EncodeNode(node, testNodePageSize)
	binary.LittleEndian.PutUint32(data[nodeEntryCountOffset:nodeChecksumOffset], ^uint32(0))
	resealNodeForTest(data, testNodePageSize)

	if _, err := DecodeNode(data, 1, testNodePageSize); err == nil {
		t.Fatal("DecodeNode accepted an entry count larger than the input")
	}
}

func TestNodeEncodedSize_MatchesEncodingWithoutAllocating(t *testing.T) {
	nodes := []*Node{
		{
			header: newNodeHeader(NodeTypeLeaf, 0),
			entries: []Entry{
				{key: []byte("alpha"), value: []byte("one")},
				{key: []byte("beta"), value: []byte{}},
			},
		},
		{
			header: newNodeHeader(NodeTypeBranch, 0),
			entries: []Entry{
				{key: []byte("middle")},
			},
			Children: []page.ID{2, 3},
		},
	}

	for index, node := range nodes {
		if got, want := node.EncodedSize(), len(EncodeNode(node, testNodePageSize)); got != want {
			t.Fatalf("node %d encoded size: got %d, want %d", index, got, want)
		}
		if got := testing.AllocsPerRun(100, func() { _ = node.EncodedSize() }); got != 0 {
			t.Fatalf("node %d encoded size allocations: got %v, want 0", index, got)
		}
	}
}

func TestEncodeNode_AllocatesOneOutputBuffer(t *testing.T) {
	node := &Node{
		header: newNodeHeader(NodeTypeLeaf, 0),
		entries: []Entry{
			{key: []byte("alpha"), value: bytes.Repeat([]byte("a"), 128)},
			{key: []byte("beta"), value: bytes.Repeat([]byte("b"), 128)},
		},
	}

	var encoded []byte
	if got := testing.AllocsPerRun(100, func() {
		encoded = EncodeNode(node, testNodePageSize)
	}); got != 1 {
		t.Fatalf("EncodeNode allocations: got %v, want 1", got)
	}
	if got, want := len(encoded), 317; got != want {
		t.Fatalf("encoded length: got %d, want %d", got, want)
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

func TestNodeClone_DoesNotShareEntryOrChildLists(t *testing.T) {
	original := &Node{
		header:   newNodeHeader(NodeTypeBranch, 1),
		entries:  []Entry{{key: []byte("middle")}},
		Children: []page.ID{2, 3},
	}

	clone := original.Clone()
	clone.entries[0] = Entry{key: []byte("changed")}
	clone.Children[0] = 20

	if got := original.entries[0].key; !bytes.Equal(got, []byte("middle")) {
		t.Fatalf("original key changed through clone: got %q", got)
	}
	if got := original.Children[0]; got != 2 {
		t.Fatalf("original child changed through clone: got %d, want 2", got)
	}
}

func TestNodeClone_AllocatesOnlyNodeHeaderAndEntryList(t *testing.T) {
	original := &Node{
		header: newNodeHeader(NodeTypeLeaf, 0),
		entries: []Entry{
			{key: []byte("alpha"), value: bytes.Repeat([]byte("a"), 128)},
			{key: []byte("beta"), value: bytes.Repeat([]byte("b"), 128)},
		},
	}

	var clone *Node
	if got := testing.AllocsPerRun(100, func() {
		clone = original.Clone()
	}); got != 3 {
		t.Fatalf("Node.Clone allocations: got %v, want 3", got)
	}
	if clone == nil || len(clone.entries) != 2 {
		t.Fatalf("cloned entries: got %+v", clone)
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

func BenchmarkWriteNode(b *testing.B) {
	const pageSize int64 = 4096

	testCases := []struct {
		name  string
		value []byte
	}{
		{name: "sparse", value: []byte("v")},
		{name: "half-full", value: bytes.Repeat([]byte("v"), 2048)},
	}

	for _, testCase := range testCases {
		b.Run(testCase.name, func(b *testing.B) {
			node := NewLeafNode(1)
			if err := node.InsertEntry(NewEntry(0, []byte("key"), testCase.value)); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.SetBytes(pageSize)
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if err := WriteNode(io.Discard, node, pageSize, true); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestNodeSplit_DoesNotRequireDatabase(t *testing.T) {
	const (
		pageSize  int64   = 64 // Two 37-byte entries exceed this page size.
		valueSize         = 20 // A leaf entry uses one 16-byte descriptor, a one-byte key, and this value.
		leftPgid  page.ID = 2  // Page 2 is the first node page after both metadata pages.
		rightPgid page.ID = 3  // Page 3 is the next page allocated for the split.
	)

	node := &Node{
		header: newNodeHeader(NodeTypeLeaf, leftPgid),
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
	if rightNode.PageID() != rightPgid {
		t.Fatalf("right page ID: got %d, want %d", rightNode.PageID(), rightPgid)
	}
	if !rightNode.IsLeaf() || len(rightNode.entries) != 1 || !bytes.Equal(rightNode.entries[0].key, []byte("b")) {
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

func (store *memoryTreeStore) LookupPage(pageID page.ID, key []byte) (Entry, bool, page.ID, error) {
	node := store.nodes[pageID]
	if node == nil {
		return Entry{}, false, 0, ErrKeyNotFound
	}
	return LookupNode(node, key)
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

// TestTreePutEntry_RejectsKeyThatCannotFitBranch checks the branch separator
// limit without changing the database page-size policy.
func TestTreePutEntry_RejectsKeyThatCannotFitBranch(t *testing.T) {
	const pageSize int64 = 128
	store := &memoryTreeStore{pageSize: pageSize, nextID: 3, nodes: make(map[page.ID]*Node)}
	tree := NewTree(store)
	root := NewLeafNode(2)
	store.StageNode(root)

	key := bytes.Repeat([]byte("k"), 88)
	if _, err := tree.PutEntry(root, NewEntry(0, key, nil)); !errors.Is(err, ErrEntryTooLarge) {
		t.Fatalf("PutEntry error: got %v, want ErrEntryTooLarge", err)
	}
	if got := len(root.entries); got != 0 {
		t.Fatalf("root entry count: got %d, want 0", got)
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
	if root.IsLeaf() {
		t.Fatal("tree did not create a branch root")
	}

	for index := 0; index < entryCount; index++ {
		key := fmt.Appendf(nil, "key-%03d", index)
		want := fmt.Appendf(nil, "value-%03d", index)
		entry, found, err := tree.FindEntryRef(root, key)
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
		entry, found, err := tree.FindEntryRef(root, key)
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
		header:   newNodeHeader(NodeTypeBranch, 1),
		entries:  []Entry{{key: []byte("m")}},
		Children: []page.ID{left.PageID(), right.PageID()},
	}
	store := &memoryTreeStore{
		pageSize: 128,
		nextID:   4,
		nodes:    map[page.ID]*Node{1: root, 2: left, 3: right},
	}
	tree := NewTree(store)
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
	root := &Node{
		header:   newNodeHeader(NodeTypeBranch, 1),
		entries:  []Entry{{key: []byte("m")}},
		Children: []page.ID{left.PageID(), right.PageID()},
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

func TestLookupMappedNode_FindsLeafEntryWithoutAllocating(t *testing.T) {
	node := NewLeafNode(7)
	if err := node.InsertEntry(NewEntry(0, []byte("alpha"), []byte("one"))); err != nil {
		t.Fatal(err)
	}
	if err := node.InsertEntry(NewEntry(0, []byte("beta"), []byte("two"))); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, testNodePageSize)
	copy(data, EncodeNode(node, testNodePageSize))

	var entry Entry
	var found bool
	var child page.ID
	var lookupErr error
	if got := testing.AllocsPerRun(100, func() {
		entry, found, child, lookupErr = LookupMappedNode(data, node.PageID(), []byte("beta"))
	}); got != 0 {
		t.Fatalf("LookupMappedNode allocations: got %v, want 0", got)
	}
	if lookupErr != nil || !found || child != 0 || !bytes.Equal(entry.Value(), []byte("two")) {
		t.Fatalf("LookupMappedNode: entry=%q found=%t child=%d err=%v", entry.Value(), found, child, lookupErr)
	}
}

func TestLookupMappedNode_SelectsBranchChild(t *testing.T) {
	root := &Node{
		header:   newNodeHeader(NodeTypeBranch, 8),
		entries:  []Entry{{key: []byte("m")}},
		Children: []page.ID{9, 10},
	}
	data := make([]byte, testNodePageSize)
	copy(data, EncodeNode(root, testNodePageSize))

	_, found, child, err := LookupMappedNode(data, root.PageID(), []byte("z"))
	if err != nil {
		t.Fatal(err)
	}
	if found || child != 10 {
		t.Fatalf("branch lookup: found=%t child=%d, want false and 10", found, child)
	}
}

func TestLookupEncodedWALNode_FindsLeafEntryWithoutAllocating(t *testing.T) {
	node := NewLeafNode(7)
	if err := node.InsertEntry(NewEntry(0, []byte("alpha"), []byte("one"))); err != nil {
		t.Fatal(err)
	}
	if err := node.InsertEntry(NewEntry(0, []byte("beta"), []byte("two"))); err != nil {
		t.Fatal(err)
	}
	data := EncodeWALNode(node)

	var entry Entry
	var found bool
	var child page.ID
	var lookupErr error
	if got := testing.AllocsPerRun(100, func() {
		entry, found, child, lookupErr = LookupEncodedWALNode(data, node.PageID(), []byte("beta"))
	}); got != 0 {
		t.Fatalf("LookupEncodedWALNode allocations: got %v, want 0", got)
	}
	if lookupErr != nil || !found || child != 0 || !bytes.Equal(entry.Value(), []byte("two")) {
		t.Fatalf("LookupEncodedWALNode: entry=%q found=%t child=%d err=%v", entry.Value(), found, child, lookupErr)
	}
}

func TestLookupEncodedWALNode_SelectsBranchChild(t *testing.T) {
	root := &Node{
		header:   newNodeHeader(NodeTypeBranch, 8),
		entries:  []Entry{{key: []byte("m")}, {key: []byte("t")}},
		Children: []page.ID{9, 10, 11},
	}
	data := EncodeWALNode(root)

	for _, testCase := range []struct {
		key       string
		wantChild page.ID
	}{
		{key: "a", wantChild: 9},
		{key: "m", wantChild: 10},
		{key: "z", wantChild: 11},
	} {
		t.Run(testCase.key, func(t *testing.T) {
			_, found, child, err := LookupEncodedWALNode(data, root.PageID(), []byte(testCase.key))
			if err != nil {
				t.Fatal(err)
			}
			if found || child != testCase.wantChild {
				t.Fatalf("branch lookup: found=%t child=%d, want false and %d", found, child, testCase.wantChild)
			}
		})
	}
}
