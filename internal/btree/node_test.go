package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"slices"
	"testing"

	"github.com/Issaminu/kvlite/internal/page"
)

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
		t.Fatalf("left entries: got %v, want [a]", node.entries)
	}
	if rightNode.PageID() != rightPgid {
		t.Fatalf("right page ID: got %d, want %d", rightNode.PageID(), rightPgid)
	}
	if !rightNode.IsLeaf() || len(rightNode.entries) != 1 || !bytes.Equal(rightNode.entries[0].key, []byte("b")) {
		t.Fatalf("right entries: got %v, want [b]", rightNode.entries)
	}
}

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
	if got, want := binary.LittleEndian.Uint16(encoded[0:2]), uint16(1); got != want {
		t.Fatalf("node type: got %d, want leaf type %d", got, want)
	}
	if got, want := page.ID(binary.LittleEndian.Uint64(encoded[2:10])), node.PageID(); got != want {
		t.Fatalf("node page ID: got %d, want %d", got, want)
	}
	if got, want := binary.LittleEndian.Uint32(encoded[10:14]), uint32(1); got != want {
		t.Fatalf("entry count: got %d, want %d", got, want)
	}

	hash := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	_, _ = hash.Write(encoded[:nodeChecksumOffset])
	_, _ = hash.Write(encoded[NodeHeaderSize:])
	if got, want := binary.LittleEndian.Uint32(encoded[nodeChecksumOffset:NodeHeaderSize]), hash.Sum32(); got != want {
		t.Fatalf("node checksum: got %x, want %x", got, want)
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
	encoded := EncodeNode(node, testNodePageSize)
	binary.LittleEndian.PutUint16(encoded[nodeTypeOffset:nodePageIDOffset], 3)
	resealNodeForTest(encoded, testNodePageSize)
	if _, err := DecodeNode(encoded, node.PageID(), testNodePageSize); !errors.Is(err, page.ErrInvalid) {
		t.Fatalf("DecodeNode error: got %v, want %v", err, page.ErrInvalid)
	}
}

func resealNodeForTest(data []byte, pageSize int64) {
	table := crc32.MakeTable(crc32.Castagnoli)
	checksum := crc32.Update(0, table, data[:nodeChecksumOffset])
	checksum = crc32.Update(checksum, table, data[NodeHeaderSize:])
	checksum = crc32.Update(checksum, table, make([]byte, pageSize-int64(len(data))))
	binary.LittleEndian.PutUint32(data[nodeChecksumOffset:NodeHeaderSize], checksum)
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
	if got, want := len(encoded), node.EncodedSize(); got != want {
		t.Fatalf("encoded length: got %d, want %d", got, want)
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
			for b.Loop() {
				if err := WriteNode(io.Discard, node, pageSize, true); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func fixedLeafImageForTest() []byte {
	const (
		entryCount      = 2
		directoryStart  = NodeHeaderSize
		descriptorSize  = 16
		firstDataOffset = directoryStart + entryCount*descriptorSize
	)
	data := make([]byte, firstDataOffset+4)
	binary.LittleEndian.PutUint16(data[nodeTypeOffset:nodePageIDOffset], uint16(NodeTypeLeaf))
	binary.LittleEndian.PutUint64(data[nodePageIDOffset:nodeEntryCountOffset], 7)
	binary.LittleEndian.PutUint32(data[nodeEntryCountOffset:nodeChecksumOffset], entryCount)

	first := data[directoryStart : directoryStart+descriptorSize]
	binary.LittleEndian.PutUint32(first[4:8], firstDataOffset)
	binary.LittleEndian.PutUint32(first[8:12], 1)
	binary.LittleEndian.PutUint32(first[12:16], 1)
	second := data[directoryStart+descriptorSize : firstDataOffset]
	binary.LittleEndian.PutUint32(second[4:8], firstDataOffset+2)
	binary.LittleEndian.PutUint32(second[8:12], 1)
	binary.LittleEndian.PutUint32(second[12:16], 1)
	copy(data[firstDataOffset:], []byte{'a', '1', 'b', '2'})
	return data
}

func fixedBranchImageForTest() []byte {
	const (
		entryCount      = 2
		directoryStart  = NodeHeaderSize
		descriptorSize  = 8
		childrenStart   = directoryStart + entryCount*descriptorSize
		firstDataOffset = childrenStart + (entryCount+1)*page.IDSize
	)
	data := make([]byte, firstDataOffset+2)
	binary.LittleEndian.PutUint16(data[nodeTypeOffset:nodePageIDOffset], uint16(NodeTypeBranch))
	binary.LittleEndian.PutUint64(data[nodePageIDOffset:nodeEntryCountOffset], 7)
	binary.LittleEndian.PutUint32(data[nodeEntryCountOffset:nodeChecksumOffset], entryCount)

	first := data[directoryStart : directoryStart+descriptorSize]
	binary.LittleEndian.PutUint32(first[0:4], firstDataOffset)
	binary.LittleEndian.PutUint32(first[4:8], 1)
	second := data[directoryStart+descriptorSize : childrenStart]
	binary.LittleEndian.PutUint32(second[0:4], firstDataOffset+1)
	binary.LittleEndian.PutUint32(second[4:8], 1)
	binary.LittleEndian.PutUint64(data[childrenStart:childrenStart+page.IDSize], 2)
	binary.LittleEndian.PutUint64(data[childrenStart+page.IDSize:childrenStart+2*page.IDSize], 3)
	binary.LittleEndian.PutUint64(data[childrenStart+2*page.IDSize:firstDataOffset], 4)
	copy(data[firstDataOffset:], []byte{'m', 't'})
	return data
}

func TestDecodeWALNode_DecodesFixedDirectories(t *testing.T) {
	leaf, err := DecodeWALNode(fixedLeafImageForTest(), 7)
	if err != nil {
		t.Fatalf("decode fixed leaf: %v", err)
	}
	if leaf.EntryCount() != 2 {
		t.Fatalf("leaf entry count: got %d, want 2", leaf.EntryCount())
	}
	entry, found, err := leaf.FindEntryRef([]byte("b"))
	if err != nil || !found || !bytes.Equal(entry.Value(), []byte("2")) {
		t.Fatalf("fixed leaf entry: found=%t value=%q err=%v", found, entry.Value(), err)
	}

	branch, err := DecodeWALNode(fixedBranchImageForTest(), 7)
	if err != nil {
		t.Fatalf("decode fixed branch: %v", err)
	}
	if branch.IsLeaf() || !slices.Equal(branch.Children, []page.ID{2, 3, 4}) {
		t.Fatalf("fixed branch children: got %v", branch.Children)
	}
}

func TestDecodeWALNode_RejectsInvalidFixedDirectory(t *testing.T) {
	const (
		leafDirectoryStart   = NodeHeaderSize
		branchDirectoryStart = NodeHeaderSize
		leafDescriptorSize   = 16
		branchDescriptorSize = 8
		leafPayloadStart     = leafDirectoryStart + 2*leafDescriptorSize
		branchChildrenStart  = branchDirectoryStart + 2*branchDescriptorSize
	)
	testCases := []struct {
		name   string
		image  func() []byte
		change func([]byte)
	}{
		{
			name:  "leaf unknown flags",
			image: fixedLeafImageForTest,
			change: func(data []byte) {
				binary.LittleEndian.PutUint32(data[leafDirectoryStart:leafDirectoryStart+4], 2)
			},
		},
		{
			name:  "leaf offset before payload",
			image: fixedLeafImageForTest,
			change: func(data []byte) {
				binary.LittleEndian.PutUint32(data[leafDirectoryStart+4:leafDirectoryStart+8], leafPayloadStart-1)
			},
		},
		{
			name:  "leaf payload gap",
			image: fixedLeafImageForTest,
			change: func(data []byte) {
				second := leafDirectoryStart + leafDescriptorSize
				binary.LittleEndian.PutUint32(data[second+4:second+8], leafPayloadStart+3)
			},
		},
		{
			name:  "leaf value outside image",
			image: fixedLeafImageForTest,
			change: func(data []byte) {
				binary.LittleEndian.PutUint32(data[leafDirectoryStart+12:leafDirectoryStart+16], ^uint32(0))
			},
		},
		{
			name:  "leaf keys out of order",
			image: fixedLeafImageForTest,
			change: func(data []byte) {
				data[leafPayloadStart] = 'z'
			},
		},
		{
			name:  "leaf duplicate keys",
			image: fixedLeafImageForTest,
			change: func(data []byte) {
				data[leafPayloadStart+2] = 'a'
			},
		},
		{
			name:   "leaf trailing byte",
			image:  fixedLeafImageForTest,
			change: func(data []byte) {},
		},
		{
			name:  "branch zero first child",
			image: fixedBranchImageForTest,
			change: func(data []byte) {
				clear(data[branchChildrenStart : branchChildrenStart+page.IDSize])
			},
		},
		{
			name:  "branch zero last child",
			image: fixedBranchImageForTest,
			change: func(data []byte) {
				start := branchChildrenStart + 2*page.IDSize
				clear(data[start : start+page.IDSize])
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			data := testCase.image()
			if testCase.name == "leaf trailing byte" {
				data = append(data, 0)
			}
			testCase.change(data)
			if _, err := DecodeWALNode(data, 7); !errors.Is(err, ErrInvalid) {
				t.Fatalf("DecodeWALNode error: got %v, want ErrInvalid", err)
			}
		})
	}
}

func TestDecodeWALNode_RejectsEmptyStoredKey(t *testing.T) {
	node := NewLeafNode(7)
	node.entries = append(node.entries, NewEntry(0, nil, []byte("value")))

	if _, err := DecodeWALNode(EncodeWALNode(node), node.PageID()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("DecodeWALNode error: got %v, want ErrInvalid", err)
	}
}

func TestLookupEncodedWALNode_UsesLeafDirectoryBinarySearch(t *testing.T) {
	node := NewLeafNode(7)
	for key := byte('a'); key <= 'h'; key++ {
		if err := node.InsertEntry(NewEntry(0, []byte{key}, []byte{key - 'a'})); err != nil {
			t.Fatal(err)
		}
	}
	data := EncodeWALNode(node)
	const firstLeafEntryOffsetField = NodeHeaderSize + encodedUint32Size
	binary.LittleEndian.PutUint32(data[firstLeafEntryOffsetField:firstLeafEntryOffsetField+4], ^uint32(0))

	entry, found, child, err := LookupEncodedWALNode(data, node.PageID(), []byte("h"))
	if err != nil || !found || child != 0 || !bytes.Equal(entry.Value(), []byte{7}) {
		t.Fatalf("binary leaf lookup: found=%t child=%d value=%v err=%v", found, child, entry.Value(), err)
	}
	if _, err := DecodeWALNode(data, node.PageID()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("full leaf decode error: got %v, want ErrInvalid", err)
	}
}

func TestLookupEncodedWALNode_UsesBranchDirectoryBinarySearch(t *testing.T) {
	root := &Node{
		header: newNodeHeader(NodeTypeBranch, 7),
		entries: []Entry{
			{key: []byte("b")}, {key: []byte("d")}, {key: []byte("f")}, {key: []byte("h")},
			{key: []byte("j")}, {key: []byte("l")}, {key: []byte("n")}, {key: []byte("p")},
		},
		Children: []page.ID{10, 11, 12, 13, 14, 15, 16, 17, 18},
	}
	data := EncodeWALNode(root)
	const firstBranchEntryOffsetField = NodeHeaderSize
	binary.LittleEndian.PutUint32(data[firstBranchEntryOffsetField:firstBranchEntryOffsetField+4], ^uint32(0))

	_, found, child, err := LookupEncodedWALNode(data, root.PageID(), []byte("z"))
	if err != nil || found || child != 18 {
		t.Fatalf("binary branch lookup: found=%t child=%d err=%v", found, child, err)
	}
	if _, err := DecodeWALNode(data, root.PageID()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("full branch decode error: got %v, want ErrInvalid", err)
	}
}

func TestVisitNodeReferencesChecksMappedAndWALPages(t *testing.T) {
	leaf := NewLeafNode(7)
	if err := leaf.InsertEntry(NewEntry(0, []byte("plain"), []byte("value"))); err != nil {
		t.Fatal(err)
	}
	if err := leaf.InsertEntry(NewEntry(BucketLeafFlag, []byte("nested"), page.EncodeID(9))); err != nil {
		t.Fatal(err)
	}
	visit := func(data []byte, mapped bool) ([]page.ID, error) {
		var refs []page.ID
		add := func(id page.ID) error { refs = append(refs, id); return nil }
		var err error
		if mapped {
			err = VisitMappedNodeReferences(data, 7, 4096, add)
		} else {
			err = VisitWALNodeReferences(data, 7, add)
		}
		return refs, err
	}
	for _, mapped := range []bool{true, false} {
		var data []byte
		if mapped {
			data = EncodeNode(leaf, 4096)
		} else {
			data = EncodeWALNode(leaf)
		}
		refs, err := visit(data, mapped)
		if err != nil || !slices.Equal(refs, []page.ID{9}) {
			t.Fatalf("mapped=%t: refs=%v, err=%v", mapped, refs, err)
		}
	}
	data := EncodeNode(leaf, 4096)
	data[len(data)-1] ^= 1
	if err := VisitMappedNodeReferences(data, 7, 4096, func(page.ID) error { return nil }); !errors.Is(err, page.ErrChecksum) {
		t.Fatalf("damaged page: %v", err)
	}
}

func TestVisitNodeReferencesReadsBranchChildren(t *testing.T) {
	left := NewLeafNode(3)
	right := NewLeafNode(4)
	branch := NewRootNode(2, left, right, []byte("middle"))
	data := EncodeNode(branch, 4096)
	var refs []page.ID
	err := VisitMappedNodeReferences(data, 2, 4096, func(id page.ID) error {
		refs = append(refs, id)
		return nil
	})
	if err != nil || !slices.Equal(refs, []page.ID{3, 4}) {
		t.Fatalf("branch refs=%v, err=%v", refs, err)
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
