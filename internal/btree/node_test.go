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
	"unsafe"

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
	// The Node holds its header, so one allocation covers both. The second one holds the entry list.
	if got := testing.AllocsPerRun(100, func() {
		clone = original.Clone()
	}); got != 2 {
		t.Fatalf("Node.Clone allocations: got %v, want 2", got)
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
	if err := WriteNode(&pageData, node, pageSize, true, true); err != nil {
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

	if _, err := DecodeNode(encoded, node.PageID(), testNodePageSize, nil, true); !errors.Is(err, page.ErrChecksum) {
		t.Fatalf("DecodeNode error: got %v, want ErrChecksum", err)
	}
}

func TestPageChecksum_ZeroCRCUsesNonzeroMarker(t *testing.T) {
	const pageSize int64 = 64
	node := NewLeafNode(7)
	// This value makes the raw CRC32C zero for this page layout.
	if err := node.InsertEntry(NewEntry(0, []byte("k"), []byte{0xac, 0x3e, 0xdf, 0xeb})); err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	if err := WriteNode(&encoded, node, pageSize, true, true); err != nil {
		t.Fatal(err)
	}
	data := encoded.Bytes()
	table := crc32.MakeTable(crc32.Castagnoli)
	raw := crc32.Update(0, table, data[:nodeChecksumOffset])
	raw = crc32.Update(raw, table, data[NodeHeaderSize:])
	if raw != 0 || binary.LittleEndian.Uint32(data[nodeChecksumOffset:NodeHeaderSize]) != 1 {
		t.Fatalf("raw CRC=%x, stored=%x; want zero and one", raw, data[nodeChecksumOffset:NodeHeaderSize])
	}
	if _, err := DecodeNode(data, 7, pageSize, nil, true); err != nil {
		t.Fatal(err)
	}
	if _, found, _, err := LookupMappedNode(data, 7, []byte("k"), nil, false, true); err != nil || !found {
		t.Fatalf("zero-CRC lookup: found=%t, %v", found, err)
	}
}

func TestLookupMappedNode_ChecksOrdinaryPageChecksum(t *testing.T) {
	for _, branch := range []bool{false, true} {
		t.Run(fmt.Sprintf("branch=%t", branch), func(t *testing.T) {
			node := NewLeafNode(7)
			if branch {
				node = NewRootNode(7, NewLeafNode(8), NewLeafNode(9), []byte("middle"))
			} else if err := node.InsertEntry(NewEntry(0, []byte("key"), []byte("value"))); err != nil {
				t.Fatal(err)
			}
			data := make([]byte, 4096)
			copy(data, EncodeNode(node, 4096))
			data[len(data)-1] ^= 1
			if _, _, _, err := LookupMappedNode(data, 7, []byte("key"), nil, false, true); !errors.Is(err, page.ErrChecksum) {
				t.Fatalf("damaged ordinary page: got %v, want checksum error", err)
			}
		})
	}
}

func TestUncheckedNodePages_KeepStructuralChecks(t *testing.T) {
	const pageSize int64 = 64
	for _, branch := range []bool{false, true} {
		for _, continued := range []bool{false, true} {
			t.Run(fmt.Sprintf("branch=%t/continued=%t", branch, continued), func(t *testing.T) {
				key, value := []byte("k"), []byte("v")
				if continued {
					key, value = bytes.Repeat(key, 128), bytes.Repeat(value, 500)
				}
				node := NewLeafNode(7)
				if branch {
					node = NewRootNode(7, NewLeafNode(8), NewLeafNode(9), key)
				} else if err := node.InsertEntry(NewEntry(0, key, value)); err != nil {
					t.Fatal(err)
				}
				ids := make([]page.ID, NodePageCount(node, pageSize))
				for i := range ids {
					ids[i] = 7 + page.ID(i)*3
				}
				pages, err := EncodeNodePages(node, pageSize, ids)
				if err != nil {
					t.Fatal(err)
				}
				for i, data := range pages {
					if err := VerifyNodeIDAndSetChecksum(data, ids[i], false); err != nil {
						t.Fatal(err)
					}
				}
				read := func(id page.ID) ([]byte, error) {
					for i, want := range ids {
						if id == want {
							return pages[i], nil
						}
					}
					return nil, ErrInvalid
				}
				decoded, err := DecodeNode(pages[0], 7, pageSize, read, true)
				if err != nil || decoded.EntryCount() != node.EntryCount() {
					t.Fatalf("unchecked decode: %v", err)
				}
				if _, _, _, err := LookupMappedNode(pages[0], 7, key, read, false, true); err != nil {
					t.Fatal(err)
				}
				binary.LittleEndian.PutUint32(pages[0][nodeEntryCountOffset:nodeChecksumOffset], ^uint32(0))
				if _, err := DecodeNode(pages[0], 7, pageSize, read, true); !errors.Is(err, ErrInvalid) {
					t.Fatalf("unchecked invalid count: got %v, want invalid", err)
				}
			})
		}
	}
}

func TestDecodeNode_RejectsCorruptedPagePaddingBeforePayloadDecode(t *testing.T) {
	const pageSize int64 = 64
	node := NewLeafNode(page.ID(7))
	if err := node.InsertEntry(NewEntry(0, []byte("k"), []byte("v"))); err != nil {
		t.Fatal(err)
	}

	var encoded bytes.Buffer
	if err := WriteNode(&encoded, node, pageSize, true, true); err != nil {
		t.Fatal(err)
	}
	data := encoded.Bytes()
	data[len(data)-1] ^= 0xff

	if _, err := DecodeNode(data, node.PageID(), pageSize, nil, true); !errors.Is(err, page.ErrChecksum) {
		t.Fatalf("DecodeNode error: got %v, want ErrChecksum", err)
	}
}

func TestDecodeNode_RejectsHeaderForWrongPage(t *testing.T) {
	node := NewLeafNode(page.ID(7))
	encoded := EncodeNode(node, testNodePageSize)

	if _, err := DecodeNode(encoded, page.ID(8), testNodePageSize, nil, true); !errors.Is(err, page.ErrInvalid) {
		t.Fatalf("DecodeNode error: got %v, want ErrInvalid", err)
	}
}

func TestDecodeNode_RejectsInvalidProtectedNodeHeader(t *testing.T) {
	node := NewLeafNode(page.ID(7))
	encoded := EncodeNode(node, testNodePageSize)
	binary.LittleEndian.PutUint16(encoded[nodeTypeOffset:nodePageIDOffset], 3)
	resealNodeForTest(encoded, testNodePageSize)
	if _, err := DecodeNode(encoded, node.PageID(), testNodePageSize, nil, true); !errors.Is(err, page.ErrInvalid) {
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
		decoded, decodeErr = DecodeNode(encoded, node.PageID(), testNodePageSize, nil, true)
	}); got != 2 {
		t.Fatalf("DecodeNode allocations: got %v, want 2 for the node with its header and the entry list", got)
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

	decoded, err := DecodeNode(encoded, node.PageID(), testNodePageSize, nil, true)
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

	if _, err := DecodeNode(data, 1, testNodePageSize, nil, true); err == nil {
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

func TestNodeSavedSizeTracksMutations(t *testing.T) {
	check := func(node *Node) {
		t.Helper()
		want := NodeHeaderSize + nodePayloadSize(node)
		if got := node.EncodedSize(); got != want {
			t.Fatalf("node %d size: got %d, want %d", node.PageID(), got, want)
		}
		if got := len(EncodeWALNode(node)); got != want {
			t.Fatalf("node %d WAL size: got %d, want %d", node.PageID(), got, want)
		}
		node.CacheEncodedSize()
	}
	leaf := NewLeafNode(8)
	for _, key := range []string{"a", "m", "z"} {
		if err := leaf.InsertEntry(NewEntry(0, []byte(key), bytes.Repeat([]byte("v"), 64))); err != nil {
			t.Fatal(err)
		}
		check(leaf)
	}
	if err := leaf.InsertEntry(NewEntry(0, []byte("m"), []byte("short"))); err != nil {
		t.Fatal(err)
	}
	check(leaf)
	if err := leaf.DeleteKeyIfPresent([]byte("m")); err != nil {
		t.Fatal(err)
	}
	check(leaf)
	clone := leaf.Clone()
	if err := clone.InsertEntry(NewEntry(0, []byte("a"), []byte("clone"))); err != nil {
		t.Fatal(err)
	}
	check(clone)
	check(leaf)
	right, separator, err := leaf.Split(9, 64)
	if err != nil {
		t.Fatal(err)
	}
	check(leaf)
	check(right)
	parent := NewRootNode(10, leaf, right, separator)
	check(parent)
	parent.InsertSplitChild(1, NewLeafNode(11), []byte("zz"))
	check(parent)
	parent.setSeparator(0, []byte("middle"))
	check(parent)
	parent.InsertSplitChild(2, NewLeafNode(12), []byte("zzz"))
	check(parent)
	branchRight, _, err := parent.Split(13, 64)
	if err != nil {
		t.Fatal(err)
	}
	check(parent)
	check(branchRight)

	left, right := NewLeafNode(20), NewLeafNode(21)
	for index, key := range []string{"a", "m", "n-longer", "z"} {
		node := right
		if index == 0 {
			node = left
		}
		if err := node.InsertEntry(NewEntry(0, []byte(key), []byte("value"))); err != nil {
			t.Fatal(err)
		}
	}
	parent = NewRootNode(22, left, right, []byte("m"))
	check(left)
	check(right)
	check(parent)
	if !redistributeChildren(parent, 0, left, right, 256, 64) {
		t.Fatal("redistribution failed")
	}
	check(left)
	check(right)
	check(parent)
	mergeChildren(parent, 0, left, right)
	check(left)
	check(parent)

	left = NewRootNode(30, NewLeafNode(40), NewLeafNode(41), []byte("a"))
	right = NewRootNode(31, NewLeafNode(42), NewLeafNode(43), []byte("n-longer"))
	right.InsertSplitChild(1, NewLeafNode(44), []byte("u-long"))
	right.InsertSplitChild(2, NewLeafNode(45), []byte("z"))
	parent = NewRootNode(32, left, right, []byte("m"))
	check(left)
	check(right)
	check(parent)
	if !redistributeChildren(parent, 0, left, right, 256, 64) {
		t.Fatal("branch redistribution failed")
	}
	check(left)
	check(right)
	check(parent)
	mergeChildren(parent, 0, left, right)
	check(left)
	check(parent)
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
	if err := WriteNode(writer, node, pageSize, true, true); err != nil {
		t.Fatal(err)
	}
	if writer.Len() != int(pageSize) {
		t.Fatalf("encoded node size: got %d bytes, want one %d-byte page", writer.Len(), pageSize)
	}
}

// TestNodeLayout_StaysInSizeClass guards the Node size. Each clone allocates one Node, which holds its header.
// A new field can move Node into a larger allocation size class and make every clone larger.
func TestNodeLayout_StaysInSizeClass(t *testing.T) {
	if size := unsafe.Sizeof(Node{}); size > 112 {
		t.Fatalf("Node size: got %d bytes, want at most 112", size)
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
				if err := WriteNode(io.Discard, node, pageSize, true, true); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDecodeContinuedLeaf measures a full decode of one continued leaf from in-memory pages.
// Ascending IDs are the usual order for new pages. Descending IDs come from reused lower pages.
func BenchmarkDecodeContinuedLeaf(b *testing.B) {
	const pageSize int64 = 4096
	for _, size := range []int{64 << 10, 1 << 20} {
		for _, order := range []string{"ascending", "descending"} {
			b.Run(fmt.Sprintf("bytes=%d/ids=%s", size, order), func(b *testing.B) {
				node := NewLeafNode(7)
				if err := node.InsertEntry(NewEntry(0, []byte("key"), bytes.Repeat([]byte("v"), size))); err != nil {
					b.Fatal(err)
				}
				ids := make([]page.ID, NodePageCount(node, pageSize))
				ids[0] = node.PageID()
				for i := 1; i < len(ids); i++ {
					ids[i] = page.ID(10 + i)
					if order == "descending" {
						ids[i] = page.ID(10 + len(ids) - i)
					}
				}
				pages, err := EncodeNodePages(node, pageSize, ids)
				if err != nil {
					b.Fatal(err)
				}
				byID := make(map[page.ID][]byte, len(pages))
				for i, data := range pages {
					byID[ids[i]] = data
				}
				read := func(id page.ID) ([]byte, error) { return byID[id], nil }
				b.SetBytes(int64(size))
				b.ReportAllocs()
				for b.Loop() {
					if _, err := DecodeLeafNode(pages[0], node.PageID(), pageSize, read, false); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
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
	leaf, err := DecodeWALNode(fixedLeafImageForTest(), 7, 4096, nil)
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

	branch, err := DecodeWALNode(fixedBranchImageForTest(), 7, 4096, nil)
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
			if _, err := DecodeWALNode(data, 7, 4096, nil); !errors.Is(err, ErrInvalid) {
				t.Fatalf("DecodeWALNode error: got %v, want ErrInvalid", err)
			}
		})
	}
}

func TestDecodeWALNode_RejectsEmptyStoredKey(t *testing.T) {
	node := NewLeafNode(7)
	node.entries = append(node.entries, NewEntry(0, nil, []byte("value")))

	if _, err := DecodeWALNode(EncodeWALNode(node), node.PageID(), 4096, nil); !errors.Is(err, ErrInvalid) {
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

	entry, found, child, err := LookupEncodedWALNode(data, node.PageID(), []byte("h"), 4096, nil, true)
	if err != nil || !found || child != 0 || !bytes.Equal(entry.Value(), []byte{7}) {
		t.Fatalf("binary leaf lookup: found=%t child=%d value=%v err=%v", found, child, entry.Value(), err)
	}
	if _, err := DecodeWALNode(data, node.PageID(), 4096, nil); !errors.Is(err, ErrInvalid) {
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

	_, found, child, err := LookupEncodedWALNode(data, root.PageID(), []byte("z"), 4096, nil, true)
	if err != nil || found || child != 18 {
		t.Fatalf("binary branch lookup: found=%t child=%d err=%v", found, child, err)
	}
	if _, err := DecodeWALNode(data, root.PageID(), 4096, nil); !errors.Is(err, ErrInvalid) {
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
			err = VisitMappedNodeReferences(data, 7, 4096, nil, add, nil, true)
		} else {
			err = VisitWALNodeReferences(data, 7, 4096, nil, add, nil)
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
	if err := VisitMappedNodeReferences(data, 7, 4096, nil, func(page.ID) error { return nil }, nil, true); !errors.Is(err, page.ErrChecksum) {
		t.Fatalf("damaged page: %v", err)
	}
}

func TestVisitNodeReferencesReadsBranchChildren(t *testing.T) {
	left := NewLeafNode(3)
	right := NewLeafNode(4)
	branch := NewRootNode(2, left, right, []byte("middle"))
	data := EncodeNode(branch, 4096)
	var refs []page.ID
	err := VisitMappedNodeReferences(data, 2, 4096, nil, func(id page.ID) error {
		refs = append(refs, id)
		return nil
	}, nil, true)
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
	decoded, err := DecodeWALNode(encoded, node.PageID(), 4096, nil)
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
		entry, found, child, lookupErr = LookupEncodedWALNode(data, node.PageID(), []byte("beta"), 4096, nil, true)
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
			_, found, child, err := LookupEncodedWALNode(data, root.PageID(), []byte(testCase.key), 4096, nil, true)
			if err != nil {
				t.Fatal(err)
			}
			if found || child != testCase.wantChild {
				t.Fatalf("branch lookup: found=%t child=%d, want false and %d", found, child, testCase.wantChild)
			}
		})
	}
}

func TestNodeClone_OwnsContinuationPageIDs(t *testing.T) {
	base := NewLeafNode(2)
	base.SetOverflowPages([]page.ID{7, 9})
	clone := base.Clone()
	clone.continuationPageIDs[0] = 11
	if !slices.Equal(base.OverflowPages(), []page.ID{7, 9}) {
		t.Fatalf("clone changed original page IDs: %v", base.OverflowPages())
	}
	clone.SetOverflowPages(nil)
	if len(clone.OverflowPages()) != 0 || len(base.OverflowPages()) != 2 {
		t.Fatal("clearing clone page IDs changed the original node")
	}
}

func TestEncodeNodePages_RejectsInvalidIDs(t *testing.T) {
	node := NewLeafNode(8)
	if err := node.InsertEntry(NewEntry(0, []byte("key"), bytes.Repeat([]byte("v"), 80))); err != nil {
		t.Fatal(err)
	}
	for _, ids := range [][]page.ID{{8, 42}, {9, 42, 91}, {8, 42, 42}, {8, 1, 91}} {
		if _, err := EncodeNodePages(node, 64, ids); !errors.Is(err, page.ErrInvalid) {
			t.Fatalf("invalid IDs %v: got %v", ids, err)
		}
	}
}

func TestNodePageCount_RequiresSpaceForContinuationID(t *testing.T) {
	node := NewLeafNode(8)
	if err := node.InsertEntry(NewEntry(0, []byte("key"), bytes.Repeat([]byte("v"), 80))); err != nil {
		t.Fatal(err)
	}
	for _, size := range []int64{0, 26, 27, 34} {
		if got := NodePageCount(node, size); got != 0 {
			t.Fatalf("page size %d cannot hold an ID and body byte: got %d pages", size, got)
		}
		if _, err := EncodeNodePages(node, size, []page.ID{8, 42}); !errors.Is(err, page.ErrInvalid) {
			t.Fatalf("page size %d: got %v", size, err)
		}
	}
}

func TestNodePages_FileRoundTrip(t *testing.T) {
	branch := NewRootNode(8, NewLeafNode(20), NewLeafNode(21), []byte("a"))
	branch.entries = []Entry{
		NewEntry(0, bytes.Repeat([]byte("a"), 50), nil),
		NewEntry(0, bytes.Repeat([]byte("b"), 50), nil),
		NewEntry(0, bytes.Repeat([]byte("c"), 50), nil),
		NewEntry(0, []byte("d"), nil),
		NewEntry(0, bytes.Repeat([]byte("e"), 50), nil),
	}
	branch.Children = []page.ID{20, 21, 22, 23, 24, 25}
	for _, test := range []struct {
		name                 string
		valueSize, wantPages int
		branch               bool
		pageSize             int64
	}{
		{"below two-page capacity", 56, 2, false, 64},
		{"exact two-page capacity", 57, 2, false, 64},
		{"above two-page capacity", 58, 3, false, 64},
		{"leaf with many groups", 2048, 55, false, 64},
		{"branch with many groups", 0, 8, true, 64},
		{"smallest overflow pages", 57, 9, false, 35},
	} {
		t.Run(test.name, func(t *testing.T) {
			pageSize := test.pageSize
			node := branch
			if !test.branch {
				node = NewLeafNode(8)
				if err := node.InsertEntry(NewEntry(0, []byte("key"), bytes.Repeat([]byte("v"), test.valueSize))); err != nil {
					t.Fatal(err)
				}
			}
			if got := NodePageCount(node, pageSize); got != test.wantPages {
				t.Fatalf("page count: got %d, want %d", got, test.wantPages)
			}
			ids := make([]page.ID, test.wantPages)
			ids[0] = node.PageID()
			for i := 1; i < len(ids); i++ {
				ids[i] = page.ID(42 + i*3)
			}
			pages, err := EncodeNodePages(node, pageSize, ids)
			if err != nil {
				t.Fatal(err)
			}
			path := t.TempDir() + "/pages"
			file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			for i, data := range pages {
				if page.ID(binary.LittleEndian.Uint64(data[2:10])) != ids[i] {
					t.Fatal("encoder changed page order")
				}
				if _, err := file.WriteAt(data, int64(ids[i])*pageSize); err != nil {
					t.Fatal(err)
				}
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			file, err = os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()

			read := func(id page.ID) ([]byte, error) {
				data := make([]byte, pageSize)
				_, err := file.ReadAt(data, int64(id)*pageSize)
				return data, err
			}
			first, err := read(node.PageID())
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := DecodeNode(first, node.PageID(), pageSize, read, true)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(EncodeWALNode(decoded), EncodeWALNode(node)) || !slices.Equal(decoded.OverflowPages(), ids[1:]) || decoded.IsLeaf() != node.IsLeaf() {
				t.Fatal("node round trip changed its contents or continuation IDs")
			}
			queries := [][]byte{[]byte("0"), []byte("z")}
			for i := 0; i < node.EntryCount(); i++ {
				queries = append(queries, node.EntryAt(i).Key())
			}
			for _, key := range queries {
				want, found, child, err := LookupNode(node, key)
				if err != nil {
					t.Fatal(err)
				}
				got, gotFound, gotChild, err := LookupMappedNode(first, node.PageID(), key, read, true, true)
				if err != nil || gotFound != found || gotChild != child || (found && !bytes.Equal(got.Value(), want.Value())) {
					t.Fatalf("mapped lookup %q differs: found=%v child=%d error=%v", key, gotFound, gotChild, err)
				}
			}
		})
	}
}

func TestNodePages_MappedLookupSkipsLargeValue(t *testing.T) {
	leaf := NewLeafNode(8)
	if err := leaf.InsertEntry(NewEntry(0, []byte("key"), bytes.Repeat([]byte("v"), 2048))); err != nil {
		t.Fatal(err)
	}
	branch := NewRootNode(8, NewLeafNode(20), NewLeafNode(21), bytes.Repeat([]byte("k"), MaxKeySize))
	for _, testCase := range []struct {
		node   *Node
		forWAL bool
	}{{leaf, false}, {branch, false}, {leaf, true}, {branch, true}} {
		node, forWAL := testCase.node, testCase.forWAL
		size := int64(64)
		if !node.IsLeaf() {
			size = 4096
		}
		ids := make([]page.ID, NodePageCount(node, size))
		ids[0] = 8
		for i := 1; i < len(ids); i++ {
			ids[i] = page.ID(42 + i*3)
		}
		pages, err := EncodeNodePages(node, size, ids)
		if err != nil {
			t.Fatal(err)
		}
		if forWAL {
			pages, err = EncodeWALNodePages(node, size, ids)
			if err != nil {
				t.Fatal(err)
			}
		}
		reads := 0
		read := func(id page.ID) ([]byte, error) {
			reads++
			for i, got := range ids {
				if id == got {
					return pages[i], nil
				}
			}
			return nil, ErrInvalid
		}
		var found bool
		var child page.ID
		query := []byte("z")
		allocations := testing.AllocsPerRun(100, func() {
			if forWAL {
				_, found, child, err = LookupEncodedWALNode(pages[0], 8, query, size, read, true)
			} else {
				_, found, child, err = LookupMappedNode(pages[0], 8, query, read, true, true)
			}
		})
		if err != nil || found || (node.IsLeaf() && child != 0) || (!node.IsLeaf() && child != 21) {
			t.Fatalf("mapped lookup: found=%v child=%d err=%v", found, child, err)
		}
		if allocations != 0 {
			t.Fatalf("mapped lookup allocated %v times", allocations)
		}
		if (node.IsLeaf() && reads > 202) || (!node.IsLeaf() && reads != 0) {
			t.Fatalf("mapped lookup read unneeded pages: %d", reads)
		}
	}
}

func TestNodePages_RejectsDamageAndCycles(t *testing.T) {
	node := NewLeafNode(8)
	if err := node.InsertEntry(NewEntry(0, []byte("key"), bytes.Repeat([]byte("v"), 80))); err != nil {
		t.Fatal(err)
	}
	ids := []page.ID{8, 42, 91}
	pages, err := EncodeNodePages(node, 64, ids)
	if err != nil {
		t.Fatal(err)
	}
	read := func(id page.ID) ([]byte, error) {
		for i, got := range ids {
			if got == id {
				return pages[i], nil
			}
		}
		return nil, ErrInvalid
	}
	invalid := bytes.Clone(pages[0])
	binary.LittleEndian.PutUint32(invalid[22:26], ^uint32(0))
	binary.LittleEndian.PutUint32(invalid[14:18], pageChecksum(invalid, 64, nodeChecksumOffset))
	if _, _, _, err := ReadMappedLeafNode(invalid, 8, 64, true); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid node ID array with valid CRC: %v", err)
	}
	if _, err := DecodeNode(invalid, 8, 64, read, true); !errors.Is(err, ErrInvalid) {
		t.Fatalf("decode invalid node ID array: %v", err)
	}
	if _, err := DecodeNode(pages[0], 8, 64, nil, true); !errors.Is(err, ErrInvalid) {
		t.Fatal("continued node accepted no reader")
	}
	pages[1][63] ^= 1
	if _, err := DecodeNode(pages[0], 8, 64, read, true); !errors.Is(err, page.ErrChecksum) {
		t.Fatalf("full decode missed damaged tail: %v", err)
	}
	if _, _, _, err := LookupMappedNode(pages[0], 8, []byte("key"), read, true, true); !errors.Is(err, page.ErrChecksum) {
		t.Fatalf("mapped lookup missed damaged tail: %v", err)
	}
	pages[1][63] ^= 1
	pages[2][63] ^= 1
	sealContinuationPage(pages[2])
	if _, err := DecodeNode(pages[0], 8, 64, read, true); !errors.Is(err, ErrInvalid) {
		t.Fatalf("full decode missed nonzero padding with valid CRC: %v", err)
	}
	pages[2][63] ^= 1
	sealContinuationPage(pages[2])
	binary.LittleEndian.PutUint64(pages[0][26:34], 8)
	binary.LittleEndian.PutUint32(pages[0][14:18], pageChecksum(pages[0], 64, nodeChecksumOffset))
	if _, err := DecodeNode(pages[0], 8, 64, read, true); !errors.Is(err, ErrInvalid) {
		t.Fatalf("full decode missed self link: %v", err)
	}
	if _, _, _, err := LookupMappedNode(pages[0], 8, []byte("key"), read, true, true); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mapped lookup missed self link: %v", err)
	}
}

func TestNodePages_SinglePageAndShrink(t *testing.T) {
	node := NewLeafNode(8)
	if err := node.InsertEntry(NewEntry(0, []byte("k"), bytes.Repeat([]byte("v"), 80))); err != nil {
		t.Fatal(err)
	}
	ids := []page.ID{8, 42, 91}
	pages, err := EncodeNodePages(node, 64, ids)
	if err != nil {
		t.Fatal(err)
	}
	read := func(id page.ID) ([]byte, error) {
		for i, got := range ids {
			if got == id {
				return pages[i], nil
			}
		}
		return nil, ErrInvalid
	}
	node, err = DecodeNode(pages[0], 8, 64, read, true)
	if err != nil {
		t.Fatal(err)
	}
	if node.header.Type != NodeTypeLeaf|overflowNodeFlag || !node.IsLeaf() {
		t.Fatal("continued leaf lost its type")
	}
	if err := node.InsertEntry(NewEntry(0, []byte("k"), bytes.Repeat([]byte("v"), 22))); err != nil {
		t.Fatal(err)
	}
	node.SetOverflowPages(nil)
	if node.header.Type != NodeTypeLeaf || node.header.BodyLength != 0 || node.header.NextPageCount != 0 {
		t.Fatal("normal node kept continuation header fields")
	}
	if NodePageCount(node, 64) != 1 {
		t.Fatal("small node still requires continuations")
	}
	pages, err = EncodeNodePages(node, 64, []page.ID{8})
	if err != nil || len(pages) != 1 || binary.LittleEndian.Uint16(pages[0][:2]) != uint16(NodeTypeLeaf) {
		t.Fatalf("normal page encoding: %v", err)
	}
	small, err := DecodeNode(pages[0], 8, 64, nil, true)
	if err != nil || len(small.OverflowPages()) != 0 || !small.IsLeaf() {
		t.Fatalf("normal page decode: %v", err)
	}
}

// TestNodePages_LookupAcrossBoundaries checks the same lookup for ordinary and
// continued nodes. Small pages split descriptors, child IDs, keys, and values.
func TestNodePages_LookupAcrossBoundaries(t *testing.T) {
	for _, branch := range []bool{false, true} {
		for _, testCase := range []struct {
			pageSize int64
			forWAL   bool
		}{
			{35, false}, {64, false}, {128, false}, {4096, false},
			{35, true}, {64, true}, {128, true}, {4096, true},
		} {
			pageSize, forWAL := testCase.pageSize, testCase.forWAL
			t.Run(fmt.Sprintf("branch=%v/page=%d/WAL=%t", branch, pageSize, forWAL), func(t *testing.T) {
				node := NewLeafNode(8)
				if branch {
					node.header.Type = NodeTypeBranch
					node.Children = []page.ID{1000}
				}
				for i := 0; i < 12; i++ {
					key := append([]byte(fmt.Sprintf("%06d", i)), bytes.Repeat([]byte{'k'}, i%3*35+1)...)
					value := bytes.Repeat([]byte{'v'}, []int{0, 1, 80, 257}[i%4])
					if branch {
						value = nil
						node.Children = append(node.Children, page.ID(1001+i))
					}
					node.entries = append(node.entries, NewEntry(0, key, value))
				}
				ids := make([]page.ID, NodePageCount(node, pageSize))
				ids[0] = node.PageID()
				for i := 1; i < len(ids); i++ {
					ids[i] = page.ID(42 + i*3)
				}
				pages, err := EncodeNodePages(node, pageSize, ids)
				if err != nil {
					t.Fatal(err)
				}
				if forWAL {
					pages, err = EncodeWALNodePages(node, pageSize, ids)
					if err != nil {
						t.Fatal(err)
					}
				}
				mapped := make(map[page.ID][]byte, len(ids))
				for i, id := range ids {
					mapped[id] = pages[i]
				}
				read := func(id page.ID) ([]byte, error) {
					if data, ok := mapped[id]; ok {
						return data, nil
					}
					return nil, ErrInvalid
				}
				var decoded *Node
				if forWAL {
					decoded, err = DecodeWALNode(pages[0], node.PageID(), pageSize, read)
				} else {
					decoded, err = DecodeNode(pages[0], node.PageID(), pageSize, read, true)
				}
				if err != nil || !bytes.Equal(EncodeWALNode(decoded), EncodeWALNode(node)) || !slices.Equal(decoded.OverflowPages(), ids[1:]) {
					t.Fatalf("full decode: %v", err)
				}
				queries := [][]byte{[]byte("0"), []byte("z")}
				for _, entry := range node.entries {
					queries = append(queries, entry.key, entry.key[:6], append(bytes.Clone(entry.key), 0))
				}
				for _, query := range queries {
					want, found, child, err := LookupNode(node, query)
					if err != nil {
						t.Fatal(err)
					}
					var got Entry
					var gotFound bool
					var gotChild page.ID
					if forWAL {
						got, gotFound, gotChild, err = LookupEncodedWALNode(pages[0], node.PageID(), query, pageSize, read, true)
					} else {
						got, gotFound, gotChild, err = LookupMappedNode(pages[0], node.PageID(), query, read, true, true)
					}
					if err != nil || gotFound != found || gotChild != child || (found && (got.flags != want.flags || !bytes.Equal(got.key, want.key) || !bytes.Equal(got.value, want.value))) {
						t.Fatalf("lookup %q: found=%v child=%d error=%v", query, gotFound, gotChild, err)
					}
				}
			})
		}
	}
}

// TestNodePages_RejectsTooSmallContinuedPage checks a valid checksum with a
// continued header that cannot hold a page ID. Full decode must return an error.
func TestNodePages_RejectsTooSmallContinuedPage(t *testing.T) {
	for _, branch := range []bool{false, true} {
		data := make([]byte, 27)
		kind := NodeTypeLeaf
		if branch {
			kind = NodeTypeBranch
		}
		binary.LittleEndian.PutUint16(data[:2], uint16(kind|overflowNodeFlag))
		binary.LittleEndian.PutUint64(data[2:10], 8)
		binary.LittleEndian.PutUint32(data[18:22], 80)
		binary.LittleEndian.PutUint32(data[14:18], pageChecksum(data, 27, nodeChecksumOffset))
		read := func(page.ID) ([]byte, error) {
			t.Fatal("invalid first page followed an ID")
			return nil, ErrInvalid
		}
		if _, err := DecodeNode(data, 8, 27, read, true); !errors.Is(err, ErrInvalid) {
			t.Fatalf("decode small continued page: %v", err)
		}
		if _, _, _, err := LookupMappedNode(data, 8, []byte("key"), read, true, true); !errors.Is(err, ErrInvalid) {
			t.Fatalf("lookup small continued page: %v", err)
		}
	}
}

// TestVisitContinuedNodeReferences checks child and storage ownership at page boundaries.
func TestVisitContinuedNodeReferences(t *testing.T) {
	for _, pageSize := range []int64{35, 64, 128, 4096} {
		for _, forWAL := range []bool{false, true} {
			for _, branch := range []bool{false, true} {
				t.Run(fmt.Sprintf("size=%d/WAL=%t/branch=%t", pageSize, forWAL, branch), func(t *testing.T) {
					node := NewLeafNode(7)
					expected := []page.ID{9}
					if branch {
						node = NewRootNode(7, NewLeafNode(3), NewLeafNode(4), bytes.Repeat([]byte("a"), int(pageSize)+7))
						node.InsertSplitChild(1, NewLeafNode(5), bytes.Repeat([]byte("z"), int(pageSize)+7))
						expected = []page.ID{3, 4, 5}
					} else {
						if err := node.InsertEntry(NewEntry(BucketLeafFlag, bytes.Repeat([]byte("a"), int(pageSize)+7), page.EncodeID(9))); err != nil {
							t.Fatal(err)
						}
						if err := node.InsertEntry(NewEntry(0, bytes.Repeat([]byte("z"), int(pageSize)+7), bytes.Repeat([]byte("v"), 10*int(pageSize)))); err != nil {
							t.Fatal(err)
						}
					}
					ids := make([]page.ID, NodePageCount(node, pageSize))
					ids[0] = 7
					for i := 1; i < len(ids); i++ {
						ids[i] = page.ID(20 + i*11)
					}
					var images [][]byte
					var err error
					if forWAL {
						images, err = EncodeWALNodePages(node, pageSize, ids)
					} else {
						images, err = EncodeNodePages(node, pageSize, ids)
					}
					if err != nil {
						t.Fatal(err)
					}
					pages := make(map[page.ID][]byte, len(ids))
					for i, id := range ids {
						pages[id] = images[i]
					}
					read := func(id page.ID) ([]byte, error) {
						data, ok := pages[id]
						if !ok {
							return nil, ErrInvalid
						}
						return data, nil
					}
					var children, continuations []page.ID
					visit := func(id page.ID) error { children = append(children, id); return nil }
					own := func(id page.ID) error { continuations = append(continuations, id); return nil }
					check := func() error {
						children, continuations = nil, nil
						if forWAL {
							return VisitWALNodeReferences(images[0], 7, pageSize, read, visit, own)
						}
						return VisitMappedNodeReferences(images[0], 7, pageSize, read, visit, own, true)
					}
					if err := check(); err != nil {
						t.Fatal(err)
					}
					if !slices.Equal(children, expected) || !slices.Equal(continuations, ids[1:]) {
						t.Fatalf("children=%v, continuation IDs=%v", children, continuations)
					}
					// Missing value pages must fail even when no nested root is stored there.
					last := ids[len(ids)-1]
					saved := pages[last]
					delete(pages, last)
					if err := check(); err == nil {
						t.Fatal("visitor accepted a missing continuation")
					}
					pages[last] = saved
					if !forWAL {
						pages[last] = bytes.Clone(saved)
						pages[last][len(saved)-1] ^= 1
						if err := check(); !errors.Is(err, page.ErrChecksum) {
							t.Fatalf("damaged continuation: %v", err)
						}
						pages[last] = saved
					}
					if err := check(); err != nil {
						t.Fatal(err)
					}
					stop := errors.New("stop ownership visit")
					if forWAL {
						err = VisitWALNodeReferences(images[0], 7, pageSize, read, visit, func(page.ID) error { return stop })
					} else {
						err = VisitMappedNodeReferences(images[0], 7, pageSize, read, visit, func(page.ID) error { return stop }, true)
					}
					if !errors.Is(err, stop) {
						t.Fatalf("callback error: %v", err)
					}
					// Valid checksums cannot make duplicate keys a valid node body.
					node.entries[1].key = bytes.Clone(node.entries[0].key)
					if forWAL {
						images, err = EncodeWALNodePages(node, pageSize, ids)
					} else {
						images, err = EncodeNodePages(node, pageSize, ids)
					}
					if err != nil {
						t.Fatal(err)
					}
					for i, id := range ids {
						pages[id] = images[i]
					}
					if err := check(); !errors.Is(err, ErrInvalid) {
						t.Fatalf("duplicate keys: %v", err)
					}

				})
			}
		}
	}
}

func TestLeafDeletionCopiesOnlyRetainedEntries(t *testing.T) {
	for _, indexes := range [][]int{{0}, {2}, {4}, {0, 2, 4}, {0, 1, 2, 3, 4}} {
		t.Run(fmt.Sprint(indexes), func(t *testing.T) {
			base := NewLeafNode(3)
			for i := range 5 {
				if err := base.InsertEntry(NewEntry(uint32(i*2), []byte{byte('a' + i)}, []byte{byte(i)})); err != nil {
					t.Fatal(err)
				}
			}
			base.SetOverflowPages([]page.ID{10, 11})
			base.header.Checksum = 42
			base.CacheEncodedSize()
			before := EncodeWALNode(base)
			want := base.Clone()
			for i := len(indexes) - 1; i >= 0; i-- {
				want.applyDelete(indexes[i])
			}
			private := base.CloneWithoutLeafEntries(indexes[0], indexes[1:], 0)
			if !bytes.Equal(EncodeWALNode(private), EncodeWALNode(want)) || private.EncodedSize() != want.EncodedSize() {
				t.Fatal("private copy changed retained leaf entries")
			}
			inPlace := base.Clone()
			storage := inPlace.entries
			inPlace.CacheEncodedSize()
			inPlace.DeleteLeafEntries(indexes[0], indexes[1:])
			if !bytes.Equal(EncodeWALNode(inPlace), EncodeWALNode(want)) || inPlace.EncodedSize() != want.EncodedSize() {
				t.Fatal("private compaction changed retained leaf entries")
			}
			for _, entry := range storage[inPlace.EntryCount():] {
				if entry.key != nil || entry.value != nil || entry.flags != 0 || entry.valueOwned {
					t.Fatal("private compaction retained a removed entry")
				}
			}
			private.continuationPageIDs[0] = 99
			private.SetPageID(100)
			if private.EntryCount() != 0 {
				private.entries[0] = NewEntry(0, []byte("changed"), nil)
			}
			if !bytes.Equal(EncodeWALNode(base), before) || base.OverflowPages()[0] != 10 || base.header.Checksum != 42 {
				t.Fatal("private deletion changed the committed node")
			}
		})
	}
}

func TestLeafDeletionKeepsCapacityForMerge(t *testing.T) {
	base := NewLeafNode(3)
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		if err := base.InsertEntry(NewEntry(0, []byte(key), []byte(key))); err != nil {
			t.Fatal(err)
		}
	}
	private := base.CloneWithoutLeafEntries(0, []int{1, 2}, 64)
	if private.EntryCount() != 2 || cap(private.entries) < base.EntryCount() || private.EncodedSize() != 54 {
		t.Fatal("underfull leaf did not keep merge capacity and its exact size")
	}
	right := NewLeafNode(5)
	for _, key := range []string{"f", "g"} {
		if err := right.InsertEntry(NewEntry(0, []byte(key), []byte(key))); err != nil {
			t.Fatal(err)
		}
	}
	parent := NewRootNode(4, private, right, []byte("f"))
	storage := &private.entries[0]
	mergeChildren(parent, 0, private, right)
	if &private.entries[0] != storage || private.EntryCount() != 4 || private.EncodedSize() != 90 {
		t.Fatal("leaf merge replaced reserved storage or kept a stale size")
	}
	if base.EntryCount() != 5 || string(base.EntryAt(0).Key()) != "a" {
		t.Fatal("reserved merge changed the committed leaf")
	}
	empty := base.CloneWithoutLeafEntries(0, []int{1, 2, 3, 4}, 64)
	if empty.EntryCount() != 0 || cap(empty.entries) < base.EntryCount() || empty.EncodedSize() != NodeHeaderSize {
		t.Fatal("empty leaf lost capacity needed by successive merges")
	}
}

// BenchmarkDeleteLeaf measures deletes from one committed leaf without file I/O.
// Each iteration restores the store's page view before it deletes the same keys.
func BenchmarkDeleteLeaf(b *testing.B) {
	for _, c := range []struct {
		count  int
		single bool
	}{{1, true}, {1, false}, {32, false}, {64, false}} {
		b.Run(fmt.Sprintf("keys=%d/single=%t", c.count, c.single), func(b *testing.B) {
			base := NewLeafNode(3)
			keys := make([][]byte, 64)
			for i := range keys {
				keys[i] = fmt.Appendf(nil, "%08d", i)
				if err := base.InsertEntry(NewEntry(0, keys[i], make([]byte, 32))); err != nil {
					b.Fatal(err)
				}
			}
			store := &memoryTreeStore{pageSize: 4096, nodes: make(map[page.ID]*Node), dirty: make(map[page.ID]*Node)}
			tree := NewTree(store)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				store.nodes[3] = base
				clear(store.dirty)
				var root *Node
				var changed bool
				var err error
				if c.single {
					root, changed, err = tree.DeleteEntry(base, keys[0])
				} else {
					root, changed, err = tree.DeleteEntries(base, keys[:c.count])
				}
				if err != nil || !changed || root.EntryCount() != 64-c.count {
					b.Fatalf("delete: changed=%t error=%v", changed, err)
				}
			}
		})
	}
}
