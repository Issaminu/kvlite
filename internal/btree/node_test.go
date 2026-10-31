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

// The lookup fast path must never accept a page that readNodeHeader rejects, and it must return the same kind and entry count.
func TestPlainNodeHeader_AcceptsOnlyPagesThatReadNodeHeaderAccepts(t *testing.T) {
	const id, pageSize = page.ID(8), 64
	header := func(kind NodeType, pageID page.ID, checksum uint32, size int) []byte {
		data := make([]byte, size)
		binary.LittleEndian.PutUint16(data[0:2], uint16(kind))
		binary.LittleEndian.PutUint64(data[2:10], uint64(pageID))
		binary.LittleEndian.PutUint32(data[10:14], 3)
		binary.LittleEndian.PutUint32(data[14:18], checksum)
		binary.LittleEndian.PutUint32(data[18:22], uint32(size-NodeHeaderSize))
		return data
	}
	checked := header(NodeTypeLeaf, id, 0, 32)
	binary.LittleEndian.PutUint32(checked[14:18], pageChecksum(checked, pageSize, nodeChecksumOffset))
	tests := []struct {
		name string
		data []byte
	}{
		{name: "leaf", data: header(NodeTypeLeaf, id, 0, 32)},
		{name: "branch", data: header(NodeTypeBranch, id, 0, 32)},
		{name: "continued leaf", data: header(NodeType(0x8001), id, 0, pageSize)},
		{name: "continued branch", data: header(NodeType(0x8002), id, 0, pageSize)},
		{name: "other page", data: header(NodeTypeLeaf, id+1, 0, 32)},
		{name: "continuation page", data: header(NodeType(3), id, 0, 32)},
		{name: "wrong checksum", data: header(NodeTypeLeaf, id, 1, 32)},
		{name: "correct checksum", data: checked},
		{name: "short", data: header(NodeTypeLeaf, id, 0, 32)[:NodeHeaderSize-1]},
		{name: "longer than page", data: header(NodeTypeLeaf, id, 0, pageSize+1)},
	}
	accepted := 0
	for _, test := range tests {
		for _, forWAL := range []bool{false, true} {
			for _, checksums := range []bool{false, true} {
				kind, count, ok := plainNodeHeader(test.data, id, pageSize, forWAL, checksums)
				if !ok {
					continue
				}
				accepted++
				h, err := readNodeHeader(test.data, id, pageSize, forWAL)
				if err != nil || kind != h.Type || count != h.EntryCount {
					t.Errorf("%s (forWAL=%t, checksums=%t): fast path accepted kind %d count %d; readNodeHeader returned %+v, %v", test.name, forWAL, checksums, kind, count, h, err)
				}
			}
		}
	}
	// Leaves and branches with a zero checksum pass in all four modes.
	// A nonzero checksum, correct or not, passes only in the mode that checks no checksum.
	if accepted != 10 {
		t.Fatalf("fast path accepted %d header and mode pairs, want 10", accepted)
	}
}

func TestNodeClone_DoesNotShareEntryOrChildLists(t *testing.T) {
	original := &MutableNode{
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
	original := &MutableNode{
		header: newNodeHeader(NodeTypeLeaf, 0),
		entries: []Entry{
			{key: []byte("alpha"), value: bytes.Repeat([]byte("a"), 128)},
			{key: []byte("beta"), value: bytes.Repeat([]byte("b"), 128)},
		},
	}

	var clone *MutableNode
	// The MutableNode holds its header, so one allocation covers both. The second one holds the entry list.
	if got := testing.AllocsPerRun(100, func() {
		clone = original.Clone()
	}); got != 2 {
		t.Fatalf("MutableNode.Clone allocations: got %v, want 2", got)
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

	node := &MutableNode{
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
	_, _ = hash.Write(encoded[nodeChecksumOffset+4:])
	if got, want := binary.LittleEndian.Uint32(encoded[nodeChecksumOffset:nodeChecksumOffset+4]), hash.Sum32(); got != want {
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
	if err := node.InsertEntry(NewEntry(0, []byte("k"), []byte{0xb2, 0x2b, 0xf3, 0x5a})); err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	if err := WriteNode(&encoded, node, pageSize, true, true); err != nil {
		t.Fatal(err)
	}
	data := encoded.Bytes()
	table := crc32.MakeTable(crc32.Castagnoli)
	raw := crc32.Update(0, table, data[:nodeChecksumOffset])
	raw = crc32.Update(raw, table, data[nodeChecksumOffset+4:])
	if raw != 0 || binary.LittleEndian.Uint32(data[nodeChecksumOffset:nodeChecksumOffset+4]) != 1 {
		t.Fatalf("raw CRC=%x, stored=%x; want zero and one", raw, data[nodeChecksumOffset:nodeChecksumOffset+4])
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
				node = NewRootNode(7, NewLeafNode(8).PageID(), NewLeafNode(9).PageID(), []byte("middle"))
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
					node = NewRootNode(7, NewLeafNode(8).PageID(), NewLeafNode(9).PageID(), key)
				} else if err := node.InsertEntry(NewEntry(0, key, value)); err != nil {
					t.Fatal(err)
				}
				ids := make([]page.ID, NodePageCount(node, pageSize))
				for i := range ids {
					ids[i] = 7 + page.ID(i)
				}
				pages, err := EncodeNodePages(node, pageSize, ids)
				if err != nil {
					t.Fatal(err)
				}
				if err := VerifyNodeIDAndSetChecksum(pages[0], ids[0], false); err != nil {
					t.Fatal(err)
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
	binary.LittleEndian.PutUint32(data[nodeChecksumOffset:nodeChecksumOffset+4], checksum)
}

func TestDecodeNode_AllocatesOnlyNodeHeaderAndEntryList(t *testing.T) {
	node := &MutableNode{
		header: newNodeHeader(NodeTypeLeaf, 0),
		entries: []Entry{
			{key: []byte("alpha"), value: bytes.Repeat([]byte("a"), 128)},
			{key: []byte("beta"), value: bytes.Repeat([]byte("b"), 128)},
		},
	}
	encoded := EncodeNode(node, testNodePageSize)

	var decoded *MutableNode
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
	node := &MutableNode{
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
	nodes := []*MutableNode{
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
	check := func(node *MutableNode) {
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
	if err := leaf.DeleteKey([]byte("m")); err != nil {
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
	parent := NewRootNode(10, leaf.PageID(), right.PageID(), separator)
	check(parent)
	parent.InsertSplitChild(1, NewLeafNode(11).PageID(), []byte("zz"))
	check(parent)
	parent.setSeparator(0, []byte("middle"))
	check(parent)
	parent.InsertSplitChild(2, NewLeafNode(12).PageID(), []byte("zzz"))
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
	parent = NewRootNode(22, left.PageID(), right.PageID(), []byte("m"))
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

	left = NewRootNode(30, NewLeafNode(40).PageID(), NewLeafNode(41).PageID(), []byte("a"))
	right = NewRootNode(31, NewLeafNode(42).PageID(), NewLeafNode(43).PageID(), []byte("n-longer"))
	right.InsertSplitChild(1, NewLeafNode(44).PageID(), []byte("u-long"))
	right.InsertSplitChild(2, NewLeafNode(45).PageID(), []byte("z"))
	parent = NewRootNode(32, left.PageID(), right.PageID(), []byte("m"))
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
	node := &MutableNode{
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

// TestNodeLayout_StaysInSizeClass guards the MutableNode size. Each clone allocates one MutableNode, which holds its header.
// A new field can move MutableNode into a larger allocation size class and make every clone larger.
func TestNodeLayout_StaysInSizeClass(t *testing.T) {
	if size := unsafe.Sizeof(MutableNode{}); size > 112 {
		t.Fatalf("MutableNode size: got %d bytes, want at most 112", size)
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

// BenchmarkDecodeNodeSpan measures decoding from raw consecutive pages.
func BenchmarkDecodeNodeSpan(b *testing.B) {
	const pageSize int64 = 4096
	for _, size := range []int{64 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			node := NewLeafNode(7)
			if err := node.InsertEntry(NewEntry(0, []byte("key"), bytes.Repeat([]byte("v"), size))); err != nil {
				b.Fatal(err)
			}
			ids := make([]page.ID, NodePageCount(node, pageSize))
			for i := range ids {
				ids[i] = 7 + page.ID(i)
			}
			pages, err := EncodeNodePages(node, pageSize, ids)
			if err != nil {
				b.Fatal(err)
			}
			read := func(id page.ID) ([]byte, error) { return pages[int(id-7)], nil }
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := DecodeNode(pages[0], 7, pageSize, read, false); err != nil {
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
	binary.LittleEndian.PutUint32(data[18:22], uint32(len(data)-NodeHeaderSize))
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
	binary.LittleEndian.PutUint32(data[18:22], uint32(len(data)-NodeHeaderSize))
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
	root := &MutableNode{
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
	branch := NewRootNode(2, left.PageID(), right.PageID(), []byte("middle"))
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
	if got := binary.LittleEndian.Uint32(encoded[nodeChecksumOffset : nodeChecksumOffset+4]); got != 0 {
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
	root := &MutableNode{
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

func TestNodeClone_KeepsStoragePagesPrivate(t *testing.T) {
	base := NewLeafNode(8)
	base.SetStoragePageCount(3)
	clone := base.Clone()
	ids := clone.OverflowPages()
	ids[0] = 99
	if !slices.Equal(base.OverflowPages(), []page.ID{9, 10}) {
		t.Fatal("derived page IDs changed the original node")
	}
	clone.SetStoragePageCount(1)
	if clone.StoragePageCount() != 1 || base.StoragePageCount() != 3 {
		t.Fatal("private storage change modified the original node")
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

func TestNodePageCount_RequiresOnlyTheNodeHeader(t *testing.T) {
	node := NewLeafNode(8)
	if err := node.InsertEntry(NewEntry(0, []byte("key"), bytes.Repeat([]byte("v"), 80))); err != nil {
		t.Fatal(err)
	}
	for _, size := range []int64{0, NodeHeaderSize - 1} {
		if got := NodePageCount(node, size); got != 0 {
			t.Fatalf("page size %d accepted: %d pages", size, got)
		}
	}
	for _, size := range []int64{NodeHeaderSize, NodeHeaderSize + 1, 32, 64} {
		count := (node.EncodedSize() + int(size) - 1) / int(size)
		if got := NodePageCount(node, size); got != count {
			t.Fatalf("page count: got %d, want %d", got, count)
		}
		ids := make([]page.ID, count)
		for i := range ids {
			ids[i] = 8 + page.ID(i)
		}
		if _, err := EncodeNodePages(node, size, ids); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNodePages_FileRoundTrip(t *testing.T) {
	branch := NewRootNode(8, NewLeafNode(20).PageID(), NewLeafNode(21).PageID(), []byte("a"))
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
		{"below two-page capacity", 86, 2, false, 64},
		{"exact two-page capacity", 87, 2, false, 64},
		{"above two-page capacity", 88, 3, false, 64},
		{"leaf with many pages", 2048, 33, false, 64},
		{"branch with many pages", 0, 5, true, 64},
		{"header-sized pages", 87, 6, false, 22},
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
				ids[i] = ids[0] + page.ID(i)
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
				if i == 0 && page.ID(binary.LittleEndian.Uint64(data[2:10])) != ids[i] {
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

func TestNodePages_CompleteMappedLookupNeedsNoPageReads(t *testing.T) {
	leaf := NewLeafNode(8)
	if err := leaf.InsertEntry(NewEntry(0, []byte("key"), bytes.Repeat([]byte("v"), 2048))); err != nil {
		t.Fatal(err)
	}
	branch := NewRootNode(8, NewLeafNode(20).PageID(), NewLeafNode(21).PageID(), bytes.Repeat([]byte("k"), MaxKeySize))
	for _, testCase := range []struct {
		node   *MutableNode
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
			ids[i] = ids[0] + page.ID(i)
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
		input := bytes.Join(pages, nil)
		var found bool
		var child page.ID
		query := []byte("z")
		allocations := testing.AllocsPerRun(100, func() {
			if forWAL {
				_, found, child, err = LookupEncodedWALNode(input, 8, query, size, read, true)
			} else {
				_, found, child, err = LookupMappedNode(input, 8, query, read, true, true)
			}
		})
		if err != nil || found || (node.IsLeaf() && child != 0) || (!node.IsLeaf() && child != 21) {
			t.Fatalf("mapped lookup: found=%v child=%d err=%v", found, child, err)
		}
		if allocations != 0 {
			t.Fatalf("mapped lookup allocated %v times", allocations)
		}
		if reads != 0 {
			t.Fatalf("mapped lookup read unneeded pages: %d", reads)
		}
	}
}

func TestNodePages_RejectsMissingOrDamagedTail(t *testing.T) {
	node := NewLeafNode(8)
	if err := node.InsertEntry(NewEntry(0, []byte("key"), bytes.Repeat([]byte("v"), 80))); err != nil {
		t.Fatal(err)
	}
	ids := []page.ID{8, 9}
	pages, err := EncodeNodePages(node, 64, ids)
	if err != nil {
		t.Fatal(err)
	}
	read := func(id page.ID) ([]byte, error) {
		if id != 9 {
			return nil, ErrInvalid
		}
		return pages[1], nil
	}
	if _, err := DecodeNode(pages[0], 8, 64, nil, true); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing reader: %v", err)
	}
	short := func(page.ID) ([]byte, error) { return pages[1][:63], nil }
	if _, err := DecodeNode(pages[0], 8, 64, short, true); !errors.Is(err, ErrInvalid) {
		t.Fatalf("short tail: %v", err)
	}
	failure := errors.New("tail read failed")
	failed := func(page.ID) ([]byte, error) { return nil, failure }
	if _, err := DecodeNode(pages[0], 8, 64, failed, true); !errors.Is(err, failure) {
		t.Fatalf("read failure: %v", err)
	}
	pages[1][63] ^= 1
	if _, err := DecodeNode(pages[0], 8, 64, read, true); !errors.Is(err, page.ErrChecksum) {
		t.Fatalf("damaged tail: %v", err)
	}
	if _, _, _, err := LookupMappedNode(pages[0], 8, []byte("key"), read, true, true); !errors.Is(err, page.ErrChecksum) {
		t.Fatalf("lookup damaged tail: %v", err)
	}
}

func TestNodePages_SinglePageAndShrink(t *testing.T) {
	node := NewLeafNode(8)
	if err := node.InsertEntry(NewEntry(0, []byte("k"), bytes.Repeat([]byte("v"), 80))); err != nil {
		t.Fatal(err)
	}
	ids := []page.ID{8, 9}
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
	if node.header.Type != NodeTypeLeaf || !node.IsLeaf() {
		t.Fatal("continued leaf lost its type")
	}
	if err := node.InsertEntry(NewEntry(0, []byte("k"), bytes.Repeat([]byte("v"), 22))); err != nil {
		t.Fatal(err)
	}
	node.SetStoragePageCount(1)
	if node.header.Type != NodeTypeLeaf || node.StoragePageCount() != 1 {
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
				for i := range 12 {
					key := append(fmt.Appendf(nil, "%06d", i), bytes.Repeat([]byte{'k'}, i%3*35+1)...)
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
					ids[i] = ids[0] + page.ID(i)
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
				var decoded *MutableNode
				if forWAL {
					decoded, err = DecodeWALNode(bytes.Join(pages, nil), node.PageID(), pageSize, read)
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
						got, gotFound, gotChild, err = LookupEncodedWALNode(bytes.Join(pages, nil), node.PageID(), query, pageSize, read, true)
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

// TestNodePages_RejectsShortHead checks the header before any tail page is read.
func TestNodePages_RejectsShortHead(t *testing.T) {
	for _, kind := range []NodeType{NodeTypeLeaf, NodeTypeBranch} {
		data := make([]byte, NodeHeaderSize)
		binary.LittleEndian.PutUint16(data[:2], uint16(kind))
		binary.LittleEndian.PutUint64(data[2:10], 8)
		data = data[:NodeHeaderSize-1]
		read := func(page.ID) ([]byte, error) { t.Fatal("short header read a tail page"); return nil, ErrInvalid }
		if _, err := DecodeNode(data, 8, 64, read, true); !errors.Is(err, ErrInvalid) {
			t.Fatalf("decode short header: %v", err)
		}
		if _, _, _, err := LookupMappedNode(data, 8, []byte("key"), read, true, true); !errors.Is(err, ErrInvalid) {
			t.Fatalf("lookup short header: %v", err)
		}
	}
}

// TestVisitNodeSpanReferences checks child and storage ownership at page boundaries.
func TestVisitNodeSpanReferences(t *testing.T) {
	for _, pageSize := range []int64{35, 64, 128, 4096} {
		for _, forWAL := range []bool{false, true} {
			for _, branch := range []bool{false, true} {
				t.Run(fmt.Sprintf("size=%d/WAL=%t/branch=%t", pageSize, forWAL, branch), func(t *testing.T) {
					node := NewLeafNode(7)
					expected := []page.ID{9}
					if branch {
						node = NewRootNode(7, NewLeafNode(3).PageID(), NewLeafNode(4).PageID(), bytes.Repeat([]byte("a"), int(pageSize)+7))
						node.InsertSplitChild(1, NewLeafNode(5).PageID(), bytes.Repeat([]byte("z"), int(pageSize)+7))
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
						ids[i] = ids[0] + page.ID(i)
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
							return VisitWALNodeReferences(bytes.Join(images, nil), 7, pageSize, read, visit, own)
						}
						return VisitMappedNodeReferences(images[0], 7, pageSize, read, visit, own, true)
					}
					if err := check(); err != nil {
						t.Fatal(err)
					}
					if !slices.Equal(children, expected) || !slices.Equal(continuations, ids[1:]) {
						t.Fatalf("children=%v, continuation IDs=%v", children, continuations)
					}
					// A main-file reader needs every tail page. A compact WAL image needs its last byte.
					last := ids[len(ids)-1]
					saved := pages[last]
					if forWAL {
						tail := images[len(images)-1]
						images[len(images)-1] = tail[:len(tail)-1]
						if err := check(); err == nil {
							t.Fatal("visitor accepted a truncated WAL image")
						}
						images[len(images)-1] = tail
					} else {
						delete(pages, last)
						if err := check(); err == nil {
							t.Fatal("visitor accepted a missing tail page")
						}
						pages[last] = saved
					}
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
						err = VisitWALNodeReferences(bytes.Join(images, nil), 7, pageSize, read, visit, func(page.ID) error { return stop })
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
			base.SetStoragePageCount(3)
			base.header.Checksum = 42
			base.CacheEncodedSize()
			before := EncodeWALNode(base)
			want := base.Clone()
			for _, indexe := range slices.Backward(indexes) {
				want.entries = slices.Delete(want.entries, indexe, indexe+1)
			}
			private := base.Clone()
			private.DeleteLeafEntries(indexes[0], indexes[1:])
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
			private.SetStoragePageCount(9)
			private.SetPageID(100)
			if private.EntryCount() != 0 {
				private.entries[0] = NewEntry(0, []byte("changed"), nil)
			}
			if !bytes.Equal(EncodeWALNode(base), before) || base.StoragePageCount() != 3 || base.header.Checksum != 42 {
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
	private := base.Clone()
	private.DeleteLeafEntries(0, []int{1, 2})
	if private.EntryCount() != 2 || cap(private.entries) < base.EntryCount() || private.EncodedSize() != 58 {
		t.Fatal("underfull leaf did not keep merge capacity and its exact size")
	}
	right := NewLeafNode(5)
	for _, key := range []string{"f", "g"} {
		if err := right.InsertEntry(NewEntry(0, []byte(key), []byte(key))); err != nil {
			t.Fatal(err)
		}
	}
	parent := NewRootNode(4, private.PageID(), right.PageID(), []byte("f"))
	storage := &private.entries[0]
	mergeChildren(parent, 0, private, right)
	if &private.entries[0] != storage || private.EntryCount() != 4 || private.EncodedSize() != 94 {
		t.Fatal("leaf merge replaced reserved storage or kept a stale size")
	}
	if base.EntryCount() != 5 || string(base.EntryAt(0).Key()) != "a" {
		t.Fatal("reserved merge changed the committed leaf")
	}
	empty := base.Clone()
	empty.DeleteLeafEntries(0, []int{1, 2, 3, 4})
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
			store := &memoryTreeStore{pageSize: 4096, nodes: make(map[page.ID]*MutableNode), dirty: make(map[page.ID]*MutableNode)}
			tree := newTestTree(store)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				store.nodes[3] = base
				clear(store.dirty)
				var root *MutableNode
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

// TestNodeCodecRejectsWrongTypeBeforeContinuationPages checks that the page type selects the body layout.
// An unknown type, or a body read with the layout of the other node type, fails before any continuation page is read.
func TestNodeCodecRejectsWrongTypeBeforeContinuationPages(t *testing.T) {
	read := func(id page.ID) ([]byte, error) {
		t.Fatal("wrong type followed continuation IDs")
		return nil, ErrInvalid
	}
	leaf := NewLeafNode(8)
	if err := leaf.InsertEntry(NewEntry(0, []byte("k"), []byte("v"))); err != nil {
		t.Fatal(err)
	}
	branch := NewRootNode(8, NewLeafNode(20).PageID(), NewLeafNode(21).PageID(), []byte("k"))
	for name, test := range map[string]struct {
		node *MutableNode
		tag  NodeType
	}{
		"unknown type":           {leaf, 7},
		"unknown continued type": {leaf, NodeType(0x8007)},
		"continuation page":      {leaf, NodeType(3)},
		"leaf read as branch":    {leaf, NodeTypeBranch},
		"branch read as leaf":    {branch, NodeTypeLeaf},
	} {
		t.Run(name, func(t *testing.T) {
			data := EncodeWALNode(test.node)
			binary.LittleEndian.PutUint16(data[:2], uint16(test.tag))
			if _, err := DecodeNode(data, 8, 64, read, false); !errors.Is(err, ErrInvalid) {
				t.Fatalf("decode: %v", err)
			}
			if _, err := DecodeWALNode(data, 8, 64, read); !errors.Is(err, ErrInvalid) {
				t.Fatalf("WAL decode: %v", err)
			}
			if _, _, _, err := LookupMappedNode(data, 8, []byte("k"), read, true, false); !errors.Is(err, ErrInvalid) {
				t.Fatalf("mapped lookup: %v", err)
			}
			if _, _, _, err := LookupEncodedWALNode(data, 8, []byte("k"), 64, read, true); !errors.Is(err, ErrInvalid) {
				t.Fatalf("WAL lookup: %v", err)
			}
			visit := func(page.ID) error { return nil }
			if err := VisitMappedNodeReferences(data, 8, 64, read, visit, visit, false); !errors.Is(err, ErrInvalid) {
				t.Fatalf("mapped reference check: %v", err)
			}
			if err := VisitWALNodeReferences(data, 8, 64, read, visit, visit); !errors.Is(err, ErrInvalid) {
				t.Fatalf("WAL reference check: %v", err)
			}
			if err := ValidateWALNode(data, 8); !errors.Is(err, ErrInvalid) {
				t.Fatalf("WAL validation: %v", err)
			}
		})
	}
}

func TestNodeSpanWriterLayout(t *testing.T) {
	for _, pageSize := range []int64{64, 4096, 16384} {
		for _, size := range []int{1, 513, 32768} {
			for _, branch := range []bool{false, true} {
				t.Run(fmt.Sprintf("page=%d/size=%d/branch=%t", pageSize, size, branch), func(t *testing.T) {
					node := NewLeafNode(8)
					var body bytes.Buffer
					if branch {
						key := bytes.Repeat([]byte("k"), size)
						node = NewRootNode(8, NewLeafNode(20).PageID(), NewLeafNode(21).PageID(), key)
						for _, value := range []uint32{22 + 8 + 16, uint32(len(key))} {
							if err := binary.Write(&body, binary.LittleEndian, value); err != nil {
								t.Fatal(err)
							}
						}
						for _, id := range []uint64{20, 21} {
							if err := binary.Write(&body, binary.LittleEndian, id); err != nil {
								t.Fatal(err)
							}
						}
						body.Write(key)
					} else {
						value := bytes.Repeat([]byte("v"), size)
						if err := node.InsertEntry(NewEntry(0, []byte("k"), value)); err != nil {
							t.Fatal(err)
						}
						for _, field := range []uint32{0, 22 + 16, 1, uint32(len(value))} {
							if err := binary.Write(&body, binary.LittleEndian, field); err != nil {
								t.Fatal(err)
							}
						}
						body.WriteByte('k')
						body.Write(value)
					}
					logicalSize := 22 + body.Len()
					count := (logicalSize + int(pageSize) - 1) / int(pageSize)
					if got := NodePageCount(node, pageSize); got != count {
						t.Fatalf("page count: got %d, want %d", got, count)
					}
					ids := make([]page.ID, count)
					for i := range ids {
						ids[i] = node.PageID() + page.ID(i)
					}
					node.SetStoragePageCount(count + 3)
					original := node.StoragePageCount()
					pages, err := EncodeNodePages(node, pageSize, ids)
					if err != nil {
						t.Fatal(err)
					}
					image := bytes.Join(pages, nil)
					if len(image) != count*int(pageSize) || !bytes.Equal(image[22:logicalSize], body.Bytes()) {
						t.Fatal("span does not contain one header followed by the complete raw body")
					}
					for _, value := range image[logicalSize:] {
						if value != 0 {
							t.Fatal("nonzero final padding")
						}
					}
					kind := node.kind()
					if binary.LittleEndian.Uint16(image[:2]) != uint16(kind) ||
						binary.LittleEndian.Uint64(image[2:10]) != uint64(node.PageID()) ||
						binary.LittleEndian.Uint32(image[10:14]) != 1 ||
						binary.LittleEndian.Uint32(image[18:22]) != uint32(body.Len()) {
						t.Fatal("span header fields differ from the format")
					}
					table := crc32.MakeTable(crc32.Castagnoli)
					sum := crc32.Update(0, table, image[:14])
					sum = crc32.Update(sum, table, image[18:])
					if sum == 0 {
						sum = 1
					}
					if binary.LittleEndian.Uint32(image[14:18]) != sum {
						t.Fatal("checksum does not cover the complete padded span")
					}
					for _, pad := range []bool{false, true} {
						var output bytes.Buffer
						if err := WriteNode(&output, node, pageSize, pad, true); err != nil {
							t.Fatal(err)
						}
						want := image[:logicalSize]
						if pad {
							want = image
						}
						if !bytes.Equal(output.Bytes(), want) {
							t.Fatal("WriteNode differs from the span encoder")
						}
					}
					walPages, err := EncodeWALNodePages(node, pageSize, ids)
					if err != nil {
						t.Fatal(err)
					}
					walImage := bytes.Join(walPages, nil)
					clear(image[14:18])
					if !bytes.Equal(walImage, image[:logicalSize]) || WALNodePagesEncodedSize(node, pageSize) != logicalSize {
						t.Fatal("WAL image contains padding, tail headers, or page ID lists")
					}
					if node.StoragePageCount() != original {
						t.Fatal("encoding changed the node's allocated page count")
					}
				})
			}
		}
	}
}

func TestNodeSpanStorageCountStaysPrivate(t *testing.T) {
	node := NewLeafNode(8)
	if err := node.InsertEntry(NewEntry(0, []byte("k"), []byte("value"))); err != nil {
		t.Fatal(err)
	}
	before := EncodeWALNode(node)
	node.SetStoragePageCount(7)
	if !bytes.Equal(EncodeWALNode(node), before) {
		t.Fatal("allocated page count changed the serialized node")
	}
	if node.Clone().StoragePageCount() != 7 {
		t.Fatal("a private clone lost the old allocated page count")
	}
}

func TestNodeSpanWriterRejectsNonconsecutivePages(t *testing.T) {
	node := NewLeafNode(8)
	if err := node.InsertEntry(NewEntry(0, []byte("k"), bytes.Repeat([]byte("v"), 160))); err != nil {
		t.Fatal(err)
	}
	ids := make([]page.ID, NodePageCount(node, 64))
	for i := range ids {
		ids[i] = node.PageID() + page.ID(i)
	}
	ids[len(ids)-1]++
	if _, err := EncodeNodePages(node, 64, ids); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nonconsecutive pages: %v", err)
	}
}

func TestNodeSpanReaderRoundTrip(t *testing.T) {
	for _, pageSize := range []int64{32, 64, 4096, 16384} {
		for _, valueSize := range []int{0, 1, 21, 22, 23, 25, 26, 27, 4095, 4096, 65536} {
			t.Run(fmt.Sprintf("page=%d/value=%d", pageSize, valueSize), func(t *testing.T) {
				node := NewLeafNode(8)
				value := bytes.Repeat([]byte("v"), valueSize)
				if len(value) != 0 {
					value[len(value)-1] = 0
				}
				if err := node.InsertEntry(NewEntry(0, []byte("k"), value)); err != nil {
					t.Fatal(err)
				}
				count := NodePageCount(node, pageSize)
				ids := make([]page.ID, count)
				for i := range ids {
					ids[i] = 8 + page.ID(i)
				}
				pages, err := EncodeNodePages(node, pageSize, ids)
				if err != nil {
					t.Fatal(err)
				}
				image := bytes.Join(pages, nil)
				read := func(id page.ID) ([]byte, error) {
					if id < 8 || id >= 8+page.ID(count) {
						return nil, ErrInvalid
					}
					return pages[int(id-8)], nil
				}
				for _, data := range [][]byte{image, pages[0]} {
					decoded, err := DecodeNode(data, 8, pageSize, read, true)
					if err != nil || decoded.StoragePageCount() != count {
						t.Fatalf("derived allocation count: node=%v err=%v want=%d", decoded, err, count)
					}
					entry, found, err := decoded.FindEntryRef([]byte("k"))
					if err != nil || !found || !bytes.Equal(entry.Value(), value) {
						t.Fatalf("decoded value: %v", err)
					}
					for _, checksums := range []bool{false, true} {
						entry, found, _, err = LookupMappedNode(data, 8, []byte("k"), read, true, checksums)
						if err != nil || !found || !bytes.Equal(entry.Value(), value) {
							t.Fatalf("mapped value: %v", err)
						}
					}
					if err := decoded.InsertOrReplaceLeafEntry(NewEntry(0, []byte("k"), []byte("x"))); err != nil {
						t.Fatal(err)
					}
					if decoded.StoragePageCount() != count {
						t.Fatal("changing the body lost the old allocated count")
					}
				}
				walPages, err := EncodeWALNodePages(node, pageSize, ids)
				if err != nil {
					t.Fatal(err)
				}
				wal := bytes.Join(walPages, nil)
				decoded, err := DecodeWALNode(wal, 8, pageSize, nil)
				if err != nil || decoded.StoragePageCount() != count {
					t.Fatalf("WAL count: %v", err)
				}
				if err := ValidateWALNode(wal, 8); err != nil {
					t.Fatal(err)
				}
				entry, found, _, err := LookupEncodedWALNode(wal, 8, []byte("k"), pageSize, nil, true)
				if err != nil || !found || !bytes.Equal(entry.Value(), value) {
					t.Fatalf("WAL lookup: %v", err)
				}

				binary.LittleEndian.PutUint16(wal[:2], 0x8001)
				if err := ValidateWALNode(wal, 8); err == nil {
					t.Fatal("removed flag was accepted")
				}
			})
		}
	}
}

func TestNodeSpanReaderReferencesAndScans(t *testing.T) {
	const pageSize int64 = 4096
	key := bytes.Repeat([]byte("k"), 8192)
	value := bytes.Repeat([]byte("v"), 12289)
	for _, branch := range []bool{false, true} {
		t.Run(fmt.Sprintf("branch=%t", branch), func(t *testing.T) {
			node := NewLeafNode(8)
			if branch {
				node = NewRootNode(8, NewLeafNode(1000).PageID(), NewLeafNode(2000).PageID(), key)
			} else if err := node.InsertEntry(NewEntry(0, key, value)); err != nil {
				t.Fatal(err)
			}
			count := NodePageCount(node, pageSize)
			ids := make([]page.ID, count)
			for i := range ids {
				ids[i] = node.PageID() + page.ID(i)
			}
			pages, err := EncodeNodePages(node, pageSize, ids)
			if err != nil {
				t.Fatal(err)
			}
			read := func(id page.ID) ([]byte, error) {
				if id < 8 || id >= 8+page.ID(count) {
					return nil, ErrInvalid
				}
				return pages[int(id-8)], nil
			}
			for _, input := range [][]byte{bytes.Join(pages, nil), pages[0]} {
				decoded, err := DecodeNode(input, 8, pageSize, read, true)
				if err != nil || decoded.StoragePageCount() != count {
					t.Fatalf("decode count: %v", err)
				}
				entry, found, child, err := LookupMappedNode(input, 8, key, read, true, true)
				if err != nil || branch && child != 2000 || !branch && (!found || !bytes.Equal(entry.Value(), value)) {
					t.Fatalf("lookup: found=%t child=%d err=%v", found, child, err)
				}
				var children, tails []page.ID
				err = VisitMappedNodeReferences(input, 8, pageSize, read, func(id page.ID) error { children = append(children, id); return nil }, func(id page.ID) error { tails = append(tails, id); return nil }, true)
				if err != nil || !slices.Equal(tails, ids[1:]) {
					t.Fatalf("storage references: %v, %v", tails, err)
				}
				if branch {
					if !slices.Equal(children, []page.ID{1000, 2000}) {
						t.Fatalf("children: %v", children)
					}
					continue
				}
				seen := 0
				visit := func(flags uint32, k, v []byte) error {
					if flags != 0 || !bytes.Equal(k, key) || !bytes.Equal(v, value) {
						t.Fatal("scan returned wrong bytes")
					}
					seen++
					return nil
				}
				if err := VisitMappedLeafRange(input, 8, nil, nil, visit, true, read); err != nil {
					t.Fatal(err)
				}
				if err := VisitMappedLeafReverse(input, 8, visit, true, read); err != nil {
					t.Fatal(err)
				}
				if seen != 2 {
					t.Fatalf("scan callbacks: got %d, want 2", seen)
				}
			}
		})
	}
}

func TestNodeSpanReaderRejectsInvalidHeader(t *testing.T) {
	plain := NewLeafNode(8)
	if err := plain.InsertEntry(NewEntry(0, []byte("k"), []byte("v"))); err != nil {
		t.Fatal(err)
	}
	paddedWAL := append(EncodeWALNode(plain), 0)
	if _, _, _, err := LookupEncodedWALNode(paddedWAL, 8, []byte("k"), 4096, nil, true); !errors.Is(err, ErrInvalid) {
		t.Fatalf("compact WAL lookup accepted extra bytes: %v", err)
	}
	header := make([]byte, NodeHeaderSize)
	binary.LittleEndian.PutUint16(header[:2], uint16(NodeTypeLeaf))
	binary.LittleEndian.PutUint64(header[2:10], 8)
	binary.LittleEndian.PutUint32(header[18:22], 8192)
	for _, test := range []struct {
		name   string
		change func([]byte) ([]byte, page.ID)
	}{
		{"short header", func(data []byte) ([]byte, page.ID) { return data[:NodeHeaderSize-1], 8 }},
		{"old overflow flag", func(data []byte) ([]byte, page.ID) { binary.LittleEndian.PutUint16(data[:2], 0x8001); return data, 8 }},
		{"old continuation type", func(data []byte) ([]byte, page.ID) { binary.LittleEndian.PutUint16(data[:2], 3); return data, 8 }},
		{"wrong page ID", func(data []byte) ([]byte, page.ID) { return data, 9 }},
		{"excess body length", func(data []byte) ([]byte, page.ID) {
			binary.LittleEndian.PutUint32(data[18:22], ^uint32(0))
			return data, 8
		}},
		{"page address overflow", func(data []byte) ([]byte, page.ID) {
			id := ^page.ID(0)
			binary.LittleEndian.PutUint64(data[2:10], uint64(id))
			return data, id
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			data, id := test.change(slices.Clone(header))
			if _, err := NodeSpan(data, id, 4096); !errors.Is(err, ErrInvalid) {
				t.Fatalf("header accepted: %v", err)
			}
		})
	}
	node := NewLeafNode(8)
	if err := node.InsertEntry(NewEntry(0, []byte("k"), bytes.Repeat([]byte("v"), 8192))); err != nil {
		t.Fatal(err)
	}
	ids := make([]page.ID, NodePageCount(node, 4096))
	for i := range ids {
		ids[i] = 8 + page.ID(i)
	}
	pages, err := EncodeNodePages(node, 4096, ids)
	if err != nil {
		t.Fatal(err)
	}
	pages[len(pages)-1][4095] ^= 1
	if _, err := DecodeNode(bytes.Join(pages, nil), 8, 4096, nil, true); !errors.Is(err, page.ErrChecksum) {
		t.Fatalf("damaged final padding accepted: %v", err)
	}
}

func TestEncodeNodeInto_ReplacesPoisonedSpan(t *testing.T) {
	const pageSize int64 = 4096
	for _, test := range []struct {
		name string
		node *MutableNode
	}{
		{"empty leaf", NewLeafNode(50)},
		{"small leaf", &MutableNode{header: newNodeHeader(NodeTypeLeaf, 50), entries: []Entry{{key: []byte("key"), value: []byte("value")}}}},
		{"large leaf", &MutableNode{header: newNodeHeader(NodeTypeLeaf, 50), entries: []Entry{{key: []byte("key"), value: bytes.Repeat([]byte{0x3c}, 65536)}}}},
		{"small branch", &MutableNode{header: newNodeHeader(NodeTypeBranch, 50), entries: []Entry{{key: []byte("middle")}}, Children: []page.ID{80, 90}}},
		{"large branch", &MutableNode{header: newNodeHeader(NodeTypeBranch, 50), entries: []Entry{{key: bytes.Repeat([]byte("m"), MaxKeySize)}}, Children: []page.ID{80, 90}}},
	} {
		for _, checksums := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/checksums=%t", test.name, checksums), func(t *testing.T) {
				node := test.node.Clone()
				oldHeader := node.header
				var expected bytes.Buffer
				if err := WriteNode(&expected, node, pageSize, true, checksums); err != nil {
					t.Fatal(err)
				}
				node.header = oldHeader
				storage := bytes.Repeat([]byte{0xa5}, expected.Len()+22)
				dst := storage[11 : len(storage)-11]
				if err := EncodeNodeInto(dst, node, pageSize, checksums); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(dst, expected.Bytes()) {
					t.Fatal("supplied span differs from the regular writer")
				}
				if !bytes.Equal(storage[:11], bytes.Repeat([]byte{0xa5}, 11)) || !bytes.Equal(storage[len(storage)-11:], bytes.Repeat([]byte{0xa5}, 11)) {
					t.Fatal("encoder changed bytes outside its destination")
				}
				if node.header != oldHeader {
					t.Fatal("encoder changed the node header")
				}
				if _, err := DecodeNode(dst, node.PageID(), pageSize, nil, checksums); err != nil {
					t.Fatal(err)
				}
				if checksums {
					dst[len(dst)-1] ^= 1
					if err := CheckNodeChecksum(dst, node.PageID()); !errors.Is(err, page.ErrChecksum) {
						t.Fatalf("padding corruption: %v", err)
					}
				}
			})
		}
	}
}

func TestEncodeNodeInto_RejectsInvalidSpan(t *testing.T) {
	node := NewLeafNode(50)
	for _, test := range []struct {
		name     string
		node     *MutableNode
		size     int
		pageSize int64
	}{
		{"nil node", nil, 4096, 4096},
		{"short page", node, NodeHeaderSize - 1, NodeHeaderSize - 1},
		{"empty destination", node, 0, 4096},
		{"short span", node, 4095, 4096},
		{"long span", node, 4097, 4096},
		{"extra page", node, 8192, 4096},
		{"metadata head", NewLeafNode(page.Meta0ID), 4096, 4096},
		{"overflowing span", &MutableNode{header: newNodeHeader(NodeTypeLeaf, ^page.ID(0)), entries: []Entry{{key: []byte("k"), value: make([]byte, 4096)}}}, 8192, 4096},
	} {
		t.Run(test.name, func(t *testing.T) {
			dst := bytes.Repeat([]byte{0xa5}, test.size)
			if err := EncodeNodeInto(dst, test.node, test.pageSize, false); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid destination: %v", err)
			}
			if !bytes.Equal(dst, bytes.Repeat([]byte{0xa5}, test.size)) {
				t.Fatal("invalid span changed before rejection")
			}
		})
	}
}

func TestNodeBorrowedValueInsertion(t *testing.T) {
	node := NewLeafNode(2)
	key, value := []byte("key"), bytes.Repeat([]byte("a"), 8192)
	if err := node.InsertEntry(NewEntryBorrowingValue(key, value)); err != nil {
		t.Fatal(err)
	}
	if !node.HasBorrowedValues() || &node.EntryAt(0).Value()[0] != &value[0] {
		t.Fatal("borrowed insertion copied the value")
	}
	key[0] = 'z'
	if string(node.EntryAt(0).Key()) != "key" {
		t.Fatal("borrowed insertion retained the key")
	}
	if err := node.InsertEntry(NewEntry(0, []byte("key"), value)); err != nil {
		t.Fatal(err)
	}
	if node.HasBorrowedValues() || &node.EntryAt(0).Value()[0] == &value[0] {
		t.Fatal("ordinary replacement retained borrowed storage")
	}
	if err := node.InsertEntry(NewEntryBorrowingValue([]byte("key"), value)); err != nil {
		t.Fatal(err)
	}
	if !node.HasBorrowedValues() || &node.EntryAt(0).Value()[0] != &value[0] {
		t.Fatal("borrowed replacement copied the value")
	}
}

func TestNodeInsertEntryMaintainsKnownEncodedSize(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached=%t", cached), func(t *testing.T) {
			node := NewLeafNode(7)
			for _, value := range []string{"a", "b", "c"} {
				if err := node.InsertEntry(NewEntry(0, []byte(value), []byte("initial"))); err != nil {
					t.Fatal(err)
				}
			}
			if cached {
				node.CacheEncodedSize()
			}
			for _, update := range []struct {
				key   string
				value []byte
			}{
				{key: "b", value: bytes.Repeat([]byte("g"), 128)},
				{key: "b", value: []byte("short")},
				{key: "b", value: nil},
				{key: "d", value: bytes.Repeat([]byte("d"), 256)},
				{key: "aa", value: []byte("middle")},
			} {
				if err := node.InsertEntry(NewEntry(0, []byte(update.key), update.value)); err != nil {
					t.Fatal(err)
				}
				want := node.computeEncodedSize()
				if got := node.EncodedSize(); got != want {
					t.Fatalf("encoded size after %q: got %d, want %d", update.key, got, want)
				}
				if cached && node.encodedSize != want {
					t.Fatalf("saved size after %q: got %d, want %d", update.key, node.encodedSize, want)
				}
				if !cached && node.encodedSize != 0 {
					t.Fatalf("unknown size was changed to %d", node.encodedSize)
				}
			}
			// Clone and split clear the cache; their next put must still size correctly.
			cloned := node.Clone()
			if err := cloned.InsertEntry(NewEntry(0, []byte("z"), []byte("clone"))); err != nil {
				t.Fatal(err)
			}
			if got, want := cloned.EncodedSize(), cloned.computeEncodedSize(); got != want {
				t.Fatalf("clone size: got %d, want %d", got, want)
			}
			node.CacheEncodedSize()
			right, _, err := node.Split(8, 128)
			if err != nil {
				t.Fatal(err)
			}
			for _, half := range []*MutableNode{node, right} {
				half.CacheEncodedSize()
				entry := half.EntryAt(0)
				if err := half.InsertEntry(NewEntry(entry.Flags(), entry.Key(), []byte("after split"))); err != nil {
					t.Fatal(err)
				}
				if got, want := half.EncodedSize(), half.computeEncodedSize(); got != want || half.encodedSize != want {
					t.Fatalf("split size: got %d, saved %d, want %d", got, half.encodedSize, want)
				}
			}
		})
	}
}

func TestNodeImageEditOwnsOnlySurvivingBytes(t *testing.T) {
	const pageSize = 128
	original := NewLeafNode(20)
	for _, entry := range []Entry{
		NewEntry(0, []byte("a"), bytes.Repeat([]byte("a"), 512)),
		NewEntry(0, []byte("b"), bytes.Repeat([]byte("b"), 512)),
		NewEntry(0, []byte("c"), nil),
	} {
		if err := original.InsertEntry(entry); err != nil {
			t.Fatal(err)
		}
	}
	data := make([]byte, NodePageCount(original, pageSize)*pageSize)
	if err := EncodeNodeInto(data, original, pageSize, false); err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(data)
	view, err := ReadNodeImage(data, 20, pageSize, false, false)
	if err != nil {
		t.Fatal(err)
	}
	private, err := view.Clone(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	if err := private.InsertEntry(NewEntry(0, []byte("a"), []byte("replacement"))); err != nil {
		t.Fatal(err)
	}
	private.DeleteLeafEntries(2, nil)
	private.OwnImageBytes()
	if !bytes.Equal(data, before) {
		t.Fatal("private edit changed original storage")
	}
	// Once surviving entries are owned, the encoded source can be unmapped or overwritten.
	clear(data)
	if string(private.EntryAt(0).Value()) != "replacement" || !bytes.Equal(private.EntryAt(1).Value(), bytes.Repeat([]byte("b"), 512)) {
		t.Fatal("edited node still borrowed an encoded image")
	}
	if private.EntryAt(0).borrowedImage || private.EntryAt(1).borrowedImage || private.EntryAt(0).borrowedKey || private.EntryAt(1).borrowedKey {
		t.Fatal("surviving entries retain image ownership flags")
	}
}

func TestNodeEntryRejectsOrderAndPackingErrors(t *testing.T) {
	const pageSize = 256
	original := NewLeafNode(20)
	for _, key := range []string{"a", "b"} {
		if err := original.InsertEntry(NewEntry(0, []byte(key), nil)); err != nil {
			t.Fatal(err)
		}
	}
	data := make([]byte, pageSize)
	if err := EncodeNodeInto(data, original, pageSize, false); err != nil {
		t.Fatal(err)
	}
	_, start, _, _ := leafSpan(data, 1)
	data[start] = 'a'
	view, err := ReadNodeImage(data, 20, pageSize, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := view.Entry(1); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Entry accepted unordered keys: %v", err)
	}
}

func TestNodeChildrenValidateOnlyRequiredMetadata(t *testing.T) {
	const pageSize = 256
	branch := NewRootNode(20, 21, 22, []byte("middle"))
	data := make([]byte, pageSize)
	if err := EncodeNodeInto(data, branch, pageSize, false); err != nil {
		t.Fatal(err)
	}
	// Unbounded traversal uses child references without consulting separator offsets.
	binary.LittleEndian.PutUint32(data[NodeHeaderSize:NodeHeaderSize+4], ^uint32(0))
	node, err := ReadNodeImage(data, 20, pageSize, false, false)
	if err != nil {
		t.Fatal(err)
	}
	var children []page.ID
	if err := visitNodeChildren(node, nil, nil, false, func(id page.ID) error { children = append(children, id); return nil }); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(children, []page.ID{21, 22}) {
		t.Fatalf("children: %v", children)
	}
	if err := visitNodeChildren(node, []byte("a"), nil, false, func(page.ID) error { return nil }); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bounded traversal accepted an invalid separator: %v", err)
	}
	if err := visitNodeChildren(node, nil, []byte("z"), false, func(page.ID) error { return nil }); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bounded traversal accepted an invalid end separator: %v", err)
	}
	if err := node.VisitReferences(pageSize, func(page.ID) error { return nil }, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("integrity check accepted an invalid separator: %v", err)
	}
	// Child references are required even when no separator is consulted.
	binary.LittleEndian.PutUint64(data[NodeHeaderSize+branchEntryDescriptorSize:NodeHeaderSize+branchEntryDescriptorSize+page.IDSize], 0)
	called := false
	if err := visitNodeChildren(node, nil, nil, false, func(page.ID) error { called = true; return nil }); !errors.Is(err, ErrInvalid) || called {
		t.Fatalf("invalid child reached traversal: called=%v error=%v", called, err)
	}
}

// TestReadNodeRejectsOversizedDirectory checks that both node readers reject an entry count that the image cannot hold.
// The 0x80000000 count is negative as a 32-bit int, so a later EntryCount would make a damaged node look empty there.
func TestReadNodeRejectsOversizedDirectory(t *testing.T) {
	const pageSize = 256
	for _, branch := range []bool{false, true} {
		source := NewLeafNode(20)
		if branch {
			source = NewRootNode(20, 21, 22, []byte("middle"))
		} else if err := source.InsertEntry(NewEntry(0, []byte("key"), nil)); err != nil {
			t.Fatal(err)
		}
		data := make([]byte, pageSize)
		if err := EncodeNodeInto(data, source, pageSize, false); err != nil {
			t.Fatal(err)
		}
		size := NodeHeaderSize + int(binary.LittleEndian.Uint32(data[18:22]))
		// The first count that does not fit is one more than the image can hold.
		firstTooLarge := uint32((size - NodeHeaderSize) / leafEntryDescriptorSize)
		if branch {
			firstTooLarge = uint32((size-NodeHeaderSize-page.IDSize)/(branchEntryDescriptorSize+page.IDSize)) + 1
		} else {
			firstTooLarge++
		}
		for _, count := range []uint32{firstTooLarge, 1 << 31, ^uint32(0)} {
			t.Run(fmt.Sprintf("branch=%t/count=%d", branch, count), func(t *testing.T) {
				image := slices.Clone(data)
				binary.LittleEndian.PutUint32(image[nodeEntryCountOffset:nodeChecksumOffset], count)
				if _, err := ReadNodeImage(image[:size], 20, pageSize, false, false); !errors.Is(err, ErrInvalid) {
					t.Fatalf("ReadNodeImage accepted the directory: %v", err)
				}
				if _, err := ReadCheckedNodeSpan(image, false); !errors.Is(err, ErrInvalid) {
					t.Fatalf("ReadCheckedNodeSpan accepted the directory: %v", err)
				}
			})
		}
	}
}

func visitNodeChildren(node Node, start, end []byte, reverse bool, visit func(page.ID) error) error {
	children, err := node.children(start, end, reverse)
	if err != nil {
		return err
	}
	for child, ok := children.next(); ok; child, ok = children.next() {
		if err := visit(child); err != nil {
			return err
		}
	}
	return nil
}

func TestNodeCloneKeepsDecodedSourceUnchanged(t *testing.T) {
	leaf := NewLeafNode(20)
	if err := leaf.InsertEntry(NewEntry(0, []byte("key"), []byte("original"))); err != nil {
		t.Fatal(err)
	}
	branch := &MutableNode{
		header:   newNodeHeader(NodeTypeBranch, 30),
		entries:  []Entry{{key: []byte("middle")}},
		Children: []page.ID{31, 32},
	}
	for name, source := range map[string]*MutableNode{"leaf": leaf, "branch": branch} {
		t.Run(name, func(t *testing.T) {
			before := EncodeWALNode(source)
			private, err := source.ReadOnly().Clone(256)
			if err != nil {
				t.Fatal(err)
			}
			if private == source {
				t.Fatal("read-only node returned its mutable backing object")
			}
			private.SetPageID(99)
			if private.IsLeaf() {
				if err := private.InsertEntry(NewEntry(0, []byte("key"), []byte("changed"))); err != nil {
					t.Fatal(err)
				}
				if err := private.InsertEntry(NewEntry(0, []byte("next"), nil)); err != nil {
					t.Fatal(err)
				}
			} else {
				private.Children[0] = 50
				private.setSeparator(0, []byte("next"))
			}
			if !bytes.Equal(EncodeWALNode(source), before) {
				t.Fatal("editing a private copy changed the read-only source")
			}
		})
	}
}
