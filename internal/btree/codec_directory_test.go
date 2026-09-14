package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	"github.com/Issaminu/kvlite/internal/page"
)

func fixedLeafImageForTest() []byte {
	const (
		entryCount      = 2
		directoryStart  = NodeHeaderSize
		descriptorSize  = 16
		firstDataOffset = directoryStart + entryCount*descriptorSize
	)
	data := make([]byte, firstDataOffset+4)
	binary.LittleEndian.PutUint16(data[0:2], nodeFormatVersion)
	binary.LittleEndian.PutUint16(data[2:4], uint16(NodeTypeLeaf))
	binary.LittleEndian.PutUint64(data[4:12], 7)
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
	binary.LittleEndian.PutUint16(data[0:2], nodeFormatVersion)
	binary.LittleEndian.PutUint16(data[2:4], uint16(NodeTypeBranch))
	binary.LittleEndian.PutUint64(data[4:12], 7)
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
				binary.LittleEndian.PutUint32(data[leafDirectoryStart+4:leafDirectoryStart+8], 51)
			},
		},
		{
			name:  "leaf payload gap",
			image: fixedLeafImageForTest,
			change: func(data []byte) {
				second := leafDirectoryStart + leafDescriptorSize
				binary.LittleEndian.PutUint32(data[second+4:second+8], 55)
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
				data[52] = 'z'
			},
		},
		{
			name:  "leaf duplicate keys",
			image: fixedLeafImageForTest,
			change: func(data []byte) {
				data[54] = 'a'
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
