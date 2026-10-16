package btree

import (
	"bytes"
	"testing"

	"github.com/Issaminu/kvlite/internal/page"
)

func TestNodeCodec_UsesFixedBranchPayloadLayout(t *testing.T) {
	node := &Node{
		header:   newNodeHeader(NodeTypeBranch, 0),
		entries:  []Entry{{key: []byte("m")}},
		Children: []page.ID{2, 3},
	}
	// Payload layout: one 8-byte descriptor, two child page IDs, then the key.
	want := []byte{
		42, 0, 0, 0,
		1, 0, 0, 0,
		2, 0, 0, 0, 0, 0, 0, 0,
		3, 0, 0, 0, 0, 0, 0, 0,
		'm',
	}

	encoded := EncodeNode(node, testNodePageSize)
	if !bytes.Equal(encoded[NodeHeaderSize:], want) {
		t.Fatalf("encode branch payload: got %x, want %x", encoded[NodeHeaderSize:], want)
	}

	decoded, err := DecodeNode(encoded, node.PageID(), testNodePageSize, nil, true)
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

func TestLookupMappedNode_SelectsBranchChild(t *testing.T) {
	root := &Node{
		header:   newNodeHeader(NodeTypeBranch, 8),
		entries:  []Entry{{key: []byte("m")}},
		Children: []page.ID{9, 10},
	}
	data := make([]byte, testNodePageSize)
	copy(data, EncodeNode(root, testNodePageSize))

	_, found, child, err := LookupMappedNode(data, root.PageID(), []byte("z"), nil, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if found || child != 10 {
		t.Fatalf("branch lookup: found=%t child=%d, want false and 10", found, child)
	}
}
