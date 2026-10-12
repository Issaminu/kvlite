package btree

import (
	"bytes"
	"errors"
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

	decoded, err := DecodeNode(encoded, node.PageID(), testNodePageSize, nil)
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

	_, found, child, err := LookupMappedNode(data, root.PageID(), []byte("z"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if found || child != 10 {
		t.Fatalf("branch lookup: found=%t child=%d, want false and 10", found, child)
	}
}

// TestBranchCodecRejectsLeaf checks type ownership through the direct codec entry points.
func TestBranchCodecRejectsLeaf(t *testing.T) {
	for _, size := range []int{1, 80} {
		node := NewLeafNode(8)
		if err := node.InsertEntry(NewEntry(0, []byte("k"), bytes.Repeat([]byte("v"), size))); err != nil {
			t.Fatal(err)
		}
		ids := make([]page.ID, NodePageCount(node, 64))
		ids[0] = 8
		for i := 1; i < len(ids); i++ {
			ids[i] = page.ID(42 + i*3)
		}
		pages, err := EncodeLeafNodePages(node, 64, ids)
		if err != nil {
			t.Fatal(err)
		}
		read := func(id page.ID) ([]byte, error) {
			t.Fatal("wrong type followed continuation IDs")
			return nil, ErrInvalid
		}
		if _, err := DecodeBranchNode(pages[0], 8, 64, read); !errors.Is(err, ErrInvalid) {
			t.Fatalf("decode wrong type: %v", err)
		}
		if _, _, _, err := ReadMappedBranchNode(pages[0], 8, 64); !errors.Is(err, ErrInvalid) {
			t.Fatalf("mapped header wrong type: %v", err)
		}
		if _, _, _, err := LookupMappedBranchNode(pages[0], 8, []byte("k"), read); !errors.Is(err, ErrInvalid) {
			t.Fatalf("mapped lookup wrong type: %v", err)
		}
		if _, err := DecodeWALBranchNode(EncodeWALNode(node), 8, 4096, nil); !errors.Is(err, ErrInvalid) {
			t.Fatalf("WAL decode wrong type: %v", err)
		}
		if _, err := EncodeBranchNodePages(node, 64, ids); !errors.Is(err, ErrInvalid) {
			t.Fatalf("encode wrong type: %v", err)
		}
	}
}
