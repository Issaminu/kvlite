package btree

import (
	"bytes"
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

	rightNode, splitIndex, separator, err := node.Split(rightPgid, pageSize)
	if err != nil {
		t.Fatal(err)
	}
	if splitIndex != 1 {
		t.Fatalf("split index: got %d, want 1", splitIndex)
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
