package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	"github.com/Issaminu/kvlite/internal/page"
)

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

func TestVisitMappedLeafRange(t *testing.T) {
	node := NewLeafNode(page.ID(7))
	for _, entry := range []Entry{
		NewEntry(0, []byte("acct/001"), []byte("one")),
		NewEntry(0, []byte("acct/002"), []byte("two")),
		NewEntry(0, []byte("event/001"), []byte("three")),
	} {
		if err := node.InsertEntry(entry); err != nil {
			t.Fatal(err)
		}
	}

	data := make([]byte, testNodePageSize)
	copy(data, EncodeNode(node, testNodePageSize))
	var keys [][]byte
	err := VisitMappedLeafRange(data, node.PageID(), nil, nil, func(_ uint32, key, _ []byte) error {
		keys = append(keys, bytes.Clone(key))
		return nil
	}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 3 || !bytes.Equal(keys[0], []byte("acct/001")) || !bytes.Equal(keys[1], []byte("acct/002")) || !bytes.Equal(keys[2], []byte("event/001")) {
		t.Fatalf("scan keys: got %q", keys)
	}

	keys = nil
	err = VisitMappedLeafRange(data, node.PageID(), []byte("acct/002"), []byte("event/001"), func(_ uint32, key, _ []byte) error {
		keys = append(keys, bytes.Clone(key))
		return nil
	}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || !bytes.Equal(keys[0], []byte("acct/002")) {
		t.Fatalf("range keys: got %q", keys)
	}
}

func TestVisitMappedLeafRange_RejectsInvalidInput(t *testing.T) {
	leaf := NewLeafNode(page.ID(7))
	data := EncodeNode(leaf, testNodePageSize)
	data[0] ^= 0xff
	if err := VisitMappedLeafRange(data, leaf.PageID(), nil, nil, func(uint32, []byte, []byte) error { return nil }, true, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("corrupt leaf error: got %v, want ErrInvalid", err)
	}

	left := NewLeafNode(page.ID(8))
	right := NewLeafNode(page.ID(9))
	branch := NewRootNode(page.ID(10), left, right, []byte("middle"))
	data = EncodeNode(branch, testNodePageSize)
	if err := VisitMappedLeafRange(data, branch.PageID(), nil, nil, func(uint32, []byte, []byte) error { return nil }, true, nil); !errors.Is(err, ErrNotLeafNode) {
		t.Fatalf("branch error: got %v, want ErrNotLeafNode", err)
	}
}

func TestVisitMappedLeafReverse(t *testing.T) {
	leaf := NewLeafNode(page.ID(7))
	for _, key := range []string{"a", "b", "c"} {
		if err := leaf.InsertEntry(NewEntry(0, []byte(key), []byte("value"))); err != nil {
			t.Fatal(err)
		}
	}
	data := make([]byte, testNodePageSize)
	copy(data, EncodeNode(leaf, testNodePageSize))
	var keys []string
	if err := VisitMappedLeafReverse(data, leaf.PageID(), func(_ uint32, key, _ []byte) error {
		keys = append(keys, string(key))
		return nil
	}, true, nil); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(keys, []string{"c", "b", "a"}) {
		t.Fatalf("reverse keys: got %q", keys)
	}

	bad := bytes.Clone(data)
	binary.LittleEndian.PutUint32(bad[NodeHeaderSize+leafEntryDescriptorSize+encodedUint32Size:], 0)
	binary.LittleEndian.PutUint32(bad[nodeChecksumOffset:NodeHeaderSize], pageChecksum(bad, testNodePageSize, nodeChecksumOffset))
	if err := VisitMappedLeafReverse(bad, leaf.PageID(), func(uint32, []byte, []byte) error { return nil }, true, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid leaf: got %v, want ErrInvalid", err)
	}
	if err := VisitMappedLeafReverse(data, leaf.PageID()+1, func(uint32, []byte, []byte) error { return nil }, true, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("wrong page: got %v, want ErrInvalid", err)
	}
	branch := NewRootNode(page.ID(10), NewLeafNode(page.ID(8)), NewLeafNode(page.ID(9)), []byte("b"))
	if err := VisitMappedLeafReverse(EncodeNode(branch, testNodePageSize), branch.PageID(), func(uint32, []byte, []byte) error { return nil }, true, nil); !errors.Is(err, ErrNotLeafNode) {
		t.Fatalf("branch: got %v, want ErrNotLeafNode", err)
	}
}

func TestNodeCodec_UsesFixedLeafPayloadLayout(t *testing.T) {
	node := NewLeafNode(page.ID(1))
	if err := node.InsertEntry(NewEntry(0, []byte("k"), []byte("v"))); err != nil {
		t.Fatal(err)
	}

	// Payload layout: one 16-byte descriptor, then the key and value.
	want := []byte{
		0, 0, 0, 0,
		34, 0, 0, 0,
		1, 0, 0, 0,
		1, 0, 0, 0,
		'k', 'v',
	}
	encoded := EncodeNode(node, testNodePageSize)
	if !bytes.Equal(encoded[NodeHeaderSize:], want) {
		t.Fatalf("encode leaf payload: got %x, want %x", encoded[NodeHeaderSize:], want)
	}

	decoded, err := DecodeNode(encoded, node.PageID(), testNodePageSize, nil, true)
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
		if _, err := DecodeNode(encoded[:size], node.PageID(), testNodePageSize, nil, true); err == nil {
			t.Errorf("decode %d-byte leaf prefix: expected an error", size)
		}
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
		entry, found, child, lookupErr = LookupMappedNode(data, node.PageID(), []byte("beta"), nil, true, true)
	}); got != 0 {
		t.Fatalf("LookupMappedNode allocations: got %v, want 0", got)
	}
	if lookupErr != nil || !found || child != 0 || !bytes.Equal(entry.Value(), []byte("two")) {
		t.Fatalf("LookupMappedNode: entry=%q found=%t child=%d err=%v", entry.Value(), found, child, lookupErr)
	}
}
