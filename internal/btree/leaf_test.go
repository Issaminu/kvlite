package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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
	branch := NewRootNode(page.ID(10), left.PageID(), right.PageID(), []byte("middle"))
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
	branch := NewRootNode(page.ID(10), NewLeafNode(page.ID(8)).PageID(), NewLeafNode(page.ID(9)).PageID(), []byte("b"))
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
		38, 0, 0, 0,
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

	for size := range encoded {
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

// TestLeafCodec_RejectsBucketEntryWithoutRootID checks that every path that reads leaf entries applies one rule:
// a nested bucket entry stores an 8-byte root page ID.
// Decode, lookup, the reference check, and the mapped scans must agree, for a leaf in one page and for a continued leaf.
func TestLeafCodec_RejectsBucketEntryWithoutRootID(t *testing.T) {
	key := []byte("bucket")
	for _, value := range [][]byte{[]byte("short"), bytes.Repeat([]byte("v"), 100)} {
		node := NewLeafNode(8)
		if err := node.InsertEntry(NewEntry(BucketLeafFlag, key, value)); err != nil {
			t.Fatal(err)
		}
		const pageSize = 64
		ids := make([]page.ID, NodePageCount(node, pageSize))
		ids[0] = 8
		for i := 1; i < len(ids); i++ {
			ids[i] = ids[0] + page.ID(i)
		}
		pages, err := EncodeNodePages(node, pageSize, ids)
		if err != nil {
			t.Fatal(err)
		}
		read := func(id page.ID) ([]byte, error) {
			if index := slices.Index(ids, id); index >= 0 {
				return pages[index], nil
			}
			return nil, ErrInvalid
		}
		if _, err := DecodeNode(pages[0], 8, pageSize, read, true); !errors.Is(err, ErrInvalid) {
			t.Fatalf("value %d bytes: decode: %v", len(value), err)
		}
		if _, _, _, err := LookupMappedNode(pages[0], 8, key, read, true, true); !errors.Is(err, ErrInvalid) {
			t.Fatalf("value %d bytes: mapped lookup: %v", len(value), err)
		}
		if err := VisitMappedNodeReferences(pages[0], 8, pageSize, read, func(page.ID) error { return nil }, nil, true); !errors.Is(err, ErrInvalid) {
			t.Fatalf("value %d bytes: reference check: %v", len(value), err)
		}
		visit := func(uint32, []byte, []byte) error { return nil }
		if err := VisitMappedLeafRange(pages[0], 8, nil, nil, visit, true, read); !errors.Is(err, ErrInvalid) {
			t.Fatalf("value %d bytes: mapped range scan: %v", len(value), err)
		}
		if err := VisitMappedLeafReverse(pages[0], 8, visit, true, read); !errors.Is(err, ErrInvalid) {
			t.Fatalf("value %d bytes: mapped reverse scan: %v", len(value), err)
		}
		if len(ids) == 1 {
			image := EncodeWALNode(node)
			if _, err := DecodeWALNode(image, 8, pageSize, nil); !errors.Is(err, ErrInvalid) {
				t.Fatalf("WAL decode: %v", err)
			}
			if _, _, _, err := LookupEncodedWALNode(image, 8, key, pageSize, nil, true); !errors.Is(err, ErrInvalid) {
				t.Fatalf("WAL lookup: %v", err)
			}
			if err := ValidateWALNode(image, 8); !errors.Is(err, ErrInvalid) {
				t.Fatalf("WAL validation: %v", err)
			}
		}
	}
}

func TestReplaceMappedValueOnePage(t *testing.T) {
	for _, size := range []int64{4096, 16384} {
		for _, checksums := range []bool{false, true} {
			t.Run(fmt.Sprintf("size=%d/checksums=%t", size, checksums), func(t *testing.T) {
				node := NewLeafNode(50)
				for index := range 12 {
					if err := node.InsertEntry(NewEntry(0, fmt.Appendf(nil, "key-%02d", index), bytes.Repeat([]byte{byte(index + 1)}, 128))); err != nil {
						t.Fatal(err)
					}
				}
				data := make([]byte, size)
				if err := EncodeNodeInto(data, node, size, checksums); err != nil {
					t.Fatal(err)
				}
				key, value := []byte("key-05"), bytes.Repeat([]byte{99}, 128)
				broken := bytes.Clone(data)
				binary.LittleEndian.PutUint32(broken[NodeHeaderSize+11*leafEntryDescriptorSize+4:], 0)
				before := bytes.Clone(broken)
				if _, _, err := PutMappedEntry(broken, node.PageID(), size, false, key, value); !errors.Is(err, ErrInvalid) || !bytes.Equal(before, broken) {
					t.Fatalf("bad unrelated descriptor changed the page: %v", err)
				}
				if checksums {
					broken = bytes.Clone(data)
					broken[len(broken)-1] ^= 1
					before = bytes.Clone(broken)
					if _, _, err := PutMappedEntry(broken, node.PageID(), size, true, key, value); !errors.Is(err, page.ErrChecksum) || !bytes.Equal(before, broken) {
						t.Fatalf("bad checksum changed the page: %v", err)
					}
				}
				handled, child, err := PutMappedEntry(data, node.PageID(), size, checksums, key, value)
				if !handled || child != 0 || err != nil {
					t.Fatalf("small mapped replacement: %t %d %v", handled, child, err)
				}
				decoded, err := DecodeNode(data, node.PageID(), size, nil, checksums)
				if err != nil {
					t.Fatal(err)
				}
				for index := range 12 {
					want := bytes.Repeat([]byte{byte(index + 1)}, 128)
					if index == 5 {
						want = value
					}
					entry := decoded.EntryAt(index)
					if !bytes.Equal(entry.Value(), want) {
						t.Fatalf("wrong value at %d", index)
					}
				}
			})
		}
	}
}

func TestInsertMappedLeafEntry(t *testing.T) {
	for _, size := range []int64{4096, 16384} {
		for _, checksums := range []bool{false, true} {
			for _, key := range []string{"b", "d", "f"} {
				t.Run(fmt.Sprintf("size=%d/checksums=%t/key=%s", size, checksums, key), func(t *testing.T) {
					node := NewLeafNode(50)
					for _, existing := range []string{"a", "c", "e"} {
						if err := node.InsertEntry(NewEntry(0, []byte(existing), bytes.Repeat([]byte(existing), 9))); err != nil {
							t.Fatal(err)
						}
					}
					data := make([]byte, size)
					if err := EncodeNodeInto(data, node, size, checksums); err != nil {
						t.Fatal(err)
					}
					value := bytes.Repeat([]byte("new"), 13)
					if key == "d" {
						value = nil
					}
					handled, child, err := PutMappedEntry(data, node.PageID(), size, checksums, []byte(key), value)
					if err != nil || !handled || child != 0 {
						t.Fatalf("insert: handled=%t error=%v", handled, err)
					}
					if err := node.InsertEntry(NewEntry(0, []byte(key), value)); err != nil {
						t.Fatal(err)
					}
					want := make([]byte, size)
					if err := EncodeNodeInto(want, node, size, checksums); err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(data, want) {
						t.Fatal("mapped insert differs from the normal encoder")
					}
				})
			}
		}
	}
}

func TestInsertMappedLeafEntryPreflight(t *testing.T) {
	const size int64 = 256
	node := NewLeafNode(50)
	for _, key := range []string{"a", "c"} {
		if err := node.InsertEntry(NewEntry(0, []byte(key), bytes.Repeat([]byte("v"), 40))); err != nil {
			t.Fatal(err)
		}
	}
	original := make([]byte, size)
	if err := EncodeNodeInto(original, node, size, true); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name       string
		key, value []byte
		damage     func([]byte)
		want       error
	}{
		{name: "first-key-change", key: []byte("0"), value: []byte("new")},
		{name: "existing-key", key: []byte("c"), value: []byte("new")},
		{name: "page-full", key: []byte("b"), value: bytes.Repeat([]byte("x"), 130)},
		{name: "large-input", key: []byte("b"), value: bytes.Repeat([]byte("x"), 257)},
		{name: "empty-key", want: ErrKeyRequired},
		{name: "unrelated-descriptor", key: []byte("b"), damage: func(data []byte) {
			binary.LittleEndian.PutUint32(data[NodeHeaderSize+leafEntryDescriptorSize+4:], 0)
			if err := VerifyNodeIDAndSetChecksum(data, node.PageID(), true); err != nil {
				t.Fatal(err)
			}
		}, want: ErrInvalid},
		{name: "checksum", key: []byte("b"), damage: func(data []byte) { data[len(data)-1] ^= 1 }, want: page.ErrChecksum},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := bytes.Clone(original)
			if test.damage != nil {
				test.damage(data)
			}
			before := bytes.Clone(data)
			handled, child, err := PutMappedEntry(data, node.PageID(), size, true, test.key, test.value)
			if handled || child != 0 || !errors.Is(err, test.want) || !bytes.Equal(data, before) {
				t.Fatalf("preflight changed data: handled=%t error=%v", handled, err)
			}
		})
	}
	data := bytes.Clone(original)
	_, _, keyEnd, end := leafSpan(data, 0)
	key, value := data[keyEnd:keyEnd+1], data[keyEnd:end]
	wantNode := node.Clone()
	if err := wantNode.InsertEntry(NewEntry(0, key, value)); err != nil {
		t.Fatal(err)
	}
	want := make([]byte, size)
	if err := EncodeNodeInto(want, wantNode, size, true); err != nil {
		t.Fatal(err)
	}
	if handled, child, err := PutMappedEntry(data, node.PageID(), size, true, key, value); err != nil || !handled || child != 0 || !bytes.Equal(data, want) {
		t.Fatalf("overlapping input: handled=%t error=%v", handled, err)
	}
}

func TestPrepareMappedPutDoesNotChangeLeaf(t *testing.T) {
	const pageSize int64 = 4096
	node := NewLeafNode(50)
	for _, key := range []string{"a", "c"} {
		if err := node.InsertEntry(NewEntry(0, []byte(key), []byte("value"))); err != nil {
			t.Fatal(err)
		}
	}
	data := make([]byte, pageSize)
	if err := EncodeNodeInto(data, node, pageSize, false); err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(data)
	for _, test := range []struct {
		key   string
		index int
		found bool
	}{{"c", 1, true}, {"b", 1, false}, {"0", 0, false}} {
		var entry Entry
		index, found, child, err := PrepareMappedPut(data, pageSize, []byte(test.key), 5, &entry)
		if err != nil || index != test.index || found != test.found || child != 0 {
			t.Fatalf("preflight %q: index=%d found=%t child=%d error=%v", test.key, index, found, child, err)
		}
		if !bytes.Equal(data, before) {
			t.Fatal("preflight changed the leaf")
		}
	}
	binary.LittleEndian.PutUint32(data[NodeHeaderSize+4:NodeHeaderSize+8], ^uint32(0))
	before = bytes.Clone(data)
	var entry Entry
	if _, _, _, err := PrepareMappedPut(data, pageSize, []byte("c"), 5, &entry); err == nil || !bytes.Equal(data, before) {
		t.Fatalf("preflight accepted or changed a malformed leaf: %v", err)
	}
}

func TestInsertEntryOwnsCopiedImageBytes(t *testing.T) {
	const pageSize = 256
	original := NewLeafNode(3)
	if err := original.InsertEntry(NewEntry(0, []byte("key"), []byte("value"))); err != nil {
		t.Fatal(err)
	}
	image := make([]byte, pageSize)
	if err := EncodeNodeInto(image, original, pageSize, false); err != nil {
		t.Fatal(err)
	}
	view, err := ReadNodeImage(image, 3, pageSize, false, false)
	if err != nil {
		t.Fatal(err)
	}
	source, err := view.Clone(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	destination := NewLeafNode(4)
	if err := destination.InsertEntry(*source.EntryAt(0)); err != nil {
		t.Fatal(err)
	}
	clear(image)
	stored := destination.EntryAt(0)
	key, value := stored.Key(), stored.Value()
	if string(key) != "key" || string(value) != "value" {
		t.Fatal("inserted entry still borrows the encoded image")
	}
	destination.OwnImageBytes()
	if &stored.Key()[0] != &key[0] || &stored.Value()[0] != &value[0] {
		t.Fatal("image ownership copied an entry whose bytes insertion already owns")
	}
}
