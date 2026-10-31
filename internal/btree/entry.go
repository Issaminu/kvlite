package btree

import "github.com/Issaminu/kvlite/internal/page"

// Entry is a key, value, and kind stored together in a leaf. Its byte slices are read-only unless the caller owns them.
type Entry struct {
	flags         uint32
	valueOwned    bool
	borrowValue   bool
	borrowedImage bool
	borrowedKey   bool
	key           []byte
	value         []byte
}

// NewEntry retains key and value until insertion, which copies them.
// flags identifies a plain value or a nested bucket root.
func NewEntry(flags uint32, key, value []byte) Entry {
	return Entry{flags: flags, key: key, value: value}
}

// NewEntryBorrowingValue makes an entry that keeps value without a copy on insertion.
// Insertion still copies the key.
// The caller must keep value unchanged until the node is written.
// A node with such a value must not enter committed memory state.
func NewEntryBorrowingValue(key, value []byte) Entry {
	return Entry{key: key, value: value, borrowValue: true}
}

func (e Entry) Flags() uint32 {
	return e.flags
}

func (e Entry) Key() []byte {
	return e.key
}

func (e Entry) Value() []byte {
	return e.value
}

// ValueIsOwned reports whether an encoded lookup made a private value buffer.
// The caller can retain and change that value without a copy.
// Values in decoded nodes and ordinary mapped pages return false.
func (e Entry) ValueIsOwned() bool {
	return e.valueOwned
}

// EncodedSize reports how many bytes this entry occupies inside an encoded node.
// A leaf uses one 16-byte descriptor followed by its key and value.
// A branch uses one 8-byte descriptor, one child page ID, and its separator key.
// The branch node also stores one extra child page ID.
func (e Entry) EncodedSize(isLeaf bool) int {
	size := branchEntryDescriptorSize + page.IDSize + len(e.key)
	if isLeaf {
		size = leafEntryDescriptorSize + len(e.key) + len(e.value)
	}
	return size
}
