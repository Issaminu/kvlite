package btree

import (
	"bytes"
	"slices"
	"sort"

	"github.com/Issaminu/kvlite/internal/page"
)

const (
	MaxKeySize   = 32768         // 32 KiB
	MaxValueSize = (1 << 31) - 2 // ~2 GiB

	// NodeHeaderSize is the 20-byte encoded size of a B+tree node header.
	NodeHeaderSize = 20

	nodeFormatVersion uint16 = 1
)

// NodeType identifies the body layout of a B+tree node.
type NodeType uint16

const (
	// NodeTypeLeaf identifies a node that stores key and value entries.
	NodeTypeLeaf NodeType = 1
	// NodeTypeBranch identifies a node that stores separator keys and child page IDs.
	NodeTypeBranch NodeType = 2
)

// NodeHeader contains the fixed fields that identify and protect one B+tree node.
// All fields use little-endian encoding:
//
//	[0:2]   format version, uint16
//	[2:4]   node type, uint16
//	[4:12]  page ID, uint64
//	[12:16] entry count, uint32
//	[16:20] CRC32C checksum, uint32
//
// Checksum covers bytes [0:16] and [20:pageSize]. It includes the node body and zero padding.
// A node change can make Checksum and EntryCount stale. [EncodeNode] and [EncodeWALNode] write the current entry count. [EncodeNode] also calculates a new checksum before it returns encoded bytes.
type NodeHeader struct {
	FormatVersion uint16
	Type          NodeType
	PageID        page.ID
	EntryCount    uint32
	Checksum      uint32
}

type Entry struct {
	flags uint32
	key   []byte
	value []byte
}

func NewEntry(flags uint32, key, value []byte) Entry {
	return Entry{flags: flags, key: key, value: value}
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

// Node contains one B+tree node and its header.
type Node struct {
	header   *NodeHeader
	entries  []Entry
	Children []page.ID // for non-leaf nodes: len(Children) == len(entries)+1
}

func NewLeafNode(pgid page.ID) *Node {
	return &Node{
		header:   newNodeHeader(NodeTypeLeaf, pgid),
		Children: []page.ID{},
		entries:  []Entry{},
	}
}

func NewRootNode(pgid page.ID, leftNode, rightNode *Node, separator []byte) *Node {
	return &Node{
		header:   newNodeHeader(NodeTypeBranch, pgid),
		entries:  []Entry{{key: slices.Clone(separator)}},
		Children: []page.ID{leftNode.PageID(), rightNode.PageID()},
	}
}

func newNodeHeader(nodeType NodeType, pgid page.ID) *NodeHeader {
	return &NodeHeader{
		FormatVersion: nodeFormatVersion,
		Type:          nodeType,
		PageID:        pgid,
	}
}

// IsLeaf reports whether n stores key and value entries.
func (n *Node) IsLeaf() bool {
	return n.header.Type == NodeTypeLeaf
}

func (n *Node) PageID() page.ID {
	return n.header.PageID
}

func (n *Node) SetPageID(pgid page.ID) {
	n.header.PageID = pgid
	n.header.Checksum = 0
}

func (n *Node) EntryCount() int {
	return len(n.entries)
}

// Clone returns a structural copy of n with the same page ID.
// The clone owns its entry and child lists but shares the immutable key and value bytes with n.
// Node changes must replace an entry or byte slice instead of changing shared bytes in place.
func (n *Node) Clone() *Node {
	header := *n.header
	return &Node{
		header:   &header,
		entries:  slices.Clone(n.entries),
		Children: slices.Clone(n.Children),
	}
}

// CloneOwned returns a structural copy whose entry bytes do not refer to n.
// It uses one backing buffer for all keys and values in the copy.
func (n *Node) CloneOwned() *Node {
	clone := n.Clone()
	dataSize := 0
	for _, entry := range clone.entries {
		dataSize += len(entry.key) + len(entry.value)
	}
	data := make([]byte, dataSize)
	offset := 0
	for index := range clone.entries {
		keyEnd := offset + len(clone.entries[index].key)
		copy(data[offset:keyEnd], clone.entries[index].key)
		clone.entries[index].key = data[offset:keyEnd:keyEnd]
		offset = keyEnd

		if clone.entries[index].value == nil {
			continue
		}
		valueEnd := offset + len(clone.entries[index].value)
		copy(data[offset:valueEnd], clone.entries[index].value)
		clone.entries[index].value = data[offset:valueEnd:valueEnd]
		offset = valueEnd
	}
	return clone
}

// Find the correct child node for this key.
// It only returns the correct child for this node, if you actually want to reach the leaf node that has the key, then this function should be called in a loop.
func (n *Node) FindChildIndex(key []byte) (int, error) {
	if n.IsLeaf() {
		return -1, ErrNotBranchNode
	}
	index := sort.Search(len(n.entries), func(index int) bool {
		return bytes.Compare(key, n.entries[index].key) < 0
	})
	return index, nil
}

// Find the index of the key in it's leaf node.
// Returns (index, isFound, err)
func (n *Node) findKeyIndex(key []byte) (int, bool, error) {
	if !n.IsLeaf() {
		return -1, false, ErrNotLeafNode
	}
	index := sort.Search(len(n.entries), func(index int) bool {
		return bytes.Compare(n.entries[index].key, key) >= 0
	})
	found := index < len(n.entries) && bytes.Equal(n.entries[index].key, key)
	return index, found, nil
}

// FindEntryRef looks a key up in this leaf without copying its key or value.
// The returned entry refers to storage owned by the node.
func (n *Node) FindEntryRef(key []byte) (Entry, bool, error) {
	if !n.IsLeaf() {
		return Entry{}, false, ErrNotLeafNode
	}
	idx, found, err := n.findKeyIndex(key)
	if err != nil || !found {
		return Entry{}, found, err
	}

	entry := n.entries[idx]
	if entry.value == nil {
		entry.value = []byte{}
	}
	return entry, true, nil
}

func cloneValue(value []byte) []byte {
	value = slices.Clone(value)
	if value == nil {
		return []byte{}
	}
	return value
}

type leafInsert struct {
	entry Entry
	index int
	found bool
}

// prepareInsert checks a leaf insertion without changing the leaf.
func (n *Node) prepareInsert(entry Entry) (leafInsert, error) {
	if !n.IsLeaf() {
		return leafInsert{}, ErrNotLeafNode
	}

	idx, found, err := n.findKeyIndex(entry.key)
	if err != nil {
		return leafInsert{}, err
	}
	if found {
		existingIsBucket := n.entries[idx].flags&BucketLeafFlag != 0
		newIsBucket := entry.flags&BucketLeafFlag != 0
		if existingIsBucket != newIsBucket {
			return leafInsert{}, ErrIncompatibleValue
		}
	}
	return leafInsert{entry: entry, index: idx, found: found}, nil
}

// applyInsert changes a leaf after prepareInsert has accepted the operation.
func (n *Node) applyInsert(prepared leafInsert) {
	n.header.Checksum = 0
	if prepared.found {
		n.entries[prepared.index].flags = prepared.entry.flags
		n.entries[prepared.index].value = cloneValue(prepared.entry.value)
		return
	}

	entry := prepared.entry
	entry.key = slices.Clone(entry.key)
	entry.value = cloneValue(entry.value)

	n.entries = append(n.entries, Entry{})
	copy(n.entries[prepared.index+1:], n.entries[prepared.index:])
	n.entries[prepared.index] = entry
}

func (n *Node) InsertEntry(entry Entry) error {
	prepared, err := n.prepareInsert(entry)
	if err != nil {
		return err
	}
	n.applyInsert(prepared)
	return nil
}
