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
)

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
// It must stay in lockstep with EncodeNode: flags(4) + a length-prefixed key
// (4 + len(key)), plus a length-prefixed value (4 + len(value)) for leaf entries.
// Branch entries hold only a separator key, so they carry no value.
func (e Entry) EncodedSize(isLeaf bool) int {
	size := 4 + 4 + len(e.key)
	if isLeaf {
		size += 4 + len(e.value)
	}
	return size
}

type Node struct {
	IsLeaf   bool
	entries  []Entry
	Children []page.ID // for non-leaf nodes: len(Children) == len(entries)+1
	Index    int       // this node's index within it's parent node's Children array
	pgid     page.ID
	parent   *Node
}

func NewLeafNode(pgid page.ID) *Node {
	return &Node{IsLeaf: true, pgid: pgid, Children: []page.ID{}, entries: []Entry{}}
}

func NewRootNode(pgid page.ID, leftNode, rightNode *Node, separator []byte) *Node {
	rootNode := &Node{
		entries:  []Entry{{key: separator}},
		Children: []page.ID{leftNode.pgid, rightNode.pgid},
		pgid:     pgid,
	}
	leftNode.parent = rootNode
	rightNode.parent = rootNode
	return rootNode
}

func (n *Node) PageID() page.ID {
	return n.pgid
}

func (n *Node) SetPageID(pgid page.ID) {
	n.pgid = pgid
}

func (n *Node) Parent() *Node {
	return n.parent
}

func (n *Node) SetParent(parent *Node) {
	n.parent = parent
}

func (n *Node) EntryCount() int {
	return len(n.entries)
}

// Clone returns an independent copy of n without a parent link.
func (n *Node) Clone() *Node {
	clone := &Node{
		IsLeaf:   n.IsLeaf,
		entries:  make([]Entry, len(n.entries)),
		Children: slices.Clone(n.Children),
		Index:    n.Index,
		pgid:     n.pgid,
	}
	for index, entry := range n.entries {
		clone.entries[index] = Entry{
			flags: entry.flags,
			key:   slices.Clone(entry.key),
			value: slices.Clone(entry.value),
		}
	}
	return clone
}

// Find the correct child node for this key.
// It only returns the correct child for this node, if you actually want to reach the leaf node that has the key, then this function should be called in a loop.
func (n *Node) FindChildIndex(key []byte) (int, error) {
	if n.IsLeaf {
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
	if !n.IsLeaf {
		return -1, false, ErrNotLeafNode
	}
	index := sort.Search(len(n.entries), func(index int) bool {
		return bytes.Compare(n.entries[index].key, key) >= 0
	})
	found := index < len(n.entries) && bytes.Equal(n.entries[index].key, key)
	return index, found, nil
}

// FindEntry looks a key up in this leaf. The returned found reports whether the
// key is present because a stored value can be empty.
func (n *Node) FindEntry(key []byte) (Entry, bool, error) {
	if !n.IsLeaf {
		return Entry{}, false, ErrNotLeafNode
	}
	idx, found, err := n.findKeyIndex(key)
	if err != nil {
		return Entry{}, false, err
	}
	if !found {
		return Entry{}, false, nil
	}

	entry := n.entries[idx]
	entry.key = slices.Clone(entry.key)
	entry.value = slices.Clone(entry.value)
	if entry.value == nil {
		entry.value = []byte{}
	}
	return entry, true, nil
}

// FindEntryRef looks a key up in this leaf without copying its key or value.
// The returned entry refers to storage owned by the node.
func (n *Node) FindEntryRef(key []byte) (Entry, bool, error) {
	if !n.IsLeaf {
		return Entry{}, false, ErrNotLeafNode
	}
	idx, found, err := n.findKeyIndex(key)
	if err != nil || !found {
		return Entry{}, found, err
	}

	if n.entries[idx].value == nil {
		n.entries[idx].value = []byte{}
	}
	return n.entries[idx], true, nil
}

func cloneValue(value []byte) []byte {
	value = slices.Clone(value)
	if value == nil {
		return []byte{}
	}
	return value
}

func (n *Node) InsertEntry(entry Entry) error {
	if !n.IsLeaf {
		return ErrNotLeafNode
	}

	idx, found, err := n.findKeyIndex(entry.key)
	if err != nil {
		return err
	}
	if found {
		existingIsBucket := n.entries[idx].flags&BucketLeafFlag != 0
		newIsBucket := entry.flags&BucketLeafFlag != 0
		if existingIsBucket != newIsBucket {
			return ErrIncompatibleValue
		}
		n.entries[idx].flags = entry.flags
		n.entries[idx].value = cloneValue(entry.value)
		return nil
	}

	entry.key = slices.Clone(entry.key)
	entry.value = cloneValue(entry.value)

	n.entries = append(n.entries, Entry{})
	copy(n.entries[idx+1:], n.entries[idx:])
	n.entries[idx] = entry

	return nil
}
