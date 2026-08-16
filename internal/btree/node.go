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
		n.entries[idx].value = slices.Clone(entry.value)
		return nil
	}

	entry.key = slices.Clone(entry.key)
	entry.value = slices.Clone(entry.value)

	n.entries = append(n.entries, Entry{})
	copy(n.entries[idx+1:], n.entries[idx:])
	n.entries[idx] = entry

	return nil
}

// Check if Node needs to be split since it surpassed the maximum node size
func (n *Node) NeedsSplit(pageSize int64) bool {
	return int64(n.EncodedSize()) > pageSize
}

func (n *Node) EncodedSize() int {
	return len(EncodeNode(n))
}

func (n *Node) chooseSplitIndex(pageSize int64) (int, error) {
	limit := int(pageSize / 2)
	currSize := 0
	separatorIndex := -1

	for i, entry := range n.entries {
		currSize += entry.EncodedSize(n.IsLeaf)

		if currSize >= limit {
			separatorIndex = i
			break
		}
	}

	if separatorIndex == -1 {
		return -1, ErrNodeNotSaturated
	}

	// Keep both sides non-empty. The half-page cut lands on index 0 when the first
	// entry alone fills half a page; without this clamp the left node would be empty
	// and the right node would keep the whole over-page content — an invalid split.
	// A leaf keeps [:sep] on the left and [sep:] on the right, so sep must be in
	// [1, len-1]. A branch also pushes entries[sep] up, so its right side is
	// [sep+1:]; sep must be in [1, len-2], which needs at least 3 entries.
	if n.IsLeaf {
		if len(n.entries) < 2 {
			return -1, ErrNodeNotSaturated
		}
		return min(max(separatorIndex, 1), len(n.entries)-1), nil
	}

	if len(n.entries) < 3 {
		return -1, ErrNodeNotSaturated
	}
	return min(max(separatorIndex, 1), len(n.entries)-2), nil
}

func (n *Node) Split(newPgid page.ID, pageSize int64) (*Node, int, []byte, error) {
	separatorIndex, err := n.chooseSplitIndex(pageSize)
	if err != nil {
		return nil, separatorIndex, nil, err
	}

	rightNode := &Node{
		IsLeaf:   n.IsLeaf,
		Children: []page.ID{},
		Index:    n.Index + 1,
		pgid:     newPgid,
	}

	keyAtSeparatorIndex := n.entries[separatorIndex].key

	// Handling entries
	if n.IsLeaf {
		rightNode.entries = slices.Clone(n.entries[separatorIndex:])
		n.entries = n.entries[:separatorIndex]
	} else {
		rightNode.entries = slices.Clone(n.entries[separatorIndex+1:])
		n.entries = n.entries[:separatorIndex] // removing `entries[separatorIndex]` from left, since it'll be moved upwards to parent

	}

	// Handling Children
	if !n.IsLeaf { // children only exist when the node is not a leaf node
		rightNode.Children = slices.Clone(n.Children[separatorIndex+1:])
		n.Children = n.Children[:separatorIndex+1]

	}

	return rightNode, separatorIndex, keyAtSeparatorIndex, nil
}

func (parent *Node) InsertSplitChild(leftNode, rightNode *Node, separator []byte) {
	rightNode.parent = parent

	parent.entries = append(parent.entries, Entry{})
	copy(parent.entries[leftNode.Index+1:], parent.entries[leftNode.Index:])
	parent.entries[leftNode.Index] = Entry{key: separator}

	parent.Children = append(parent.Children, 0)
	copy(parent.Children[rightNode.Index+1:], parent.Children[rightNode.Index:])
	parent.Children[rightNode.Index] = rightNode.pgid
}
