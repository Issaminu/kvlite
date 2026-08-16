package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"sort"

	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
)

const (
	MaxKeySize   = 32768         // 32 KiB
	MaxValueSize = (1 << 31) - 2 // ~2 GiB
)

const (
	// Node scalar fields and key/value lengths use uint32 values, which occupy
	// four bytes when encoded.
	encodedUint32Size = 4
	// A node starts with a one-byte leaf marker and a uint32 entry count.
	nodeHeaderSize = 1 + encodedUint32Size
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

// Decodes one bounded node buffer into a Node.
func DecodeNode(data []byte) (*Node, error) {
	if len(data) < nodeHeaderSize {
		return nil, fmt.Errorf("read node header: %w", ErrInvalid)
	}
	node := &Node{IsLeaf: data[0] != 0}
	count := binary.LittleEndian.Uint32(data[1:nodeHeaderSize])
	data = data[nodeHeaderSize:]

	// looping through the entries (keys and values) of this node
	for i := uint32(0); i < count; i++ {
		if len(data) < encodedUint32Size {
			return nil, fmt.Errorf("read node flags %d: %w", i, ErrInvalid)
		}
		flags := binary.LittleEndian.Uint32(data[:encodedUint32Size])
		data = data[encodedUint32Size:]

		key, remaining, err := decodeLengthPrefixedBytes(data)
		if err != nil {
			return nil, fmt.Errorf("read node key %d: %w", i, err)
		}
		data = remaining
		if !node.IsLeaf { // meaning we can only read the key
			node.entries = append(node.entries, Entry{flags: flags, key: key})
			continue
		}
		value, remaining, err := decodeLengthPrefixedBytes(data)
		if err != nil {
			return nil, fmt.Errorf("read node value %d: %w", i, err)
		}
		data = remaining
		node.entries = append(node.entries, Entry{flags: flags, key: key, value: value})
	}

	if !node.IsLeaf {
		childCount := uint64(count) + 1
		childrenSize := childCount * uint64(page.IDSize)
		if childrenSize > uint64(len(data)) {
			return nil, fmt.Errorf("read node children: %w", ErrInvalid)
		}
		node.Children = make([]page.ID, int(childCount))
		for i := range node.Children {
			node.Children[i] = page.ID(binary.LittleEndian.Uint64(data[:page.IDSize]))
			data = data[page.IDSize:]
		}
	}
	return node, nil
}

func decodeLengthPrefixedBytes(data []byte) ([]byte, []byte, error) {
	if len(data) < encodedUint32Size {
		return nil, nil, ErrInvalid
	}
	length := binary.LittleEndian.Uint32(data[:encodedUint32Size])
	data = data[encodedUint32Size:]
	if uint64(length) > uint64(len(data)) {
		return nil, nil, ErrInvalid
	}

	size := int(length)
	value := make([]byte, size)
	copy(value, data[:size])
	return value, data[size:], nil
}

func WriteNode(w io.Writer, node *Node, pageSize int64, shouldPad bool) error {
	encoded := EncodeNode(node)
	currNodeSize := len(encoded)

	if int64(currNodeSize) > pageSize {
		return ErrNodeTooLarge
	}

	if err := fileio.WriteFull(w, encoded); err != nil {
		return fmt.Errorf("write node: %w", err)
	}
	if shouldPad && int64(currNodeSize) < pageSize {
		padding := make([]byte, int(pageSize)-currNodeSize)
		if err := fileio.WriteFull(w, padding); err != nil {
			return fmt.Errorf("write node padding: %w", err)
		}
	}
	return nil
}

func EncodeNode(node *Node) []byte {
	data := make([]byte, 0)
	isLeafByte := byte(0)
	if node.IsLeaf {
		isLeafByte = 1
	}
	data = append(data, isLeafByte)
	data = binary.LittleEndian.AppendUint32(data, uint32(len(node.entries)))

	for _, e := range node.entries {
		data = binary.LittleEndian.AppendUint32(data, e.flags)
		data = binary.LittleEndian.AppendUint32(data, uint32(len(e.key)))
		data = append(data, e.key...)
		if node.IsLeaf {
			data = binary.LittleEndian.AppendUint32(data, uint32(len(e.value)))
			data = append(data, e.value...)
		}
	}

	if !node.IsLeaf {
		for _, child := range node.Children {
			data = binary.LittleEndian.AppendUint64(data, uint64(child))
		}
	}
	return data
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
