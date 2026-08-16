package kvlite

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"sort"
)

const (
	MaxKeySize   = 32768         // 32 KiB
	MaxValueSize = (1 << 31) - 2 // ~2 GiB
)

type KVLength uint32 // Size of the prefixed length of an Entry's `key` or `Value`

type Entry struct {
	flags uint32
	key   []byte
	value []byte
}

// encodedSize reports how many bytes this entry occupies inside an encoded node.
// It must stay in lockstep with encodeNode: flags(4) + a length-prefixed key
// (4 + len(key)), plus a length-prefixed value (4 + len(value)) for leaf entries.
// Branch entries hold only a separator key, so they carry no value.
func (e Entry) encodedSize(isLeaf bool) int {
	size := 4 + 4 + len(e.key)
	if isLeaf {
		size += 4 + len(e.value)
	}
	return size
}

type Node struct {
	db       *DB
	IsLeaf   bool
	entries  []Entry
	Children []Pgid // for non-leaf nodes: len(Children) == len(entries)+1
	Index    int    // this node's index within it's parent node's Children array
	pgid     Pgid
	parent   *Node
}

// Find the correct child node for this key.
// It only returns the correct child for this node, if you actually want to reach the leaf node that has the key, then this function should be called in a loop.
func (n *Node) findChildIndex(key []byte) (int, error) {
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

// get looks a key up in this leaf. The returned found reports whether the key is
// present; callers must use it rather than a nil value to decide "missing",
// because a stored value can legitimately be empty. When found is true, the
// returned value is always non-nil (an empty stored value comes back as a
// zero-length slice), so a nil value never means "present but empty".
func (n *Node) get(key []byte) (value []byte, flags uint32, found bool, err error) {
	if !n.IsLeaf {
		return nil, 0, false, ErrNotLeafNode
	}
	idx, found, err := n.findKeyIndex(key)
	if err != nil {
		return nil, 0, false, err
	}
	if !found {
		return nil, 0, false, nil
	}

	value = slices.Clone(n.entries[idx].value)
	if value == nil {
		value = []byte{}
	}
	return value, n.entries[idx].flags, true, nil
}

func (n *Node) insert(key, value []byte, flags uint32) error {
	if !n.IsLeaf {
		return ErrNotLeafNode
	}

	keyCopy := slices.Clone(key)
	valueCopy := slices.Clone(value)

	idx, found, err := n.findKeyIndex(key)

	if err != nil {
		return err
	}
	if found {
		existingIsBucket := n.entries[idx].flags&BucketLeafFlag != 0
		newIsBucket := flags&BucketLeafFlag != 0
		if existingIsBucket != newIsBucket {
			return ErrIncompatibleValue
		}
		n.entries[idx].flags = flags
		n.entries[idx].value = valueCopy
		return nil
	}

	newEntry := Entry{flags: flags, key: keyCopy, value: valueCopy}

	n.entries = append(n.entries, Entry{})
	copy(n.entries[idx+1:], n.entries[idx:])
	n.entries[idx] = newEntry

	return nil
}

func readNode(r *bytes.Reader) (*Node, error) {
	node := &Node{}
	node, err := decodeNode(r)

	if err != nil {
		return nil, err
	}
	return node, nil
}

// Decodes one bounded node buffer into a Node.
func decodeNode(r *bytes.Reader) (*Node, error) {
	node := &Node{}
	var isLeafByte byte
	if err := binary.Read(r, binary.LittleEndian, &isLeafByte); err != nil {
		return nil, fmt.Errorf("read node isLeaf: %w", err)
	}
	node.IsLeaf = isLeafByte != 0

	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return nil, fmt.Errorf("read node key count: %w", err)
	}

	// looping through the entries (keys and values) of this node
	for i := uint32(0); i < count; i++ {
		var flags uint32
		if err := binary.Read(r, binary.LittleEndian, &flags); err != nil {
			return nil, fmt.Errorf("read node flags %d: %w", i, err)
		}
		key, err := readLengthPrefixedBytes[KVLength](r)
		if err != nil {
			return nil, fmt.Errorf("read node key %d: %w", i, err)
		}
		if !node.IsLeaf { // meaning we can only read the key
			node.entries = append(node.entries, Entry{flags: flags, key: key})
			continue
		}
		value, err := readLengthPrefixedBytes[KVLength](r)
		if err != nil {
			return nil, fmt.Errorf("read node value %d: %w", i, err)
		}
		node.entries = append(node.entries, Entry{flags: flags, key: key, value: value})
	}

	if !node.IsLeaf {
		node.Children = make([]Pgid, count+1)
		for i := range node.Children {
			var pgid uint64
			if err := binary.Read(r, binary.LittleEndian, &pgid); err != nil {
				return nil, fmt.Errorf("read node child pgid %d: %w", i, err)
			}
			node.Children[i] = Pgid(pgid)
		}
	}
	return node, nil
}

func writeNode(w io.Writer, node *Node, shouldPad bool) error {
	buf := new(bytes.Buffer)
	pageSize := int(node.db.meta.pageSize)

	encodeNode(node, buf)

	currNodeSize := buf.Len()

	if currNodeSize > pageSize {
		return ErrNodeTooLarge
	}

	if _, err := w.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("write node: %w", err)
	}
	if shouldPad && currNodeSize < pageSize {
		padding := make([]byte, pageSize-currNodeSize)
		if _, err := w.Write(padding); err != nil {
			return fmt.Errorf("write node padding: %w", err)
		}
	}
	return nil
}

// Encodes a *Node instance into a *bytes.Buffer
func encodeNode(node *Node, buf *bytes.Buffer) error {
	isLeafByte := byte(0)
	if node.IsLeaf {
		isLeafByte = 1
	}
	if err := binary.Write(buf, binary.LittleEndian, isLeafByte); err != nil {
		return fmt.Errorf("write node isLeaf: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(node.entries))); err != nil {
		return fmt.Errorf("write node key count: %w", err)
	}

	for _, e := range node.entries {
		if err := binary.Write(buf, binary.LittleEndian, e.flags); err != nil {
			return fmt.Errorf("write node flags: %w", err)
		}
		if err := writeLengthPrefixedBytes[KVLength](buf, e.key); err != nil {
			return fmt.Errorf("write node key: %w", err)
		}
		if node.IsLeaf {
			if err := writeLengthPrefixedBytes[KVLength](buf, e.value); err != nil {
				return fmt.Errorf("write node value: %w", err)
			}
		}
	}

	if !node.IsLeaf {
		for _, child := range node.Children {
			if err := binary.Write(buf, binary.LittleEndian, uint64(child)); err != nil {
				return fmt.Errorf("write node child pgid: %w", err)
			}
		}
	}
	return nil
}

// Node needs to be split since it surpassed the maximum node size
func (n *Node) needsSplit() bool {
	return n.serializedSize() > int(n.db.meta.pageSize)
}

func (n *Node) serializedSize() int {
	buf := new(bytes.Buffer)
	encodeNode(n, buf)
	return buf.Len()
}

func (n *Node) chooseSplitIndex() (int, error) {
	limit := int(n.db.meta.pageSize / 2)
	currSize := 0
	separatorIndex := -1

	for i, entry := range n.entries {
		currSize += entry.encodedSize(n.IsLeaf)

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

func (n *Node) split(newPgid Pgid) (*Node, int, []byte, error) {
	separatorIndex, err := n.chooseSplitIndex()
	if err != nil {
		return nil, separatorIndex, nil, err
	}

	rightNode := n.db.newLeafNode(newPgid)

	rightNode.IsLeaf = n.IsLeaf
	rightNode.Index = n.Index + 1

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

func (parent *Node) insertSplitChild(leftNode, rightNode *Node, separator []byte) {
	rightNode.parent = parent

	parent.entries = append(parent.entries, Entry{})
	copy(parent.entries[leftNode.Index+1:], parent.entries[leftNode.Index:])
	parent.entries[leftNode.Index] = Entry{key: separator}

	parent.Children = append(parent.Children, 0)
	copy(parent.Children[rightNode.Index+1:], parent.Children[rightNode.Index:])
	parent.Children[rightNode.Index] = rightNode.pgid
}
