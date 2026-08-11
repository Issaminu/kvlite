package kvlite

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
)

const (
	MaxKeySize   = 32768         // 32 KiB
	MaxValueSize = (1 << 31) - 2 // ~2 GiB
)

type KVLength uint32 // Size of the prefixed length of an Entry's `key` or `Value`

type Entry struct {
	key   []byte
	value []byte
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
	low, high := 0, len(n.entries)
	for low < high {
		mid := low + (high-low)/2
		if bytes.Compare(key, n.entries[mid].key) < 0 {
			high = mid
		} else {
			low = mid + 1
		}
	}
	return low, nil
}

// Find the index of the key in it's leaf node.
// Returns (index, isFound, err)
func (n *Node) findKeyIndex(key []byte) (int, bool, error) {
	if !n.IsLeaf {
		return -1, false, ErrNotLeafNode
	}
	low, high := 0, len(n.entries)
	for low < high {
		mid := low + (high-low)/2
		cmp := bytes.Compare(key, n.entries[mid].key)
		if cmp < 0 {
			high = mid
		} else if cmp > 0 {
			low = mid + 1
		} else {
			return mid, true, nil
		}
	}
	return low, false, nil
}

func (n *Node) get(key []byte) ([]byte, bool, error) {
	if !n.IsLeaf {
		return nil, false, ErrNotLeafNode
	}
	idx, found, err := n.findKeyIndex(key)

	if err != nil {
		return nil, false, err
	}

	if !found {
		return nil, false, nil
	}

	value := slices.Clone(n.entries[idx].value)
	return value, true, nil
}

func (n *Node) insert(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > MaxValueSize {
		return ErrValueTooLarge
	}
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
		n.entries[idx].value = valueCopy
		return nil
	}

	newEntry := Entry{key: keyCopy, value: valueCopy}

	n.entries = append(n.entries, Entry{})
	copy(n.entries[idx+1:], n.entries[idx:])
	n.entries[idx] = newEntry

	return nil
}

func readNode(r io.Reader) (*Node, error) {
	var isLeafByte [1]byte // []byte because io.ReadFull expectes a []byte as the destination buffer
	if _, err := io.ReadFull(r, isLeafByte[:]); err != nil {
		return nil, fmt.Errorf("read node isLeaf: %w", err)
	}

	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return nil, fmt.Errorf("read node key count: %w", err)
	}

	node := &Node{IsLeaf: isLeafByte[0] != 0}
	// looping through the entries (keys and values) of this node
	for i := uint32(0); i < count; i++ {
		key, err := readLengthPrefixedBytes[KVLength](r)
		if err != nil {
			return nil, fmt.Errorf("read node key %d: %w", i, err)
		}
		if !node.IsLeaf { // meaning we can only read the key
			node.entries = append(node.entries, Entry{key: key})
			continue
		}
		value, err := readLengthPrefixedBytes[KVLength](r)
		if err != nil {
			return nil, fmt.Errorf("read node value %d: %w", i, err)
		}
		node.entries = append(node.entries, Entry{key: key, value: value})
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
	var buf bytes.Buffer
	pageSize := int(node.db.meta.pageSize)

	node.encode(&buf)

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

func (n *Node) encode(buf *bytes.Buffer) error {
	isLeafByte := byte(0)
	if n.IsLeaf {
		isLeafByte = 1
	}
	if err := binary.Write(buf, binary.LittleEndian, isLeafByte); err != nil {
		return fmt.Errorf("write node isLeaf: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(n.entries))); err != nil {
		return fmt.Errorf("write node key count: %w", err)
	}

	for _, e := range n.entries {
		if err := writeLengthPrefixedBytes[KVLength](buf, e.key); err != nil {
			return fmt.Errorf("write node key: %w", err)
		}
		if n.IsLeaf {
			if err := writeLengthPrefixedBytes[KVLength](buf, e.value); err != nil {
				return fmt.Errorf("write node value: %w", err)
			}
		}
	}

	if !n.IsLeaf {
		for _, child := range n.Children {
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
	var buf bytes.Buffer
	n.encode(&buf)
	return buf.Len()
}

func (n *Node) split(newPgid Pgid) (*Node, int, []byte, error) { // Returns (new_right_node, seperatorIndex, key at seperatorIndex, error)
	limit := int(n.db.meta.pageSize / 2)
	currSize := 0

	var seperatorIndex = -1

	for i, entry := range n.entries {
		currSize += len(entry.key)
		if n.IsLeaf {
			currSize += len(entry.value)
		}

		if currSize >= limit {
			seperatorIndex = i
			break
		}
	}

	if seperatorIndex == -1 {
		return nil, seperatorIndex, nil, ErrNodeNotSaturated
	}

	rightNode := n.db.newLeafNode(newPgid)

	rightNode.IsLeaf = n.IsLeaf
	rightNode.Index = n.Index + 1

	var keyAtSeperatorIndex []byte
	keyAtSeperatorIndex = n.entries[seperatorIndex].key

	// Handling entries
	if n.IsLeaf {
		rightNode.entries = slices.Clone(n.entries[seperatorIndex:])
		n.entries = n.entries[:seperatorIndex]
	} else {
		rightNode.entries = slices.Clone(n.entries[seperatorIndex+1:])
		n.entries = n.entries[:seperatorIndex] // removing `entries[seperatorIndex]` from left, since it'll be moved upwards to parent

	}

	// Handling Children
	if !n.IsLeaf { // children only exist when the node is not a leaf node
		rightNode.Children = slices.Clone(n.Children[seperatorIndex+1:])
		n.Children = n.Children[:seperatorIndex+1]

	}

	return rightNode, seperatorIndex, keyAtSeperatorIndex, nil
}
