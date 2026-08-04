package kvlite

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	MaxKeySize   = 32768         // 32 KiB
	MaxValueSize = (1 << 31) - 2 // ~2 GiB
)

type entry struct {
	key   []byte
	value []byte
}

type Node struct {
	IsLeaf   bool
	entries  []entry
	Children []Pgid // branch only: len == len(entries)+1
	Parent   *Node
	Index    int
}

func newLeafNode() *Node {
	return &Node{IsLeaf: true}
}

func (n *Node) findChildIndex(key []byte) int {
	low, high := 0, len(n.entries)
	for low < high {
		mid := low + (high-low)/2
		if bytes.Compare(key, n.entries[mid].key) >= 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low
}

func (n *Node) findKeyIndex(key []byte) (int, bool) {
	low, high := 0, len(n.entries)
	for low < high {
		mid := low + (high-low)/2
		cmp := bytes.Compare(key, n.entries[mid].key)
		if cmp < 0 {
			high = mid
		} else if cmp > 0 {
			low = mid + 1
		} else {
			return mid, true
		}
	}
	return low, false
}

func (n *Node) get(key []byte) ([]byte, bool, error) {
	if !n.IsLeaf {
		return nil, false, errNotImplemented
	}
	idx, found := n.findKeyIndex(key)
	if !found {
		return nil, false, nil
	}
	value := append([]byte(nil), n.entries[idx].value...)
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
		return errNotImplemented
	}

	keyCopy := append([]byte(nil), key...)
	valueCopy := append([]byte(nil), value...)

	idx, found := n.findKeyIndex(key)
	if found {
		n.entries[idx].value = valueCopy
		return nil
	}

	n.entries = append(n.entries, entry{})
	copy(n.entries[idx+1:], n.entries[idx:])
	n.entries[idx] = entry{key: keyCopy, value: valueCopy}
	return nil
}

func readNode(r io.Reader) (*Node, error) {
	var isLeafByte [1]byte
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
		key, err := readBytes(r)
		if err != nil {
			return nil, fmt.Errorf("read node key %d: %w", i, err)
		}
		if node.IsLeaf {
			value, err := readBytes(r)
			if err != nil {
				return nil, fmt.Errorf("read node value %d: %w", i, err)
			}
			node.entries = append(node.entries, entry{key: key, value: value})
		} else {
			node.entries = append(node.entries, entry{key: key})
		}
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

func writeNode(w io.Writer, node *Node) error {
	isLeafByte := byte(0)
	if node.IsLeaf {
		isLeafByte = 1
	}
	if err := writeFull(w, []byte{isLeafByte}); err != nil {
		return fmt.Errorf("write node isLeaf: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(node.entries))); err != nil {
		return fmt.Errorf("write node key count: %w", err)
	}

	for _, e := range node.entries {
		if err := writeBytes(w, e.key); err != nil {
			return fmt.Errorf("write node key: %w", err)
		}
		if node.IsLeaf {
			if err := writeBytes(w, e.value); err != nil {
				return fmt.Errorf("write node value: %w", err)
			}
		}
	}

	if !node.IsLeaf {
		for _, child := range node.Children {
			if err := binary.Write(w, binary.LittleEndian, uint64(child)); err != nil {
				return fmt.Errorf("write node child pgid: %w", err)
			}
		}
	}

	return nil
}

func readBytes(r io.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}
	if length > MaxValueSize {
		return nil, fmt.Errorf("entry length %d exceeds max", length)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}

func writeBytes(w io.Writer, data []byte) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(data))); err != nil {
		return err
	}
	return writeFull(w, data)
}
