package btree

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
)

const (
	// Node scalar fields and key/value lengths use uint32 values, which occupy
	// four bytes when encoded.
	encodedUint32Size = 4
	// A node starts with a one-byte leaf marker and a uint32 entry count.
	nodeHeaderSize = 1 + encodedUint32Size
)

// DecodeNode decodes one bounded node from data.
// On success, the node owns data and keeps read-only key and value slices that
// refer to it. The caller must not change or reuse data after a successful call.
func DecodeNode(data []byte) (*Node, error) {
	if len(data) < nodeHeaderSize {
		return nil, fmt.Errorf("read node header: %w", ErrInvalid)
	}
	node := &Node{IsLeaf: data[0] != 0}
	count := binary.LittleEndian.Uint32(data[1:nodeHeaderSize])
	minimumEntrySize := 2 * encodedUint32Size // Flags and key length.
	if node.IsLeaf {
		minimumEntrySize += encodedUint32Size // Value length.
	}
	if uint64(count) > uint64(len(data)-nodeHeaderSize)/uint64(minimumEntrySize) {
		return nil, fmt.Errorf("read node entry count: %w", ErrInvalid)
	}
	node.entries = make([]Entry, 0, int(count))
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

// decodeLengthPrefixedBytes reads one little-endian uint32 length followed by that number of bytes.
// It returns the value and the unread input without copying the value.
// The value refers to data and must stay read-only.
// It returns ErrInvalid when data does not contain the full length or value.
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
	// The third index sets the capacity to size.
	// This forces append to allocate instead of writing into the unread part of data.
	return data[:size:size], data[size:], nil
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
	data := make([]byte, 0, node.EncodedSize())
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
