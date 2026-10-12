package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/Issaminu/kvlite/internal/page"
)

// A leaf body contains N descriptors, then N packed key and value pairs.
// Each descriptor has four little-endian uint32 fields:
//
//	[0:4]   flags
//	[4:8]   data offset from the start of the node
//	[8:12]  key size
//	[12:16] value size
//
// The key begins at the data offset. The value follows the key.
const leafEntryDescriptorSize = 4 * encodedUint32Size

// encodeLeafBody writes the descriptor directory, then the packed keys and values.
func encodeLeafBody(data []byte, node *Node) {
	payloadOffset := NodeHeaderSize + len(node.entries)*leafEntryDescriptorSize
	for index, entry := range node.entries {
		start := NodeHeaderSize + index*leafEntryDescriptorSize
		descriptor := data[start : start+leafEntryDescriptorSize]
		binary.LittleEndian.PutUint32(descriptor[:encodedUint32Size], entry.flags)
		binary.LittleEndian.PutUint32(descriptor[encodedUint32Size:2*encodedUint32Size], uint32(payloadOffset))
		binary.LittleEndian.PutUint32(descriptor[2*encodedUint32Size:3*encodedUint32Size], uint32(len(entry.key)))
		binary.LittleEndian.PutUint32(descriptor[3*encodedUint32Size:], uint32(len(entry.value)))
		payloadOffset += copy(data[payloadOffset:], entry.key)
		payloadOffset += copy(data[payloadOffset:], entry.value)
	}
}

// VisitMappedLeafRange visits entries in the half-open range [start, end).
// It checks descriptor offsets and byte bounds. It does not check the page checksum or unused padding.
// The key and value passed to visit refer to data.
func VisitMappedLeafRange(data []byte, expectedPageID page.ID, start, end []byte, visit func(uint32, []byte, []byte) error) error {
	if len(data) < NodeHeaderSize {
		return fmt.Errorf("read leaf page: %w", ErrInvalid)
	}
	if nodeType := NodeType(binary.LittleEndian.Uint16(data[nodeTypeOffset:nodePageIDOffset])); nodeType != NodeTypeLeaf {
		if nodeType == NodeTypeBranch {
			return ErrNotLeafNode
		}
		return fmt.Errorf("read leaf type %d: %w", nodeType, ErrInvalid)
	}
	if pageID := page.ID(binary.LittleEndian.Uint64(data[nodePageIDOffset:nodeEntryCountOffset])); pageID != expectedPageID {
		return fmt.Errorf("read leaf page ID %d, want %d: %w", pageID, expectedPageID, ErrInvalid)
	}

	entryCount := int(binary.LittleEndian.Uint32(data[nodeEntryCountOffset:nodeChecksumOffset]))
	if entryCount > (len(data)-NodeHeaderSize)/leafEntryDescriptorSize {
		return fmt.Errorf("read leaf entry count: %w", ErrInvalid)
	}
	payloadStart := NodeHeaderSize + entryCount*leafEntryDescriptorSize
	nextOffset := payloadStart
	for index := 0; index < entryCount; index++ {
		descriptorStart := NodeHeaderSize + index*leafEntryDescriptorSize
		descriptor := data[descriptorStart : descriptorStart+leafEntryDescriptorSize]
		flags := binary.LittleEndian.Uint32(descriptor[:encodedUint32Size])
		offset := uint64(binary.LittleEndian.Uint32(descriptor[encodedUint32Size : 2*encodedUint32Size]))
		keyEnd := offset + uint64(binary.LittleEndian.Uint32(descriptor[2*encodedUint32Size:3*encodedUint32Size]))
		valueEnd := keyEnd + uint64(binary.LittleEndian.Uint32(descriptor[3*encodedUint32Size:]))
		if offset != uint64(nextOffset) || valueEnd > uint64(len(data)) {
			return fmt.Errorf("read leaf entry %d: %w", index, ErrInvalid)
		}
		nextOffset = int(valueEnd)
		key := data[int(offset):int(keyEnd):int(keyEnd)]
		if len(start) > 0 && bytes.Compare(key, start) < 0 {
			continue
		}
		if len(end) > 0 && bytes.Compare(key, end) >= 0 {
			return nil
		}
		if err := visit(flags, key, data[int(keyEnd):int(valueEnd):int(valueEnd)]); err != nil {
			return err
		}
	}
	return nil
}

// VisitMappedLeafReverse visits one mapped leaf from its largest key to its smallest key.
// The key and value slices refer to data.
func VisitMappedLeafReverse(data []byte, expectedPageID page.ID, visit func(uint32, []byte, []byte) error) error {
	if len(data) < NodeHeaderSize {
		return fmt.Errorf("read leaf page: %w", ErrInvalid)
	}
	if nodeType := NodeType(binary.LittleEndian.Uint16(data[nodeTypeOffset:nodePageIDOffset])); nodeType != NodeTypeLeaf {
		if nodeType == NodeTypeBranch {
			return ErrNotLeafNode
		}
		return fmt.Errorf("read leaf type %d: %w", nodeType, ErrInvalid)
	}
	if pageID := page.ID(binary.LittleEndian.Uint64(data[nodePageIDOffset:nodeEntryCountOffset])); pageID != expectedPageID {
		return fmt.Errorf("read leaf page ID %d, want %d: %w", pageID, expectedPageID, ErrInvalid)
	}

	entryCount := int(binary.LittleEndian.Uint32(data[nodeEntryCountOffset:nodeChecksumOffset]))
	if entryCount > (len(data)-NodeHeaderSize)/leafEntryDescriptorSize {
		return fmt.Errorf("read leaf entry count: %w", ErrInvalid)
	}
	payloadStart := NodeHeaderSize + entryCount*leafEntryDescriptorSize
	if entryCount > 0 {
		firstOffset := binary.LittleEndian.Uint32(data[NodeHeaderSize+encodedUint32Size : NodeHeaderSize+2*encodedUint32Size])
		if uint64(firstOffset) != uint64(payloadStart) {
			return fmt.Errorf("read leaf entry 0: %w", ErrInvalid)
		}
	}
	var nextOffset uint64
	for index := entryCount - 1; index >= 0; index-- {
		descriptorStart := NodeHeaderSize + index*leafEntryDescriptorSize
		descriptor := data[descriptorStart : descriptorStart+leafEntryDescriptorSize]
		flags := binary.LittleEndian.Uint32(descriptor[:encodedUint32Size])
		offset := uint64(binary.LittleEndian.Uint32(descriptor[encodedUint32Size : 2*encodedUint32Size]))
		keyEnd := offset + uint64(binary.LittleEndian.Uint32(descriptor[2*encodedUint32Size:3*encodedUint32Size]))
		valueEnd := keyEnd + uint64(binary.LittleEndian.Uint32(descriptor[3*encodedUint32Size:]))
		if offset < uint64(payloadStart) || valueEnd > uint64(len(data)) || (index < entryCount-1 && valueEnd != nextOffset) {
			return fmt.Errorf("read leaf entry %d: %w", index, ErrInvalid)
		}
		nextOffset = offset
		if err := visit(flags, data[int(offset):int(keyEnd):int(keyEnd)], data[int(keyEnd):int(valueEnd):int(valueEnd)]); err != nil {
			return err
		}
	}
	return nil
}

// readEncodedLeaf reads the leaf body after the shared header checks.
// It uses the decode, validation, and point lookup modes of readEncodedNode.
func readEncodedLeaf(data []byte, expectedPageID page.ID, storedChecksum uint32, key []byte, createNode bool, allowPadding bool, visit func(page.ID) error) (*Node, Entry, bool, page.ID, error) {
	entryCount := binary.LittleEndian.Uint32(data[nodeEntryCountOffset:nodeChecksumOffset])
	if uint64(entryCount) > uint64((len(data)-NodeHeaderSize)/leafEntryDescriptorSize) {
		return nil, Entry{}, false, 0, fmt.Errorf("read node entry count: %w", ErrInvalid)
	}
	payloadStart := NodeHeaderSize + int(entryCount)*leafEntryDescriptorSize

	readEntry := func(index uint32) (Entry, int, int, error) {
		start := NodeHeaderSize + int(index)*leafEntryDescriptorSize
		fields := data[start : start+leafEntryDescriptorSize]
		flags := binary.LittleEndian.Uint32(fields[:encodedUint32Size])
		offset := binary.LittleEndian.Uint32(fields[encodedUint32Size : 2*encodedUint32Size])
		keySize := binary.LittleEndian.Uint32(fields[2*encodedUint32Size : 3*encodedUint32Size])
		valueSize := binary.LittleEndian.Uint32(fields[3*encodedUint32Size:])
		if keySize == 0 || uint64(keySize) > uint64(MaxKeySize) {
			return Entry{}, 0, 0, fmt.Errorf("read node key %d: %w", index, ErrInvalid)
		}
		if flags&^uint32(BucketLeafFlag) != 0 || uint64(valueSize) > uint64(MaxValueSize) {
			return Entry{}, 0, 0, fmt.Errorf("read node entry %d: %w", index, ErrInvalid)
		}
		keyEnd := uint64(offset) + uint64(keySize)
		end := keyEnd + uint64(valueSize)
		if uint64(offset) < uint64(payloadStart) || end > uint64(len(data)) {
			return Entry{}, 0, 0, fmt.Errorf("read node entry %d: %w", index, ErrInvalid)
		}
		return Entry{flags: flags, key: data[int(offset):int(keyEnd):int(keyEnd)],
			value: data[int(keyEnd):int(end):int(end)]}, int(offset), int(end), nil
	}

	var node *Node
	if createNode {
		node = &Node{
			header: &NodeHeader{
				Type:       NodeTypeLeaf,
				PageID:     expectedPageID,
				EntryCount: entryCount,
				Checksum:   storedChecksum,
			},
			entries: make([]Entry, 0, int(entryCount)),
		}
	}

	if createNode || key == nil {
		nextOffset := payloadStart
		var previousKey []byte

		for index := uint32(0); index < entryCount; index++ {
			entry, start, end, err := readEntry(index)
			if err != nil {
				return nil, Entry{}, false, 0, err
			}
			if start != nextOffset {
				return nil, Entry{}, false, 0, fmt.Errorf("read node entry offset %d: %w", index, ErrInvalid)
			}
			if index > 0 && bytes.Compare(previousKey, entry.key) >= 0 {
				return nil, Entry{}, false, 0, fmt.Errorf("read node key order %d: %w", index, ErrInvalid)
			}
			previousKey = entry.key
			nextOffset = end
			if visit != nil && entry.flags&BucketLeafFlag != 0 {
				child, err := page.DecodeID(entry.value)
				if err != nil {
					return nil, Entry{}, false, 0, err
				}
				if err := visit(child); err != nil {
					return nil, Entry{}, false, 0, err
				}
			}
			if createNode {
				node.entries = append(node.entries, entry)
			}
		}

		if !allowPadding && nextOffset != len(data) {
			return nil, Entry{}, false, 0, fmt.Errorf("read WAL node size: %w", ErrInvalid)
		}
		return node, Entry{}, false, 0, nil
	}

	low, high := uint32(0), entryCount
	for low < high {
		middle := low + (high-low)/2
		entry, _, _, err := readEntry(middle)
		if err != nil {
			return nil, Entry{}, false, 0, err
		}
		comparison := bytes.Compare(entry.key, key)
		// Find the first key at or above key.
		if comparison >= 0 {
			high = middle
		} else {
			low = middle + 1
		}
	}

	if low == entryCount {
		return nil, Entry{}, false, 0, nil
	}
	entry, _, _, err := readEntry(low)
	if err != nil {
		return nil, Entry{}, false, 0, err
	}
	if !bytes.Equal(entry.key, key) {
		return nil, Entry{}, false, 0, nil
	}
	return nil, entry, true, 0, nil
}
