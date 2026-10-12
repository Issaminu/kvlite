package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/Issaminu/kvlite/internal/page"
)

// A branch body contains N descriptors, N+1 child page IDs, then N packed keys.
// Each descriptor has two little-endian uint32 fields:
//
//	[0:4] data offset from the start of the node
//	[4:8] key size
//
// The child IDs start at NodeHeaderSize + N*branchEntryDescriptorSize.
// Each child ID uses eight bytes. A key starts at its descriptor's data offset.
const branchEntryDescriptorSize = 2 * encodedUint32Size

// encodeBranchBody writes descriptors, child page IDs, then the packed separator keys.
func encodeBranchBody(data []byte, node *Node) {
	childrenStart := NodeHeaderSize + len(node.entries)*branchEntryDescriptorSize
	for index := 0; index <= len(node.entries); index++ {
		start := childrenStart + index*page.IDSize
		binary.LittleEndian.PutUint64(data[start:start+page.IDSize], uint64(node.Children[index]))
	}
	payloadOffset := childrenStart + (len(node.entries)+1)*page.IDSize
	for index, entry := range node.entries {
		start := NodeHeaderSize + index*branchEntryDescriptorSize
		descriptor := data[start : start+branchEntryDescriptorSize]
		binary.LittleEndian.PutUint32(descriptor[:encodedUint32Size], uint32(payloadOffset))
		binary.LittleEndian.PutUint32(descriptor[encodedUint32Size:], uint32(len(entry.key)))
		payloadOffset += copy(data[payloadOffset:], entry.key)
	}
}

// readEncodedBranch reads the branch body after the shared header checks.
// It uses the decode, validation, and point lookup modes of readEncodedNode.
func readEncodedBranch(data []byte, expectedPageID page.ID, storedChecksum uint32, key []byte, createNode bool, allowPadding bool, visit func(page.ID) error) (*Node, Entry, bool, page.ID, error) {
	entryCount := binary.LittleEndian.Uint32(data[nodeEntryCountOffset:nodeChecksumOffset])
	// Each separator has one descriptor and child ID. Count the extra child first.
	payloadSize := len(data) - NodeHeaderSize - page.IDSize
	if payloadSize < 0 {
		return nil, Entry{}, false, 0, fmt.Errorf("read node children: %w", ErrInvalid)
	}
	if uint64(entryCount) > uint64(payloadSize/(branchEntryDescriptorSize+page.IDSize)) {
		return nil, Entry{}, false, 0, fmt.Errorf("read node entry count: %w", ErrInvalid)
	}
	childrenStart := NodeHeaderSize + int(entryCount)*branchEntryDescriptorSize
	payloadStart := childrenStart + (int(entryCount)+1)*page.IDSize

	readEntry := func(index uint32) (Entry, int, int, error) {
		start := NodeHeaderSize + int(index)*branchEntryDescriptorSize
		fields := data[start : start+branchEntryDescriptorSize]
		offset := binary.LittleEndian.Uint32(fields[:encodedUint32Size])
		keySize := binary.LittleEndian.Uint32(fields[encodedUint32Size:])
		if keySize == 0 || uint64(keySize) > uint64(MaxKeySize) {
			return Entry{}, 0, 0, fmt.Errorf("read node key %d: %w", index, ErrInvalid)
		}
		end := uint64(offset) + uint64(keySize)
		if uint64(offset) < uint64(payloadStart) || end > uint64(len(data)) {
			return Entry{}, 0, 0, fmt.Errorf("read node entry %d: %w", index, ErrInvalid)
		}
		return Entry{key: data[int(offset):int(end):int(end)]}, int(offset), int(end), nil
	}

	readChild := func(index uint32) (page.ID, error) {
		start := childrenStart + int(index)*page.IDSize
		child := page.ID(binary.LittleEndian.Uint64(data[start : start+page.IDSize]))
		if page.IsMetaID(child) {
			return 0, fmt.Errorf("read node child page ID %d: %w", child, ErrInvalid)
		}
		return child, nil
	}

	var node *Node
	if createNode {
		node = &Node{
			header: &NodeHeader{
				Type:       NodeTypeBranch,
				PageID:     expectedPageID,
				EntryCount: entryCount,
				Checksum:   storedChecksum,
			},
			entries: make([]Entry, 0, int(entryCount)),
		}

		node.Children = make([]page.ID, 0, int(entryCount)+1)
	}

	if createNode || key == nil {
		nextOffset := payloadStart
		var previousKey []byte

		for index := uint32(0); index <= entryCount; index++ {
			child, err := readChild(index)
			if err != nil {
				return nil, Entry{}, false, 0, err
			}
			if visit != nil {
				if err := visit(child); err != nil {
					return nil, Entry{}, false, 0, err
				}
			}
			if createNode {
				node.Children = append(node.Children, child)
			}
		}

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
		// Find the first key above key.
		if comparison > 0 {
			high = middle
		} else {
			low = middle + 1
		}
	}

	childPageID, err := readChild(low)
	if err != nil {
		return nil, Entry{}, false, 0, err
	}
	return nil, Entry{}, false, childPageID, nil
}
