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
// Child i+1 is on the right side of separator key i.
// node_codec.go contains the node header, checksums, and the rules that leaves and branches share. continuation_codec.go reads and writes the pages of a continued node.
const branchEntryDescriptorSize = 2 * encodedUint32Size

func branchPayloadSize(node *Node) int {
	size := page.IDSize
	for _, entry := range node.entries {
		size += branchEntryDescriptorSize + page.IDSize + len(entry.key)
	}
	return size
}

func branchBodyOffset(node *Node) int {
	return NodeHeaderSize + len(node.entries)*branchEntryDescriptorSize
}

func branchDirectorySize(node *Node) int {
	return len(node.entries)*branchEntryDescriptorSize + (len(node.entries)+1)*page.IDSize
}

// putBranchDirectory writes one descriptor for each separator, then every child page ID, into dst.
// The separator keys follow the directory in entry order.
func putBranchDirectory(dst []byte, node *Node) {
	children := node.Children[:len(node.entries)+1]
	childrenStart := len(node.entries) * branchEntryDescriptorSize
	offset := NodeHeaderSize + childrenStart + len(children)*page.IDSize
	for index, entry := range node.entries {
		descriptor := dst[index*branchEntryDescriptorSize : (index+1)*branchEntryDescriptorSize]
		binary.LittleEndian.PutUint32(descriptor[:4], uint32(offset))
		binary.LittleEndian.PutUint32(descriptor[4:8], uint32(len(entry.key)))
		offset += len(entry.key)
	}
	for index, id := range children {
		start := childrenStart + index*page.IDSize
		binary.LittleEndian.PutUint64(dst[start:start+page.IDSize], uint64(id))
	}
}

// putBranchBody writes the directory and keys of a branch that fits in one page.
// image has the compact size of the node and starts with its header.
func putBranchBody(image []byte, node *Node) {
	offset := NodeHeaderSize + branchDirectorySize(node)
	putBranchDirectory(image[NodeHeaderSize:offset], node)
	for _, entry := range node.entries {
		offset += copy(image[offset:], entry.key)
	}
}

// branchKey reads the key bounds of separator index of a branch image without checks.
// The key is at image[start:end]. The 64-bit sum of the 32-bit fields cannot overflow.
func branchKey(image []byte, index int) (start, end uint64) {
	// A constant slice length lets the compiler drop the bounds checks of the loads.
	at := NodeHeaderSize + index*branchEntryDescriptorSize
	fields := image[at : at+branchEntryDescriptorSize : at+branchEntryDescriptorSize]
	start = uint64(binary.LittleEndian.Uint32(fields[:4]))
	end = start + uint64(binary.LittleEndian.Uint32(fields[4:8]))
	return start, end
}

// branchChild reads child page ID index of a branch image that has count separators.
// ok is false for a metadata page ID.
func branchChild(image []byte, count, index int) (page.ID, bool) {
	// The child IDs follow the descriptors of the separators.
	at := NodeHeaderSize + count*branchEntryDescriptorSize + index*page.IDSize
	child := page.ID(binary.LittleEndian.Uint64(image[at : at+page.IDSize]))
	return child, !page.IsMetaID(child)
}

func childError(index int) error {
	return fmt.Errorf("read node child %d: %w", index, ErrInvalid)
}

// branchDataStart checks that count separators and count+1 child IDs fit in a branch image of imageSize bytes and returns the offset of the first key, which follows them.
func branchDataStart(imageSize uint64, count uint32) (uint64, error) {
	// Each separator has one descriptor and one child ID. A branch has one more child ID than separators.
	size := uint64(count)*(branchEntryDescriptorSize+page.IDSize) + page.IDSize
	if size > imageSize-NodeHeaderSize {
		return 0, errEntryCount
	}
	return NodeHeaderSize + size, nil
}

// decodeBranchBody decodes every child ID and separator of one contiguous branch image.
// The separator keys borrow image. h becomes the node header.
// exact requires the last key to end at the end of image.
func decodeBranchBody(image []byte, h NodeHeader, exact bool) (*Node, error) {
	dataStart, err := branchDataStart(uint64(len(image)), h.EntryCount)
	if err != nil {
		return nil, err
	}
	count, imageSize := int(h.EntryCount), uint64(len(image))
	children := make([]page.ID, count+1)
	for index := range children {
		child, ok := branchChild(image, count, index)
		if !ok {
			return nil, childError(index)
		}
		children[index] = child
	}
	entries := make([]Entry, count)
	next, previous := dataStart, []byte(nil)
	for index := range count {
		start, end := branchKey(image, index)
		if !validKey(start, end, dataStart, imageSize) {
			return nil, entryError(index)
		}
		key := image[start:end:end]
		if !inOrder(start, next, previous, key) {
			return nil, entryError(index)
		}
		next, previous = end, key
		// Set only the key, for the same reason as in decodeLeafBody.
		entries[index].key = key
	}
	if exact && next != imageSize {
		return nil, errBodySize
	}
	return &Node{header: h, entries: entries, Children: children}, nil
}

// checkBranchBody checks every child ID and separator of one contiguous branch image in the same way as decodeBranchBody, without a Node.
// visit receives each child page ID. visit can be nil.
func checkBranchBody(image []byte, h NodeHeader, exact bool, visit func(page.ID) error) error {
	dataStart, err := branchDataStart(uint64(len(image)), h.EntryCount)
	if err != nil {
		return err
	}
	count, imageSize := int(h.EntryCount), uint64(len(image))
	for index := range count + 1 {
		child, ok := branchChild(image, count, index)
		if !ok {
			return childError(index)
		}
		if visit != nil {
			if err := visit(child); err != nil {
				return err
			}
		}
	}
	next, previous := dataStart, []byte(nil)
	for index := range count {
		start, end := branchKey(image, index)
		if !validKey(start, end, dataStart, imageSize) {
			return entryError(index)
		}
		key := image[start:end:end]
		if !inOrder(start, next, previous, key) {
			return entryError(index)
		}
		next, previous = end, key
	}
	if exact && next != imageSize {
		return errBodySize
	}
	return nil
}

// lookupBranchBody returns the child that routes key in a contiguous branch image.
// That child is on the right side of the last separator that is not larger than key, so a key equal to a separator goes right.
// It reads only the separators on the search path.
func lookupBranchBody(image []byte, h NodeHeader, key []byte) (page.ID, error) {
	dataStart, err := branchDataStart(uint64(len(image)), h.EntryCount)
	if err != nil {
		return 0, err
	}
	count, imageSize := int(h.EntryCount), uint64(len(image))
	// Find the first separator that is larger than key. Its index is also the index of the child.
	low, high := 0, count
	for low < high {
		middle := (low + high) / 2
		start, end := branchKey(image, middle)
		if !validKey(start, end, dataStart, imageSize) {
			return 0, entryError(middle)
		}
		if bytes.Compare(image[start:end], key) <= 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	child, ok := branchChild(image, count, low)
	if !ok {
		return 0, childError(low)
	}
	return child, nil
}
