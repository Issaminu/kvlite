package btree

import (
	"bytes"
	"encoding/binary"

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
// node_codec.go contains the node header, checksums, and the rules that leaves and branches share. continuation_codec.go reads and writes the pages of a continued node.
const leafEntryDescriptorSize = 4 * encodedUint32Size

func leafPayloadSize(node *Node) int {
	size := 0
	for _, entry := range node.entries {
		size += leafEntryDescriptorSize + len(entry.key) + len(entry.value)
	}
	return size
}

func leafBodyOffset(node *Node) int {
	return NodeHeaderSize + len(node.entries)*leafEntryDescriptorSize
}

func leafDirectorySize(node *Node) int {
	return len(node.entries) * leafEntryDescriptorSize
}

// putLeafDescriptor writes the descriptor of entry, whose key starts at offset.
func putLeafDescriptor(descriptor []byte, entry *Entry, offset int) {
	binary.LittleEndian.PutUint32(descriptor[:4], entry.flags)
	binary.LittleEndian.PutUint32(descriptor[4:8], uint32(offset))
	binary.LittleEndian.PutUint32(descriptor[8:12], uint32(len(entry.key)))
	binary.LittleEndian.PutUint32(descriptor[12:16], uint32(len(entry.value)))
}

// putLeafBody writes the descriptors, keys, and values of a leaf that fits in one page.
// image has the compact size of the node and starts with its header.
func putLeafBody(image []byte, node *Node) {
	offset := leafBodyOffset(node)
	for index := range node.entries {
		entry := &node.entries[index]
		putLeafDescriptor(image[NodeHeaderSize+index*leafEntryDescriptorSize:], entry, offset)
		offset += copy(image[offset:], entry.key)
		offset += copy(image[offset:], entry.value)
	}
}

// putLeafDirectory writes only the descriptors of a continued leaf into dst.
// The keys and values follow the directory in entry order.
func putLeafDirectory(dst []byte, node *Node) {
	offset := leafBodyOffset(node)
	for index := range node.entries {
		entry := &node.entries[index]
		putLeafDescriptor(dst[index*leafEntryDescriptorSize:], entry, offset)
		offset += len(entry.key) + len(entry.value)
	}
}

// leafSpan reads descriptor index of a leaf image without checks.
// The key is at image[start:keyEnd], and the value is at image[keyEnd:end].
// The 64-bit sums of the 32-bit fields cannot overflow.
// It makes no calls, so the compiler inlines it in each entry loop.
func leafSpan(image []byte, index int) (flags uint32, start, keyEnd, end uint64) {
	// A constant slice length lets the compiler drop the bounds checks of the loads.
	at := NodeHeaderSize + index*leafEntryDescriptorSize
	fields := image[at : at+leafEntryDescriptorSize : at+leafEntryDescriptorSize]
	flags = binary.LittleEndian.Uint32(fields[:4])
	start = uint64(binary.LittleEndian.Uint32(fields[4:8]))
	keyEnd = start + uint64(binary.LittleEndian.Uint32(fields[8:12]))
	end = keyEnd + uint64(binary.LittleEndian.Uint32(fields[12:16]))
	return flags, start, keyEnd, end
}

// validLeaf reports whether the fields that leafSpan read are valid: the key passes validKey, the flags and value size are valid, and the value ends inside the image.
func validLeaf(flags uint32, start, keyEnd, end, dataStart, imageSize uint64) bool {
	return validKey(start, keyEnd, dataStart, imageSize) && flags&^BucketLeafFlag == 0 && end-keyEnd <= MaxValueSize && end <= imageSize
}

// leafDataStart checks that count descriptors fit in a leaf image of imageSize bytes and returns the offset of the first key, which follows them.
func leafDataStart(imageSize uint64, count uint32) (uint64, error) {
	if uint64(count)*leafEntryDescriptorSize > imageSize-NodeHeaderSize {
		return 0, errEntryCount
	}
	return NodeHeaderSize + uint64(count)*leafEntryDescriptorSize, nil
}

// decodeLeafBody decodes every entry of one contiguous leaf image.
// The entry keys and values borrow image. h becomes the node header.
// exact requires the last entry to end at the end of image.
func decodeLeafBody(image []byte, h NodeHeader, exact bool) (*Node, error) {
	dataStart, err := leafDataStart(uint64(len(image)), h.EntryCount)
	if err != nil {
		return nil, err
	}
	count, imageSize := int(h.EntryCount), uint64(len(image))
	entries := make([]Entry, count)
	next, previous := dataStart, []byte(nil)
	for index := range count {
		flags, start, keyEnd, end := leafSpan(image, index)
		if !validLeaf(flags, start, keyEnd, end, dataStart, imageSize) {
			return nil, entryError(index)
		}
		key := image[start:keyEnd:keyEnd]
		if !inOrder(start, next, previous, key) {
			return nil, entryError(index)
		}
		next, previous = end, key
		// Set the fields one at a time.
		// While the garbage collector runs, a store of a complete Entry needs a slower bulk write barrier.
		entry := &entries[index]
		entry.flags, entry.key, entry.value = flags, key, image[keyEnd:end:end]
	}
	if exact && next != imageSize {
		return nil, errBodySize
	}
	return &Node{header: h, entries: entries}, nil
}

// checkLeafBody checks every entry of one contiguous leaf image in the same way as decodeLeafBody, without a Node.
// A nested bucket entry must store an 8-byte root page ID. visit receives each root page ID. visit can be nil.
func checkLeafBody(image []byte, h NodeHeader, exact bool, visit func(page.ID) error) error {
	dataStart, err := leafDataStart(uint64(len(image)), h.EntryCount)
	if err != nil {
		return err
	}
	imageSize := uint64(len(image))
	next, previous := dataStart, []byte(nil)
	for index := range int(h.EntryCount) {
		flags, start, keyEnd, end := leafSpan(image, index)
		if !validLeaf(flags, start, keyEnd, end, dataStart, imageSize) {
			return entryError(index)
		}
		key := image[start:keyEnd:keyEnd]
		if !inOrder(start, next, previous, key) {
			return entryError(index)
		}
		next, previous = end, key
		if flags&BucketLeafFlag != 0 {
			if end-keyEnd != page.IDSize {
				return entryError(index)
			}
			if visit != nil {
				if err := visit(page.ID(binary.LittleEndian.Uint64(image[keyEnd:end]))); err != nil {
					return err
				}
			}
		}
	}
	if exact && next != imageSize {
		return errBodySize
	}
	return nil
}

// lookupLeafBody finds key in a contiguous leaf image with a binary search.
// It reads only the descriptors and keys on the search path.
// The returned key and value borrow image. Set includeKey to false to omit the key.
func lookupLeafBody(image []byte, entryCount uint32, key []byte, includeKey bool) (Entry, bool, error) {
	dataStart, err := leafDataStart(uint64(len(image)), entryCount)
	if err != nil {
		return Entry{}, false, err
	}
	count, imageSize := int(entryCount), uint64(len(image))
	low, high := 0, count
	for low < high {
		middle := (low + high) / 2
		flags, start, keyEnd, end := leafSpan(image, middle)
		if !validLeaf(flags, start, keyEnd, end, dataStart, imageSize) {
			return Entry{}, false, entryError(middle)
		}
		switch comparison := bytes.Compare(image[start:keyEnd], key); {
		case comparison < 0:
			low = middle + 1
		case comparison > 0:
			high = middle
		default:
			entry := Entry{flags: flags, value: image[keyEnd:end:end]}
			if includeKey {
				entry.key = image[start:keyEnd:keyEnd]
			}
			return entry, true, nil
		}
	}
	// A missing key also checks the descriptor at its sorted position.
	if low < count {
		if flags, start, keyEnd, end := leafSpan(image, low); !validLeaf(flags, start, keyEnd, end, dataStart, imageSize) {
			return Entry{}, false, entryError(low)
		}
	}
	return Entry{}, false, nil
}

// mappedLeafImage checks the header of a mapped leaf for an ordered scan and returns one contiguous image of the leaf.
// A one-page leaf returns data. A continued leaf returns a rebuilt image, because the scan returns its values.
// A rebuilt image gets the full checks of checkLeafBody. A one-page image gets only the shorter checks of each scan.
// A branch returns ErrNotLeafNode before any other check, so the caller can read it as a branch.
// It returns only the image: a scan that gets more results from this call is slower.
func mappedLeafImage(data []byte, id page.ID, checksums bool, readPage func(page.ID) ([]byte, error)) ([]byte, error) {
	switch mappedNodeType(data) {
	case NodeTypeBranch, NodeTypeBranch | overflowNodeFlag:
		return nil, ErrNotLeafNode
	case NodeTypeLeaf | overflowNodeFlag:
		image, h, _, err := nodeImage(data, id, int64(len(data)), readPage, false, checksums)
		if err != nil {
			return nil, err
		}
		if err := checkLeafBody(image, h, true, nil); err != nil {
			return nil, err
		}
		return image, nil
	}
	if _, _, ok := plainNodeHeader(data, id, int64(len(data)), false, checksums); ok {
		return data, nil
	}
	if _, err := readNodeHeader(data, id, int64(len(data)), false, checksums); err != nil {
		return nil, err
	}
	return data, nil
}

// VisitMappedLeafRange visits entries in the half-open range [start, end).
// It checks descriptor offsets and byte bounds. checksums selects nonzero checksum checks.
// Normal leaf results borrow data. Continued leaf results borrow a rebuilt image.
// readPage supplies continuation pages. Pass nil for a normal leaf.
func VisitMappedLeafRange(data []byte, expectedPageID page.ID, start, end []byte, visit func(uint32, []byte, []byte) error, checksums bool, readPage func(page.ID) ([]byte, error)) error {
	image, err := mappedLeafImage(data, expectedPageID, checksums, readPage)
	if err != nil {
		return err
	}
	count := binary.LittleEndian.Uint32(image[nodeEntryCountOffset:nodeChecksumOffset])
	dataStart, err := leafDataStart(uint64(len(image)), count)
	if err != nil {
		return err
	}
	// Each entry must start where the previous entry ends and must end inside the image.
	// The loop values are int values: uint32 and uint64 loop values made the scan about 7% slower.
	next := int(dataStart)
	for index := range int(count) {
		flags, keyStart, keyEnd, valueEnd := leafSpan(image, index)
		if keyStart != uint64(next) || valueEnd > uint64(len(image)) {
			return entryError(index)
		}
		next = int(valueEnd)
		key := image[int(keyStart):int(keyEnd):int(keyEnd)]
		if len(start) > 0 && bytes.Compare(key, start) < 0 {
			continue
		}
		if len(end) > 0 && bytes.Compare(key, end) >= 0 {
			return nil
		}
		if err := visit(flags, key, image[int(keyEnd):int(valueEnd):int(valueEnd)]); err != nil {
			return err
		}
	}
	return nil
}

// VisitMappedLeafReverse visits one mapped leaf from its largest key to its smallest key.
// It checks the same fields as VisitMappedLeafRange.
// Normal leaf results borrow data. Continued leaf results borrow a rebuilt image.
// checksums selects nonzero checksum checks. readPage supplies continuation pages.
// Pass nil for readPage when the leaf has no continuation pages.
func VisitMappedLeafReverse(data []byte, expectedPageID page.ID, visit func(uint32, []byte, []byte) error, checksums bool, readPage func(page.ID) ([]byte, error)) error {
	image, err := mappedLeafImage(data, expectedPageID, checksums, readPage)
	if err != nil {
		return err
	}
	count := binary.LittleEndian.Uint32(image[nodeEntryCountOffset:nodeChecksumOffset])
	dataStart, err := leafDataStart(uint64(len(image)), count)
	if err != nil || count == 0 {
		return err
	}
	// The first entry must start the data area, and the last entry must end inside the image.
	// Then each entry that ends where the following entry starts also ends inside the image.
	last := int(count) - 1
	_, _, _, next := leafSpan(image, last)
	if _, first, _, _ := leafSpan(image, 0); first != dataStart {
		return entryError(0)
	}
	if next > uint64(len(image)) {
		return entryError(last)
	}
	for index := last; index >= 0; index-- {
		flags, keyStart, keyEnd, valueEnd := leafSpan(image, index)
		if keyStart < dataStart || valueEnd != next {
			return entryError(index)
		}
		next = keyStart
		if err := visit(flags, image[keyStart:keyEnd:keyEnd], image[keyEnd:valueEnd:valueEnd]); err != nil {
			return err
		}
	}
	return nil
}
