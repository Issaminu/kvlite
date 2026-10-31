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
// node_codec.go contains the node header, checksums, and the rules that leaves and branches share. Large nodes use one consecutive span.
const leafEntryDescriptorSize = 4 * encodedUint32Size

func leafPayloadSize(node *MutableNode) int {
	size := 0
	for _, entry := range node.entries {
		size += leafEntryDescriptorSize + len(entry.key) + len(entry.value)
	}
	return size
}

func leafBodyOffset(node *MutableNode) int {
	return NodeHeaderSize + len(node.entries)*leafEntryDescriptorSize
}

// putLeafDescriptor writes the descriptor of entry, whose key starts at offset.
func putLeafDescriptor(descriptor []byte, entry *Entry, offset int) {
	binary.LittleEndian.PutUint32(descriptor[:4], entry.flags)
	binary.LittleEndian.PutUint32(descriptor[4:8], uint32(offset))
	binary.LittleEndian.PutUint32(descriptor[8:12], uint32(len(entry.key)))
	binary.LittleEndian.PutUint32(descriptor[12:16], uint32(len(entry.value)))
}

// putLeafBody writes the descriptors, keys, and values of one logical leaf.
// image has the compact size of the node and starts with its header.
func putLeafBody(image []byte, node *MutableNode) {
	offset := leafBodyOffset(node)
	for index := range node.entries {
		entry := &node.entries[index]
		putLeafDescriptor(image[NodeHeaderSize+index*leafEntryDescriptorSize:], entry, offset)
		offset += copy(image[offset:], entry.key)
		offset += copy(image[offset:], entry.value)
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

// validLeaf reports whether the fields that leafSpan read are valid: the key passes validKey, the value ends inside the image, and the flags and value size are valid.
// A plain value can have up to MaxValueSize bytes. A nested bucket entry stores an 8-byte root page ID.
// Every path that reads leaf entries uses this rule, so decode, lookup, and the reference check accept the same leaves.
// The mapped scans of a one-page leaf check only the flags and value rule (see VisitMappedLeafRange).
func validLeaf(flags uint32, start, keyEnd, end, dataStart, imageSize uint64) bool {
	valueSize := end - keyEnd
	return validKey(start, keyEnd, dataStart, imageSize) &&
		(flags == 0 && valueSize <= MaxValueSize || flags == BucketLeafFlag && valueSize == page.IDSize) && end <= imageSize
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
func decodeLeafBody(image []byte, h NodeHeader, exact bool) (*MutableNode, error) {
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
	return &MutableNode{header: h, entries: entries}, nil
}

// checkLeafBody checks every entry of one contiguous leaf image in the same way as decodeLeafBody, without a MutableNode.
// visit receives the root page ID of each nested bucket. visit can be nil.
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
		// validLeaf accepts a nested bucket entry only with an 8-byte root page ID.
		if flags&BucketLeafFlag != 0 && visit != nil {
			if err := visit(page.ID(binary.LittleEndian.Uint64(image[keyEnd:end]))); err != nil {
				return err
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
// The index is the position of the key, or where a missing key would be inserted.
// A found entry is written into entry, which must be zero (see lookupNode). Its key and value borrow image. Set includeKey to false to omit the key.
func lookupLeafBody(image []byte, entryCount uint32, key []byte, includeKey bool, entry *Entry) (int, bool, error) {
	dataStart, err := leafDataStart(uint64(len(image)), entryCount)
	if err != nil {
		return 0, false, err
	}
	count, imageSize := int(entryCount), uint64(len(image))
	low, high := 0, count
	for low < high {
		middle := (low + high) / 2
		flags, start, keyEnd, end := leafSpan(image, middle)
		if !validLeaf(flags, start, keyEnd, end, dataStart, imageSize) {
			return 0, false, entryError(middle)
		}
		switch comparison := bytes.Compare(image[start:keyEnd], key); {
		case comparison < 0:
			low = middle + 1
		case comparison > 0:
			high = middle
		default:
			entry.flags, entry.value = flags, image[keyEnd:end:end]
			if includeKey {
				entry.key = image[start:keyEnd:keyEnd]
			}
			return middle, true, nil
		}
	}
	// A missing key also checks the descriptor at its sorted position.
	if low < count {
		if flags, start, keyEnd, end := leafSpan(image, low); !validLeaf(flags, start, keyEnd, end, dataStart, imageSize) {
			return 0, false, entryError(low)
		}
	}
	return low, false, nil
}

// mappedLeafImage checks the header of a mapped leaf for an ordered scan and returns one contiguous image of the leaf.
// A complete mapped span returns borrowed logical bytes.
// A head-only input needs a raw-page reader and a temporary span copy.
// A branch returns ErrNotLeafNode before any other check, so the caller can read it as a branch.
// It returns only the image: a scan that gets more results from this call is slower.
func mappedLeafImage(data []byte, id page.ID, checksums bool, readPage func(page.ID) ([]byte, error)) ([]byte, error) {
	kind := mappedNodeType(data)
	if kind == NodeTypeBranch {
		return nil, ErrNotLeafNode
	}
	image, _, err := nodeImage(data, id, int64(len(data)), readPage, false, checksums)
	return image, err
}

// checkedLeafImage accepts a complete padded span after its header was checked.
// The caller must first use NodeSpan and CheckNodeChecksumMode.
func checkedLeafImage(data []byte, checksums bool) ([]byte, error) {
	kind := mappedNodeType(data)
	if kind == NodeTypeBranch {
		return nil, ErrNotLeafNode
	}
	if checksums {
		if err := checkPageChecksum(data, int64(len(data)), nodeChecksumOffset, true); err != nil {
			return nil, err
		}
	}
	size := NodeHeaderSize + int(binary.LittleEndian.Uint32(data[18:22]))
	return data[:size:size], nil
}

// VisitMappedLeafRange visits entries in the half-open range [start, end).
// It checks descriptor offsets, byte bounds, and the flags and value rule of validLeaf. checksums selects nonzero checksum checks.
// Results borrow the complete span. A head-only input first builds that span.
// readPage supplies continuation pages. Pass nil for a normal leaf.
func VisitMappedLeafRange(data []byte, expectedPageID page.ID, start, end []byte, visit func(uint32, []byte, []byte) error, checksums bool, readPage func(page.ID) ([]byte, error)) error {
	image, err := mappedLeafImage(data, expectedPageID, checksums, readPage)
	if err != nil {
		return err
	}
	return visitLeafRange(image, start, end, visit)
}

func visitLeafRange(image, start, end []byte, visit func(uint32, []byte, []byte) error) error {
	count := binary.LittleEndian.Uint32(image[nodeEntryCountOffset:nodeChecksumOffset])
	dataStart, err := leafDataStart(uint64(len(image)), count)
	if err != nil {
		return err
	}
	// Each entry must start where the previous entry ends and must end inside the image.
	// The flags must select a plain value or a nested bucket entry with an 8-byte root page ID, as validLeaf requires.
	// A plain value in one page cannot be larger than MaxValueSize, and checkLeafBody checked each rebuilt image.
	// The scan does not check key sizes: the complete validLeaf check made the scan about 5% slower.
	// The loop values are int values: uint32 and uint64 loop values made the scan about 7% slower.
	next := int(dataStart)
	for index := range int(count) {
		flags, keyStart, keyEnd, valueEnd := leafSpan(image, index)
		if keyStart != uint64(next) || valueEnd > uint64(len(image)) || flags != 0 && (flags != BucketLeafFlag || valueEnd-keyEnd != page.IDSize) {
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
// Results borrow the complete span. A head-only input first builds that span.
// checksums selects nonzero checksum checks. readPage supplies continuation pages.
// Pass nil for readPage when the leaf has no continuation pages.
func VisitMappedLeafReverse(data []byte, expectedPageID page.ID, visit func(uint32, []byte, []byte) error, checksums bool, readPage func(page.ID) ([]byte, error)) error {
	image, err := mappedLeafImage(data, expectedPageID, checksums, readPage)
	if err != nil {
		return err
	}
	return visitLeafReverse(image, visit)
}

func visitLeafReverse(image []byte, visit func(uint32, []byte, []byte) error) error {
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
		if keyStart < dataStart || valueEnd != next || flags != 0 && (flags != BucketLeafFlag || valueEnd-keyEnd != page.IDSize) {
			return entryError(index)
		}
		next = keyStart
		if err := visit(flags, image[keyStart:keyEnd:keyEnd], image[keyEnd:valueEnd:valueEnd]); err != nil {
			return err
		}
	}
	return nil
}

// VisitCheckedLeafRange visits entries in [start, end) from a checked complete span.
// The caller must first check the header with NodeSpan and CheckNodeChecksumMode.
// The data length must equal the page count times the database page size.
// It checks the same descriptors as VisitMappedLeafRange. checksums also checks the span checksum.
// Results borrow data. A callback error stops the visit and is returned unchanged.
// A branch returns ErrNotLeafNode without a checksum check.
func VisitCheckedLeafRange(data, start, end []byte, visit func(uint32, []byte, []byte) error, checksums bool) error {
	image, err := checkedLeafImage(data, checksums)
	if err != nil {
		return err
	}
	return visitLeafRange(image, start, end, visit)
}

// VisitCheckedLeafReverse visits a checked complete span from its largest key to its smallest key.
// The caller must first check the header with NodeSpan and CheckNodeChecksumMode.
// The data length must equal the page count times the database page size.
// It checks the same descriptors as VisitMappedLeafReverse. checksums also checks the span checksum.
// Results borrow data. A callback error stops the visit and is returned unchanged.
// A branch returns ErrNotLeafNode without a checksum check.
func VisitCheckedLeafReverse(data []byte, visit func(uint32, []byte, []byte) error, checksums bool) error {
	image, err := checkedLeafImage(data, checksums)
	if err != nil {
		return err
	}
	return visitLeafReverse(image, visit)
}

// PrepareMappedPut checks a mapped leaf and looks up key without changing bytes.
// data must hold a complete node with a checked header and checksum; key must be valid.
// A branch returns its next child. A leaf returns the key's position, whether it exists, and its borrowed entry.
// Unsupported continued leaves return found=false and index=0, which also prevents mapped insertion.
// entry must be zero before the call. Its bytes remain valid while the caller excludes writes.
func PrepareMappedPut(data []byte, pageSize int64, key []byte, valueSize int, entry *Entry) (index int, found bool, child page.ID, err error) {
	count := binary.LittleEndian.Uint32(data[10:14])
	image := data[:NodeHeaderSize+int(binary.LittleEndian.Uint32(data[18:22]))]
	if mappedNodeType(data) == NodeTypeBranch {
		child, err = lookupBranchBody(image, count, key)
		return 0, false, child, err
	}
	if int64(len(data)) > pageSize && (count != 1 || int64(valueSize) <= pageSize) {
		return 0, false, 0, nil
	}
	if err := checkLeafBody(image, NodeHeader{EntryCount: count}, true, nil); err != nil {
		return 0, false, 0, err
	}
	index, found, err = lookupLeafBody(image, count, key, false, entry)
	return index, found, 0, err
}

// PutMappedEntry changes a plain entry without changing the first key or page span.
// It replaces a same-size value or inserts a small entry into a one-page leaf.
// data must be writable and excluded from readers. A branch returns its next child.
// Unsupported leaves return false without changes. All checks precede the write.
// Input bytes can overlap data.
func PutMappedEntry(data []byte, id page.ID, pageSize int64, checksums bool, key, value []byte) (handled bool, child page.ID, err error) {
	if err := validateLookupKey(key); err != nil {
		return false, 0, err
	}
	if _, _, err := nodeImage(data, id, pageSize, nil, false, checksums); err != nil {
		return false, 0, err
	}
	var entry Entry
	index, found, child, err := PrepareMappedPut(data, pageSize, key, len(value), &entry)
	if err != nil || child != 0 {
		return false, child, err
	}
	if found {
		if entry.flags != 0 || len(value) != len(entry.value) {
			return false, 0, nil
		}
		if bytes.Equal(entry.value, value) {
			return true, 0, nil
		}
		copy(entry.value, value)
	} else {
		// Keep borrowed input bytes intact while the packed body moves.
		var input [256]byte
		added := leafEntryDescriptorSize + len(key) + len(value)
		imageSize := NodeHeaderSize + int(binary.LittleEndian.Uint32(data[18:22]))
		if index == 0 || int64(len(data)) != pageSize || len(key) > len(input) || len(value) > len(input)-len(key) || added > len(data)-imageSize {
			return false, 0, nil
		}
		keySize, valueSize := len(key), len(value)
		copy(input[:], key)
		copy(input[keySize:], value)
		count := int(binary.LittleEndian.Uint32(data[10:14]))
		dataStart := NodeHeaderSize + count*leafEntryDescriptorSize
		position := imageSize
		if index < count {
			_, start, _, _ := leafSpan(data[:imageSize], index)
			position = int(start)
		}
		newSize := imageSize + added
		copy(data[position+added:newSize], data[position:imageSize])
		copy(data[dataStart+leafEntryDescriptorSize:position+leafEntryDescriptorSize], data[dataStart:position])
		copy(data[NodeHeaderSize+(index+1)*leafEntryDescriptorSize:dataStart+leafEntryDescriptorSize], data[NodeHeaderSize+index*leafEntryDescriptorSize:dataStart])
		for slot := 0; slot <= count; slot++ {
			at := NodeHeaderSize + slot*leafEntryDescriptorSize
			if slot == index {
				putLeafDescriptor(data[at:], &Entry{key: input[:keySize], value: input[keySize : keySize+valueSize]}, position+leafEntryDescriptorSize)
				continue
			}
			offset := int(binary.LittleEndian.Uint32(data[at+4:at+8])) + leafEntryDescriptorSize
			if slot > index {
				offset += keySize + valueSize
			}
			binary.LittleEndian.PutUint32(data[at+4:at+8], uint32(offset))
		}
		position += leafEntryDescriptorSize
		copy(data[position:], input[:keySize+valueSize])
		binary.LittleEndian.PutUint32(data[10:14], uint32(count+1))
		binary.LittleEndian.PutUint32(data[18:22], uint32(newSize-NodeHeaderSize))
	}
	// The header length and page ID were checked above and remain unchanged.
	return true, 0, VerifyNodeIDAndSetChecksum(data, id, checksums)
}
