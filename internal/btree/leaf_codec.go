package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"

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

// leafContinuationHeaderSize adds the body length and next-ID count to the normal header.
//
//	[18:22] total logical body length, uint32
//	[22:26] next page count N, uint32
//	[26:26+8*N] continuation IDs
//	[26+8*N:P] first body bytes
//
// Offsets in entry descriptors still include the normal 18-byte logical header.
const leafContinuationHeaderSize = NodeHeaderSize + 2*encodedUint32Size

func leafPayloadSize(node *Node) int {
	size := 0
	for _, entry := range node.entries {
		size += leafEntryDescriptorSize + len(entry.key) + len(entry.value)
	}
	return size
}
func leafEncodedSize(node *Node) int { return node.EncodedSize() }
func leafBodyOffset(node *Node) int {
	return NodeHeaderSize + len(node.entries)*leafEntryDescriptorSize
}

// EncodeLeafNode writes a compact leaf page image and sets node's checksum.
// node must be a leaf node. The checksum includes zero padding up to pageSize.
// The returned bytes belong to the caller. They do not include padding.
// pageSize must not be smaller than the encoded node.
func EncodeLeafNode(node *Node, pageSize int64) []byte {
	data := make([]byte, leafEncodedSize(node))
	encodeLeafWALNode(data, node)
	node.header.Checksum = pageChecksum(data, pageSize, nodeChecksumOffset)
	binary.LittleEndian.PutUint32(data[14:18], node.header.Checksum)
	return data
}

// encodeLeafWALNode writes a compact leaf image with no page padding or node checksum.
// data must have leafEncodedSize bytes. A verified WAL record protects these bytes.
func encodeLeafWALNode(data []byte, node *Node) {
	clear(data)
	binary.LittleEndian.PutUint16(data[:2], uint16(NodeTypeLeaf))
	binary.LittleEndian.PutUint64(data[2:10], uint64(node.PageID()))
	binary.LittleEndian.PutUint32(data[10:14], uint32(len(node.entries)))
	encodeLeafBody(data, node)
}

// ReadMappedLeafNode checks one leaf page header and checksum without decoding entries.
// It returns the header, encoded continuation IDs, and body bytes.
// The byte slices borrow data and cannot grow into other fields.
// A continued first page has its own body-length and next-ID fields.
// The continuation codec checks each later page separately.
// checksums selects verification of nonzero checksum fields.
func ReadMappedLeafNode(data []byte, id page.ID, pageSize int64, checksums bool) (NodeHeader, []byte, []byte, error) {
	return readLeafNodeHeader(data, id, pageSize, false, checksums)
}

// readLeafNodeHeader shares header checks between mapped pages and verified WAL payloads.
func readLeafNodeHeader(data []byte, id page.ID, pageSize int64, forWAL, checksums bool) (NodeHeader, []byte, []byte, error) {
	if len(data) < NodeHeaderSize || int64(len(data)) > pageSize {
		return NodeHeader{}, nil, nil, ErrInvalid
	}
	h := NodeHeader{Type: NodeType(binary.LittleEndian.Uint16(data[:2])), PageID: page.ID(binary.LittleEndian.Uint64(data[2:10])),
		EntryCount: binary.LittleEndian.Uint32(data[10:14]), Checksum: binary.LittleEndian.Uint32(data[14:18])}
	if h.Type & ^overflowNodeFlag != NodeTypeLeaf || h.PageID != id {
		return NodeHeader{}, nil, nil, ErrInvalid
	}
	fixed := NodeHeaderSize
	if h.Type&overflowNodeFlag != 0 {
		if int64(len(data)) != pageSize || pageSize <= leafContinuationHeaderSize+page.IDSize {
			return NodeHeader{}, nil, nil, ErrInvalid
		}
		fixed = leafContinuationHeaderSize
		h.BodyLength = binary.LittleEndian.Uint32(data[18:22])
		h.NextPageCount = binary.LittleEndian.Uint32(data[22:26])
		if uint64(h.NextPageCount) > uint64((pageSize-int64(fixed)-1)/page.IDSize) {
			return NodeHeader{}, nil, nil, ErrInvalid
		}
	}
	if forWAL {
		if h.Checksum != 0 {
			return NodeHeader{}, nil, nil, ErrInvalid
		}
	} else if checksums && h.Checksum != 0 && h.Checksum != pageChecksum(data, pageSize, nodeChecksumOffset) {
		return NodeHeader{}, nil, nil, fmt.Errorf("node page %d: %w", id, page.ErrChecksum)
	}
	start := fixed + int(h.NextPageCount)*page.IDSize
	return h, data[fixed:start:start], data[start:len(data):len(data)], nil
}

// DecodeLeafNode verifies and decodes a leaf, including all of its continuation pages.
// readPage supplies continuation pages. Pass nil for a normal node.
// A continued node requires a non-nil readPage function.
// A normal node owns data. A continued node owns a rebuilt logical image.
// Its entry bytes remain valid until that owned storage changes.
// The complete leaf body is checked. checksums selects nonzero checksum checks.
func DecodeLeafNode(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error), checksums bool) (*Node, error) {
	return decodeLeafNode(data, id, pageSize, readPage, false, checksums)
}

// decodeLeafNode shares the body layout for main-file pages and WAL payloads.
// forWAL requires zero page checksums. checksums selects main-file checksum checks.
func decodeLeafNode(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error), forWAL, checksums bool) (*Node, error) {
	if mappedNodeType(data) == NodeTypeLeaf|overflowNodeFlag {
		if readPage == nil {
			return nil, fmt.Errorf("node %d requires continuation pages: %w", id, ErrInvalid)
		}
		h, _, _, err := readLeafNodeHeader(data, id, pageSize, forWAL, checksums)
		if err != nil {
			return nil, err
		}
		if uint64(h.BodyLength)+NodeHeaderSize <= uint64(pageSize) || uint64(h.BodyLength) > uint64(MaxValueSize+MaxKeySize+leafEntryDescriptorSize) {
			return nil, ErrInvalid
		}
		image := make([]byte, NodeHeaderSize+int(h.BodyLength))
		copy(image[:14], data[:14])
		binary.LittleEndian.PutUint16(image[:2], uint16(NodeTypeLeaf))
		ids, err := copyContinuationBody(data, leafContinuationHeaderSize, image[NodeHeaderSize:], readPage, forWAL, checksums)
		if err != nil {
			return nil, err
		}
		node, _, _, _, err := readEncodedLeaf(image, id, 0, nil, true, false, nil, nil, true)
		if err != nil {
			return nil, err
		}
		*node.header = h
		node.continuationPageIDs = ids
		return node, nil
	}
	if len(data) < NodeHeaderSize {
		return nil, fmt.Errorf("read node header: %w", ErrInvalid)
	}
	if pageSize < NodeHeaderSize || int64(len(data)) > pageSize {
		return nil, fmt.Errorf("read node page size %d from %d bytes: %w", pageSize, len(data), ErrInvalid)
	}
	stored := binary.LittleEndian.Uint32(data[14:18])
	if forWAL {
		if stored != 0 {
			return nil, fmt.Errorf("read WAL node checksum %x: %w", stored, ErrInvalid)
		}
	} else if checksums && stored != 0 && stored != pageChecksum(data, pageSize, nodeChecksumOffset) {
		return nil, fmt.Errorf("verify node checksum: %w", page.ErrChecksum)
	}
	node, _, _, _, err := readEncodedLeaf(data, id, stored, nil, true, !forWAL, nil, nil, true)
	return node, err
}

// DecodeWALLeafNode decodes a compact leaf from a verified WAL record.
// Each page checksum must be zero. The WAL records must already be verified.
// pageSize is the physical database page size. readPage supplies verified
// continuation payloads. Pass nil for a normal node. A normal node owns data;
// a continued node owns a rebuilt logical image. Do not change its owned bytes.
func DecodeWALLeafNode(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error)) (*Node, error) {
	return decodeLeafNode(data, id, pageSize, readPage, true, false)
}

// LookupMappedLeafNode searches a leaf's encoded directory without creating a Node.
// Normal pages return borrowed entry bytes. checksums selects nonzero checksum checks.
// Continued pages check each visited page. A found leaf value owns its bytes.
// A continued key borrows the first page if it fits there; otherwise it owns a copy.
// Set includeKey to false to omit the returned key and its copy.
// A continued miss does not copy the complete body.
// Keep data unchanged while a result uses its borrowed bytes.
// Pass nil for a normal node. A continued node requires readPage.
// readPage must keep continuation bytes unchanged until the lookup returns.
func LookupMappedLeafNode(data []byte, id page.ID, key []byte, readPage func(page.ID) ([]byte, error), includeKey, checksums bool) (Entry, bool, page.ID, error) {
	return lookupLeafNode(data, id, key, int64(len(data)), readPage, false, includeKey, checksums)
}

// lookupLeafNode searches the same encoded directory in mapped pages and WAL images.
// forWAL uses verified compact payloads. It checks zero page checksums.
func lookupLeafNode(data []byte, id page.ID, key []byte, pageSize int64, readPage func(page.ID) ([]byte, error), forWAL bool, includeKey, checksums bool) (Entry, bool, page.ID, error) {
	if err := validateLookupKey(key); err != nil {
		return Entry{}, false, 0, err
	}
	if mappedNodeType(data) == NodeTypeLeaf|overflowNodeFlag {
		if readPage == nil {
			return Entry{}, false, 0, ErrInvalid
		}
		h, _, _, err := readLeafNodeHeader(data, id, pageSize, forWAL, checksums)
		if err != nil {
			return Entry{}, false, 0, err
		}
		if uint64(h.BodyLength)+NodeHeaderSize <= uint64(pageSize) || uint64(h.BodyLength) > uint64(MaxValueSize+MaxKeySize+leafEntryDescriptorSize) {
			return Entry{}, false, 0, ErrInvalid
		}
		group, groupOffset, cached := data, 0, data
		fragment := func(offset int) ([]byte, error) {
			part, _, err := continuationBodyBytes(data, leafContinuationHeaderSize, offset, readPage, &group, &groupOffset, &cached, forWAL, checksums)
			return part, err
		}
		_, entry, found, child, err := readEncodedLeaf(data, id, 0, key, false, true, nil, fragment, includeKey)
		return entry, found, child, err
	}
	if forWAL {
		if len(data) < NodeHeaderSize || int64(len(data)) > pageSize {
			return Entry{}, false, 0, ErrInvalid
		}
		if crc := binary.LittleEndian.Uint32(data[14:18]); crc != 0 {
			return Entry{}, false, 0, fmt.Errorf("read WAL node checksum %x: %w", crc, ErrInvalid)
		}
	} else if err := checkPageChecksum(data, pageSize, nodeChecksumOffset, checksums); err != nil {
		return Entry{}, false, 0, err
	}
	_, entry, found, child, err := readEncodedLeaf(data, id, 0, key, false, !forWAL, nil, nil, includeKey)
	return entry, found, child, err
}

func lookupWALLeafNode(data []byte, id page.ID, key []byte, pageSize int64, readPage func(page.ID) ([]byte, error), includeKey bool) (Entry, bool, page.ID, error) {
	return lookupLeafNode(data, id, key, pageSize, readPage, true, includeKey, false)
}

func validateWALLeafNode(data []byte, id page.ID) error {
	if len(data) < NodeHeaderSize {
		return fmt.Errorf("read node header: %w", ErrInvalid)
	}
	if crc := binary.LittleEndian.Uint32(data[14:18]); crc != 0 {
		return fmt.Errorf("read WAL node checksum %x: %w", crc, ErrInvalid)
	}
	_, _, _, _, err := readEncodedLeaf(data, id, 0, nil, false, false, nil, nil, true)
	return err
}

// visitLeafNodeReferences validates the encoded leaf body and its owned pages.
// Continued bodies use the same descriptor reader as point lookup. Only keys
// need a temporary buffer for order checks. Large values remain in their pages.
func visitLeafNodeReferences(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error), visit, visitContinuation func(page.ID) error, forWAL, checksums bool) error {
	h, _, _, err := readLeafNodeHeader(data, id, pageSize, forWAL, checksums)
	if err != nil {
		return err
	}
	var fragment func(int) ([]byte, error)
	if h.Type&overflowNodeFlag != 0 {
		if readPage == nil || uint64(h.BodyLength)+NodeHeaderSize <= uint64(pageSize) || uint64(h.BodyLength) > uint64(MaxValueSize+MaxKeySize+leafEntryDescriptorSize) {
			return ErrInvalid
		}
		group, groupOffset, cached := data, 0, data
		fragment = func(offset int) ([]byte, error) {
			part, _, err := continuationBodyBytes(data, leafContinuationHeaderSize, offset, readPage, &group, &groupOffset, &cached, forWAL, checksums)
			return part, err
		}
	}
	_, _, _, _, err = readEncodedLeaf(data, id, h.Checksum, nil, false, !forWAL && fragment == nil, visit, fragment, true)
	if err != nil || fragment == nil {
		return err
	}
	return walkContinuationPages(data, leafContinuationHeaderSize, readPage, forWAL, func(pageID page.ID, _ []byte) error {
		if pageID != id && visitContinuation != nil {
			return visitContinuation(pageID)
		}
		return nil
	}, checksums)
}

func leafPageCount(node *Node, pageSize int64) int {
	size := leafEncodedSize(node)
	if pageSize >= NodeHeaderSize && int64(size) <= pageSize {
		return 1
	}
	return continuationPageCount(size-NodeHeaderSize, pageSize, leafContinuationHeaderSize)
}

// EncodeLeafNodePages writes this leaf and its continuation pages into owned buffers.
// ids must start with node.PageID and contain exactly leafPageCount distinct non-metadata IDs.
// The caller must own the allocated IDs. Encoding does not change node or ids.
func EncodeLeafNodePages(node *Node, pageSize int64, ids []page.ID) ([][]byte, error) {
	return encodeLeafNodePages(node, pageSize, ids, false)
}

func encodeLeafNodePages(node *Node, pageSize int64, ids []page.ID, forWAL bool) ([][]byte, error) {
	encodedSize := leafEncodedSize(node)
	if node.header.Type & ^overflowNodeFlag != NodeTypeLeaf || uint64(encodedSize) > uint64(^uint32(0)) ||
		len(ids) == 0 || len(ids) != leafPageCount(node, pageSize) || ids[0] != node.PageID() || page.IsMetaID(ids[0]) {
		return nil, ErrInvalid
	}
	if len(ids) == 1 {
		size := int(pageSize)
		if forWAL {
			size = encodedSize
		}
		data := make([]byte, size)
		encodeLeafWALNode(data[:encodedSize], node)
		if !forWAL {
			binary.LittleEndian.PutUint32(data[14:18], pageChecksum(data, pageSize, nodeChecksumOffset))
		}
		return [][]byte{data}, nil
	}
	pages, err := allocateContinuationPages(ids, pageSize, leafContinuationHeaderSize)
	if err != nil {
		return nil, err
	}
	first := pages[0]
	binary.LittleEndian.PutUint16(first[:2], uint16(NodeTypeLeaf|overflowNodeFlag))
	binary.LittleEndian.PutUint64(first[2:10], uint64(node.PageID()))
	binary.LittleEndian.PutUint32(first[10:14], uint32(len(node.entries)))
	binary.LittleEndian.PutUint32(first[18:22], uint32(encodedSize-NodeHeaderSize))
	index, at := 0, 0

	offset := NodeHeaderSize + len(node.entries)*leafEntryDescriptorSize
	for _, entry := range node.entries {
		var fields [leafEntryDescriptorSize]byte
		binary.LittleEndian.PutUint32(fields[:4], entry.flags)
		binary.LittleEndian.PutUint32(fields[4:8], uint32(offset))
		binary.LittleEndian.PutUint32(fields[8:12], uint32(len(entry.key)))
		binary.LittleEndian.PutUint32(fields[12:16], uint32(len(entry.value)))
		copyContinuationBytes(pages, leafContinuationHeaderSize, &index, &at, fields[:])
		offset += len(entry.key) + len(entry.value)
	}
	for _, entry := range node.entries {
		copyContinuationBytes(pages, leafContinuationHeaderSize, &index, &at, entry.key)
		copyContinuationBytes(pages, leafContinuationHeaderSize, &index, &at, entry.value)
	}

	finishContinuationPages(pages, index, at, forWAL)
	return pages, nil
}

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

// readEncodedLeaf checks the leaf header and reads its body.
// It can decode a node, validate its full body, or search its encoded directory.
// A nil fragment reads data directly. Continued lookup and reference checks
// supply mapped fragments at logical offsets, including the normal 18-byte header.
// The owning entry point verifies the first page. WAL records must already
// have passed their record checksum checks.
// includeKey applies only to directory lookup results.
func readEncodedLeaf(data []byte, expectedPageID page.ID, storedChecksum uint32, key []byte, createNode bool, allowPadding bool, visit func(page.ID) error, fragment func(int) ([]byte, error), includeKey bool) (*Node, Entry, bool, page.ID, error) {
	if len(data) < NodeHeaderSize {
		return nil, Entry{}, false, 0, fmt.Errorf("read node header: %w", ErrInvalid)
	}
	nodeType := NodeType(binary.LittleEndian.Uint16(data[:2]))
	if nodeType != NodeTypeLeaf && !(nodeType == NodeTypeLeaf|overflowNodeFlag && fragment != nil && !createNode) {
		return nil, Entry{}, false, 0, fmt.Errorf("read node type %d: %w", nodeType, ErrInvalid)
	}
	if id := page.ID(binary.LittleEndian.Uint64(data[2:10])); id != expectedPageID {
		return nil, Entry{}, false, 0, fmt.Errorf("read node page ID %d, want %d: %w", id, expectedPageID, ErrInvalid)
	}

	bodySize := len(data)
	if fragment != nil {
		bodySize = NodeHeaderSize + int(binary.LittleEndian.Uint32(data[18:22]))
	}

	entryCount := binary.LittleEndian.Uint32(data[nodeEntryCountOffset:nodeChecksumOffset])
	if uint64(entryCount) > uint64((bodySize-NodeHeaderSize)/leafEntryDescriptorSize) {
		return nil, Entry{}, false, 0, fmt.Errorf("read node entry count: %w", ErrInvalid)
	}
	payloadStart := NodeHeaderSize + int(entryCount)*leafEntryDescriptorSize

	readEntry := func(index uint32) (uint32, int, int, int, error) {
		start := NodeHeaderSize + int(index)*leafEntryDescriptorSize
		var fields []byte
		if fragment == nil {
			fields = data[start : start+leafEntryDescriptorSize]
		} else {
			var scratch [leafEntryDescriptorSize]byte
			if err := copyEncodedBody(start, scratch[:], fragment); err != nil {
				return 0, 0, 0, 0, err
			}
			fields = scratch[:]
		}
		flags := binary.LittleEndian.Uint32(fields[:encodedUint32Size])
		offset := binary.LittleEndian.Uint32(fields[encodedUint32Size : 2*encodedUint32Size])
		keySize := binary.LittleEndian.Uint32(fields[2*encodedUint32Size : 3*encodedUint32Size])
		valueSize := binary.LittleEndian.Uint32(fields[3*encodedUint32Size:])
		if keySize == 0 || uint64(keySize) > uint64(MaxKeySize) {
			return 0, 0, 0, 0, fmt.Errorf("read node key %d: %w", index, ErrInvalid)
		}
		if flags&^uint32(BucketLeafFlag) != 0 || uint64(valueSize) > uint64(MaxValueSize) {
			return 0, 0, 0, 0, fmt.Errorf("read node entry %d: %w", index, ErrInvalid)
		}
		keyEnd := uint64(offset) + uint64(keySize)
		end := keyEnd + uint64(valueSize)
		if uint64(offset) < uint64(payloadStart) || end > uint64(bodySize) {
			return 0, 0, 0, 0, fmt.Errorf("read node entry %d: %w", index, ErrInvalid)
		}
		return flags, int(offset), int(keyEnd), int(end), nil
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
			flags, start, keyEnd, end, err := readEntry(index)
			if err != nil {
				return nil, Entry{}, false, 0, err
			}
			entry := Entry{flags: flags}
			if fragment == nil {
				entry.key, entry.value = data[start:keyEnd:keyEnd], data[keyEnd:end:end]
			} else {
				if index > 0 {
					cmp, err := compareEncodedKey(start, keyEnd-start, previousKey, fragment)
					if err != nil {
						return nil, Entry{}, false, 0, err
					}
					if cmp <= 0 {
						return nil, Entry{}, false, 0, ErrInvalid
					}
				}
				previousKey = slices.Grow(previousKey[:0], keyEnd-start)[:keyEnd-start]
				if err := copyEncodedBody(start, previousKey, fragment); err != nil {
					return nil, Entry{}, false, 0, err
				}
				entry.key = previousKey
				if flags&BucketLeafFlag != 0 {
					if end-keyEnd != page.IDSize {
						return nil, Entry{}, false, 0, ErrInvalid
					}
					var child [page.IDSize]byte
					if err := copyEncodedBody(keyEnd, child[:], fragment); err != nil {
						return nil, Entry{}, false, 0, err
					}
					entry.value = child[:]
				}
			}
			if start != nextOffset {
				return nil, Entry{}, false, 0, fmt.Errorf("read node entry offset %d: %w", index, ErrInvalid)
			}
			if index > 0 && fragment == nil && bytes.Compare(previousKey, entry.key) >= 0 {
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

		if !allowPadding && nextOffset != bodySize {
			return nil, Entry{}, false, 0, fmt.Errorf("read node body size: %w", ErrInvalid)
		}
		return node, Entry{}, false, 0, nil
	}

	low, high := uint32(0), entryCount
	for low < high {
		middle := low + (high-low)/2
		flags, start, keyEnd, end, err := readEntry(middle)
		if err != nil {
			return nil, Entry{}, false, 0, err
		}
		comparison := 0
		if fragment == nil {
			comparison = bytes.Compare(data[start:keyEnd], key)
		} else {
			comparison, err = compareEncodedKey(start, keyEnd-start, key, fragment)
			if err != nil {
				return nil, Entry{}, false, 0, err
			}
		}
		if comparison == 0 {
			entry := Entry{flags: flags}
			if fragment == nil {
				if includeKey {
					entry.key = data[start:keyEnd:keyEnd]
				}
				entry.value = data[keyEnd:end:end]
			} else {
				if includeKey {
					// Keep a key in the first page borrowed. Other keys use a copy of the equal query bytes.
					physical := leafContinuationHeaderSize + int(binary.LittleEndian.Uint32(data[22:26]))*page.IDSize + start - NodeHeaderSize
					if physical+keyEnd-start <= len(data) {
						entry.key = data[physical : physical+keyEnd-start : physical+keyEnd-start]
					} else {
						entry.key = bytes.Clone(key)
					}
				}
				entry.value = make([]byte, end-keyEnd)
				if err := copyEncodedBody(keyEnd, entry.value, fragment); err != nil {
					return nil, Entry{}, false, 0, err
				}
				entry.valueOwned = true
			}
			return nil, entry, true, 0, nil
		}
		if comparison > 0 {
			high = middle
		} else {
			low = middle + 1
		}
	}
	if low < entryCount {
		if _, _, _, _, err := readEntry(low); err != nil {
			return nil, Entry{}, false, 0, err
		}
	}
	return nil, Entry{}, false, 0, nil
}

// VisitMappedLeafRange visits entries in the half-open range [start, end).
// It checks descriptor offsets and byte bounds. checksums selects nonzero checksum checks.
// The key and value passed to visit refer to data.
func VisitMappedLeafRange(data []byte, expectedPageID page.ID, start, end []byte, visit func(uint32, []byte, []byte) error, checksums bool) error {
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

	if err := checkPageChecksum(data, int64(len(data)), nodeChecksumOffset, checksums); err != nil {
		return err
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
// The key and value slices refer to data. checksums selects nonzero checksum checks.
func VisitMappedLeafReverse(data []byte, expectedPageID page.ID, visit func(uint32, []byte, []byte) error, checksums bool) error {
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

	if err := checkPageChecksum(data, int64(len(data)), nodeChecksumOffset, checksums); err != nil {
		return err
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
