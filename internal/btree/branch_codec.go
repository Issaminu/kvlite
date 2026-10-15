package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"

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

func branchPayloadSize(node *Node) int {
	size := page.IDSize
	for _, entry := range node.entries {
		size += branchEntryDescriptorSize + page.IDSize + len(entry.key)
	}
	return size
}
func branchEncodedSize(node *Node) int { return node.EncodedSize() }
func branchBodyOffset(node *Node) int {
	return NodeHeaderSize + len(node.entries)*branchEntryDescriptorSize
}

// EncodeBranchNode writes a compact branch page image and sets node's checksum.
// node must be a branch node. The checksum includes zero padding up to pageSize.
// The returned bytes belong to the caller. They do not include padding.
// pageSize must not be smaller than the encoded node.
func EncodeBranchNode(node *Node, pageSize int64) []byte {
	data := make([]byte, branchEncodedSize(node))
	encodeBranchWALNode(data, node)
	node.header.Checksum = pageChecksum(data, pageSize, nodeChecksumOffset)
	binary.LittleEndian.PutUint32(data[14:18], node.header.Checksum)
	return data
}

// encodeBranchWALNode writes a compact branch image with no page padding or node checksum.
// data must have branchEncodedSize bytes. A verified WAL record protects these bytes.
func encodeBranchWALNode(data []byte, node *Node) {
	clear(data)
	binary.LittleEndian.PutUint16(data[:2], uint16(NodeTypeBranch))
	binary.LittleEndian.PutUint64(data[2:10], uint64(node.PageID()))
	binary.LittleEndian.PutUint32(data[10:14], uint32(len(node.entries)))
	encodeBranchBody(data, node)
}

// ReadMappedBranchNode checks one branch page header and checksum without decoding entries.
// It returns the header, encoded continuation IDs, and body bytes.
// The byte slices borrow data and cannot grow into other fields.
// A continued first page has its own body-length and next-ID fields.
// The continuation codec checks each later page separately.
// checksums selects verification of nonzero checksum fields.
func ReadMappedBranchNode(data []byte, id page.ID, pageSize int64, checksums bool) (NodeHeader, []byte, []byte, error) {
	return readBranchNodeHeader(data, id, pageSize, false, checksums)
}

// readBranchNodeHeader shares header checks between mapped pages and verified WAL payloads.
func readBranchNodeHeader(data []byte, id page.ID, pageSize int64, forWAL, checksums bool) (NodeHeader, []byte, []byte, error) {
	if len(data) < NodeHeaderSize || int64(len(data)) > pageSize {
		return NodeHeader{}, nil, nil, ErrInvalid
	}
	h := NodeHeader{Type: NodeType(binary.LittleEndian.Uint16(data[:2])), PageID: page.ID(binary.LittleEndian.Uint64(data[2:10])),
		EntryCount: binary.LittleEndian.Uint32(data[10:14]), Checksum: binary.LittleEndian.Uint32(data[14:18])}
	if h.Type & ^overflowNodeFlag != NodeTypeBranch || h.PageID != id {
		return NodeHeader{}, nil, nil, ErrInvalid
	}
	fixed := NodeHeaderSize
	if h.Type&overflowNodeFlag != 0 {
		if int64(len(data)) != pageSize || pageSize <= continuedNodeHeaderSize+page.IDSize {
			return NodeHeader{}, nil, nil, ErrInvalid
		}
		fixed = continuedNodeHeaderSize
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

// DecodeBranchNode verifies and decodes a branch, including all of its continuation pages.
// readPage supplies continuation pages. Pass nil for a normal node.
// A continued node requires a non-nil readPage function.
// A normal node owns data. A continued node owns a rebuilt logical image.
// Its entry bytes remain valid until that owned storage changes.
// The complete branch body is checked. checksums selects nonzero checksum checks.
func DecodeBranchNode(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error), checksums bool) (*Node, error) {
	return decodeBranchNode(data, id, pageSize, readPage, false, checksums)
}

// decodeBranchNode shares the body layout for main-file pages and WAL payloads.
// forWAL requires zero page checksums. checksums selects main-file checksum checks.
func decodeBranchNode(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error), forWAL, checksums bool) (*Node, error) {
	if mappedNodeType(data) == NodeTypeBranch|overflowNodeFlag {
		if readPage == nil {
			return nil, fmt.Errorf("node %d requires continuation pages: %w", id, ErrInvalid)
		}
		h, _, _, err := readBranchNodeHeader(data, id, pageSize, forWAL, checksums)
		if err != nil {
			return nil, err
		}
		if uint64(h.BodyLength)+NodeHeaderSize <= uint64(pageSize) || uint64(h.BodyLength) > uint64(MaxValueSize+MaxKeySize+branchEntryDescriptorSize+page.IDSize) {
			return nil, ErrInvalid
		}
		image := make([]byte, NodeHeaderSize+int(h.BodyLength))
		copy(image[:14], data[:14])
		binary.LittleEndian.PutUint16(image[:2], uint16(NodeTypeBranch))
		ids, err := copyContinuationBody(data, image[NodeHeaderSize:], readPage, forWAL, checksums)
		if err != nil {
			return nil, err
		}
		node, _, _, _, err := readEncodedBranch(image, id, 0, nil, true, false, nil, nil)
		if err != nil {
			return nil, err
		}
		node.header = h
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
	node, _, _, _, err := readEncodedBranch(data, id, stored, nil, true, !forWAL, nil, nil)
	return node, err
}

// DecodeWALBranchNode decodes a compact branch from a verified WAL record.
// Each page checksum must be zero. The WAL records must already be verified.
// pageSize is the physical database page size. readPage supplies verified
// continuation payloads. Pass nil for a normal node. A normal node owns data;
// a continued node owns a rebuilt logical image. Do not change its owned bytes.
func DecodeWALBranchNode(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error)) (*Node, error) {
	return decodeBranchNode(data, id, pageSize, readPage, true, false)
}

// LookupMappedBranchNode searches a branch's encoded directory without creating a Node.
// Each search returns the selected child page ID. checksums selects nonzero checksum checks.
// Continued pages check each visited page and do not copy the complete body.
// Pass nil for a normal node. A continued node requires readPage.
// readPage must keep continuation bytes unchanged until the lookup returns.
func LookupMappedBranchNode(data []byte, id page.ID, key []byte, readPage func(page.ID) ([]byte, error), checksums bool) (Entry, bool, page.ID, error) {
	if checksums {
		return lookupCheckedBranchNode(data, id, key, readPage)
	}
	return lookupBranchNode(data, id, key, int64(len(data)), readPage, false, false)
}

// lookupCheckedBranchNode verifies the checksum of a normal mapped branch page and then searches it.
// The header read in lookupBranchNode verifies the first page of a continued branch.
func lookupCheckedBranchNode(data []byte, id page.ID, key []byte, readPage func(page.ID) ([]byte, error)) (Entry, bool, page.ID, error) {
	if mappedNodeType(data) != NodeTypeBranch|overflowNodeFlag {
		if err := checkPageChecksum(data, int64(len(data)), nodeChecksumOffset, true); err != nil {
			return Entry{}, false, 0, err
		}
	}
	return lookupBranchNode(data, id, key, int64(len(data)), readPage, false, true)
}

// lookupBranchNode searches the same encoded directory in mapped pages and WAL images.
// forWAL uses verified compact payloads. It checks zero page checksums.
// It does not verify the checksum of a normal mapped page. lookupCheckedBranchNode does that first.
func lookupBranchNode(data []byte, id page.ID, key []byte, pageSize int64, readPage func(page.ID) ([]byte, error), forWAL, checksums bool) (Entry, bool, page.ID, error) {
	if err := validateLookupKey(key); err != nil {
		return Entry{}, false, 0, err
	}
	if mappedNodeType(data) == NodeTypeBranch|overflowNodeFlag {
		if readPage == nil {
			return Entry{}, false, 0, ErrInvalid
		}
		h, _, _, err := readBranchNodeHeader(data, id, pageSize, forWAL, checksums)
		if err != nil {
			return Entry{}, false, 0, err
		}
		if uint64(h.BodyLength)+NodeHeaderSize <= uint64(pageSize) || uint64(h.BodyLength) > uint64(MaxValueSize+MaxKeySize+branchEntryDescriptorSize+page.IDSize) {
			return Entry{}, false, 0, ErrInvalid
		}
		var cursor continuationCursor
		fragment := func(offset int) ([]byte, error) {
			part, _, err := continuationBodyBytes(data, offset, readPage, &cursor, forWAL, checksums)
			return part, err
		}
		_, entry, found, child, err := readEncodedBranch(data, id, 0, key, false, true, nil, fragment)
		return entry, found, child, err
	}
	if forWAL {
		if len(data) < NodeHeaderSize || int64(len(data)) > pageSize {
			return Entry{}, false, 0, ErrInvalid
		}
		if crc := binary.LittleEndian.Uint32(data[14:18]); crc != 0 {
			return Entry{}, false, 0, fmt.Errorf("read WAL node checksum %x: %w", crc, ErrInvalid)
		}
	}
	_, entry, found, child, err := readEncodedBranch(data, id, 0, key, false, !forWAL, nil, nil)
	return entry, found, child, err
}

func lookupWALBranchNode(data []byte, id page.ID, key []byte, pageSize int64, readPage func(page.ID) ([]byte, error)) (Entry, bool, page.ID, error) {
	return lookupBranchNode(data, id, key, pageSize, readPage, true, false)
}

func validateWALBranchNode(data []byte, id page.ID) error {
	if len(data) < NodeHeaderSize {
		return fmt.Errorf("read node header: %w", ErrInvalid)
	}
	if crc := binary.LittleEndian.Uint32(data[14:18]); crc != 0 {
		return fmt.Errorf("read WAL node checksum %x: %w", crc, ErrInvalid)
	}
	_, _, _, _, err := readEncodedBranch(data, id, 0, nil, false, false, nil, nil)
	return err
}

// visitBranchNodeReferences validates the encoded branch body and its owned pages.
// Continued bodies use the same descriptor reader as point lookup. Only keys
// need a temporary buffer for order checks.
func visitBranchNodeReferences(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error), visit, visitContinuation func(page.ID) error, forWAL, checksums bool) error {
	h, _, _, err := readBranchNodeHeader(data, id, pageSize, forWAL, checksums)
	if err != nil {
		return err
	}
	var fragment func(int) ([]byte, error)
	if h.Type&overflowNodeFlag != 0 {
		if readPage == nil || uint64(h.BodyLength)+NodeHeaderSize <= uint64(pageSize) || uint64(h.BodyLength) > uint64(MaxValueSize+MaxKeySize+branchEntryDescriptorSize+page.IDSize) {
			return ErrInvalid
		}
		var cursor continuationCursor
		fragment = func(offset int) ([]byte, error) {
			part, _, err := continuationBodyBytes(data, offset, readPage, &cursor, forWAL, checksums)
			return part, err
		}
	}
	_, _, _, _, err = readEncodedBranch(data, id, h.Checksum, nil, false, !forWAL && fragment == nil, visit, fragment)
	if err != nil || fragment == nil {
		return err
	}
	return walkContinuationPages(data, readPage, forWAL, func(pageID page.ID, _ []byte) error {
		if pageID != id && visitContinuation != nil {
			return visitContinuation(pageID)
		}
		return nil
	}, checksums)
}

func branchPageCount(node *Node, pageSize int64) int {
	size := branchEncodedSize(node)
	if pageSize >= NodeHeaderSize && int64(size) <= pageSize {
		return 1
	}
	return continuationPageCount(size-NodeHeaderSize, pageSize, continuedNodeHeaderSize)
}

// EncodeBranchNodePages writes this branch and its continuation pages into owned buffers.
// ids must start with node.PageID and contain exactly branchPageCount distinct non-metadata IDs.
// The caller must own the allocated IDs. Encoding does not change node or ids.
func EncodeBranchNodePages(node *Node, pageSize int64, ids []page.ID) ([][]byte, error) {
	return encodeBranchNodePages(node, pageSize, ids, false, nil)
}

func encodeBranchNodePages(node *Node, pageSize int64, ids []page.ID, forWAL bool, pages [][]byte) ([][]byte, error) {
	encodedSize := branchEncodedSize(node)
	if node.header.Type & ^overflowNodeFlag != NodeTypeBranch || uint64(encodedSize) > uint64(^uint32(0)) ||
		len(ids) == 0 || len(ids) != branchPageCount(node, pageSize) || ids[0] != node.PageID() || page.IsMetaID(ids[0]) {
		return nil, ErrInvalid
	}
	if len(ids) == 1 {
		size := int(pageSize)
		if forWAL {
			size = encodedSize
		}
		var data []byte
		if pages == nil {
			data = make([]byte, size)
		} else {
			data = pages[0][:size:size]
		}
		encodeBranchWALNode(data[:encodedSize], node)
		if !forWAL {
			binary.LittleEndian.PutUint32(data[14:18], pageChecksum(data, pageSize, nodeChecksumOffset))
		}
		if pages == nil {
			pages = make([][]byte, 1)
		}
		pages[0] = data
		return pages, nil
	}
	pages, err := prepareContinuationPages(ids, pageSize, pages)
	if err != nil {
		return nil, err
	}
	first := pages[0]
	binary.LittleEndian.PutUint16(first[:2], uint16(NodeTypeBranch|overflowNodeFlag))
	binary.LittleEndian.PutUint64(first[2:10], uint64(node.PageID()))
	binary.LittleEndian.PutUint32(first[10:14], uint32(len(node.entries)))
	binary.LittleEndian.PutUint32(first[18:22], uint32(encodedSize-NodeHeaderSize))
	index, at := 0, 0

	offset := NodeHeaderSize + len(node.entries)*branchEntryDescriptorSize + len(node.Children)*page.IDSize
	for _, entry := range node.entries {
		var fields [branchEntryDescriptorSize]byte
		binary.LittleEndian.PutUint32(fields[:4], uint32(offset))
		binary.LittleEndian.PutUint32(fields[4:8], uint32(len(entry.key)))
		copyContinuationBytes(pages, &index, &at, fields[:])
		offset += len(entry.key)
	}
	var field [page.IDSize]byte
	for _, id := range node.Children {
		binary.LittleEndian.PutUint64(field[:], uint64(id))
		copyContinuationBytes(pages, &index, &at, field[:])
	}
	for _, entry := range node.entries {
		copyContinuationBytes(pages, &index, &at, entry.key)
	}

	finishContinuationPages(pages, index, at, forWAL)
	return pages, nil
}

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

// readEncodedBranch checks the branch header and reads its body.
// It can decode a node, validate its full body, or search its encoded directory.
// A nil fragment reads data directly. Continued lookup and reference checks
// supply mapped fragments at logical offsets, including the normal 18-byte header.
// The owning entry point verifies the first page. WAL records must already
// have passed their record checksum checks.
func readEncodedBranch(data []byte, expectedPageID page.ID, storedChecksum uint32, key []byte, createNode bool, allowPadding bool, visit func(page.ID) error, fragment func(int) ([]byte, error)) (*Node, Entry, bool, page.ID, error) {
	if len(data) < NodeHeaderSize {
		return nil, Entry{}, false, 0, fmt.Errorf("read node header: %w", ErrInvalid)
	}
	nodeType := NodeType(binary.LittleEndian.Uint16(data[:2]))
	if nodeType != NodeTypeBranch && !(nodeType == NodeTypeBranch|overflowNodeFlag && fragment != nil && !createNode) {
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
	// Each separator has one descriptor and child ID. Count the extra child first.
	payloadSize := bodySize - NodeHeaderSize - page.IDSize
	if payloadSize < 0 {
		return nil, Entry{}, false, 0, fmt.Errorf("read node children: %w", ErrInvalid)
	}
	if uint64(entryCount) > uint64(payloadSize/(branchEntryDescriptorSize+page.IDSize)) {
		return nil, Entry{}, false, 0, fmt.Errorf("read node entry count: %w", ErrInvalid)
	}
	childrenStart := NodeHeaderSize + int(entryCount)*branchEntryDescriptorSize
	payloadStart := childrenStart + (int(entryCount)+1)*page.IDSize

	readEntry := func(index uint32) (int, int, error) {
		start := NodeHeaderSize + int(index)*branchEntryDescriptorSize
		var fields []byte
		if fragment == nil {
			fields = data[start : start+branchEntryDescriptorSize]
		} else {
			var scratch [branchEntryDescriptorSize]byte
			if err := copyEncodedBody(start, scratch[:], fragment); err != nil {
				return 0, 0, err
			}
			fields = scratch[:]
		}
		offset := binary.LittleEndian.Uint32(fields[:encodedUint32Size])
		keySize := binary.LittleEndian.Uint32(fields[encodedUint32Size:])
		if keySize == 0 || uint64(keySize) > uint64(MaxKeySize) {
			return 0, 0, fmt.Errorf("read node key %d: %w", index, ErrInvalid)
		}
		end := uint64(offset) + uint64(keySize)
		if uint64(offset) < uint64(payloadStart) || end > uint64(bodySize) {
			return 0, 0, fmt.Errorf("read node entry %d: %w", index, ErrInvalid)
		}
		return int(offset), int(end), nil
	}

	readChild := func(index uint32) (page.ID, error) {
		start := childrenStart + int(index)*page.IDSize
		var child page.ID
		if fragment == nil {
			child = page.ID(binary.LittleEndian.Uint64(data[start : start+page.IDSize]))
		} else {
			var scratch [page.IDSize]byte
			if err := copyEncodedBody(start, scratch[:], fragment); err != nil {
				return 0, err
			}
			child = page.ID(binary.LittleEndian.Uint64(scratch[:]))
		}
		if page.IsMetaID(child) {
			return 0, ErrInvalid
		}
		return child, nil
	}

	var node *Node
	if createNode {
		node = &Node{
			header: NodeHeader{
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
			start, end, err := readEntry(index)
			if err != nil {
				return nil, Entry{}, false, 0, err
			}
			var entry Entry
			if fragment == nil {
				entry.key = data[start:end:end]
			} else {
				if index > 0 {
					cmp, err := compareEncodedKey(start, end-start, previousKey, fragment)
					if err != nil {
						return nil, Entry{}, false, 0, err
					}
					if cmp <= 0 {
						return nil, Entry{}, false, 0, ErrInvalid
					}
				}
				previousKey = slices.Grow(previousKey[:0], end-start)[:end-start]
				if err := copyEncodedBody(start, previousKey, fragment); err != nil {
					return nil, Entry{}, false, 0, err
				}
				entry.key = previousKey
			}
			if start != nextOffset {
				return nil, Entry{}, false, 0, fmt.Errorf("read node entry offset %d: %w", index, ErrInvalid)
			}
			if index > 0 && fragment == nil && bytes.Compare(previousKey, entry.key) >= 0 {
				return nil, Entry{}, false, 0, fmt.Errorf("read node key order %d: %w", index, ErrInvalid)
			}
			previousKey = entry.key
			nextOffset = end

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
		start, end, err := readEntry(middle)
		if err != nil {
			return nil, Entry{}, false, 0, err
		}
		comparison := 0
		if fragment == nil {
			comparison = bytes.Compare(data[start:end], key)
		} else {
			comparison, err = compareEncodedKey(start, end-start, key, fragment)
			if err != nil {
				return nil, Entry{}, false, 0, err
			}
		}
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
