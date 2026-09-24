package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"slices"

	"github.com/Issaminu/kvlite/internal/checksum"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
)

const (
	nodeVersionOffset    = 0
	nodeTypeOffset       = 2
	nodePageIDOffset     = 4
	nodeEntryCountOffset = 12
	nodeChecksumOffset   = 16

	// encodedUint32Size is the size of each uint32 field.
	encodedUint32Size = 4
	// Each descriptor data offset is relative to the start of the encoded node.
	// A leaf body stores N descriptors, then N packed key and value pairs.
	// A leaf descriptor stores flags, data offset, key size, and value size.
	leafEntryDescriptorSize = 4 * encodedUint32Size
	// A branch body stores N descriptors, N+1 child page IDs, then N packed keys.
	// A branch descriptor stores data offset and key size.
	branchEntryDescriptorSize = 2 * encodedUint32Size

	checksumZeroBlockSize = 4096
)

var checksumZeroBlock [checksumZeroBlockSize]byte

// DecodeNode verifies and decodes a node.
// It checks the checksum before it reads the header or body.
// The checksum covers pageSize bytes except for the checksum field.
// Missing bytes after data are zero padding.
//
// expectedPageID must match the ID in the node header.
// On success, the node owns data.
// Its keys and values refer to data.
// The caller must not change or reuse data.
//
// DecodeNode returns [page.ErrChecksum] if the checksum does not match.
// It returns [page.ErrVersionMismatch] if the format version is not supported.
// It returns [page.ErrInvalid] if the size, type, page ID, or body is not valid.
func DecodeNode(data []byte, expectedPageID page.ID, pageSize int64) (*Node, error) {
	if len(data) < NodeHeaderSize {
		return nil, fmt.Errorf("read node header: %w", ErrInvalid)
	}
	if pageSize < NodeHeaderSize || int64(len(data)) > pageSize {
		return nil, fmt.Errorf("read node page size %d from %d bytes: %w", pageSize, len(data), ErrInvalid)
	}

	storedChecksum := binary.LittleEndian.Uint32(data[nodeChecksumOffset:NodeHeaderSize])
	if storedChecksum != nodePageChecksum(data, pageSize) {
		return nil, fmt.Errorf("verify node checksum: %w", page.ErrChecksum)
	}
	node, _, _, _, err := readEncodedNode(data, expectedPageID, storedChecksum, nil, true, true)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// DecodeWALNode decodes a compact node from a verified WAL record.
// It does not check a node checksum.
// The WAL record checksum protects data.
// A WAL node has no page padding and stores zero in its node checksum field.
// DecodeWALNode rejects a nonzero node checksum.
// expectedPageID must match the ID in the node header.
//
// On success, the node owns data.
// Its keys and values refer to data.
// The caller must not change or reuse data.
//
// DecodeWALNode returns [page.ErrVersionMismatch] if the format version is not supported.
// It returns [page.ErrInvalid] if the node is not valid.
func DecodeWALNode(data []byte, expectedPageID page.ID) (*Node, error) {
	if len(data) < NodeHeaderSize {
		return nil, fmt.Errorf("read node header: %w", ErrInvalid)
	}
	if checksum := binary.LittleEndian.Uint32(data[nodeChecksumOffset:NodeHeaderSize]); checksum != 0 {
		return nil, fmt.Errorf("read WAL node checksum %x: %w", checksum, ErrInvalid)
	}
	node, _, _, _, err := readEncodedNode(data, expectedPageID, 0, nil, true, false)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// ValidateWALNode checks every field in one compact node from a verified WAL record.
// It does not create a [Node] or change data.
func ValidateWALNode(data []byte, expectedPageID page.ID) error {
	if len(data) < NodeHeaderSize {
		return fmt.Errorf("read node header: %w", ErrInvalid)
	}
	if checksum := binary.LittleEndian.Uint32(data[nodeChecksumOffset:NodeHeaderSize]); checksum != 0 {
		return fmt.Errorf("read WAL node checksum %x: %w", checksum, ErrInvalid)
	}
	_, _, _, _, err := readEncodedNode(data, expectedPageID, 0, nil, false, false)
	return err
}

func validateLookupKey(key []byte) error {
	if len(key) == 0 {
		return ErrKeyRequired
	}
	if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	}
	return nil
}

// LookupMappedNode finds key in one mapped database page without decoding a [Node].
// It checks each field that it reads.
// It does not check the page checksum.
// The caller must keep data unchanged while it uses the result.
// A found entry refers to data and must not outlive it.
// A nonzero child page ID selects the next branch page.
func LookupMappedNode(data []byte, expectedPageID page.ID, key []byte) (Entry, bool, page.ID, error) {
	if err := validateLookupKey(key); err != nil {
		return Entry{}, false, 0, err
	}
	if len(data) < NodeHeaderSize {
		return Entry{}, false, 0, fmt.Errorf("read node header: %w", ErrInvalid)
	}
	_, entry, found, child, err := readEncodedNode(data, expectedPageID, 0, key, false, true)
	return entry, found, child, err
}

// VisitMappedLeafRange visits entries in the half-open range [start, end).
// It checks descriptor offsets and byte bounds. It does not check the page checksum or unused padding.
// The key and value passed to visit refer to data.
func VisitMappedLeafRange(data []byte, expectedPageID page.ID, start, end []byte, visit func(uint32, []byte, []byte) error) error {
	if len(data) < NodeHeaderSize {
		return fmt.Errorf("read leaf page: %w", ErrInvalid)
	}
	if version := binary.LittleEndian.Uint16(data[nodeVersionOffset:nodeTypeOffset]); version != nodeFormatVersion {
		return fmt.Errorf("read leaf format version %d: %w", version, page.ErrVersionMismatch)
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

// LookupEncodedWALNode finds key in one compact node from a verified WAL record.
// It checks each field that it reads.
// It requires a zero node checksum.
// The WAL record checksum protects the data.
// A found entry refers to data and must not outlive it.
// A nonzero child page ID selects the next branch page.
func LookupEncodedWALNode(data []byte, expectedPageID page.ID, key []byte) (Entry, bool, page.ID, error) {
	if err := validateLookupKey(key); err != nil {
		return Entry{}, false, 0, err
	}
	if len(data) < NodeHeaderSize {
		return Entry{}, false, 0, fmt.Errorf("read node header: %w", ErrInvalid)
	}
	if checksum := binary.LittleEndian.Uint32(data[nodeChecksumOffset:NodeHeaderSize]); checksum != 0 {
		return Entry{}, false, 0, fmt.Errorf("read WAL node checksum %x: %w", checksum, ErrInvalid)
	}
	_, entry, found, child, err := readEncodedNode(data, expectedPageID, 0, key, false, false)
	return entry, found, child, err
}

// readEncodedNode reads one fixed-directory node.
//
// If createNode is true, readEncodedNode ignores key. It validates the complete body and returns a [Node].
//
// If createNode is false and key is nil, readEncodedNode validates the complete body without creating a [Node].
//
// If createNode is false and key is not nil, readEncodedNode performs a point lookup. A leaf match returns an [Entry] and true. A branch returns its child page ID. A missing leaf key returns zero results. Point lookup validates only the fields that it reads.
//
// Node entries and returned entry bytes refer to data. The caller must keep data unchanged while it uses these results.
//
// The caller must check the header length and the source checksum before this call. allowPadding applies only to complete-body validation.
func readEncodedNode(data []byte, expectedPageID page.ID, storedChecksum uint32, key []byte, createNode bool, allowPadding bool) (*Node, Entry, bool, page.ID, error) {
	formatVersion := binary.LittleEndian.Uint16(data[nodeVersionOffset:nodeTypeOffset])
	if formatVersion != nodeFormatVersion {
		return nil, Entry{}, false, 0, fmt.Errorf("read node format version %d: %w", formatVersion, page.ErrVersionMismatch)
	}
	nodeType := NodeType(binary.LittleEndian.Uint16(data[nodeTypeOffset:nodePageIDOffset]))
	if nodeType != NodeTypeLeaf && nodeType != NodeTypeBranch {
		return nil, Entry{}, false, 0, fmt.Errorf("read node type %d: %w", nodeType, ErrInvalid)
	}
	pageID := page.ID(binary.LittleEndian.Uint64(data[nodePageIDOffset:nodeEntryCountOffset]))
	if pageID != expectedPageID {
		return nil, Entry{}, false, 0, fmt.Errorf("read node page ID %d, want %d: %w", pageID, expectedPageID, ErrInvalid)
	}

	entryCount := binary.LittleEndian.Uint32(data[nodeEntryCountOffset:nodeChecksumOffset])
	directoryStart := NodeHeaderSize
	descriptorSize := leafEntryDescriptorSize
	minimumEntrySize := descriptorSize
	// payload size = bytes available for descriptors and entry data.
	payloadSize := len(data) - directoryStart
	if nodeType == NodeTypeBranch {
		descriptorSize = branchEntryDescriptorSize
		minimumEntrySize = descriptorSize + page.IDSize
		if payloadSize < page.IDSize {
			return nil, Entry{}, false, 0, fmt.Errorf("read node children: %w", ErrInvalid)
		}
		// A branch has one more child than separator keys.
		payloadSize -= page.IDSize
	}

	maximumEntryCount := uint64(payloadSize / minimumEntrySize)
	if uint64(entryCount) > maximumEntryCount {
		return nil, Entry{}, false, 0, fmt.Errorf("read node entry count: %w", ErrInvalid)
	}
	childrenStart := directoryStart + int(entryCount)*descriptorSize
	payloadStart := childrenStart

	// For branch nodes, we need to skip over the child page IDs that come after each entry descriptor.
	// Branch nodes have one more child than they have separator keys (entryCount + 1 children total).
	// Each child page ID takes up page.IDSize bytes, so we advance payloadStart by that amount.
	if nodeType == NodeTypeBranch {
		payloadStart += (int(entryCount) + 1) * page.IDSize
	}

	readEntry := func(index uint32) (Entry, int, int, error) {
		descriptorStart := directoryStart + int(index)*descriptorSize
		fields := data[descriptorStart : descriptorStart+descriptorSize]
		var flags, valueSize uint32
		offsetField := 0
		keySizeField := encodedUint32Size
		if nodeType == NodeTypeLeaf {
			flags = binary.LittleEndian.Uint32(fields[:encodedUint32Size])
			offsetField = encodedUint32Size
			keySizeField = 2 * encodedUint32Size
			valueSize = binary.LittleEndian.Uint32(fields[3*encodedUint32Size : leafEntryDescriptorSize])
		}

		offset := binary.LittleEndian.Uint32(fields[offsetField : offsetField+encodedUint32Size])
		keySize := binary.LittleEndian.Uint32(fields[keySizeField : keySizeField+encodedUint32Size])
		if keySize == 0 || uint64(keySize) > uint64(MaxKeySize) {
			return Entry{}, 0, 0, fmt.Errorf("read node key %d: %w", index, ErrInvalid)
		}
		if flags&^uint32(BucketLeafFlag) != 0 || uint64(valueSize) > uint64(MaxValueSize) {
			return Entry{}, 0, 0, fmt.Errorf("read node entry %d: %w", index, ErrInvalid)
		}

		start := uint64(offset)
		keyEnd := start + uint64(keySize)
		end := keyEnd + uint64(valueSize)
		if start < uint64(payloadStart) || end > uint64(len(data)) {
			return Entry{}, 0, 0, fmt.Errorf("read node entry %d: %w", index, ErrInvalid)
		}
		startIndex, keyEndIndex, endIndex := int(start), int(keyEnd), int(end)
		entry := Entry{
			flags: flags,
			key:   data[startIndex:keyEndIndex:keyEndIndex],
		}
		if nodeType == NodeTypeLeaf {
			entry.value = data[keyEndIndex:endIndex:endIndex]
		}
		return entry, startIndex, endIndex, nil
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
				FormatVersion: nodeFormatVersion,
				Type:          nodeType,
				PageID:        expectedPageID,
				EntryCount:    entryCount,
				Checksum:      storedChecksum,
			},
			entries: make([]Entry, 0, int(entryCount)),
		}
		if nodeType == NodeTypeBranch {
			node.Children = make([]page.ID, 0, int(entryCount)+1)
		}
	}

	if createNode || key == nil {
		nextOffset := payloadStart
		var previousKey []byte
		if nodeType == NodeTypeBranch {
			for index := uint32(0); index <= entryCount; index++ {
				child, err := readChild(index)
				if err != nil {
					return nil, Entry{}, false, 0, err
				}
				if createNode {
					node.Children = append(node.Children, child)
				}
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
		// A leaf uses a lower bound. A branch uses an upper bound.
		if comparison > 0 || (nodeType == NodeTypeLeaf && comparison == 0) {
			high = middle
		} else {
			low = middle + 1
		}
	}

	if nodeType == NodeTypeLeaf {
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

	childPageID, err := readChild(low)
	if err != nil {
		return nil, Entry{}, false, 0, err
	}
	return nil, Entry{}, false, childPageID, nil
}

// WriteNode writes a node with a checksum for pageSize bytes.
// If shouldPad is true, it adds zeros and writes pageSize bytes.
// If shouldPad is false, it does not write the zeros.
// The checksum still covers the missing zeros.
//
// WriteNode stores the checksum in the node header.
// It returns ErrNodeTooLarge if the node is larger than pageSize.
// It completes partial writes or returns the writer error.
func WriteNode(w io.Writer, node *Node, pageSize int64, shouldPad bool) error {
	encoded := EncodeNode(node, pageSize)
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

// EncodeNode returns a new compact node with a checksum for pageSize bytes.
// It does not include page padding.
// The checksum includes the missing zero padding.
// The checksum covers all page bytes except the checksum field.
// EncodeNode also stores the checksum in the node header.
//
// pageSize must not be smaller than the encoded node.
// Use [WriteNode] to check the size before a write.
func EncodeNode(node *Node, pageSize int64) []byte {
	data := EncodeWALNode(node)
	node.header.Checksum = nodePageChecksum(data, pageSize)
	binary.LittleEndian.PutUint32(data[nodeChecksumOffset:NodeHeaderSize], node.header.Checksum)
	return data
}

// EncodeWALNode returns a new compact node for a WAL record.
// It does not include page padding.
// It writes zero in the node checksum field.
// The WAL record stores the data length and checksum.
// EncodeWALNode does not change the checksum in node.
func EncodeWALNode(node *Node) []byte {
	data := make([]byte, WALNodeEncodedSize(node))
	encodeWALNode(data, node)
	return data
}

// WALNodeEncodedSize returns the compact encoded size of node in a WAL record.
func WALNodeEncodedSize(node *Node) int {
	return NodeHeaderSize + nodePayloadSize(node)
}

// AppendEncodedWALNode appends one compact node to data. It writes zero in the node checksum field because the WAL record checksum protects the data.
func AppendEncodedWALNode(data []byte, node *Node) []byte {
	start := len(data)
	encodedSize := WALNodeEncodedSize(node)
	data = slices.Grow(data, encodedSize)
	data = data[:start+encodedSize]
	encodeWALNode(data[start:], node)
	return data
}

func encodeWALNode(data []byte, node *Node) {
	clear(data)
	binary.LittleEndian.PutUint16(data[nodeVersionOffset:nodeTypeOffset], node.header.FormatVersion)
	binary.LittleEndian.PutUint16(data[nodeTypeOffset:nodePageIDOffset], uint16(node.header.Type))
	binary.LittleEndian.PutUint64(data[nodePageIDOffset:nodeEntryCountOffset], uint64(node.header.PageID))
	binary.LittleEndian.PutUint32(data[nodeEntryCountOffset:nodeChecksumOffset], uint32(len(node.entries)))
	directoryStart := NodeHeaderSize
	descriptorSize := leafEntryDescriptorSize
	if !node.IsLeaf() {
		descriptorSize = branchEntryDescriptorSize
	}
	childrenStart := directoryStart + len(node.entries)*descriptorSize
	payloadOffset := childrenStart
	if !node.IsLeaf() {
		for index := 0; index <= len(node.entries); index++ {
			start := childrenStart + index*page.IDSize
			binary.LittleEndian.PutUint64(data[start:start+page.IDSize], uint64(node.Children[index]))
		}
		payloadOffset += (len(node.entries) + 1) * page.IDSize
	}

	for index, entry := range node.entries {
		descriptorStart := directoryStart + index*descriptorSize
		descriptor := data[descriptorStart : descriptorStart+descriptorSize]
		if node.IsLeaf() {
			binary.LittleEndian.PutUint32(descriptor[:encodedUint32Size], entry.flags)
			binary.LittleEndian.PutUint32(descriptor[encodedUint32Size:2*encodedUint32Size], uint32(payloadOffset))
			binary.LittleEndian.PutUint32(descriptor[2*encodedUint32Size:3*encodedUint32Size], uint32(len(entry.key)))
			binary.LittleEndian.PutUint32(descriptor[3*encodedUint32Size:leafEntryDescriptorSize], uint32(len(entry.value)))
			payloadOffset += copy(data[payloadOffset:], entry.key)
			payloadOffset += copy(data[payloadOffset:], entry.value)
			continue
		}
		binary.LittleEndian.PutUint32(descriptor[:encodedUint32Size], uint32(payloadOffset))
		binary.LittleEndian.PutUint32(descriptor[encodedUint32Size:branchEntryDescriptorSize], uint32(len(entry.key)))
		payloadOffset += copy(data[payloadOffset:], entry.key)
	}
}

// VerifyNodeIDAndSetChecksum checks the node ID and sets the page checksum.
// len(data) is the page size.
// The checksum covers all bytes except the checksum field.
// This includes zero padding.
//
// VerifyNodeIDAndSetChecksum does not check the node type or body.
// It returns [page.ErrInvalid] if data has no full header.
// It also returns [page.ErrInvalid] if the node ID does not match expectedPageID.
func VerifyNodeIDAndSetChecksum(data []byte, expectedPageID page.ID) error {
	if len(data) < NodeHeaderSize {
		return fmt.Errorf("set node checksum: read header: %w", ErrInvalid)
	}
	pageID := page.ID(binary.LittleEndian.Uint64(data[nodePageIDOffset:nodeEntryCountOffset]))
	if pageID != expectedPageID {
		return fmt.Errorf("set node checksum: read page ID %d, want %d: %w", pageID, expectedPageID, ErrInvalid)
	}
	checksum := nodePageChecksum(data, int64(len(data)))
	binary.LittleEndian.PutUint32(data[nodeChecksumOffset:NodeHeaderSize], checksum)
	return nil
}

func nodePayloadSize(node *Node) int {
	size := 0
	if !node.IsLeaf() {
		size += page.IDSize
	}
	for _, entry := range node.entries {
		size += entry.EncodedSize(node.IsLeaf())
	}
	return size
}

// nodePageChecksum calculates CRC32C for a page.
// It skips the checksum field.
// It treats missing bytes before pageSize as zeros.
func nodePageChecksum(data []byte, pageSize int64) uint32 {
	sum := checksum.Sum32(data[:nodeChecksumOffset], data[NodeHeaderSize:])

	// For a full database page, this loop does not run because len(data) == pageSize.
	for remaining := pageSize - int64(len(data)); remaining > 0; {
		chunkSize := min(remaining, int64(len(checksumZeroBlock)))
		sum = checksum.Update32(sum, checksumZeroBlock[:chunkSize])
		remaining -= chunkSize
	}
	return sum
}
