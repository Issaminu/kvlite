package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/Issaminu/kvlite/internal/checksum"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
)

const (
	nodeVersionOffset  = 0
	nodeTypeOffset     = 2
	nodePageIDOffset   = 4
	nodeChecksumOffset = 12

	// encodedUint32Size is the size of each uint32 field.
	encodedUint32Size = 4

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
// It returns [page.ErrInvalid] if the size, type, page ID, body, or padding is not valid.
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
	return decodeEncodedNode(data, expectedPageID, storedChecksum)
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
	return decodeEncodedNode(data, expectedPageID, 0)
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
	return lookupEncodedNode(data, expectedPageID, key)
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
	return lookupEncodedNode(data, expectedPageID, key)
}

// encodedNodeReader parses one node body without allocating memory.
// Full decode and direct lookup both use this reader.
// They enforce one byte format.
type encodedNodeReader struct {
	nodeType   NodeType
	entryCount uint32
	body       []byte
}

// newEncodedNodeReader checks the fields shared by decode and direct lookup.
// The body starts with a uint32 entry count.
// Each entry stores flags and a length-prefixed key.
// A leaf entry also stores a length-prefixed value.
// A branch stores one more child page ID than its entry count.
// All numbers use little-endian encoding.
func newEncodedNodeReader(data []byte, expectedPageID page.ID) (encodedNodeReader, error) {
	formatVersion := binary.LittleEndian.Uint16(data[nodeVersionOffset:nodeTypeOffset])
	if formatVersion != nodeFormatVersion {
		return encodedNodeReader{}, fmt.Errorf("read node format version %d: %w", formatVersion, page.ErrVersionMismatch)
	}
	nodeType := NodeType(binary.LittleEndian.Uint16(data[nodeTypeOffset:nodePageIDOffset]))
	if nodeType != NodeTypeLeaf && nodeType != NodeTypeBranch {
		return encodedNodeReader{}, fmt.Errorf("read node type %d: %w", nodeType, ErrInvalid)
	}
	pageID := page.ID(binary.LittleEndian.Uint64(data[nodePageIDOffset:nodeChecksumOffset]))
	if pageID != expectedPageID {
		return encodedNodeReader{}, fmt.Errorf("read node page ID %d, want %d: %w", pageID, expectedPageID, ErrInvalid)
	}

	body := data[NodeHeaderSize:]
	if len(body) < encodedUint32Size {
		return encodedNodeReader{}, fmt.Errorf("read node entry count: %w", ErrInvalid)
	}
	entryCount := binary.LittleEndian.Uint32(body[:encodedUint32Size])
	// Every entry stores flags and a key length. A leaf also stores a value length.
	minimumEntrySize := 2 * encodedUint32Size
	isLeaf := nodeType == NodeTypeLeaf
	if isLeaf {
		minimumEntrySize += encodedUint32Size
	}

	// payload size = len(what remains in the body slice) - size of entryCount
	payloadSize := len(body) - encodedUint32Size
	maximumEntryCount := uint64(payloadSize / minimumEntrySize)
	if uint64(entryCount) > maximumEntryCount {
		return encodedNodeReader{}, fmt.Errorf("read node entry count: %w", ErrInvalid)
	}
	return encodedNodeReader{
		nodeType:   nodeType,
		entryCount: entryCount,
		body:       body[encodedUint32Size:],
	}, nil
}

func (reader *encodedNodeReader) readEntry(index uint32) (Entry, error) {
	if len(reader.body) < encodedUint32Size {
		return Entry{}, fmt.Errorf("read node flags %d: %w", index, ErrInvalid)
	}
	entry := Entry{flags: binary.LittleEndian.Uint32(reader.body[:encodedUint32Size])}
	reader.body = reader.body[encodedUint32Size:]

	entryKey, remaining, err := decodeLengthPrefixedBytes(reader.body)
	if err != nil {
		return Entry{}, fmt.Errorf("read node key %d: %w", index, err)
	}
	entry.key = entryKey
	reader.body = remaining
	if reader.nodeType == NodeTypeLeaf {
		value, remaining, err := decodeLengthPrefixedBytes(reader.body)
		if err != nil {
			return Entry{}, fmt.Errorf("read node value %d: %w", index, err)
		}
		entry.value = value
		reader.body = remaining
	}
	return entry, nil
}

func (reader *encodedNodeReader) readChildren() ([]byte, error) {
	if reader.nodeType == NodeTypeLeaf {
		return nil, nil
	}

	childCount := uint64(reader.entryCount) + 1
	childrenSize := childCount * uint64(page.IDSize)
	if childrenSize > uint64(len(reader.body)) {
		return nil, fmt.Errorf("read node children: %w", ErrInvalid)
	}
	return reader.body[:childrenSize:childrenSize], nil
}

// readChildrenAndValidate returns the encoded child list after all entries are read.
// A leaf has no child list. All remaining bytes after the node must be zero.
func (reader *encodedNodeReader) readChildrenAndValidate() ([]byte, error) {
	children, err := reader.readChildren()
	if err != nil {
		return nil, err
	}
	reader.body = reader.body[len(children):]
	if !allZero(reader.body) {
		return nil, fmt.Errorf("read node padding: %w", ErrInvalid)
	}
	return children, nil
}

func decodeEncodedNode(data []byte, expectedPageID page.ID, storedChecksum uint32) (*Node, error) {
	reader, err := newEncodedNodeReader(data, expectedPageID)
	if err != nil {
		return nil, err
	}
	node := &Node{
		header: &NodeHeader{
			FormatVersion: nodeFormatVersion,
			Type:          reader.nodeType,
			PageID:        expectedPageID,
			Checksum:      storedChecksum,
		},
		entries: make([]Entry, 0, int(reader.entryCount)),
	}
	for index := uint32(0); index < reader.entryCount; index++ {
		entry, err := reader.readEntry(index)
		if err != nil {
			return nil, err
		}
		node.entries = append(node.entries, entry)
	}

	children, err := reader.readChildrenAndValidate()
	if err != nil {
		return nil, err
	}
	if reader.nodeType == NodeTypeBranch {
		node.Children = make([]page.ID, int(reader.entryCount)+1)
		for index := range node.Children {
			offset := index * page.IDSize
			node.Children[index] = page.ID(binary.LittleEndian.Uint64(children[offset : offset+page.IDSize]))
		}
	}
	return node, nil
}

func lookupEncodedNode(data []byte, expectedPageID page.ID, key []byte) (Entry, bool, page.ID, error) {
	reader, err := newEncodedNodeReader(data, expectedPageID)
	if err != nil {
		return Entry{}, false, 0, err
	}

	if reader.nodeType == NodeTypeLeaf {
		for index := uint32(0); index < reader.entryCount; index++ {
			entry, err := reader.readEntry(index)
			if err != nil {
				return Entry{}, false, 0, err
			}
			comparison := bytes.Compare(entry.key, key)
			if comparison == 0 {
				return entry, true, 0, nil
			}
			if comparison > 0 {
				return Entry{}, false, 0, nil
			}
		}
		return Entry{}, false, 0, nil
	}

	childIndex := reader.entryCount
	for index := uint32(0); index < reader.entryCount; index++ {
		entry, err := reader.readEntry(index)
		if err != nil {
			return Entry{}, false, 0, err
		}
		if childIndex == reader.entryCount && bytes.Compare(key, entry.key) < 0 {
			// we found the childIndex now, we should be able to `break` ...
			childIndex = index
			// ... but we can't break, since we need to consume all `reader.entries` before we can call `readChildren()` below
		}
	}

	children, err := reader.readChildren()
	if err != nil {
		return Entry{}, false, 0, err
	}
	childOffset := uint64(childIndex) * uint64(page.IDSize)
	childPageID := page.ID(binary.LittleEndian.Uint64(children[childOffset : childOffset+page.IDSize]))
	if childPageID == page.Meta0ID {
		return Entry{}, false, 0, fmt.Errorf("read node child page ID %d: %w", childPageID, ErrInvalid)
	}
	return Entry{}, false, childPageID, nil
}

// decodeLengthPrefixedBytes reads one uint32 length and its bytes.
// It does not copy the bytes.
// The returned value refers to data and has no spare capacity.
// An append cannot overwrite the unread bytes.
// It returns ErrInvalid if data does not contain the full value.
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
	return data[:size:size], data[size:], nil
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
	payloadSize := nodePayloadSize(node)
	data := make([]byte, NodeHeaderSize, NodeHeaderSize+payloadSize)
	binary.LittleEndian.PutUint16(data[nodeVersionOffset:nodeTypeOffset], node.header.FormatVersion)
	binary.LittleEndian.PutUint16(data[nodeTypeOffset:nodePageIDOffset], uint16(node.header.Type))
	binary.LittleEndian.PutUint64(data[nodePageIDOffset:nodeChecksumOffset], uint64(node.header.PageID))
	data = binary.LittleEndian.AppendUint32(data, uint32(len(node.entries)))

	for _, e := range node.entries {
		data = binary.LittleEndian.AppendUint32(data, e.flags)
		data = binary.LittleEndian.AppendUint32(data, uint32(len(e.key)))
		data = append(data, e.key...)
		if node.IsLeaf() {
			data = binary.LittleEndian.AppendUint32(data, uint32(len(e.value)))
			data = append(data, e.value...)
		}
	}

	if !node.IsLeaf() {
		for _, child := range node.Children {
			data = binary.LittleEndian.AppendUint64(data, uint64(child))
		}
	}
	return data
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
	pageID := page.ID(binary.LittleEndian.Uint64(data[nodePageIDOffset:nodeChecksumOffset]))
	if pageID != expectedPageID {
		return fmt.Errorf("set node checksum: read page ID %d, want %d: %w", pageID, expectedPageID, ErrInvalid)
	}
	checksum := nodePageChecksum(data, int64(len(data)))
	binary.LittleEndian.PutUint32(data[nodeChecksumOffset:NodeHeaderSize], checksum)
	return nil
}

func nodePayloadSize(node *Node) int {
	size := encodedUint32Size
	for _, entry := range node.entries {
		size += entry.EncodedSize(node.IsLeaf())
	}
	if !node.IsLeaf() {
		size += len(node.Children) * page.IDSize
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

func allZero(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}
