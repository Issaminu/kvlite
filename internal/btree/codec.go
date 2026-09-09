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
	node, _, _, _, err := readEncodedNode(data, expectedPageID, storedChecksum, nil, true)
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
	node, _, _, _, err := readEncodedNode(data, expectedPageID, 0, nil, true)
	if err != nil {
		return nil, err
	}
	return node, nil
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
	_, entry, found, child, err := readEncodedNode(data, expectedPageID, 0, key, false)
	return entry, found, child, err
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
	_, entry, found, child, err := readEncodedNode(data, expectedPageID, 0, key, false)
	return entry, found, child, err
}

// readEncodedNode reads one encoded node in node mode or point lookup mode.
//
// If createNode is true, readEncodedNode ignores key. It validates the complete body and returns a [Node]. The other results are zero.
//
// If createNode is false, readEncodedNode does not create a [Node]. A leaf match returns an [Entry] and true. A branch returns its child page ID. A missing leaf key returns zero results. Point lookup validates only the fields that it reads.
//
// Node entries and returned entry bytes refer to data. The caller must keep data unchanged while it uses these results.
//
// The caller must check the header length and the source checksum before this call.
func readEncodedNode(data []byte, expectedPageID page.ID, storedChecksum uint32, key []byte, createNode bool) (*Node, Entry, bool, page.ID, error) {
	formatVersion := binary.LittleEndian.Uint16(data[nodeVersionOffset:nodeTypeOffset])
	if formatVersion != nodeFormatVersion {
		return nil, Entry{}, false, 0, fmt.Errorf("read node format version %d: %w", formatVersion, page.ErrVersionMismatch)
	}
	nodeType := NodeType(binary.LittleEndian.Uint16(data[nodeTypeOffset:nodePageIDOffset]))
	if nodeType != NodeTypeLeaf && nodeType != NodeTypeBranch {
		return nil, Entry{}, false, 0, fmt.Errorf("read node type %d: %w", nodeType, ErrInvalid)
	}
	pageID := page.ID(binary.LittleEndian.Uint64(data[nodePageIDOffset:nodeChecksumOffset]))
	if pageID != expectedPageID {
		return nil, Entry{}, false, 0, fmt.Errorf("read node page ID %d, want %d: %w", pageID, expectedPageID, ErrInvalid)
	}

	body := data[NodeHeaderSize:]
	if len(body) < encodedUint32Size {
		return nil, Entry{}, false, 0, fmt.Errorf("read node entry count: %w", ErrInvalid)
	}
	entryCount := binary.LittleEndian.Uint32(body[:encodedUint32Size])
	// Every entry stores flags and a key length. A leaf also stores a value length.
	minimumEntrySize := 2 * encodedUint32Size
	if nodeType == NodeTypeLeaf {
		minimumEntrySize += encodedUint32Size
	}

	// payload size = len(what remains in the body slice) - size of entryCount
	payloadSize := len(body) - encodedUint32Size
	maximumEntryCount := uint64(payloadSize / minimumEntrySize)
	if uint64(entryCount) > maximumEntryCount {
		return nil, Entry{}, false, 0, fmt.Errorf("read node entry count: %w", ErrInvalid)
	}
	body = body[encodedUint32Size:]

	var node *Node
	if createNode {
		node = &Node{
			header: &NodeHeader{
				FormatVersion: nodeFormatVersion,
				Type:          nodeType,
				PageID:        expectedPageID,
				Checksum:      storedChecksum,
			},
			entries: make([]Entry, 0, int(entryCount)),
		}
	}

	// Branch child IDs follow all variable-size entries. Branch lookup must read every entry before it can read the selected child.
	childIndex := entryCount
	for index := uint32(0); index < entryCount; index++ {
		if len(body) < encodedUint32Size {
			return nil, Entry{}, false, 0, fmt.Errorf("read node flags %d: %w", index, ErrInvalid)
		}
		entry := Entry{flags: binary.LittleEndian.Uint32(body[:encodedUint32Size])}
		body = body[encodedUint32Size:]

		if len(body) < encodedUint32Size {
			return nil, Entry{}, false, 0, fmt.Errorf("read node key %d: %w", index, ErrInvalid)
		}
		keySize := binary.LittleEndian.Uint32(body[:encodedUint32Size])
		body = body[encodedUint32Size:]
		if uint64(keySize) > uint64(len(body)) {
			return nil, Entry{}, false, 0, fmt.Errorf("read node key %d: %w", index, ErrInvalid)
		}
		size := int(keySize)
		entry.key = body[:size:size]
		body = body[size:]

		if nodeType == NodeTypeLeaf {
			if len(body) < encodedUint32Size {
				return nil, Entry{}, false, 0, fmt.Errorf("read node value %d: %w", index, ErrInvalid)
			}
			valueSize := binary.LittleEndian.Uint32(body[:encodedUint32Size])
			body = body[encodedUint32Size:]
			if uint64(valueSize) > uint64(len(body)) {
				return nil, Entry{}, false, 0, fmt.Errorf("read node value %d: %w", index, ErrInvalid)
			}
			size = int(valueSize)
			entry.value = body[:size:size]
			body = body[size:]
		}

		if createNode {
			node.entries = append(node.entries, entry)
		} else if nodeType == NodeTypeLeaf {
			comparison := bytes.Compare(entry.key, key)
			if comparison == 0 {
				return nil, entry, true, 0, nil
			}
			if comparison > 0 {
				return nil, Entry{}, false, 0, nil
			}
		} else if childIndex == entryCount && bytes.Compare(key, entry.key) < 0 {
			childIndex = index
		}
	}

	if nodeType == NodeTypeLeaf && !createNode {
		return nil, Entry{}, false, 0, nil
	}

	childCount := uint64(entryCount) + 1
	childrenSize := childCount * uint64(page.IDSize)
	if nodeType == NodeTypeLeaf {
		childrenSize = 0
	}
	if childrenSize > uint64(len(body)) {
		return nil, Entry{}, false, 0, fmt.Errorf("read node children: %w", ErrInvalid)
	}
	children := body[:int(childrenSize):int(childrenSize)]
	body = body[len(children):]

	if createNode {
		if !allZero(body) {
			return nil, Entry{}, false, 0, fmt.Errorf("read node padding: %w", ErrInvalid)
		}
		if nodeType == NodeTypeBranch {
			node.Children = make([]page.ID, int(childCount))
			for index := range node.Children {
				offset := index * page.IDSize
				node.Children[index] = page.ID(binary.LittleEndian.Uint64(children[offset : offset+page.IDSize]))
			}
		}
		return node, Entry{}, false, 0, nil
	}

	childOffset := uint64(childIndex) * uint64(page.IDSize)
	childPageID := page.ID(binary.LittleEndian.Uint64(children[childOffset : childOffset+page.IDSize]))
	if childPageID == page.Meta0ID {
		return nil, Entry{}, false, 0, fmt.Errorf("read node child page ID %d: %w", childPageID, ErrInvalid)
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
