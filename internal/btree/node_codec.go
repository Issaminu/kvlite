package btree

import (
	"encoding/binary"
	"fmt"
	"io"
	"slices"

	"github.com/Issaminu/kvlite/internal/checksum"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
)

const (
	nodeTypeOffset       = 0
	nodePageIDOffset     = 2
	nodeEntryCountOffset = 10
	nodeChecksumOffset   = 14

	// encodedUint32Size is the size of each uint32 field.
	encodedUint32Size = 4
)

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
	node, _, _, _, err := readEncodedNode(data, expectedPageID, storedChecksum, nil, true, true, nil)
	if err != nil {
		return nil, err
	}
	return node, nil
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
	_, entry, found, child, err := readEncodedNode(data, expectedPageID, 0, key, false, true, nil)
	return entry, found, child, err
}

// readEncodedNode reads one fixed-directory node.
//
// If createNode is true, readEncodedNode ignores key. It validates the complete body and returns a [Node].
//
// If createNode is false and key is nil, readEncodedNode validates the complete body without creating a [Node].
// In that mode, visit can receive each child page during validation.
//
// If createNode is false and key is not nil, readEncodedNode performs a point lookup. A leaf match returns an [Entry] and true. A branch returns its child page ID. A missing leaf key returns zero results. Point lookup validates only the fields that it reads.
//
// Node entries and returned entry bytes refer to data. The caller must keep data unchanged while it uses these results.
//
// The caller must check the header length and the source checksum before this call. allowPadding applies only to complete-body validation.
func readEncodedNode(data []byte, expectedPageID page.ID, storedChecksum uint32, key []byte, createNode bool, allowPadding bool, visit func(page.ID) error) (*Node, Entry, bool, page.ID, error) {
	nodeType := NodeType(binary.LittleEndian.Uint16(data[nodeTypeOffset:nodePageIDOffset]))
	if nodeType != NodeTypeLeaf && nodeType != NodeTypeBranch {
		return nil, Entry{}, false, 0, fmt.Errorf("read node type %d: %w", nodeType, ErrInvalid)
	}
	pageID := page.ID(binary.LittleEndian.Uint64(data[nodePageIDOffset:nodeEntryCountOffset]))
	if pageID != expectedPageID {
		return nil, Entry{}, false, 0, fmt.Errorf("read node page ID %d, want %d: %w", pageID, expectedPageID, ErrInvalid)
	}

	if nodeType == NodeTypeLeaf {
		return readEncodedLeaf(data, expectedPageID, storedChecksum, key, createNode, allowPadding, visit)
	}
	return readEncodedBranch(data, expectedPageID, storedChecksum, key, createNode, allowPadding, visit)
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

// WALBodyOffset returns the first byte after the entry descriptor directory in a compact WAL node image.
func (n *Node) WALBodyOffset() int {
	descriptorSize := leafEntryDescriptorSize
	if !n.IsLeaf() {
		descriptorSize = branchEntryDescriptorSize
	}
	return NodeHeaderSize + len(n.entries)*descriptorSize
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
// It returns [page.ErrInvalid] if the node is not valid.
func DecodeWALNode(data []byte, expectedPageID page.ID) (*Node, error) {
	if len(data) < NodeHeaderSize {
		return nil, fmt.Errorf("read node header: %w", ErrInvalid)
	}
	if checksum := binary.LittleEndian.Uint32(data[nodeChecksumOffset:NodeHeaderSize]); checksum != 0 {
		return nil, fmt.Errorf("read WAL node checksum %x: %w", checksum, ErrInvalid)
	}
	node, _, _, _, err := readEncodedNode(data, expectedPageID, 0, nil, true, false, nil)
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
	_, _, _, _, err := readEncodedNode(data, expectedPageID, 0, nil, false, false, nil)
	return err
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
	_, entry, found, child, err := readEncodedNode(data, expectedPageID, 0, key, false, false, nil)
	return entry, found, child, err
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
	binary.LittleEndian.PutUint16(data[nodeTypeOffset:nodePageIDOffset], uint16(node.header.Type))
	binary.LittleEndian.PutUint64(data[nodePageIDOffset:nodeEntryCountOffset], uint64(node.header.PageID))
	binary.LittleEndian.PutUint32(data[nodeEntryCountOffset:nodeChecksumOffset], uint32(len(node.entries)))
	if node.IsLeaf() {
		encodeLeafBody(data, node)
	} else {
		encodeBranchBody(data, node)
	}
}

const checksumZeroBlockSize = 4096

var checksumZeroBlock [checksumZeroBlockSize]byte

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

// VisitNodeReferences visits each child page in a decoded node.
// A leaf contributes only roots of nested buckets.
func VisitNodeReferences(node *Node, visit func(page.ID) error) error {
	if !node.IsLeaf() {
		for _, child := range node.Children {
			if err := visit(child); err != nil {
				return err
			}
		}
		return nil
	}
	for _, entry := range node.entries {
		if entry.flags&BucketLeafFlag == 0 {
			continue
		}
		child, err := page.DecodeID(entry.value)
		if err != nil {
			return err
		}
		if err := visit(child); err != nil {
			return err
		}
	}
	return nil
}

// VisitMappedNodeReferences checks one main-file node and visits its child pages.
// It checks the page checksum first. It checks node fields as it visits each page.
func VisitMappedNodeReferences(data []byte, expectedPageID page.ID, pageSize int64, visit func(page.ID) error) error {
	if len(data) < NodeHeaderSize || pageSize < NodeHeaderSize || int64(len(data)) > pageSize {
		return ErrInvalid
	}
	stored := binary.LittleEndian.Uint32(data[nodeChecksumOffset:NodeHeaderSize])
	if stored != nodePageChecksum(data, pageSize) {
		return page.ErrChecksum
	}
	return visitEncodedNodeReferences(data, expectedPageID, stored, true, visit)
}

// VisitWALNodeReferences checks one verified WAL node and visits its child pages.
// The WAL record checksum protects the node bytes.
func VisitWALNodeReferences(data []byte, expectedPageID page.ID, visit func(page.ID) error) error {
	if len(data) < NodeHeaderSize {
		return ErrInvalid
	}
	if binary.LittleEndian.Uint32(data[nodeChecksumOffset:NodeHeaderSize]) != 0 {
		return ErrInvalid
	}
	return visitEncodedNodeReferences(data, expectedPageID, 0, false, visit)
}

func visitEncodedNodeReferences(data []byte, expectedPageID page.ID, checksum uint32, allowPadding bool, visit func(page.ID) error) error {
	_, _, _, _, err := readEncodedNode(data, expectedPageID, checksum, nil, false, allowPadding, visit)
	return err
}
