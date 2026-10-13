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

const encodedUint32Size = 4
const checksumZeroBlockSize = 4096

var checksumZeroBlock [checksumZeroBlockSize]byte

// mappedNodeType reads only the type tag common to every page type.
func mappedNodeType(data []byte) NodeType {
	if len(data) < 2 {
		return 0
	}
	return NodeType(binary.LittleEndian.Uint16(data[:2]))
}

// pageChecksum calculates CRC32C with a format-supplied checksum offset.
// Missing bytes before pageSize are zero padding. The four checksum bytes are skipped.
// The caller must provide a full header with that checksum field.
// A CRC result of zero is stored as one. Zero marks an unchecked data page.
func pageChecksum(data []byte, pageSize int64, checksumOffset int) uint32 {
	sum := checksum.Sum32(data[:checksumOffset], data[checksumOffset+encodedUint32Size:])
	for remaining := pageSize - int64(len(data)); remaining > 0; {
		n := min(remaining, int64(len(checksumZeroBlock)))
		sum = checksum.Update32(sum, checksumZeroBlock[:n])
		remaining -= n
	}
	if sum == 0 {
		return 1
	}
	return sum
}

// CheckNodeChecksumMode checks the page type and checksum field bounds.
// Enabled mode requires a nonzero field. Disabled mode accepts any field value.
// It does not calculate a checksum.
func CheckNodeChecksumMode(data []byte, enabled bool) error {
	offset := nodeChecksumOffset
	switch mappedNodeType(data) {
	case NodeTypeLeaf, NodeTypeBranch, NodeTypeLeaf | overflowNodeFlag, NodeTypeBranch | overflowNodeFlag:
	case NodeTypeContinuation:
		offset = continuationChecksumOffset
	default:
		return ErrInvalid
	}
	if len(data) < offset+encodedUint32Size {
		return ErrInvalid
	}
	if enabled && binary.LittleEndian.Uint32(data[offset:offset+encodedUint32Size]) == 0 {
		return page.ErrChecksum
	}
	return nil
}

// CheckNodeChecksum verifies a required checksum and page ID without decoding the body.
// Checksum conversion uses it before publishing the enabled mode.
func CheckNodeChecksum(data []byte, id page.ID) error {
	if err := CheckNodeChecksumMode(data, true); err != nil {
		return err
	}
	if page.ID(binary.LittleEndian.Uint64(data[2:10])) != id {
		return ErrInvalid
	}
	offset := nodeChecksumOffset
	if mappedNodeType(data) == NodeTypeContinuation {
		offset = continuationChecksumOffset
	}
	return checkPageChecksum(data, int64(len(data)), offset, true)
}

// checkPageChecksum checks a nonzero data-page checksum when checksums is true.
// Zero skips calculation. The database mode check rejects zero in enabled files.
func checkPageChecksum(data []byte, pageSize int64, offset int, checksums bool) error {
	if len(data) < offset+encodedUint32Size || int64(len(data)) > pageSize {
		return ErrInvalid
	}
	stored := binary.LittleEndian.Uint32(data[offset : offset+encodedUint32Size])
	if checksums && stored != 0 && stored != pageChecksum(data, pageSize, offset) {
		return page.ErrChecksum
	}
	return nil
}

// DecodeNode selects the leaf or branch codec and verifies the complete node.
// readPage supplies continuation pages. Pass nil for a normal node.
// A continued node requires a non-nil readPage function.
// The returned node owns data or a rebuilt logical body. Do not change its owned bytes.
// checksums selects verification of nonzero checksum fields.
func DecodeNode(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error), checksums bool) (*Node, error) {
	if kind := mappedNodeType(data); kind == NodeTypeBranch || kind == NodeTypeBranch|overflowNodeFlag {
		return DecodeBranchNode(data, id, pageSize, readPage, checksums)
	}
	return DecodeLeafNode(data, id, pageSize, readPage, checksums)
}

// LookupMappedNode selects the leaf or branch mapped lookup.
// A leaf returns an entry. A branch returns a child page ID.
// Normal entry bytes borrow data. Continued leaf values own their bytes.
// A continued key borrows the first page when it fits there; otherwise it owns a copy.
// Set includeKey to false to omit the returned key and its copy.
// Keep data unchanged while a result uses its borrowed bytes.
// Pass nil for a normal node. A continued node requires readPage.
// readPage keeps visited continuation pages unchanged until this call returns.
// checksums selects verification of nonzero checksum fields.
func LookupMappedNode(data []byte, id page.ID, key []byte, readPage func(page.ID) ([]byte, error), includeKey, checksums bool) (Entry, bool, page.ID, error) {
	if err := validateLookupKey(key); err != nil {
		return Entry{}, false, 0, err
	}
	if kind := mappedNodeType(data); kind == NodeTypeBranch || kind == NodeTypeBranch|overflowNodeFlag {
		return LookupMappedBranchNode(data, id, key, readPage, checksums)
	}
	return LookupMappedLeafNode(data, id, key, readPage, includeKey, checksums)
}

// EncodeNode selects the node's codec and updates its checksum.
// The returned compact image belongs to the caller and excludes page padding.
// pageSize must not be smaller than the encoded node. WriteNode checks this before writing.
func EncodeNode(node *Node, pageSize int64) []byte {
	if node.IsLeaf() {
		return EncodeLeafNode(node, pageSize)
	}
	return EncodeBranchNode(node, pageSize)
}

func nodePayloadSize(node *Node) int {
	if node.IsLeaf() {
		return leafPayloadSize(node)
	}
	return branchPayloadSize(node)
}

// DecodeWALNode selects the codec for a compact node from a verified WAL record.
// Each page checksum must be zero. pageSize is the physical database page size.
// readPage supplies verified continuation payloads. Pass nil for a normal node.
// The returned node owns data or a rebuilt logical image. Do not change its owned bytes.
func DecodeWALNode(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error)) (*Node, error) {
	if kind := mappedNodeType(data); kind == NodeTypeBranch || kind == NodeTypeBranch|overflowNodeFlag {
		return DecodeWALBranchNode(data, id, pageSize, readPage)
	}
	return DecodeWALLeafNode(data, id, pageSize, readPage)
}

// ValidateWALNode checks one WAL node payload without constructing a Node.
// The WAL record must already have passed its checksum check.
// Every page uses a compact image with a zero page checksum.
// Header and ID-array checks use the compact payload length. Checkpoint adds padding.
// This does not follow continuation IDs or validate the complete continued body.
func ValidateWALNode(data []byte, id page.ID) error {
	switch mappedNodeType(data) {
	case NodeTypeLeaf:
		return validateWALLeafNode(data, id)
	case NodeTypeBranch:
		return validateWALBranchNode(data, id)
	case NodeTypeLeaf | overflowNodeFlag:
		_, _, _, err := readLeafNodeHeader(data, id, int64(len(data)), true, false)
		return err
	case NodeTypeBranch | overflowNodeFlag:
		_, _, _, err := readBranchNodeHeader(data, id, int64(len(data)), true, false)
		return err
	case NodeTypeContinuation:
		_, _, _, err := readContinuationNodeHeader(data, id, int64(len(data)), true, false)
		return err
	default:
		return ErrInvalid
	}
}

// LookupEncodedWALNode selects the codec for a point lookup in a verified WAL record.
// It searches encoded bytes without creating a Node or copying the full body.
// pageSize is the physical database page size. readPage supplies verified
// continuation payloads. Pass nil for a normal node. Every page checksum is zero.
// Normal entries borrow data. A continued leaf owns its value. Its key borrows
// the first page when it fits there; otherwise it owns a copy. A branch returns
// a child page ID. Keep source bytes unchanged while using borrowed results.
// Set includeKey to false to omit the returned key and its copy.
func LookupEncodedWALNode(data []byte, id page.ID, key []byte, pageSize int64, readPage func(page.ID) ([]byte, error), includeKey bool) (Entry, bool, page.ID, error) {
	if kind := mappedNodeType(data); kind == NodeTypeBranch || kind == NodeTypeBranch|overflowNodeFlag {
		return lookupWALBranchNode(data, id, key, pageSize, readPage)
	}
	return lookupWALLeafNode(data, id, key, pageSize, readPage, includeKey)
}

// EncodeWALNode returns an owned compact node image without padding or a node checksum.
// The WAL record supplies the checksum. This function does not change node's checksum.
func EncodeWALNode(node *Node) []byte {
	data := make([]byte, WALNodeEncodedSize(node))
	encodeWALNode(data, node)
	return data
}

// WALNodeEncodedSize reports the selected codec's compact image size.
func WALNodeEncodedSize(node *Node) int {
	if node.IsLeaf() {
		return leafEncodedSize(node)
	}
	return branchEncodedSize(node)
}

// AppendEncodedWALNode appends an owned compact image to data using the selected codec.
func AppendEncodedWALNode(data []byte, node *Node) []byte {
	start := len(data)
	size := WALNodeEncodedSize(node)
	data = slices.Grow(data, size)
	data = data[:start+size]
	encodeWALNode(data[start:], node)
	return data
}

func encodeWALNode(data []byte, node *Node) {
	if node.IsLeaf() {
		encodeLeafWALNode(data, node)
	} else {
		encodeBranchWALNode(data, node)
	}
}

// VisitMappedNodeReferences checks a main-file node and its continuation pages.
// visit receives branch children or nested bucket roots. visitContinuation
// receives continuation IDs. The full body is checked.
// checksums selects nonzero checksum checks for each page.
// readPage supplies continuation pages. Pass nil for a normal node.
// No Node or complete body copy is created. Callbacks can run before an error;
// the caller must finish all checks before it changes ownership.
func VisitMappedNodeReferences(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error), visit, visitContinuation func(page.ID) error, checksums bool) error {
	if mappedNodeType(data) & ^overflowNodeFlag == NodeTypeBranch {
		return visitBranchNodeReferences(data, id, pageSize, readPage, visit, visitContinuation, false, checksums)
	}
	return visitLeafNodeReferences(data, id, pageSize, readPage, visit, visitContinuation, false, checksums)
}

// VisitWALNodeReferences checks a node from verified compact WAL records.
// Its callbacks and readPage have the same roles as VisitMappedNodeReferences.
// Each payload must have passed its WAL checksum check. Page checksums must be
// zero. This checks the complete body without creating a Node or copying values.
func VisitWALNodeReferences(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error), visit, visitContinuation func(page.ID) error) error {
	if mappedNodeType(data) & ^overflowNodeFlag == NodeTypeBranch {
		return visitBranchNodeReferences(data, id, pageSize, readPage, visit, visitContinuation, true, false)
	}
	return visitLeafNodeReferences(data, id, pageSize, readPage, visit, visitContinuation, true, false)
}

// NodePageCount selects the codec's count, including its first page.
// It returns zero when a required continued header and ID cannot fit.
func NodePageCount(node *Node, pageSize int64) int {
	if node.IsLeaf() {
		return leafPageCount(node, pageSize)
	}
	return branchPageCount(node, pageSize)
}

// EncodeNodePages selects the leaf or branch page encoder.
// The returned pages belong to the caller. Encoding does not change node or ids.
func EncodeNodePages(node *Node, pageSize int64, ids []page.ID) ([][]byte, error) {
	if node.IsLeaf() {
		return EncodeLeafNodePages(node, pageSize, ids)
	}
	return EncodeBranchNodePages(node, pageSize, ids)
}

// EncodeWALNodePages selects the leaf or branch encoder for compact WAL page images.
// ids has the same requirements as EncodeNodePages. Each image omits page padding
// and has a zero page checksum. The WAL record protects its bytes. Stored key and
// value bytes, including trailing zeros, stay intact. The caller owns the images.
func EncodeWALNodePages(node *Node, pageSize int64, ids []page.ID) ([][]byte, error) {
	if node.IsLeaf() {
		return encodeLeafNodePages(node, pageSize, ids, true, nil)
	}
	return encodeBranchNodePages(node, pageSize, ids, true, nil)
}

// WALNodePagesEncodedSize returns the total bytes in compact WAL page images.
// It excludes WAL record headers and checksums. It returns zero if the node
// cannot fit the required page headers. Page IDs are not checked here.
func WALNodePagesEncodedSize(node *Node, pageSize int64) int {
	count := NodePageCount(node, pageSize)
	if count == 0 {
		return 0
	}
	size := WALNodeEncodedSize(node)
	if count == 1 {
		return size
	}
	firstHeader := leafContinuationHeaderSize
	if !node.IsLeaf() {
		firstHeader = branchContinuationHeaderSize
	}
	return size - NodeHeaderSize + firstHeader + (count-1)*(ContinuationHeaderSize+page.IDSize)
}

// EncodeWALNodePagesInto writes compact WAL images into caller-owned pages.
// ids has the same requirements as EncodeWALNodePages. Each destination has
// pageSize bytes, except the last, which can omit padding. Destinations must
// not overlap each other or node data.
// The encoder shortens the last slice to remove padding. It can change pages
// before an error. The caller must keep successful images unchanged while used.
func EncodeWALNodePagesInto(node *Node, pageSize int64, ids []page.ID, pages [][]byte) error {
	if len(pages) == 0 || len(pages) != len(ids) {
		return ErrInvalid
	}
	size := WALNodePagesEncodedSize(node, pageSize)
	if size == 0 || len(pages) != NodePageCount(node, pageSize) {
		return ErrInvalid
	}
	for i, data := range pages {
		minimum := int(pageSize)
		if i == len(pages)-1 {
			minimum = size - i*int(pageSize)
		}
		if len(data) < minimum || int64(len(data)) > pageSize {
			return ErrInvalid
		}
	}
	var err error
	if node.IsLeaf() {
		_, err = encodeLeafNodePages(node, pageSize, ids, true, pages)
	} else {
		_, err = encodeBranchNodePages(node, pageSize, ids, true, pages)
	}
	return err
}

// VerifyNodeIDAndSetChecksum verifies the ID and sets the selected page checksum.
// enabled calculates a checksum. Otherwise the checksum field is zero.
// It does not validate the body. len(data) is the page size.
func VerifyNodeIDAndSetChecksum(data []byte, id page.ID, enabled bool) error {
	if mappedNodeType(data) == NodeTypeContinuation {
		return verifyPageIDAndSetChecksum(data, id, ContinuationHeaderSize, continuationChecksumOffset, enabled)
	}
	return verifyPageIDAndSetChecksum(data, id, NodeHeaderSize, nodeChecksumOffset, enabled)
}

// verifyPageIDAndSetChecksum uses the ID field shared by all page types.
// The owning format supplies its minimum header length and checksum position.
func verifyPageIDAndSetChecksum(data []byte, id page.ID, headerSize, checksumOffset int, enabled bool) error {
	if len(data) < headerSize {
		return fmt.Errorf("set node checksum: read header: %w", ErrInvalid)
	}
	if got := page.ID(binary.LittleEndian.Uint64(data[2:10])); got != id {
		return fmt.Errorf("set node checksum: read page ID %d, want %d: %w", got, id, ErrInvalid)
	}
	var sum uint32
	if enabled {
		sum = pageChecksum(data, int64(len(data)), checksumOffset)
	}
	binary.LittleEndian.PutUint32(data[checksumOffset:checksumOffset+encodedUint32Size], sum)
	return nil
}

// WriteNode writes a node for pageSize bytes.
// dataPageChecksums calculates a checksum. Otherwise the checksum field is zero.
// If shouldPad is true, it adds zeros and writes pageSize bytes.
// If shouldPad is false, it does not write the zeros.
// The checksum still covers the missing zeros.
//
// WriteNode stores the checksum in the node header.
// It returns ErrNodeTooLarge if the node is larger than pageSize.
// It completes partial writes or returns the writer error.
func WriteNode(w io.Writer, node *Node, pageSize int64, shouldPad, dataPageChecksums bool) error {
	var encoded []byte
	if dataPageChecksums {
		encoded = EncodeNode(node, pageSize)
	} else {
		encoded = EncodeWALNode(node)
		node.header.Checksum = 0
	}
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

// copyEncodedBody copies only the requested bytes from mapped body fragments.
// A descriptor or child ID uses a small caller-owned buffer. A point lookup
// supplies storage only for the matching value. It never copies the full body.
func copyEncodedBody(offset int, dst []byte, fragment func(int) ([]byte, error)) error {
	for len(dst) > 0 {
		part, err := fragment(offset)
		if err != nil {
			return err
		}
		n := copy(dst, part)
		offset += n
		dst = dst[n:]
	}
	return nil
}

// compareEncodedKey compares mapped key bytes without assembling a continued key.
// Each page fragment is compared with the matching part of key.
// It stops at the first difference. Normal keys use [bytes.Compare] directly.
func compareEncodedKey(offset, size int, key []byte, fragment func(int) ([]byte, error)) (int, error) {
	keySize := len(key)
	for remaining := min(size, keySize); remaining > 0; {
		part, err := fragment(offset)
		if err != nil {
			return 0, err
		}
		n := min(remaining, len(part))
		if cmp := bytes.Compare(part[:n], key[:n]); cmp != 0 {
			return cmp, nil
		}
		offset += n
		key = key[n:]
		remaining -= n
	}
	if size < keySize {
		return -1, nil
	}
	if size > keySize {
		return 1, nil
	}
	return 0, nil
}
