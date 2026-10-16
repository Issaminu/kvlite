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

// continuedNodeHeaderSize adds the body length and next-ID count to the normal header of the first page of a continued leaf or branch.
//
//	[18:22] total logical body length, uint32
//	[22:26] next page count N, uint32
//	[26:26+8*N] continuation IDs
//	[26+8*N:P] first body bytes
//
// Offsets in entry descriptors still include the normal 18-byte logical header.
const continuedNodeHeaderSize = NodeHeaderSize + 2*encodedUint32Size

// maxContinuedBodyLength limits the declared body length of a continued node before decode allocates it.
// One leaf entry, or one branch entry with its child ID, uses 16 bytes in addition to its key and value.
const maxContinuedBodyLength = MaxValueSize + MaxKeySize + leafEntryDescriptorSize

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

// DecodeNode verifies and decodes a leaf or branch, including all of its continuation pages.
// readPage supplies continuation pages. Pass nil for a normal node.
// A continued node requires a non-nil readPage function.
// The returned node owns data or a rebuilt logical body. Do not change its owned bytes.
// checksums selects verification of nonzero checksum fields.
func DecodeNode(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error), checksums bool) (*Node, error) {
	return decodeNode(data, id, pageSize, readPage, false, checksums)
}

// LookupMappedNode finds key in a mapped leaf, or the child page that routes key in a mapped branch.
// A leaf returns an entry. A branch returns a child page ID.
// The entry of a node in one page borrows data. Keep data unchanged while the result uses it.
// A continued node is read in place, and the lookup reads only the pages that it needs.
// The entry of a continued leaf owns a copy of its value (see [Entry.ValueIsOwned]), and its key is a copy of key.
// Set includeKey to false to omit the returned key.
// Pass nil readPage for a normal node. A continued node requires readPage.
// readPage keeps visited continuation pages unchanged until this call returns.
// checksums selects verification of nonzero checksum fields.
func LookupMappedNode(data []byte, id page.ID, key []byte, readPage func(page.ID) ([]byte, error), includeKey, checksums bool) (entry Entry, found bool, child page.ID, err error) {
	entry, found, child, err = lookupNode(data, id, key, int64(len(data)), readPage, false, includeKey, checksums)
	return
}

// EncodeNode writes a compact page image and updates the node's checksum.
// The returned compact image belongs to the caller and excludes page padding.
// The checksum includes zero padding up to pageSize.
// pageSize must not be smaller than the encoded node. WriteNode checks this before writing.
func EncodeNode(node *Node, pageSize int64) []byte {
	data := EncodeWALNode(node)
	node.header.Checksum = pageChecksum(data, pageSize, nodeChecksumOffset)
	binary.LittleEndian.PutUint32(data[nodeChecksumOffset:NodeHeaderSize], node.header.Checksum)
	return data
}

func nodePayloadSize(node *Node) int {
	if node.IsLeaf() {
		return leafPayloadSize(node)
	}
	return branchPayloadSize(node)
}

// DecodeWALNode decodes a compact leaf or branch from a verified WAL record.
// Each page checksum must be zero. pageSize is the physical database page size.
// readPage supplies verified continuation payloads. Pass nil for a normal node.
// The returned node owns data or a rebuilt logical image. Do not change its owned bytes.
func DecodeWALNode(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error)) (*Node, error) {
	return decodeNode(data, id, pageSize, readPage, true, false)
}

// ValidateWALNode checks one WAL node payload without constructing a Node.
// The WAL record must already have passed its checksum check.
// Every page uses a compact image with a zero page checksum.
// Header and ID-array checks use the compact payload length. Checkpoint adds padding.
// This does not follow continuation IDs or validate the complete continued body.
func ValidateWALNode(data []byte, id page.ID) error {
	if mappedNodeType(data) == NodeTypeContinuation {
		_, _, _, err := readContinuationNodeHeader(data, id, int64(len(data)), true, false)
		return err
	}
	h, err := readNodeHeader(data, id, int64(len(data)), true, false)
	if err != nil || h.isContinued() {
		return err
	}
	return checkBody(data, h, true, nil)
}

// LookupEncodedWALNode finds key in a leaf from a verified WAL record, or the child page that routes key in a branch.
// It searches the encoded bytes without creating a Node.
// pageSize is the physical database page size. Every page checksum is zero.
// readPage supplies verified continuation payloads. Pass nil for a normal node.
// The entry of a node in one page borrows data. Keep data unchanged while the result uses it.
// A continued node is read in place, and the lookup reads only the pages that it needs.
// The entry of a continued leaf owns a copy of its value, and its key is a copy of key.
// Set includeKey to false to omit the returned key.
func LookupEncodedWALNode(data []byte, id page.ID, key []byte, pageSize int64, readPage func(page.ID) ([]byte, error), includeKey bool) (Entry, bool, page.ID, error) {
	return lookupNode(data, id, key, pageSize, readPage, true, includeKey, false)
}

// EncodeWALNode returns an owned compact node image without padding or a node checksum.
// The WAL record supplies the checksum. This function does not change node's checksum.
func EncodeWALNode(node *Node) []byte {
	data := make([]byte, WALNodeEncodedSize(node))
	encodeWALNode(data, node)
	return data
}

// WALNodeEncodedSize reports the compact image size. Leaves and branches use the same saved size.
func WALNodeEncodedSize(node *Node) int {
	return node.EncodedSize()
}

// AppendEncodedWALNode appends an owned compact image to data.
func AppendEncodedWALNode(data []byte, node *Node) []byte {
	start := len(data)
	size := WALNodeEncodedSize(node)
	data = slices.Grow(data, size)
	data = data[:start+size]
	encodeWALNode(data[start:], node)
	return data
}

// encodeWALNode writes a compact image with no page padding or node checksum.
// data must have WALNodeEncodedSize bytes. A verified WAL record protects these bytes.
func encodeWALNode(data []byte, node *Node) {
	clear(data)
	putNodeHeader(data, node.kind(), node)
	if node.IsLeaf() {
		putLeafBody(data, node)
	} else {
		putBranchBody(data, node)
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
	return visitNodeReferences(data, id, pageSize, readPage, visit, visitContinuation, false, checksums)
}

// VisitWALNodeReferences checks a node from verified compact WAL records.
// Its callbacks and readPage have the same roles as VisitMappedNodeReferences.
// Each payload must have passed its WAL checksum check. Page checksums must be
// zero. This checks the complete body without creating a Node or copying values.
func VisitWALNodeReferences(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error), visit, visitContinuation func(page.ID) error) error {
	return visitNodeReferences(data, id, pageSize, readPage, visit, visitContinuation, true, false)
}

// NodePageCount returns the number of pages that the encoded node uses, including its first page.
// It returns zero when a required continued header and ID cannot fit.
func NodePageCount(node *Node, pageSize int64) int {
	size := node.EncodedSize()
	if pageSize >= NodeHeaderSize && int64(size) <= pageSize {
		return 1
	}
	return continuationPageCount(size-NodeHeaderSize, pageSize, continuedNodeHeaderSize)
}

// EncodeNodePages writes this node and its continuation pages into owned buffers.
// ids must start with node.PageID and contain exactly NodePageCount distinct non-metadata IDs.
// The caller must own the allocated IDs. Encoding does not change node or ids.
func EncodeNodePages(node *Node, pageSize int64, ids []page.ID) ([][]byte, error) {
	return encodeNodePages(node, pageSize, ids, false, nil)
}

// EncodeWALNodePages writes compact WAL page images.
// ids has the same requirements as EncodeNodePages.
// Each image omits page padding and has a zero page checksum.
// The WAL record protects its bytes.
// Stored key and value bytes, including trailing zeros, stay intact.
// The caller owns the images.
func EncodeWALNodePages(node *Node, pageSize int64, ids []page.ID) ([][]byte, error) {
	return encodeNodePages(node, pageSize, ids, true, nil)
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
	return size - NodeHeaderSize + continuedNodeHeaderSize + (count-1)*(ContinuationHeaderSize+page.IDSize)
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
	_, err := encodeNodePages(node, pageSize, ids, true, pages)
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

// readNodeHeader checks the first page of a leaf or branch and returns its header.
// Mapped pages and verified WAL payloads use the same checks.
// forWAL requires a zero checksum. Otherwise checksums verifies a nonzero checksum.
// The check verifies the checksum before it uses the continued-header fields.
// The first page of a continued node also has a body length and a next-ID count.
// The body must need more than one page and must not be larger than the largest body that one entry can create.
// The continuation codec checks each later page separately.
func readNodeHeader(data []byte, id page.ID, pageSize int64, forWAL, checksums bool) (NodeHeader, error) {
	if len(data) < NodeHeaderSize || int64(len(data)) > pageSize {
		return NodeHeader{}, ErrInvalid
	}
	h := NodeHeader{Type: NodeType(binary.LittleEndian.Uint16(data[:2])), PageID: page.ID(binary.LittleEndian.Uint64(data[2:10])),
		EntryCount: binary.LittleEndian.Uint32(data[10:14]), Checksum: binary.LittleEndian.Uint32(data[14:18])}
	if kind := h.kind(); kind != NodeTypeLeaf && kind != NodeTypeBranch || h.PageID != id {
		return NodeHeader{}, ErrInvalid
	}
	if h.isContinued() && (int64(len(data)) != pageSize || pageSize <= continuedNodeHeaderSize+page.IDSize) {
		return NodeHeader{}, ErrInvalid
	}
	if forWAL {
		if h.Checksum != 0 {
			return NodeHeader{}, fmt.Errorf("read WAL node checksum %x: %w", h.Checksum, ErrInvalid)
		}
	} else if checksums && h.Checksum != 0 && h.Checksum != pageChecksum(data, pageSize, nodeChecksumOffset) {
		return NodeHeader{}, fmt.Errorf("node page %d: %w", id, page.ErrChecksum)
	}
	if h.isContinued() {
		h.BodyLength = binary.LittleEndian.Uint32(data[18:22])
		h.NextPageCount = binary.LittleEndian.Uint32(data[22:26])
		if uint64(h.BodyLength)+NodeHeaderSize <= uint64(pageSize) || uint64(h.BodyLength) > maxContinuedBodyLength ||
			uint64(h.NextPageCount) > uint64((pageSize-continuedNodeHeaderSize-1)/page.IDSize) {
			return NodeHeader{}, ErrInvalid
		}
	}
	return h, nil
}

// plainNodeHeader reads the type and entry count of a one-page leaf or branch that needs no checksum check.
// It returns ok=false for every other page. readNodeHeader then checks that page and reports the error.
// It returns only small values, so a lookup does not copy a NodeHeader for each page.
func plainNodeHeader(data []byte, id page.ID, pageSize int64, forWAL, checksums bool) (kind NodeType, count uint32, ok bool) {
	if len(data) < NodeHeaderSize || int64(len(data)) > pageSize {
		return 0, 0, false
	}
	kind = NodeType(binary.LittleEndian.Uint16(data[:2]))
	if kind != NodeTypeLeaf && kind != NodeTypeBranch || page.ID(binary.LittleEndian.Uint64(data[2:10])) != id {
		return 0, 0, false
	}
	// A WAL payload needs a zero checksum, and a mapped page with checksums on needs a verified one.
	if checksum := binary.LittleEndian.Uint32(data[14:18]); checksum != 0 && (forWAL || checksums) {
		return 0, 0, false
	}
	return kind, binary.LittleEndian.Uint32(data[10:14]), true
}

// kind returns NodeTypeLeaf or NodeTypeBranch without the continuation flag.
func (h NodeHeader) kind() NodeType {
	return h.Type &^ overflowNodeFlag
}

// isContinued reports whether h names the first page of a continued node.
func (h NodeHeader) isContinued() bool {
	return h.Type&overflowNodeFlag != 0
}

// continuedDirectoryScratchSize is the size of the caller-owned copy of a directory that a continued body stores across pages.
// A continued leaf has one entry and a continued branch has at most two separators (see Node.NeedsSplit), so their descriptors and child IDs fit.
// A larger directory uses new storage.
const continuedDirectoryScratchSize = 64

// validKey reports whether a key at image[start:end] has a valid size and lies in the data area of the image.
// The data area starts at dataStart, after the descriptors, and ends at imageSize.
// Leaves and branches use the same rule.
func validKey(start, end, dataStart, imageSize uint64) bool {
	// end-start-1 < MaxKeySize also rejects an empty key, because the subtraction wraps.
	return end-start-1 < MaxKeySize && start >= dataStart && end <= imageSize
}

// inOrder reports whether an entry follows the previous entry: its key starts at next, where the previous entry ends, and is larger than the previous key.
// A key is never empty, so an empty previous key accepts the first entry.
func inOrder(start, next uint64, previous, key []byte) bool {
	return start == next && bytes.Compare(previous, key) < 0
}

// errEntryCount reports an entry count whose descriptors do not fit in the image.
var errEntryCount = fmt.Errorf("read node entry count: %w", ErrInvalid)

// entryError reports an invalid descriptor, an entry out of order, or a key out of order.
func entryError(index int) error {
	return fmt.Errorf("read node entry %d: %w", index, ErrInvalid)
}

// errBodySize reports a body that has bytes after its last entry.
var errBodySize = fmt.Errorf("read node body size: %w", ErrInvalid)

// decodeNode verifies and decodes a leaf or branch, including all of its continuation pages.
// forWAL requires zero page checksums. checksums selects main-file checksum checks.
// A normal node owns data. A continued node owns a rebuilt logical image.
func decodeNode(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error), forWAL, checksums bool) (*Node, error) {
	image, h, ids, err := nodeImage(data, id, pageSize, readPage, forWAL, checksums)
	if err != nil {
		return nil, err
	}
	// A main-file page has zero padding after its body. A WAL image and a rebuilt image end at their last entry.
	node, err := decodeBody(image, h, forWAL || h.isContinued())
	if err != nil {
		return nil, err
	}
	node.continuationPageIDs = ids
	return node, nil
}

// nodeImage checks the header of a leaf or branch and returns one contiguous image of it.
// A node that fits in one page returns data.
// A continued node returns a new image that has a normal header and the complete body, and the IDs of its continuation pages.
// Every read of a node works on one contiguous image, so the body code never sees a page boundary.
// h is the header of the first page.
func nodeImage(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error), forWAL, checksums bool) (image []byte, h NodeHeader, ids []page.ID, err error) {
	h, err = readNodeHeader(data, id, pageSize, forWAL, checksums)
	if err != nil {
		return nil, NodeHeader{}, nil, err
	}
	if !h.isContinued() {
		return data, h, nil, nil
	}
	if readPage == nil {
		return nil, NodeHeader{}, nil, continuationPagesError(id)
	}
	image = make([]byte, NodeHeaderSize+int(h.BodyLength))
	copy(image[:nodeChecksumOffset], data[:nodeChecksumOffset])
	binary.LittleEndian.PutUint16(image[:2], uint16(h.kind()))
	ids, err = copyContinuationBody(data, image[NodeHeaderSize:], readPage, forWAL, checksums)
	if err != nil {
		return nil, NodeHeader{}, nil, err
	}
	return image, h, ids, nil
}

// decodeBody decodes the entries of one contiguous image.
// h becomes the node header. exact requires the last entry to end at the end of image.
func decodeBody(image []byte, h NodeHeader, exact bool) (*Node, error) {
	if h.kind() == NodeTypeLeaf {
		return decodeLeafBody(image, h, exact)
	}
	return decodeBranchBody(image, h, exact)
}

// checkBody checks one contiguous leaf or branch image in the same way as decodeBody, without a Node.
// visit receives the child page IDs of a branch or the nested bucket roots of a leaf. visit can be nil.
func checkBody(image []byte, h NodeHeader, exact bool, visit func(page.ID) error) error {
	if h.kind() == NodeTypeLeaf {
		return checkLeafBody(image, h, exact, visit)
	}
	return checkBranchBody(image, h, exact, visit)
}

// continuationPagesError reports a continued node that the caller gave no readPage function.
func continuationPagesError(id page.ID) error {
	return fmt.Errorf("node %d requires continuation pages: %w", id, ErrInvalid)
}

// lookupNode finds key in a leaf, or the child that routes key in a branch.
// A node in one page is searched in place, so the result borrows data.
// A continued node is read page by page with a continuedBody, so a lookup reads only the pages that it needs.
// forWAL uses verified compact payloads and requires zero page checksums.
func lookupNode(data []byte, id page.ID, key []byte, pageSize int64, readPage func(page.ID) ([]byte, error), forWAL, includeKey, checksums bool) (Entry, bool, page.ID, error) {
	if err := validateLookupKey(key); err != nil {
		return Entry{}, false, 0, err
	}
	// Most lookups read a one-page node with no checksum to check.
	if kind, count, ok := plainNodeHeader(data, id, pageSize, forWAL, checksums); ok {
		if kind == NodeTypeBranch {
			child, err := lookupBranchBody(data, count, key)
			return Entry{}, false, child, err
		}
		entry, found, err := lookupLeafBody(data, count, key, includeKey)
		return entry, found, 0, err
	}
	h, err := readNodeHeader(data, id, pageSize, forWAL, checksums)
	if err != nil {
		return Entry{}, false, 0, err
	}
	if h.isContinued() {
		if readPage == nil {
			return Entry{}, false, 0, continuationPagesError(id)
		}
		var cursor continuationCursor
		body := continuedBody{first: data, readPage: readPage, forWAL: forWAL, checksums: checksums, cursor: &cursor}
		if h.kind() == NodeTypeBranch {
			child, err := lookupContinuedBranch(&body, h, key)
			return Entry{}, false, child, err
		}
		entry, found, err := lookupContinuedLeaf(&body, h, key, includeKey)
		return entry, found, 0, err
	}
	if h.kind() == NodeTypeBranch {
		child, err := lookupBranchBody(data, h.EntryCount, key)
		return Entry{}, false, child, err
	}
	entry, found, err := lookupLeafBody(data, h.EntryCount, key, includeKey)
	return entry, found, 0, err
}

// visitNodeReferences checks the complete body and every owned page of a leaf or branch.
// A node in one page is checked in place. A one-page main-file node can have zero padding after its body.
// A continued node is read page by page with a continuedBody, so the check does not copy values.
// Then walkContinuationPages checks every continuation page and passes its ID to visitContinuation.
func visitNodeReferences(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error), visit, visitContinuation func(page.ID) error, forWAL, checksums bool) error {
	h, err := readNodeHeader(data, id, pageSize, forWAL, checksums)
	if err != nil {
		return err
	}
	if !h.isContinued() {
		return checkBody(data, h, forWAL, visit)
	}
	if readPage == nil {
		return continuationPagesError(id)
	}
	var cursor continuationCursor
	body := continuedBody{first: data, readPage: readPage, forWAL: forWAL, checksums: checksums, cursor: &cursor}
	if h.kind() == NodeTypeBranch {
		err = checkContinuedBranch(&body, h, visit)
	} else {
		err = checkContinuedLeaf(&body, h, visit)
	}
	if err != nil {
		return err
	}
	return walkContinuationPages(data, readPage, forWAL, func(pageID page.ID, _ []byte) error {
		if pageID != id && visitContinuation != nil {
			return visitContinuation(pageID)
		}
		return nil
	}, checksums)
}

// A body is a fixed-size directory followed by the keys in entry order.
// A leaf stores each value after its key. A branch stores only keys.
// The node type selects the directory layout. See leaf_codec.go and branch_codec.go.
// A node in one page writes its body with putLeafBody or putBranchBody.
// A continued node writes its directory with putDirectory and streams the keys and values after it.

// directorySize returns the size of the directory: the descriptors, and the child IDs of a branch.
func directorySize(node *Node) int {
	if node.IsLeaf() {
		return leafDirectorySize(node)
	}
	return branchDirectorySize(node)
}

// putDirectory writes the directory of node into dst, which has directorySize bytes.
func putDirectory(dst []byte, node *Node) {
	if node.IsLeaf() {
		putLeafDirectory(dst, node)
	} else {
		putBranchDirectory(dst, node)
	}
}

// putNodeHeader writes the type, page ID, and entry count. It does not change the checksum field.
func putNodeHeader(data []byte, nodeType NodeType, node *Node) {
	binary.LittleEndian.PutUint16(data[:2], uint16(nodeType))
	binary.LittleEndian.PutUint64(data[2:10], uint64(node.PageID()))
	binary.LittleEndian.PutUint32(data[10:14], uint32(len(node.entries)))
}

// encodeNodePages writes main-file pages or compact WAL images of one leaf or branch.
// pages supplies destinations. Pass nil to allocate them.
func encodeNodePages(node *Node, pageSize int64, ids []page.ID, forWAL bool, pages [][]byte) ([][]byte, error) {
	encodedSize := node.EncodedSize()
	if uint64(encodedSize) > uint64(^uint32(0)) || len(ids) == 0 || len(ids) != NodePageCount(node, pageSize) ||
		ids[0] != node.PageID() || page.IsMetaID(ids[0]) {
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
		encodeWALNode(data[:encodedSize], node)
		if !forWAL {
			binary.LittleEndian.PutUint32(data[nodeChecksumOffset:NodeHeaderSize], pageChecksum(data, pageSize, nodeChecksumOffset))
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
	putNodeHeader(first, node.kind()|overflowNodeFlag, node)
	binary.LittleEndian.PutUint32(first[18:22], uint32(encodedSize-NodeHeaderSize))
	// A continued node has few entries, so its directory uses a small temporary buffer.
	// The keys and values stream into the pages without a copy of the full body.
	var scratch [continuedDirectoryScratchSize]byte
	directory := scratch[:]
	if size := directorySize(node); size > len(directory) {
		directory = make([]byte, size)
	} else {
		directory = directory[:size]
	}
	putDirectory(directory, node)
	index, at := 0, 0
	copyContinuationBytes(pages, &index, &at, directory)
	leaf := node.IsLeaf()
	for _, entry := range node.entries {
		copyContinuationBytes(pages, &index, &at, entry.key)
		if leaf {
			copyContinuationBytes(pages, &index, &at, entry.value)
		}
	}
	finishContinuationPages(pages, index, at, forWAL)
	return pages, nil
}
