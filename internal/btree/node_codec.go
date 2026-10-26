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

// MaxNodeImageSize bounds a logical image before a reader allocates it.
// A large leaf has one entry. A large branch has at most two separators.
const MaxNodeImageSize = MaxValueSize + 2*MaxKeySize + 128

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

// NodeSpan calculates the storage page count from the header and body length.
// It checks the header, node type, size limit, and page ID range before a caller reads the span.
func NodeSpan(data []byte, id page.ID, pageSize int64) (int, error) {
	if pageSize < NodeHeaderSize || len(data) < NodeHeaderSize {
		return 0, ErrInvalid
	}
	kind := mappedNodeType(data)
	if kind != NodeTypeLeaf && kind != NodeTypeBranch {
		return 0, ErrInvalid
	}
	if page.ID(binary.LittleEndian.Uint64(data[2:10])) != id {
		return 0, ErrInvalid
	}
	size := uint64(binary.LittleEndian.Uint32(data[18:22])) + NodeHeaderSize
	count := (size + uint64(pageSize) - 1) / uint64(pageSize)
	if size > MaxNodeImageSize || uint64(id) > ^uint64(0)-count+1 {
		return 0, ErrInvalid
	}
	return int(count), nil
}

// CheckNodeChecksumMode checks the head type and checksum field.
// Enabled mode requires a stored checksum. It does not calculate the checksum.
func CheckNodeChecksumMode(data []byte, enabled bool) error {
	kind := mappedNodeType(data)
	if len(data) < NodeHeaderSize || kind != NodeTypeLeaf && kind != NodeTypeBranch {
		return ErrInvalid
	}
	if enabled && binary.LittleEndian.Uint32(data[14:18]) == 0 {
		return page.ErrChecksum
	}
	return nil
}

// CheckNodeChecksum verifies the checksum of a complete padded node span.
func CheckNodeChecksum(data []byte, id page.ID) error {
	if err := CheckNodeChecksumMode(data, true); err != nil {
		return err
	}
	if page.ID(binary.LittleEndian.Uint64(data[2:10])) != id {
		return ErrInvalid
	}
	return checkPageChecksum(data, int64(len(data)), nodeChecksumOffset, true)
}

func checkPageChecksum(data []byte, pageSize int64, offset int, checksums bool) error {
	if len(data) < offset+4 || int64(len(data)) > pageSize {
		return ErrInvalid
	}
	stored := binary.LittleEndian.Uint32(data[offset : offset+4])
	if checksums && stored != 0 && stored != pageChecksum(data, pageSize, offset) {
		return page.ErrChecksum
	}
	return nil
}

func readNodeHeader(data []byte, id page.ID, pageSize int64, forWAL bool) (NodeHeader, error) {
	_, err := NodeSpan(data, id, pageSize)
	if err != nil {
		return NodeHeader{}, err
	}
	h := NodeHeader{Type: mappedNodeType(data), PageID: id, EntryCount: binary.LittleEndian.Uint32(data[10:14]), Checksum: binary.LittleEndian.Uint32(data[14:18]), BodyLength: binary.LittleEndian.Uint32(data[18:22])}
	if forWAL && h.Checksum != 0 {
		return NodeHeader{}, ErrInvalid
	}
	return h, nil
}

// plainNodeHeader checks whether a lookup can use data without more page reads or a checksum calculation.
func plainNodeHeader(data []byte, id page.ID, pageSize int64, forWAL, checksums bool) (NodeType, uint32, bool) {
	if len(data) < NodeHeaderSize || int64(len(data)) > pageSize {
		return 0, 0, false
	}
	kind := mappedNodeType(data)
	if kind != NodeTypeLeaf && kind != NodeTypeBranch || page.ID(binary.LittleEndian.Uint64(data[2:10])) != id {
		return 0, 0, false
	}
	size := uint64(binary.LittleEndian.Uint32(data[18:22])) + NodeHeaderSize
	if size > uint64(len(data)) || forWAL && size != uint64(len(data)) {
		return 0, 0, false
	}
	if sum := binary.LittleEndian.Uint32(data[14:18]); sum != 0 && (forWAL || checksums) {
		return 0, 0, false
	}
	return kind, binary.LittleEndian.Uint32(data[10:14]), true
}

// nodeImage returns the logical bytes of one node. A complete mapped span is borrowed.
// A caller with only its head page supplies raw tail pages through readPage.
func nodeImage(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error), forWAL, checksums bool) ([]byte, NodeHeader, error) {
	h, err := readNodeHeader(data, id, pageSize, forWAL)
	if err != nil {
		return nil, NodeHeader{}, err
	}
	size := NodeHeaderSize + int(h.BodyLength)
	count := (size + int(pageSize) - 1) / int(pageSize)
	span := count * int(pageSize)
	if forWAL {
		if len(data) != size {
			return nil, NodeHeader{}, ErrInvalid
		}
	} else if len(data) < span && count > 1 {
		if len(data) != int(pageSize) || readPage == nil {
			return nil, NodeHeader{}, ErrInvalid
		}
		image := make([]byte, span)
		copy(image, data)
		for i := 1; i < count; i++ {
			part, e := readPage(id + page.ID(i))
			if e != nil {
				return nil, NodeHeader{}, e
			}
			if len(part) != int(pageSize) {
				return nil, NodeHeader{}, ErrInvalid
			}
			copy(image[i*int(pageSize):], part)
		}
		data = image
	} else if len(data) > span || len(data) < size {
		return nil, NodeHeader{}, ErrInvalid
	}
	if !forWAL {
		if err := checkPageChecksum(data, int64(span), nodeChecksumOffset, checksums); err != nil {
			return nil, NodeHeader{}, err
		}
	}
	return data[:size:size], h, nil
}

// DecodeNode reads one leaf or branch. The node borrows data when data contains its full span.
// The caller must keep these bytes unchanged for the node lifetime.
func DecodeNode(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error), checksums bool) (*Node, error) {
	return decodeNode(data, id, pageSize, readPage, false, checksums)
}

// DecodeWALNode reads a complete compact logical image protected by its WAL record.
func DecodeWALNode(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error)) (*Node, error) {
	return decodeNode(data, id, pageSize, readPage, true, false)
}

func decodeNode(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error), forWAL, checksums bool) (*Node, error) {
	image, h, err := nodeImage(data, id, pageSize, readPage, forWAL, checksums)
	if err != nil {
		return nil, err
	}
	node, err := decodeBody(image, h, true)
	if err != nil {
		return nil, err
	}

	node.SetStoragePageCount((len(image) + int(pageSize) - 1) / int(pageSize))
	return node, nil
}

func decodeBody(image []byte, h NodeHeader, exact bool) (*Node, error) {
	if h.Type == NodeTypeLeaf {
		return decodeLeafBody(image, h, exact)
	}
	return decodeBranchBody(image, h, exact)
}
func checkBody(image []byte, h NodeHeader, exact bool, visit func(page.ID) error) error {
	if h.Type == NodeTypeLeaf {
		return checkLeafBody(image, h, exact, visit)
	}
	return checkBranchBody(image, h, exact, visit)
}

// ValidateWALNode checks a complete compact image. The WAL checksum must already be valid.
func ValidateWALNode(data []byte, id page.ID) error {
	if len(data) < NodeHeaderSize || len(data) > MaxNodeImageSize || binary.LittleEndian.Uint32(data[14:18]) != 0 || page.ID(binary.LittleEndian.Uint64(data[2:10])) != id || page.IsMetaID(id) {
		return ErrInvalid
	}
	kind := mappedNodeType(data)
	if kind != NodeTypeLeaf && kind != NodeTypeBranch {
		return ErrInvalid
	}
	if uint64(binary.LittleEndian.Uint32(data[18:22]))+NodeHeaderSize != uint64(len(data)) {
		return ErrInvalid
	}
	return checkBody(data, NodeHeader{Type: kind, EntryCount: binary.LittleEndian.Uint32(data[10:14])}, true, nil)
}

// LookupMappedNode searches a complete mapped node span, or a head with a raw-page reader.
// Its entry borrows the span. The caller must keep the span unchanged while it uses the entry.
func LookupMappedNode(data []byte, id page.ID, key []byte, readPage func(page.ID) ([]byte, error), includeKey, checksums bool) (entry Entry, found bool, child page.ID, err error) {
	found, child, err = lookupNode(data, id, key, int64(len(data)), readPage, false, includeKey, checksums, &entry)
	return
}

// LookupCheckedNodeSpan searches a complete padded span with a checked header.
// The caller must first use NodeSpan and CheckNodeChecksumMode to check the header.
// The data length must equal the page count times the database page size.
// This function checks the key and each descriptor used by the search.
// checksums also checks the checksum of the complete span.
// The result borrows data. The caller must keep data unchanged while it uses the result.
func LookupCheckedNodeSpan(data, key []byte, includeKey, checksums bool) (entry Entry, found bool, child page.ID, err error) {
	if err = validateLookupKey(key); err != nil {
		return
	}
	if checksums {
		if err = checkPageChecksum(data, int64(len(data)), nodeChecksumOffset, true); err != nil {
			return
		}
	}
	size := NodeHeaderSize + int(binary.LittleEndian.Uint32(data[18:22]))
	image := data[:size:size]
	count := binary.LittleEndian.Uint32(data[10:14])
	if mappedNodeType(data) == NodeTypeBranch {
		child, err = lookupBranchBody(image, count, key)
		return
	}
	_, found, err = lookupLeafBody(image, count, key, includeKey, &entry)
	return
}

// LookupEncodedWALNode searches a complete compact logical WAL image without a Node.
func LookupEncodedWALNode(data []byte, id page.ID, key []byte, pageSize int64, readPage func(page.ID) ([]byte, error), includeKey bool) (entry Entry, found bool, child page.ID, err error) {
	found, child, err = lookupNode(data, id, key, pageSize, readPage, true, includeKey, false, &entry)
	return
}

func lookupNode(data []byte, id page.ID, key []byte, pageSize int64, readPage func(page.ID) ([]byte, error), forWAL, includeKey, checksums bool, entry *Entry) (bool, page.ID, error) {
	if err := validateLookupKey(key); err != nil {
		return false, 0, err
	}
	if kind, count, ok := plainNodeHeader(data, id, pageSize, forWAL, checksums); ok {
		size := NodeHeaderSize + int(binary.LittleEndian.Uint32(data[18:22]))
		image := data[:size:size]
		if kind == NodeTypeBranch {
			child, e := lookupBranchBody(image, count, key)
			return false, child, e
		}
		_, found, e := lookupLeafBody(image, count, key, includeKey, entry)
		return found, 0, e
	}
	image, h, err := nodeImage(data, id, pageSize, readPage, forWAL, checksums)
	if err != nil {
		return false, 0, err
	}
	if h.Type == NodeTypeBranch {
		child, e := lookupBranchBody(image, h.EntryCount, key)
		return false, child, e
	}
	_, found, e := lookupLeafBody(image, h.EntryCount, key, includeKey, entry)
	return found, 0, e
}

// VisitMappedNodeReferences checks a complete node and visits its child and storage page IDs.
func VisitMappedNodeReferences(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error), visit, visitContinuation func(page.ID) error, checksums bool) error {
	return visitNodeReferences(data, id, pageSize, readPage, visit, visitContinuation, false, checksums)
}

// VisitWALNodeReferences checks a complete compact WAL image and visits its references.
func VisitWALNodeReferences(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error), visit, visitContinuation func(page.ID) error) error {
	return visitNodeReferences(data, id, pageSize, readPage, visit, visitContinuation, true, false)
}
func visitNodeReferences(data []byte, id page.ID, pageSize int64, readPage func(page.ID) ([]byte, error), visit, visitContinuation func(page.ID) error, forWAL, checksums bool) error {
	image, h, err := nodeImage(data, id, pageSize, readPage, forWAL, checksums)
	if err != nil {
		return err
	}
	if err := checkBody(image, h, true, visit); err != nil {
		return err
	}
	if visitContinuation != nil {
		for i := 1; i < (len(image)+int(pageSize)-1)/int(pageSize); i++ {
			if err := visitContinuation(id + page.ID(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

// EncodeWALNode returns the complete compact logical image with a zero checksum.
func EncodeWALNode(node *Node) []byte {
	data := make([]byte, WALNodeEncodedSize(node))
	encodeWALNode(data, node)
	return data
}
func WALNodeEncodedSize(node *Node) int { return node.EncodedSize() }
func AppendEncodedWALNode(data []byte, node *Node) []byte {
	at := len(data)
	data = slices.Grow(data, node.EncodedSize())
	data = data[:at+node.EncodedSize()]
	encodeWALNode(data[at:], node)
	return data
}
func encodeWALNode(data []byte, node *Node) {

	clear(data[nodeChecksumOffset : nodeChecksumOffset+encodedUint32Size])
	putNodeHeader(data, node)
	if node.IsLeaf() {
		putLeafBody(data, node)
	} else {
		putBranchBody(data, node)
	}
}

func putNodeHeader(data []byte, node *Node) {
	binary.LittleEndian.PutUint16(data[:2], uint16(node.kind()))
	binary.LittleEndian.PutUint64(data[2:10], uint64(node.PageID()))
	binary.LittleEndian.PutUint32(data[10:14], uint32(node.EntryCount()))
	binary.LittleEndian.PutUint32(data[18:22], uint32(node.EncodedSize()-NodeHeaderSize))
}

// EncodeNodeInto writes node into a complete padded span in dst.
// The destination must have exactly NodePageCount(node, pageSize)*pageSize bytes.
// It must not overlap the node's key or value bytes. The function does not retain dst.
// The node and its stored page count remain unchanged.
func EncodeNodeInto(dst []byte, node *Node, pageSize int64, checksums bool) error {
	if node == nil || pageSize < NodeHeaderSize || page.IsMetaID(node.PageID()) {
		return ErrInvalid
	}
	size := node.EncodedSize()
	if size < NodeHeaderSize || size > MaxNodeImageSize {
		return ErrInvalid
	}
	count := NodePageCount(node, pageSize)
	if count <= 0 || page.ID(count)-1 > ^page.ID(0)-node.PageID() ||
		int64(len(dst))/pageSize != int64(count) || int64(len(dst))%pageSize != 0 {
		return ErrInvalid
	}
	encodeWALNode(dst[:size], node)
	clear(dst[size:])
	if checksums {
		binary.LittleEndian.PutUint32(dst[nodeChecksumOffset:nodeChecksumOffset+encodedUint32Size], pageChecksum(dst, int64(len(dst)), nodeChecksumOffset))
	}
	return nil
}

// EncodeNode returns a compact logical image. Its checksum covers the complete padded span.
func EncodeNode(node *Node, pageSize int64) []byte {
	data := EncodeWALNode(node)
	span := int64(NodePageCount(node, pageSize)) * pageSize
	node.header.Checksum = pageChecksum(data, span, nodeChecksumOffset)
	binary.LittleEndian.PutUint32(data[14:18], node.header.Checksum)
	return data
}
func nodePayloadSize(node *Node) int {
	if node.IsLeaf() {
		return leafPayloadSize(node)
	}
	return branchPayloadSize(node)
}

// NodePageCount returns the number of consecutive pages used by node.
func NodePageCount(node *Node, pageSize int64) int {
	if pageSize < NodeHeaderSize {
		return 0
	}
	return int((int64(node.EncodedSize()) + pageSize - 1) / pageSize)
}

// EncodeNodePages returns the consecutive physical pages of node. Only the head has a header.
func EncodeNodePages(node *Node, pageSize int64, ids []page.ID) ([][]byte, error) {
	return encodeNodePages(node, pageSize, ids, false, nil)
}
func EncodeWALNodePages(node *Node, pageSize int64, ids []page.ID) ([][]byte, error) {
	return encodeNodePages(node, pageSize, ids, true, nil)
}
func WALNodePagesEncodedSize(node *Node, pageSize int64) int {
	if NodePageCount(node, pageSize) == 0 {
		return 0
	}
	return node.EncodedSize()
}
func EncodeWALNodePagesInto(node *Node, pageSize int64, ids []page.ID, pages [][]byte) error {
	_, err := encodeNodePages(node, pageSize, ids, true, pages)
	return err
}
func encodeNodePages(node *Node, pageSize int64, ids []page.ID, forWAL bool, pages [][]byte) ([][]byte, error) {
	count := NodePageCount(node, pageSize)
	if count == 0 || len(ids) != count || ids[0] != node.PageID() || page.IsMetaID(ids[0]) {
		return nil, ErrInvalid
	}
	for i, id := range ids {
		if id != ids[0]+page.ID(i) || id < ids[0] {
			return nil, ErrInvalid
		}
	}
	size := count * int(pageSize)
	if forWAL {
		size = node.EncodedSize()
	}
	image := make([]byte, size)
	encodeWALNode(image[:node.EncodedSize()], node)
	if !forWAL {
		binary.LittleEndian.PutUint32(image[14:18], pageChecksum(image, int64(size), nodeChecksumOffset))
	}
	if pages == nil {
		pages = make([][]byte, count)
	} else if len(pages) != count {
		return nil, ErrInvalid
	}
	for i := range pages {
		start := i * int(pageSize)
		end := min(size, start+int(pageSize))
		if pages[i] == nil {
			pages[i] = image[start:end:end]
		} else {
			if len(pages[i]) < end-start || int64(len(pages[i])) > pageSize {
				return nil, ErrInvalid
			}
			pages[i] = pages[i][: end-start : end-start]
			copy(pages[i], image[start:end])
		}
	}
	return pages, nil
}

// VerifyNodeIDAndSetChecksum seals a complete padded node span.
func VerifyNodeIDAndSetChecksum(data []byte, id page.ID, enabled bool) error {
	return verifyPageIDAndSetChecksum(data, id, NodeHeaderSize, nodeChecksumOffset, enabled)
}
func verifyPageIDAndSetChecksum(data []byte, id page.ID, headerSize, offset int, enabled bool) error {
	if len(data) < headerSize || page.ID(binary.LittleEndian.Uint64(data[2:10])) != id {
		return ErrInvalid
	}
	var sum uint32
	if enabled {
		sum = pageChecksum(data, int64(len(data)), offset)
	}
	binary.LittleEndian.PutUint32(data[offset:offset+4], sum)
	return nil
}

// WriteNode writes the complete consecutive span. shouldPad includes its final zero padding.
func WriteNode(w io.Writer, node *Node, pageSize int64, shouldPad, dataPageChecksums bool) error {
	count := NodePageCount(node, pageSize)
	if count == 0 {
		return ErrNodeTooLarge
	}
	data := EncodeWALNode(node)
	size := int64(count) * pageSize
	if dataPageChecksums {
		node.header.Checksum = pageChecksum(data, size, nodeChecksumOffset)
		binary.LittleEndian.PutUint32(data[14:18], node.header.Checksum)
	} else {
		node.header.Checksum = 0
	}
	if err := fileio.WriteFull(w, data); err != nil {
		return err
	}
	if shouldPad {
		for remaining := size - int64(len(data)); remaining > 0; {
			n := min(remaining, int64(len(checksumZeroBlock)))
			if err := fileio.WriteFull(w, checksumZeroBlock[:n]); err != nil {
				return err
			}
			remaining -= n
		}
	}
	return nil
}

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
