package btree

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/Issaminu/kvlite/internal/page"
)

// ReadMappedContinuationNode verifies one continuation page without allocating.
// It returns header values, encoded next IDs, and body bytes. The slices borrow
// data and cannot grow into other fields. Keep data unchanged while using them.
// It checks array bounds but does not follow page IDs.
// checksums selects verification of the nonzero checksum field.
func ReadMappedContinuationNode(data []byte, id page.ID, pageSize int64, checksums bool) (ContinuationHeader, []byte, []byte, error) {
	return readContinuationNodeHeader(data, id, pageSize, false, checksums)
}

// readContinuationNodeHeader shares header checks between mapped pages and verified WAL payloads.
func readContinuationNodeHeader(data []byte, id page.ID, pageSize int64, forWAL, checksums bool) (ContinuationHeader, []byte, []byte, error) {
	if int64(len(data)) != pageSize || pageSize <= ContinuationHeaderSize {
		return ContinuationHeader{}, nil, nil, ErrInvalid
	}
	h := ContinuationHeader{
		Type:          NodeType(binary.LittleEndian.Uint16(data[:2])),
		PageID:        page.ID(binary.LittleEndian.Uint64(data[2:10])),
		Checksum:      binary.LittleEndian.Uint32(data[10:14]),
		NextPageCount: binary.LittleEndian.Uint32(data[14:18]),
	}
	if h.Type != NodeTypeContinuation || h.PageID != id {
		return ContinuationHeader{}, nil, nil, ErrInvalid
	}
	if forWAL {
		if h.Checksum != 0 {
			return ContinuationHeader{}, nil, nil, ErrInvalid
		}
	} else if checksums && h.Checksum != 0 && h.Checksum != pageChecksum(data, pageSize, continuationChecksumOffset) {
		return ContinuationHeader{}, nil, nil, fmt.Errorf("continuation page %d: %w", id, page.ErrChecksum)
	}
	// Each eight-byte ID must leave at least one byte for body data.
	if uint64(h.NextPageCount) > uint64((pageSize-ContinuationHeaderSize-1)/page.IDSize) {
		return ContinuationHeader{}, nil, nil, ErrInvalid
	}
	start := ContinuationHeaderSize + int(h.NextPageCount)*page.IDSize
	return h, data[ContinuationHeaderSize:start:start], data[start:len(data):len(data)], nil
}

// DecodeContinuationNode copies the header and decodes this page's ID array.
// On success, the node owns data. Its body refers to data. Do not change or reuse
// data after this call. This does not rebuild the owning leaf or branch node.
// checksums selects verification of nonzero checksum fields.
func DecodeContinuationNode(data []byte, id page.ID, pageSize int64, checksums bool) (*ContinuationNode, error) {
	h, encodedIDs, body, err := ReadMappedContinuationNode(data, id, pageSize, checksums)
	if err != nil {
		return nil, err
	}
	ids := make([]page.ID, h.NextPageCount)
	for i := range ids {
		ids[i] = page.ID(binary.LittleEndian.Uint64(encodedIDs[i*page.IDSize : (i+1)*page.IDSize]))
	}
	return &ContinuationNode{header: &h, nextPageIDs: ids, body: body}, nil
}

// EncodeContinuationNode returns a new encoded page and calculates its checksum.
// It writes the current ID count. It does not change or retain n's fields.
func EncodeContinuationNode(n *ContinuationNode, pageSize int64) ([]byte, error) {
	if n == nil || n.header == nil || n.header.Type != NodeTypeContinuation || pageSize <= ContinuationHeaderSize ||
		int64(len(n.nextPageIDs)) > (pageSize-ContinuationHeaderSize-1)/page.IDSize ||
		int64(len(n.body)) > pageSize-ContinuationHeaderSize-int64(len(n.nextPageIDs))*page.IDSize {
		return nil, ErrInvalid
	}
	data := make([]byte, pageSize)
	start := encodeContinuationHeader(data, n.PageID(), n.nextPageIDs)
	copy(data[start:], n.body)
	sealContinuationPage(data)
	return data, nil
}

// finishContinuationPages uses the final write position to omit only padding from WAL images.
// It keeps trailing zero body bytes. An exact page boundary has index=len(pages)
// and needs no trim. Main-file images stay padded and receive their page checksums.
func finishContinuationPages(pages [][]byte, index, at int, forWAL bool) {
	if forWAL {
		if index < len(pages) {
			last := pages[index]
			end := ContinuationHeaderSize + int(binary.LittleEndian.Uint32(last[14:18]))*page.IDSize + at
			pages[index] = last[:end:end]
		}
		return
	}
	first := pages[0]
	binary.LittleEndian.PutUint32(first[14:18], pageChecksum(first, int64(len(first)), nodeChecksumOffset))
	for _, data := range pages[1:] {
		sealContinuationPage(data)
	}
}

// encodeContinuationHeader initializes a page and returns its body offset.
// The caller provides zeroed storage with room for the IDs and at least one body byte.
func encodeContinuationHeader(data []byte, id page.ID, ids []page.ID) int {
	binary.LittleEndian.PutUint16(data[:2], uint16(NodeTypeContinuation))
	binary.LittleEndian.PutUint64(data[2:10], uint64(id))
	binary.LittleEndian.PutUint32(data[14:18], uint32(len(ids)))
	for i, id := range ids {
		binary.LittleEndian.PutUint64(data[18+i*page.IDSize:18+(i+1)*page.IDSize], uint64(id))
	}
	return ContinuationHeaderSize + len(ids)*page.IDSize
}

func sealContinuationPage(data []byte) {
	crc := continuationChecksumOffset
	binary.LittleEndian.PutUint32(data[crc:crc+4], pageChecksum(data, int64(len(data)), crc))
}

// continuationCursor remembers the pages that continuationBodyBytes reached in one continued body.
// The zero value starts at the first page. A cursor belongs to one caller and one body.
// It never stores the first page, so the caller can keep it and the body in local variables.
type continuationCursor struct {
	group       []byte // last page with an ID array after the first page; nil means the first page
	groupOffset int    // body offset of group
	cached      []byte // last checked continuation page; nil before the first read
}

// continuationBodyBytes returns the mapped fragment at a logical node offset.
// offset includes the normal 18-byte node header. The returned ID identifies
// the physical page that holds the fragment. The bytes borrow that page.
// The owning codec checks the first page. This function checks each later page.
// cursor remembers the last ID-array page reached and one verified page.
// Its group offset increases when a group is skipped, so a repeated link
// cannot make this loop run without a bound.
// A later group starts at the last ID of the current group. Other IDs point
// directly to data pages with no ID array. No node entries are decoded here.
// forWAL selects compact payloads with zero page checksums. Their WAL records
// must already be verified. checksums selects main-file checksum checks.
func continuationBodyBytes(first []byte, offset int, read func(page.ID) ([]byte, error), cursor *continuationCursor, forWAL, checksums bool) ([]byte, page.ID, error) {
	size := int(binary.LittleEndian.Uint32(first[18:22]))
	offset -= NodeHeaderSize
	if offset < 0 || offset >= size {
		return nil, 0, ErrInvalid
	}
	group, base := cursor.group, cursor.groupOffset
	if group == nil || offset < base {
		group, base = nil, 0
	}
	for {
		data, fixed := first, continuedNodeHeaderSize
		if group != nil {
			data, fixed = group, ContinuationHeaderSize
		}
		count := int(binary.LittleEndian.Uint32(data[fixed-4 : fixed]))
		want := min(continuationPageCount(size-base, int64(len(first)), fixed)-1, (len(first)-fixed-1)/page.IDSize)
		if count != want {
			return nil, 0, ErrInvalid
		}
		start := fixed + count*page.IDSize
		local := offset - base
		bodySize := len(first) - start
		if local < bodySize {
			end := min(len(data), start+size-base)
			return data[start+local : end], page.ID(binary.LittleEndian.Uint64(data[2:10])), nil
		}
		if count == 0 {
			return nil, 0, ErrInvalid
		}
		local -= bodySize
		pageBodySize := len(first) - ContinuationHeaderSize
		index := min(local/pageBodySize, count-1)
		id := page.ID(binary.LittleEndian.Uint64(data[fixed+index*page.IDSize : fixed+(index+1)*page.IDSize]))
		if page.IsMetaID(id) || id == page.ID(binary.LittleEndian.Uint64(first[2:10])) {
			return nil, 0, ErrInvalid
		}
		next := cursor.cached
		if next == nil || id != page.ID(binary.LittleEndian.Uint64(next[2:10])) {
			var err error
			next, err = read(id)
			if err != nil {
				return nil, 0, err
			}
			pageSize := int64(len(first))
			if forWAL {
				pageSize = int64(len(next))
			}
			h, _, _, err := readContinuationNodeHeader(next, id, pageSize, forWAL, checksums)
			if err != nil {
				return nil, 0, err
			}
			if forWAL {
				// Only the final payload can be short. Its length must include all
				// remaining body bytes, with no padding or omitted bytes.
				pageBase := base + bodySize + index*pageBodySize
				want := min(len(first), ContinuationHeaderSize+int(h.NextPageCount)*page.IDSize+size-pageBase)
				if len(next) != want {
					return nil, 0, ErrInvalid
				}
			}
			cursor.cached = next
		}
		if index == count-1 {
			base += bodySize + (count-1)*pageBodySize
			group = next
			if base > cursor.groupOffset {
				cursor.group, cursor.groupOffset = group, base
			}
			continue
		}
		if binary.LittleEndian.Uint32(next[14:18]) != 0 {
			return nil, 0, ErrInvalid
		}
		at := local % pageBodySize
		pageBase := base + bodySize + index*pageBodySize
		end := min(len(next), ContinuationHeaderSize+size-pageBase)
		return next[ContinuationHeaderSize+at : end], id, nil
	}
}

// continuedBody reads the body of a continued leaf or branch in place, one page at a time.
// Offsets are logical body offsets: they count from the start of the normal 18-byte header, as descriptor offsets do.
// A reader that needs only some bytes, such as a point lookup, reads only the pages that hold them.
// It never copies the complete body.
type continuedBody struct {
	first     []byte                        // the first page, with a checked header
	readPage  func(page.ID) ([]byte, error) // supplies each continuation page
	forWAL    bool                          // pages are verified compact WAL payloads
	checksums bool                          // main-file pages verify nonzero checksums
	// cursor remembers the pages reached so far. It points to a separate variable of the caller.
	// Escape analysis treats a struct as one value, and it treats the pages in a cursor as escaping, so a cursor field would make readPage escape and allocate in every caller.
	cursor *continuationCursor
}

// read copies len(dst) body bytes at offset into dst.
func (body *continuedBody) read(offset int, dst []byte) error {
	for len(dst) > 0 {
		part, _, err := continuationBodyBytes(body.first, offset, body.readPage, body.cursor, body.forWAL, body.checksums)
		if err != nil {
			return err
		}
		n := copy(dst, part)
		offset += n
		dst = dst[n:]
	}
	return nil
}

// compare compares size body bytes at offset with key, as bytes.Compare does.
// It compares one page part at a time and stops at the first difference.
func (body *continuedBody) compare(offset, size int, key []byte) (int, error) {
	length := min(size, len(key))
	for compared := 0; compared < length; {
		part, _, err := continuationBodyBytes(body.first, offset+compared, body.readPage, body.cursor, body.forWAL, body.checksums)
		if err != nil {
			return 0, err
		}
		part = part[:min(len(part), length-compared)]
		if comparison := bytes.Compare(part, key[compared:compared+len(part)]); comparison != 0 {
			return comparison, nil
		}
		compared += len(part)
	}
	// All compared bytes are equal, so the shorter one sorts first.
	return cmp.Compare(size, len(key)), nil
}

// directory reads the directory of size bytes into a buffer that starts like a node image: the directory starts at NodeHeaderSize.
// So leafSpan, branchKey, and branchChild read it as they read an image.
// scratch holds a small directory without an allocation. A larger directory uses new storage.
func (body *continuedBody) directory(size int, scratch []byte) ([]byte, error) {
	buffer := scratch
	if NodeHeaderSize+size > len(buffer) {
		buffer = make([]byte, NodeHeaderSize+size)
	}
	buffer = buffer[:NodeHeaderSize+size]
	return buffer, body.read(NodeHeaderSize, buffer[NodeHeaderSize:])
}

// lookupContinuedLeaf finds key in a continued leaf without copying its body.
// It compares keys page by page and copies only the value of the matching entry.
// A continued leaf has one entry (see Node.NeedsSplit), so it checks the entries in order and does no binary search.
// The value is a private copy, and the key is a copy of key.
// Set includeKey to false to omit the key.
func lookupContinuedLeaf(body *continuedBody, h NodeHeader, key []byte, includeKey bool) (Entry, bool, error) {
	imageSize := NodeHeaderSize + uint64(h.BodyLength)
	dataStart, err := leafDataStart(imageSize, h.EntryCount)
	if err != nil {
		return Entry{}, false, err
	}
	var scratch [NodeHeaderSize + continuedDirectoryScratchSize]byte
	directory, err := body.directory(int(dataStart)-NodeHeaderSize, scratch[:])
	if err != nil {
		return Entry{}, false, err
	}
	for index := range int(h.EntryCount) {
		flags, start, keyEnd, end := leafSpan(directory, index)
		if !validLeaf(flags, start, keyEnd, end, dataStart, imageSize) {
			return Entry{}, false, entryError(index)
		}
		comparison, err := body.compare(int(start), int(keyEnd-start), key)
		if err != nil || comparison > 0 {
			// Keys are in order, so a larger key ends the search.
			return Entry{}, false, err
		}
		if comparison < 0 {
			continue
		}
		entry := Entry{flags: flags, value: make([]byte, end-keyEnd), valueOwned: true}
		if err := body.read(int(keyEnd), entry.value); err != nil {
			return Entry{}, false, err
		}
		if includeKey {
			// The stored key equals key, so a copy of key needs no more page reads.
			// The result never borrows from body: escape analysis treats body as one value, so readPage would escape and allocate in every caller.
			entry.key = bytes.Clone(key)
		}
		return entry, true, nil
	}
	return Entry{}, false, nil
}

// lookupContinuedBranch returns the child that routes key in a continued branch without copying its body.
// It compares separators page by page, so a separator that differs from key early reads no other page.
// A continued branch has at most two separators (see Node.NeedsSplit), so it checks them in order and does no binary search.
func lookupContinuedBranch(body *continuedBody, h NodeHeader, key []byte) (page.ID, error) {
	imageSize := NodeHeaderSize + uint64(h.BodyLength)
	dataStart, err := branchDataStart(imageSize, h.EntryCount)
	if err != nil {
		return 0, err
	}
	var scratch [NodeHeaderSize + continuedDirectoryScratchSize]byte
	directory, err := body.directory(int(dataStart)-NodeHeaderSize, scratch[:])
	if err != nil {
		return 0, err
	}
	// Find the first separator that is larger than key. Its index is also the index of the child.
	count, index := int(h.EntryCount), 0
	for ; index < count; index++ {
		start, end := branchKey(directory, index)
		if !validKey(start, end, dataStart, imageSize) {
			return 0, entryError(index)
		}
		comparison, err := body.compare(int(start), int(end-start), key)
		if err != nil {
			return 0, err
		}
		if comparison > 0 {
			break
		}
	}
	child, ok := branchChild(directory, count, index)
	if !ok {
		return 0, childError(index)
	}
	return child, nil
}

// checkContinuedLeaf checks every entry of a continued leaf in the same way as decodeLeafBody, without a copy of its body.
// It copies only the root page IDs of nested buckets, and each key except the last for the order check of the next key. The values stay in their pages.
// visit receives the root page ID of each nested bucket. visit can be nil.
func checkContinuedLeaf(body *continuedBody, h NodeHeader, visit func(page.ID) error) error {
	imageSize := NodeHeaderSize + uint64(h.BodyLength)
	dataStart, err := leafDataStart(imageSize, h.EntryCount)
	if err != nil {
		return err
	}
	var scratch [NodeHeaderSize + continuedDirectoryScratchSize]byte
	directory, err := body.directory(int(dataStart)-NodeHeaderSize, scratch[:])
	if err != nil {
		return err
	}
	count, next := int(h.EntryCount), dataStart
	// previous is a copy of the previous key. Each key is compared with it in place, so one buffer is enough.
	var previous []byte
	for index := range count {
		flags, start, keyEnd, end := leafSpan(directory, index)
		if !validLeaf(flags, start, keyEnd, end, dataStart, imageSize) || start != next {
			return entryError(index)
		}
		if index > 0 {
			comparison, err := body.compare(int(start), int(keyEnd-start), previous)
			if err != nil {
				return err
			}
			if comparison <= 0 {
				return entryError(index)
			}
		}
		// Only a later key needs the copy, so the last key is not copied.
		if index+1 < count {
			previous = slices.Grow(previous[:0], int(keyEnd-start))[:keyEnd-start]
			if err := body.read(int(start), previous); err != nil {
				return err
			}
		}
		next = end
		// validLeaf accepts a nested bucket entry only with an 8-byte root page ID.
		if flags&BucketLeafFlag != 0 && visit != nil {
			var root [page.IDSize]byte
			if err := body.read(int(keyEnd), root[:]); err != nil {
				return err
			}
			if err := visit(page.ID(binary.LittleEndian.Uint64(root[:]))); err != nil {
				return err
			}
		}
	}
	if next != imageSize {
		return errBodySize
	}
	return nil
}

// checkContinuedBranch checks every child ID and separator of a continued branch in the same way as decodeBranchBody, without a copy of its body.
// It copies only each separator except the last, for the order check of the next separator. visit receives each child page ID. visit can be nil.
func checkContinuedBranch(body *continuedBody, h NodeHeader, visit func(page.ID) error) error {
	imageSize := NodeHeaderSize + uint64(h.BodyLength)
	dataStart, err := branchDataStart(imageSize, h.EntryCount)
	if err != nil {
		return err
	}
	var scratch [NodeHeaderSize + continuedDirectoryScratchSize]byte
	directory, err := body.directory(int(dataStart)-NodeHeaderSize, scratch[:])
	if err != nil {
		return err
	}
	count := int(h.EntryCount)
	for index := range count + 1 {
		child, ok := branchChild(directory, count, index)
		if !ok {
			return childError(index)
		}
		if visit != nil {
			if err := visit(child); err != nil {
				return err
			}
		}
	}
	next := dataStart
	// previous is a copy of the previous separator, as in checkContinuedLeaf.
	var previous []byte
	for index := range count {
		start, end := branchKey(directory, index)
		if !validKey(start, end, dataStart, imageSize) || start != next {
			return entryError(index)
		}
		if index > 0 {
			comparison, err := body.compare(int(start), int(end-start), previous)
			if err != nil {
				return err
			}
			if comparison <= 0 {
				return entryError(index)
			}
		}
		if index+1 < count {
			previous = slices.Grow(previous[:0], int(end-start))[:end-start]
			if err := body.read(int(start), previous); err != nil {
				return err
			}
		}
		next = end
	}
	if next != imageSize {
		return errBodySize
	}
	return nil
}

// copyContinuationBody copies the body for a full decoded leaf or branch.
// The owning codec verifies first and gives dst the declared body length.
// It returns the continuation IDs for retirement. It rejects repeated IDs
// and nonzero final padding. forWAL selects verified compact WAL payloads.
// Mapped point lookups do not copy the full body.
func copyContinuationBody(first []byte, dst []byte, read func(page.ID) ([]byte, error), forWAL, checksums bool) ([]page.ID, error) {
	if len(dst) != int(binary.LittleEndian.Uint32(first[18:22])) {
		return nil, ErrInvalid
	}
	pages := continuationPageCount(len(dst), int64(len(first)), continuedNodeHeaderSize)
	if pages < 2 {
		return nil, ErrInvalid
	}
	ids := make([]page.ID, 0, pages-1)
	firstID := page.ID(binary.LittleEndian.Uint64(first[2:10]))
	at := 0
	err := walkContinuationPages(first, read, forWAL, func(id page.ID, part []byte) error {
		if id != firstID {
			ids = append(ids, id)
		}
		at += copy(dst[at:], part)
		return nil
	}, checksums)
	return ids, err
}

// walkContinuationPages verifies every storage page in body order.
// It rejects repeated IDs and nonzero final padding.
// visit receives borrowed body bytes. The owning leaf or branch codec must verify the first page.
// Full decode copies these bytes. Ownership checks read them without a body copy.
func walkContinuationPages(first []byte, read func(page.ID) ([]byte, error), forWAL bool, visit func(page.ID, []byte) error, checksums bool) error {
	size := int(binary.LittleEndian.Uint32(first[18:22]))
	pages := continuationPageCount(size, int64(len(first)), continuedNodeHeaderSize)
	if pages < 2 {
		return ErrInvalid
	}
	seen := pageIDSet{ids: make([]page.ID, 0, pages)}
	var cursor continuationCursor
	firstID := page.ID(binary.LittleEndian.Uint64(first[2:10]))
	for at := 0; at < size; {
		part, id, err := continuationBodyBytes(first, NodeHeaderSize+at, read, &cursor, forWAL, checksums)
		if err != nil {
			return err
		}
		if !seen.add(id) {
			return ErrInvalid
		}
		if at+len(part) == size && id != firstID {
			last := cursor.cached
			start := ContinuationHeaderSize + int(binary.LittleEndian.Uint32(last[14:18]))*page.IDSize + len(part)
			for _, b := range last[start:] {
				if b != 0 {
					return ErrInvalid
				}
			}
		}
		if err := visit(id, part); err != nil {
			return err
		}
		at += len(part)
	}
	return nil
}

// pageIDSet finds a repeated page ID among the pages of one continued node.
// New pages usually have ascending IDs, and ascending IDs need no map.
// The first ID that is not larger than the one before it makes the set build a map from all earlier IDs.
type pageIDSet struct {
	ids  []page.ID            // Ascending IDs added before the map exists.
	seen map[page.ID]struct{} // All added IDs after the first out-of-order ID.
}

// add records id. It returns false when the set already contains id.
func (set *pageIDSet) add(id page.ID) bool {
	if set.seen == nil {
		if len(set.ids) == 0 || id > set.ids[len(set.ids)-1] {
			set.ids = append(set.ids, id)
			return true
		}
		set.seen = make(map[page.ID]struct{}, cap(set.ids))
		for _, previous := range set.ids {
			set.seen[previous] = struct{}{}
		}
	}
	if _, exists := set.seen[id]; exists {
		return false
	}
	set.seen[id] = struct{}{}
	return true
}

// hasRepeatedPageID reports whether ids contains one ID more than once.
// Ascending IDs need no map.
func hasRepeatedPageID(ids []page.ID) bool {
	for index := 1; index < len(ids); index++ {
		if ids[index] > ids[index-1] {
			continue
		}
		seen := make(map[page.ID]struct{}, len(ids))
		for _, id := range ids {
			if _, exists := seen[id]; exists {
				return true
			}
			seen[id] = struct{}{}
		}
		return false
	}
	return false
}

// continuationPageCount counts pages needed for a body with grouped continuation IDs.
// Each later page contributes its body space and consumes one ID in an earlier page.
func continuationPageCount(bodySize int, pageSize int64, firstHeaderSize int) int {
	capacity := int(pageSize) - ContinuationHeaderSize - page.IDSize
	if int(pageSize) <= firstHeaderSize+page.IDSize || capacity <= 0 {
		return 0
	}
	extra := ContinuationHeaderSize + page.IDSize - firstHeaderSize
	return (bodySize - extra + capacity - 1) / capacity
}

// prepareContinuationPages writes headers and grouped continuation IDs.
// It uses supplied destinations or allocates page buffers when pages is nil.
// The owning codec writes the first page's type-specific fields and body.
// Continuation pages get their own header here. Their checksums follow body encoding.
func prepareContinuationPages(ids []page.ID, pageSize int64, pages [][]byte) ([][]byte, error) {
	if hasRepeatedPageID(ids) {
		return nil, ErrInvalid
	}
	allocate := pages == nil
	if allocate {
		pages = make([][]byte, len(ids))
	}
	chunkSize := min(max(1<<20, int(pageSize)), len(ids)*int(pageSize))
	var chunk []byte
	arrayPageIndex := 0
	for i, id := range ids {
		if page.IsMetaID(id) {
			return nil, ErrInvalid
		}
		var data []byte
		if allocate {
			if len(chunk) < int(pageSize) {
				chunk = make([]byte, min(chunkSize, (len(ids)-i)*int(pageSize)))
			}
			data = chunk[:int(pageSize):int(pageSize)]
			chunk = chunk[int(pageSize):]
		} else {
			data = pages[i]
		}
		fixed := ContinuationHeaderSize
		if i == 0 {
			fixed = continuedNodeHeaderSize
		}
		// Supplied buffers can contain old header fields, including checksums.
		clear(data[:fixed])
		var nextIDs []page.ID
		if i == arrayPageIndex {
			count := min(len(ids)-i-1, (int(pageSize)-fixed-1)/page.IDSize)
			nextIDs = ids[i+1 : i+1+count]
			arrayPageIndex = i + count
		}
		if i == 0 {
			binary.LittleEndian.PutUint32(data[fixed-4:fixed], uint32(len(nextIDs)))
			for n, next := range nextIDs {
				binary.LittleEndian.PutUint64(data[fixed+n*page.IDSize:fixed+(n+1)*page.IDSize], uint64(next))
			}
		} else {
			encodeContinuationHeader(data, id, nextIDs)
		}
		pages[i] = data
	}
	return pages, nil
}

// copyContinuationBytes appends body bytes across the prepared page buffers.
// index and at keep the write position between descriptors and payload slices.
// The owning codec supplies those bytes. This function does not interpret them.
func copyContinuationBytes(pages [][]byte, index, at *int, source []byte) {
	for len(source) > 0 {
		fixed := ContinuationHeaderSize
		if *index == 0 {
			fixed = continuedNodeHeaderSize
		}
		data := pages[*index]
		start := fixed + int(binary.LittleEndian.Uint32(data[fixed-4:fixed]))*page.IDSize
		n := copy(data[start+*at:], source)
		*at += n
		source = source[n:]
		if start+*at == len(data) {
			*index++
			*at = 0
		}
	}
}
