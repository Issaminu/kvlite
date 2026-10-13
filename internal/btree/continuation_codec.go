package btree

import (
	"encoding/binary"
	"fmt"

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

// continuationBodyBytes returns the mapped fragment at a logical node offset.
// offset includes the normal 18-byte node header. The returned ID identifies
// the physical page that holds the fragment. The bytes borrow that page.
// The owning codec checks the first page. This function checks each later page.
// group and groupOffset remember the last ID-array page reached by this lookup.
// cached remembers one verified page. These variables belong to the caller.
// groupOffset is the body offset of group. It increases when a group is
// skipped, so a repeated link cannot make this loop run without a bound.
// A later group starts at the last ID of the current group. Other IDs point
// directly to data pages with no ID array. No node entries are decoded here.
// forWAL selects compact payloads with zero page checksums. Their WAL records
// must already be verified. checksums selects main-file checksum checks.
func continuationBodyBytes(first []byte, firstHeaderSize, offset int, read func(page.ID) ([]byte, error), group *[]byte, groupOffset *int, cached *[]byte, forWAL, checksums bool) ([]byte, page.ID, error) {
	size := int(binary.LittleEndian.Uint32(first[18:22]))
	offset -= NodeHeaderSize
	if offset < 0 || offset >= size {
		return nil, 0, ErrInvalid
	}
	data, base := *group, *groupOffset
	if offset < base {
		data, base = first, 0
	}
	for {
		fixed := ContinuationHeaderSize
		if base == 0 {
			fixed = firstHeaderSize
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
		next := *cached
		if id != page.ID(binary.LittleEndian.Uint64(next[2:10])) {
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
			*cached = next
		}
		if index == count-1 {
			base += bodySize + (count-1)*pageBodySize
			data = next
			if base > *groupOffset {
				*group, *groupOffset = data, base
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

// copyContinuationBody copies the body for a full decoded leaf or branch.
// The owning codec verifies first and gives dst the declared body length.
// It returns the continuation IDs for retirement. It rejects repeated IDs
// and nonzero final padding. forWAL selects verified compact WAL payloads.
// Mapped point lookups do not copy the full body.
func copyContinuationBody(first []byte, firstHeaderSize int, dst []byte, read func(page.ID) ([]byte, error), forWAL, checksums bool) ([]page.ID, error) {
	if len(dst) != int(binary.LittleEndian.Uint32(first[18:22])) {
		return nil, ErrInvalid
	}
	pages := continuationPageCount(len(dst), int64(len(first)), firstHeaderSize)
	if pages < 2 {
		return nil, ErrInvalid
	}
	ids := make([]page.ID, 0, pages-1)
	firstID := page.ID(binary.LittleEndian.Uint64(first[2:10]))
	at := 0
	err := walkContinuationPages(first, firstHeaderSize, read, forWAL, func(id page.ID, part []byte) error {
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
func walkContinuationPages(first []byte, firstHeaderSize int, read func(page.ID) ([]byte, error), forWAL bool, visit func(page.ID, []byte) error, checksums bool) error {
	size := int(binary.LittleEndian.Uint32(first[18:22]))
	pages := continuationPageCount(size, int64(len(first)), firstHeaderSize)
	if pages < 2 {
		return ErrInvalid
	}
	seen := make(map[page.ID]struct{}, pages)
	group, groupOffset, cached := first, 0, first
	firstID := page.ID(binary.LittleEndian.Uint64(first[2:10]))
	for at := 0; at < size; {
		part, id, err := continuationBodyBytes(first, firstHeaderSize, NodeHeaderSize+at, read, &group, &groupOffset, &cached, forWAL, checksums)
		if err != nil {
			return err
		}
		if _, exists := seen[id]; exists {
			return ErrInvalid
		}
		seen[id] = struct{}{}
		if at+len(part) == size && id != firstID {
			start := ContinuationHeaderSize + int(binary.LittleEndian.Uint32(cached[14:18]))*page.IDSize + len(part)
			for _, b := range cached[start:] {
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
func prepareContinuationPages(ids []page.ID, pageSize int64, firstHeaderSize int, pages [][]byte) ([][]byte, error) {
	allocate := pages == nil
	if allocate {
		pages = make([][]byte, len(ids))
	}
	seen := make(map[page.ID]struct{}, len(ids))
	chunkSize := min(max(1<<20, int(pageSize)), len(ids)*int(pageSize))
	var chunk []byte
	arrayPageIndex := 0
	for i, id := range ids {
		if page.IsMetaID(id) {
			return nil, ErrInvalid
		}
		if _, ok := seen[id]; ok {
			return nil, ErrInvalid
		}
		seen[id] = struct{}{}
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
			fixed = firstHeaderSize
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
func copyContinuationBytes(pages [][]byte, firstHeaderSize int, index, at *int, source []byte) {
	for len(source) > 0 {
		fixed := ContinuationHeaderSize
		if *index == 0 {
			fixed = firstHeaderSize
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
