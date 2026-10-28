package kvlite

import (
	"encoding/binary"
	"fmt"
	"maps"
	"math/bits"
	"slices"

	"github.com/Issaminu/kvlite/internal/page"
)

const (
	// The two metadata pages are followed by the first group of bitmap frames.
	firstAllocationSegmentID page.ID = page.Meta1ID + 1
	// firstTreePageID is a lower bound. The first root follows the complete bitmap group.
	firstTreePageID page.ID = firstAllocationSegmentID + 1

	allocationSegmentMagic uint32 = 0x5246564B // The little-endian bytes spell "KVFR".
)

// Each allocation segment uses one page with its own header and checksum:
//
//	[0:4]  magic number
//	[4:8]  CRC32C checksum
//	[8:16] segment page ID
//
// The bitmap follows the header. A set bit marks one used database page.
const (
	allocationMagicSize    = 4
	allocationChecksumSize = 4
	allocationPageIDSize   = page.IDSize

	allocationMagicOffset    = 0
	allocationChecksumOffset = allocationMagicOffset + allocationMagicSize
	allocationPageIDOffset   = allocationChecksumOffset + allocationChecksumSize
	allocationHeaderSize     = allocationPageIDOffset + allocationPageIDSize
)

// allocationSegment is one encoded bitmap page and its memory-only free-page cursor.
// data contains the header and bitmap. firstFreeBit is an offset in this segment, not a database page ID.
type allocationSegment struct {
	data         []byte
	firstFreeBit page.ID
}

// bitmapBits returns a mutable view of the bitmap bytes in data.
// The returned slice aliases data and remains valid for the lifetime of the segment.
func (segment *allocationSegment) bitmapBits() []byte {
	return segment.data[allocationHeaderSize:]
}

// allocationBitmap is the committed allocation view for one open database.
// segments contain the persistent bits. firstFreeSegment is a memory-only search cursor.
type allocationBitmap struct {
	pageSize         int64
	groupSegments    int // independent bitmap frames reserved together in each physical group
	segments         []allocationSegment
	firstFreeSegment int
	spanCount        int     // request size last searched; equal or larger requests can reuse spanCursor
	spanCursor       page.ID // earlier reusable pages have no fitting run; zero restarts the search
}

// allocationChanges overlays one committed bitmap with uncommitted allocation work.
// A missing dirty segment is read from base. A dirty segment is one private bitmap page.
// Retired pages remain allocated until finalize prepares the commit.
type allocationChanges struct {
	base             *allocationBitmap          // committed read-only segments
	dirtySegments    map[int]*allocationSegment // private segments, keyed by segment index
	segmentCount     int                        // private count after segment additions or removals
	firstFreeSegment int                        // private search cursor
	spanCount        int                        // private span search length
	spanCursor       page.ID                    // private span search cursor
	retired          map[page.ID]uint64         // masks of retired tree pages, keyed by 64-page word
	obsoleteWALPages map[page.ID]struct{}       // allocation pages removed from the file tail
	pagesReused      uint64
	pagesRetired     uint64
	tailPagesRemoved uint64
}

// newAllocationChanges starts an empty transaction overlay on bitmap.
// It copies only the bitmap pages that the transaction changes.
func newAllocationChanges(bitmap *allocationBitmap) *allocationChanges {
	return &allocationChanges{
		base:             bitmap,
		segmentCount:     len(bitmap.segments),
		firstFreeSegment: bitmap.firstFreeSegment,
		spanCount:        bitmap.spanCount,
		spanCursor:       bitmap.spanCursor,
	}
}

// clone creates an independent view for the next callback in a write batch.
// The callback can fail without changing allocation work from earlier callbacks.
func (changes *allocationChanges) clone() *allocationChanges {
	dirtySegments := make(map[int]*allocationSegment, len(changes.dirtySegments))
	for index, segment := range changes.dirtySegments {
		dirtySegments[index] = cloneAllocationSegment(segment)
	}
	return &allocationChanges{
		base:             changes.base,
		dirtySegments:    dirtySegments,
		segmentCount:     changes.segmentCount,
		firstFreeSegment: changes.firstFreeSegment,
		spanCount:        changes.spanCount,
		spanCursor:       changes.spanCursor,
		retired:          maps.Clone(changes.retired),
		obsoleteWALPages: maps.Clone(changes.obsoleteWALPages),
		pagesReused:      changes.pagesReused,
		pagesRetired:     changes.pagesRetired,
		tailPagesRemoved: changes.tailPagesRemoved,
	}
}

func cloneAllocationSegment(segment *allocationSegment) *allocationSegment {
	return &allocationSegment{
		data:         slices.Clone(segment.data),
		firstFreeBit: segment.firstFreeBit,
	}
}

// allocate returns the lowest reusable tree page at or below lastPage.
// It extends the logical file when no reusable page exists. A new segment reserves its complete storage span before the returned tree page.
func (changes *allocationChanges) allocate(lastPage page.ID) (page.ID, error) {
	return changes.allocateSpan(lastPage, 1)
}

// allocateSpan reserves consecutive pages without using any allocation storage page.
// Retired pages remain allocated until every new span has been assigned.
func (changes *allocationChanges) allocateSpan(lastPage page.ID, count int) (page.ID, error) {
	pagesPerGroup := changes.base.pagesPerGroup()
	reserved := page.ID(changes.base.segmentsPerGroup())
	if count <= 0 || page.ID(count) > pagesPerGroup-reserved-2 {
		return 0, fmt.Errorf("allocate node span of %d pages: %w", count, page.ErrInvalid)
	}
	start := page.ID(0)
	if count > 1 {
		// A smaller request can fit in an earlier gap that the previous search skipped.
		if count < changes.spanCount {
			changes.spanCursor = 0
		}
		changes.spanCount = count
		start = changes.spanCursor
	}
	for {
		candidate, found := changes.findFreePage(start, lastPage)
		if !found {
			break
		}
		groupStart := candidate / pagesPerGroup * pagesPerGroup
		if pagesPerGroup-1 > ^page.ID(0)-groupStart {
			return 0, page.ErrInvalid
		}
		limit := min(lastPage, groupStart+pagesPerGroup-1)
		if page.ID(count)-1 > limit-candidate {
			if groupStart+pagesPerGroup-1 == ^page.ID(0) {
				break
			}
			start = groupStart + pagesPerGroup
			continue
		}
		free := true
		for step := page.ID(0); step < page.ID(count); step++ {
			if changes.allocated(candidate + step) {
				start = candidate + step + 1
				free = false
				break
			}
		}
		if !free {
			continue
		}
		changes.markSpan(candidate, count, lastPage)
		if count > 1 {
			changes.spanCursor = candidate + page.ID(count)
		}
		return candidate, nil
	}

	if lastPage == ^page.ID(0) {
		return 0, fmt.Errorf("allocate after page %d: %w", lastPage, page.ErrInvalid)
	}
	candidate := lastPage + 1
	for {
		group := candidate / pagesPerGroup
		groups := page.ID(changes.segmentCount / changes.base.segmentsPerGroup())
		if group > groups {
			return 0, page.ErrInvalid
		}
		if group == groups {
			changes.addSegment()
		}
		head := group * pagesPerGroup
		if group == 0 {
			head = firstAllocationSegmentID
		}
		if candidate < head+reserved {
			candidate = head + reserved
		}
		bit := candidate % pagesPerGroup
		if page.ID(count) <= pagesPerGroup-bit {
			if page.ID(count)-1 > ^page.ID(0)-candidate {
				return 0, page.ErrInvalid
			}
			changes.markSpan(candidate, count, lastPage)
			if count > 1 {
				changes.spanCursor = candidate + page.ID(count)
			}
			return candidate, nil
		}
		if pagesPerGroup-bit > ^page.ID(0)-candidate {
			return 0, page.ErrInvalid
		}
		candidate += pagesPerGroup - bit
	}
}

// findFreePage searches logical bitmap frames without treating their boundaries as data gaps.
func (changes *allocationChanges) findFreePage(start, lastPage page.ID) (page.ID, bool) {
	pagesPerSegment := changes.base.pagesPerSegment()
	first := max(changes.firstFreeSegment, changes.base.segmentIndex(start))
	for index := first; index < changes.segmentCount; index++ {
		base := page.ID(index) * pagesPerSegment
		if base > lastPage {
			break
		}
		bit := changes.segment(index).firstFreeBit
		if start > base {
			bit = max(bit, start-base)
		}
		bit = changes.findFirstFreeBit(index, bit)
		if bit < pagesPerSegment && base+bit <= lastPage {
			return base + bit, true
		}
	}
	return 0, false
}

func (changes *allocationChanges) markSpan(head page.ID, count int, lastPage page.ID) {
	for step := page.ID(0); step < page.ID(count); step++ {
		id := head + step
		changes.markAllocated(id)
		if id <= lastPage {
			changes.pagesReused++
		}
	}
}

// extendSpan keeps a node at its current head when the following pages are free.
func (changes *allocationChanges) extendSpan(head page.ID, oldCount, count int, lastPage page.ID) bool {
	if count < oldCount || count <= 0 || page.ID(count)-1 > ^page.ID(0)-head {
		return false
	}
	end := head + page.ID(count) - 1
	if head/changes.base.pagesPerGroup() != end/changes.base.pagesPerGroup() {
		return false
	}
	for step := oldCount; step < count; step++ {
		id := head + page.ID(step)
		if changes.isSegmentPage(id) || changes.allocated(id) {
			return false
		}
	}
	changes.markSpan(head+page.ID(oldCount), count-oldCount, lastPage)
	return true
}

// retire delays reuse of an allocated tree page until finalize prepares the commit.
// It rejects reserved pages, free pages, allocation pages, and repeated retirement.
func (changes *allocationChanges) retire(pageID page.ID) error {
	return changes.retireSpan(pageID, 1)
}

// retireSpan records consecutive pages without changing their allocation bits.
// Every page retains retire's ownership checks; word masks combine their membership.
func (changes *allocationChanges) retireSpan(head page.ID, count int) error {
	if count <= 0 || page.ID(count)-1 > ^page.ID(0)-head {
		return fmt.Errorf("retire invalid span %d/%d: %w", head, count, page.ErrInvalid)
	}
	for offset := 0; offset < count; offset++ {
		id := head + page.ID(offset)
		if id < firstTreePageID || changes.isSegmentPage(id) {
			return fmt.Errorf("retire reserved page %d: %w", id, page.ErrInvalid)
		}
		if !changes.allocated(id) {
			return fmt.Errorf("retire free page %d: %w", id, page.ErrInvalid)
		}
	}
	if changes.retired == nil {
		changes.retired = make(map[page.ID]uint64)
	}
	for remaining := count; remaining > 0; {
		word, offset := head/64, head%64
		width := min(remaining, 64-int(offset))
		mask := (^uint64(0) >> (64 - width)) << offset
		if overlap := changes.retired[word] & mask; overlap != 0 {
			id := word*64 + page.ID(bits.TrailingZeros64(overlap))
			return fmt.Errorf("retire page %d twice: %w", id, page.ErrInvalid)
		}
		changes.retired[word] |= mask
		changes.pagesRetired += uint64(width)
		head += page.ID(width)
		remaining -= width
	}
	return nil
}

func (changes *allocationChanges) isRetired(id page.ID) bool {
	return changes.retired[id/64]&(uint64(1)<<(id%64)) != 0
}

// forEachRetired visits raw page IDs while keeping word masks private to allocation changes.
func (changes *allocationChanges) forEachRetired(visit func(page.ID)) {
	for word, mask := range changes.retired {
		for mask != 0 {
			visit(word*64 + page.ID(bits.TrailingZeros64(mask)))
			mask &= mask - 1
		}
	}
}

func (changes *allocationChanges) changed() bool {
	return len(changes.dirtySegments) != 0 || len(changes.retired) != 0
}

// finalize releases retired pages and lowers metadata when the allocated tail shrinks.
// It returns true when it changes metadata's last page ID.
func (changes *allocationChanges) finalize(meta *page.Meta) bool {
	for word, mask := range changes.retired {
		changes.releaseWord(word, mask)
	}
	// A live data page at the file end prevents tail reclamation.
	if changes.allocated(meta.LastPage()) && !changes.isSegmentPage(meta.LastPage()) {
		return false
	}
	segmentCount := changes.segmentCount
	changes.dropEmptyTailSegments()
	// A removed segment can still have an old page image in the WAL overlay.
	// Record its page ID so the commit can remove that image.
	for index := changes.segmentCount; index < segmentCount; index++ {
		if changes.obsoleteWALPages == nil {
			changes.obsoleteWALPages = make(map[page.ID]struct{})
		}
		changes.obsoleteWALPages[changes.base.segmentPageID(index)] = struct{}{}
	}

	highest := changes.highestAllocated()
	if highest >= meta.LastPage() {
		return false
	}
	changes.tailPagesRemoved += uint64(meta.LastPage() - highest)
	meta.SetLastPage(highest)
	return true
}

// walRetiredPages names tree and allocation pages that must leave the WAL view at commit.
// A removed tail segment can still have an older WAL image even though it is no longer in the bitmap.
func (changes *allocationChanges) walRetiredPages() map[page.ID]struct{} {
	if len(changes.retired) == 0 && len(changes.obsoleteWALPages) == 0 {
		return nil
	}
	retired := make(map[page.ID]struct{}, int(changes.pagesRetired)+len(changes.obsoleteWALPages))
	changes.forEachRetired(func(id page.ID) { retired[id] = struct{}{} })
	for pageID := range changes.obsoleteWALPages {
		retired[pageID] = struct{}{}
	}
	return retired
}

// segment returns the transaction's private segment when one exists.
// Otherwise, it returns the read-only committed segment.
func (changes *allocationChanges) segment(index int) *allocationSegment {
	if segment, ok := changes.dirtySegments[index]; ok {
		return segment
	}
	return &changes.base.segments[index]
}

// writableSegment returns a private mutable segment for this transaction.
// The first write copies the one bitmap page visible through segment.
func (changes *allocationChanges) writableSegment(index int) *allocationSegment {
	if segment, ok := changes.dirtySegments[index]; ok {
		return segment
	}
	private := cloneAllocationSegment(changes.segment(index))
	if changes.dirtySegments == nil {
		changes.dirtySegments = make(map[int]*allocationSegment)
	}
	changes.dirtySegments[index] = private
	return private
}

func (changes *allocationChanges) allocated(pageID page.ID) bool {
	index := changes.base.segmentIndex(pageID)
	if index >= changes.segmentCount {
		return false
	}
	bit := pageID % changes.base.pagesPerSegment()
	return allocationBitIsSet(changes.segment(index).bitmapBits(), bit)
}

func (changes *allocationChanges) isSegmentPage(pageID page.ID) bool {
	return isAllocationSegmentPage(pageID, changes.base.pagesPerSegment(), page.ID(changes.segmentCount-1), page.ID(changes.base.segmentsPerGroup()))
}

// markAllocated sets one private allocation bit and advances both free-page cursors when needed.
func (changes *allocationChanges) markAllocated(pageID page.ID) {
	index := changes.base.segmentIndex(pageID)
	if index >= changes.segmentCount || changes.allocated(pageID) {
		return
	}
	segment := changes.writableSegment(index)
	bit := pageID % changes.base.pagesPerSegment()
	setAllocationBit(segment.bitmapBits(), bit)
	if bit == segment.firstFreeBit {
		segment.firstFreeBit = changes.findFirstFreeBit(index, bit+1)
	}
	if index == changes.firstFreeSegment && changes.segmentFull(index) {
		changes.advanceFirstFree()
	}
}

// release clears one private allocation bit and moves both free-page cursors back when needed.
// It ignores reserved, absent, free, and allocation-segment pages.
func (changes *allocationChanges) release(pageID page.ID) {
	index := changes.base.segmentIndex(pageID)
	if pageID < firstTreePageID || index >= changes.segmentCount || !changes.allocated(pageID) || changes.isSegmentPage(pageID) {
		return
	}
	segment := changes.writableSegment(index)
	bit := pageID % changes.base.pagesPerSegment()
	clearAllocationBit(segment.bitmapBits(), bit)
	changes.rewindFreeCursors(segment, index, pageID)
}

// releaseWord combines ordinary retired pages while retaining release's boundary guards.
func (changes *allocationChanges) releaseWord(word page.ID, mask uint64) {
	head := word * 64
	pagesPerSegment := changes.base.pagesPerSegment()
	index := changes.base.segmentIndex(head)
	if pagesPerSegment%64 != 0 || head < firstTreePageID || index >= changes.segmentCount || changes.isSegmentPage(head) || changes.isSegmentPage(head+63) {
		for mask != 0 {
			changes.release(head + page.ID(bits.TrailingZeros64(mask)))
			mask &= mask - 1
		}
		return
	}
	at := int((head % pagesPerSegment) / 8)
	current := binary.LittleEndian.Uint64(changes.segment(index).bitmapBits()[at : at+8])
	mask &= current
	if mask == 0 {
		return
	}
	segment := changes.writableSegment(index)
	binary.LittleEndian.PutUint64(segment.bitmapBits()[at:at+8], current&^mask)
	changes.rewindFreeCursors(segment, index, head+page.ID(bits.TrailingZeros64(mask)))
}

func (changes *allocationChanges) rewindFreeCursors(segment *allocationSegment, index int, pageID page.ID) {
	bit := pageID % changes.base.pagesPerSegment()
	if bit < segment.firstFreeBit {
		segment.firstFreeBit = bit
	}
	if index < changes.firstFreeSegment {
		changes.firstFreeSegment = index
	}
	if changes.spanCount > 1 {
		// Freeing this page can complete a span that starts up to spanCount-1 pages earlier.
		// Move the search back without checking neighbors; allocateSpan checks the bits.
		// Keep an earlier saved cursor, and clamp page IDs at zero.
		earliestStart := page.ID(0)
		if pageID >= page.ID(changes.spanCount-1) {
			earliestStart = pageID - page.ID(changes.spanCount-1)
		}
		changes.spanCursor = min(changes.spanCursor, earliestStart)
	}
}

func (changes *allocationChanges) segmentFull(index int) bool {
	return changes.segment(index).firstFreeBit >= changes.base.pagesPerSegment()
}

func (changes *allocationChanges) findFirstFreeBit(index int, start page.ID) page.ID {
	// Segment zero also describes metadata and its allocation span. Their bits stay set.
	if index == 0 && start < firstTreePageID {
		start = firstTreePageID
	}
	pagesPerSegment := changes.base.pagesPerSegment()
	if start >= pagesPerSegment {
		return pagesPerSegment
	}

	data := changes.segment(index).bitmapBits()
	byteIndex := int(start / 8)
	bitOffset := uint(start % 8)
	if bit, found := firstClearBit(data[byteIndex], bitOffset); found {
		return min(page.ID(byteIndex*8)+bit, pagesPerSegment)
	}
	for byteIndex++; byteIndex < len(data); byteIndex++ {
		if bit, found := firstClearBit(data[byteIndex], 0); found {
			return min(page.ID(byteIndex*8)+bit, pagesPerSegment)
		}
	}
	return pagesPerSegment
}

func (changes *allocationChanges) advanceFirstFree() {
	for changes.firstFreeSegment < changes.segmentCount && changes.segmentFull(changes.firstFreeSegment) {
		changes.firstFreeSegment++
	}
}

// addSegment creates a full group of private bitmap frames and reserves their pages.
func (changes *allocationChanges) addSegment() {
	first := changes.segmentCount
	count := changes.base.segmentsPerGroup()
	if changes.dirtySegments == nil {
		changes.dirtySegments = make(map[int]*allocationSegment)
	}
	for index := first; index < first+count; index++ {
		start := page.ID(0)
		if index == 0 {
			start = firstTreePageID
		}
		changes.dirtySegments[index] = newAllocationSegment(changes.base.pageSize, changes.base.segmentPageID(index), start)
	}
	changes.segmentCount += count
	for index := first; index < first+count; index++ {
		changes.markAllocated(changes.base.segmentPageID(index))
	}
}

// highestAllocated returns the highest page ID whose allocation bit is set.
// It searches backward because tail reclamation needs only the final allocated page.
func (changes *allocationChanges) highestAllocated() page.ID {
	pagesPerSegment := changes.base.pagesPerSegment()
	for index := changes.segmentCount - 1; index >= 0; index-- {
		firstPage := page.ID(index) * pagesPerSegment
		if bit, found := highestSetBit(changes.segment(index).bitmapBits()); found {
			return firstPage + page.ID(bit)
		}
	}
	return page.Meta1ID
}

// dropEmptyTailSegments removes complete tail groups that own only their bitmap frames.
// The first group always remains.
func (changes *allocationChanges) dropEmptyTailSegments() {
	count := changes.base.segmentsPerGroup()
	for changes.segmentCount > count {
		first := changes.segmentCount - count
		if changes.highestAllocated() != changes.base.segmentPageID(first)+page.ID(count)-1 {
			return
		}
		changes.segmentCount -= count
		for index := first; index < first+count; index++ {
			delete(changes.dirtySegments, index)
		}
		if changes.firstFreeSegment > first {
			changes.firstFreeSegment = first
		}
	}
}

// publish makes private segment changes visible at commit.
// A file database must first complete its WAL commit.
// The caller must exclude all other database operations.
func (changes *allocationChanges) publish() *allocationBitmap {
	bitmap := changes.base
	if changes.segmentCount < len(bitmap.segments) {
		bitmap.segments = bitmap.segments[:changes.segmentCount]
	} else {
		for len(bitmap.segments) < changes.segmentCount {
			bitmap.segments = append(bitmap.segments, allocationSegment{})
		}
	}
	for index, segment := range changes.dirtySegments {
		if index < changes.segmentCount {
			bitmap.segments[index] = *segment
		}
	}
	bitmap.firstFreeSegment = changes.firstFreeSegment
	bitmap.spanCount = changes.spanCount
	bitmap.spanCursor = changes.spanCursor
	return bitmap
}

// newAllocationBitmap reserves metadata, the first bitmap group, and the initial tree root.
func newAllocationBitmap(pageSize int64) *allocationBitmap {
	bitmap := &allocationBitmap{pageSize: pageSize, groupSegments: int(page.AllocationSegmentPages(pageSize))}
	bitmap.addSegment()
	for _, pageID := range []page.ID{page.Meta0ID, page.Meta1ID, firstAllocationSegmentID + page.AllocationSegmentPages(pageSize)} {
		bitmap.markAllocated(pageID)
	}
	return bitmap
}

// pagesPerSegment returns the database pages described by one bitmap frame.
func (bitmap *allocationBitmap) pagesPerSegment() page.ID {
	if len(bitmap.segments) != 0 && len(bitmap.segments[0].data) > allocationHeaderSize {
		return page.ID(len(bitmap.segments[0].data)-allocationHeaderSize) * 8
	}
	return page.ID((bitmap.pageSize - allocationHeaderSize) * 8)
}

func (bitmap *allocationBitmap) segmentsPerGroup() int {
	if bitmap.groupSegments != 0 {
		return bitmap.groupSegments
	}
	return int(page.AllocationSegmentPages(bitmap.pageSize))
}

func (bitmap *allocationBitmap) pagesPerGroup() page.ID {
	return bitmap.pagesPerSegment() * page.ID(bitmap.segmentsPerGroup())
}

// segmentIndex returns the allocation-segment index that describes pageID.
// Each bitmap covers the next pagesPerSegment page IDs, regardless of where it is stored.
func (bitmap *allocationBitmap) segmentIndex(pageID page.ID) int {
	pagesCoveredPerBitmap := bitmap.pagesPerSegment()
	return int(pageID / pagesCoveredPerBitmap)
}

// segmentPageID returns the file page that stores bitmap segment index.
// Each group stores its bitmap pages together, then its data pages.
// Only the first group starts after the two database metadata pages.
// With 4 KiB pages, segments 0–16 occupy file pages 2–18.
// Segment 17 starts the next group at file page 554880.
func (bitmap *allocationBitmap) segmentPageID(index int) page.ID {
	bitmapsPerGroup := bitmap.segmentsPerGroup()
	groupIndex := index / bitmapsPerGroup
	bitmapIndexInGroup := index % bitmapsPerGroup

	firstBitmapPage := page.ID(groupIndex) * bitmap.pagesPerGroup()
	if groupIndex == 0 {
		firstBitmapPage = firstAllocationSegmentID
	}
	return firstBitmapPage + page.ID(bitmapIndexInGroup)
}

// addSegment appends one complete group of independent bitmap frames.
func (bitmap *allocationBitmap) addSegment() {
	first, count := len(bitmap.segments), bitmap.segmentsPerGroup()
	for index := first; index < first+count; index++ {
		start := page.ID(0)
		if index == 0 {
			start = firstTreePageID
		}
		bitmap.segments = append(bitmap.segments, *newAllocationSegment(bitmap.pageSize, bitmap.segmentPageID(index), start))
	}
	for index := first; index < first+count; index++ {
		bitmap.markAllocated(bitmap.segmentPageID(index))
	}
}

func (bitmap *allocationBitmap) writableSegment(index int) *allocationSegment {
	return &bitmap.segments[index]
}

func (bitmap *allocationBitmap) allocated(pageID page.ID) bool {
	index := bitmap.segmentIndex(pageID)
	if index >= len(bitmap.segments) {
		return false
	}
	bit := pageID % bitmap.pagesPerSegment()
	return allocationBitIsSet(bitmap.segments[index].bitmapBits(), bit)
}

func allocationBitMask(bit page.ID) byte {
	return 1 << uint(bit%8)
}

func allocationBitIsSet(data []byte, bit page.ID) bool {
	return data[bit/8]&allocationBitMask(bit) != 0
}

func setAllocationBit(data []byte, bit page.ID) {
	data[bit/8] |= allocationBitMask(bit)
}

func clearAllocationBit(data []byte, bit page.ID) {
	data[bit/8] &^= allocationBitMask(bit)
}

// firstClearBit returns the first zero bit at or after start.
func firstClearBit(value byte, start uint) (page.ID, bool) {
	// Invert the used bits. Then clear each candidate before start.
	candidates := ^value & (^byte(0) << start)
	if candidates == 0 {
		return 0, false
	}
	return page.ID(bits.TrailingZeros8(candidates)), true
}

// markAllocated sets one committed allocation bit and advances both free-page cursors when needed.
func (bitmap *allocationBitmap) markAllocated(pageID page.ID) {
	index := bitmap.segmentIndex(pageID)
	if index >= len(bitmap.segments) || bitmap.allocated(pageID) {
		return
	}
	segment := bitmap.writableSegment(index)
	bit := pageID % bitmap.pagesPerSegment()
	setAllocationBit(segment.bitmapBits(), bit)
	if bit == segment.firstFreeBit {
		segment.firstFreeBit = bitmap.findFirstFreeBit(index, bit+1)
	}
	if index == bitmap.firstFreeSegment && bitmap.segmentFull(index) {
		bitmap.advanceFirstFree()
	}
}

func (bitmap *allocationBitmap) segmentFull(index int) bool {
	return bitmap.segments[index].firstFreeBit >= bitmap.pagesPerSegment()
}

func (bitmap *allocationBitmap) findFirstFreeBit(index int, start page.ID) page.ID {
	// Segment zero also describes metadata and its allocation span. Their bits stay set.
	if index == 0 && start < firstTreePageID {
		start = firstTreePageID
	}
	pagesPerSegment := bitmap.pagesPerSegment()
	if start >= pagesPerSegment {
		return pagesPerSegment
	}

	data := bitmap.segments[index].bitmapBits()
	byteIndex := int(start / 8)
	bitOffset := uint(start % 8)
	if bit, found := firstClearBit(data[byteIndex], bitOffset); found {
		return min(page.ID(byteIndex*8)+bit, pagesPerSegment)
	}
	for byteIndex++; byteIndex < len(data); byteIndex++ {
		if bit, found := firstClearBit(data[byteIndex], 0); found {
			return min(page.ID(byteIndex*8)+bit, pagesPerSegment)
		}
	}
	return pagesPerSegment
}

func (bitmap *allocationBitmap) advanceFirstFree() {
	for bitmap.firstFreeSegment < len(bitmap.segments) && bitmap.segmentFull(bitmap.firstFreeSegment) {
		bitmap.firstFreeSegment++
	}
}

func (bitmap *allocationBitmap) isSegmentPage(pageID page.ID) bool {
	return isAllocationSegmentPage(pageID, bitmap.pagesPerSegment(), page.ID(len(bitmap.segments)-1), page.ID(bitmap.segmentsPerGroup()))
}

func (bitmap *allocationBitmap) isSegmentHead(pageID page.ID) bool {
	return bitmap.isSegmentPage(pageID)
}

// isAllocationSegmentPage reports whether pageID stores a bitmap frame in an existing group.
func isAllocationSegmentPage(pageID, pagesPerSegment, lastSegment, span page.ID) bool {
	if span == 0 || pagesPerSegment == 0 {
		return false
	}
	groupPages := pagesPerSegment * span
	group := pageID / groupPages
	if group >= (lastSegment+1)/span {
		return false
	}
	head := group * groupPages
	if group == 0 {
		head = firstAllocationSegmentID
	}
	return pageID >= head && pageID-head < span
}

// highestSetBit returns the index of the last set bit.
// Whole words let it skip empty trailing bytes with fewer loads.
func highestSetBit(data []byte) (int, bool) {
	end := len(data)
	for end >= 8 {
		start := end - 8
		if value := binary.LittleEndian.Uint64(data[start:end]); value != 0 {
			return start*8 + bits.Len64(value) - 1, true
		}
		end = start
	}
	for index := end - 1; index >= 0; index-- {
		if value := data[index]; value != 0 {
			return index*8 + bits.Len8(value) - 1, true
		}
	}
	return 0, false
}

// highestAllocated returns the highest page ID whose allocation bit is set.
func (bitmap *allocationBitmap) highestAllocated() page.ID {
	pagesPerSegment := bitmap.pagesPerSegment()
	for index := len(bitmap.segments) - 1; index >= 0; index-- {
		firstPage := page.ID(index) * pagesPerSegment
		if bit, found := highestSetBit(bitmap.segments[index].bitmapBits()); found {
			return firstPage + page.ID(bit)
		}
	}
	return page.Meta1ID
}

// ownsNodeSpan checks all allocation bits without a page-by-page ID walk.
// A node must stay within one allocation region and exclude its bitmap span.
func (bitmap *allocationBitmap) ownsNodeSpan(head page.ID, count int) bool {
	if count <= 0 || head < firstTreePageID || page.ID(count)-1 > ^page.ID(0)-head {
		return false
	}
	end := head + page.ID(count) - 1
	capacity := bitmap.pagesPerSegment()
	index := int(head / capacity)
	bit := head % capacity
	if index >= len(bitmap.segments) || isAllocationSegmentPage(head, capacity, page.ID(len(bitmap.segments)-1), page.ID(bitmap.segmentsPerGroup())) {
		return false
	}
	if count == 1 {
		return allocationBitIsSet(bitmap.segments[index].bitmapBits(), bit)
	}
	// A range inside one logical frame cannot cross a physical group.
	if page.ID(count) <= capacity-bit {
		return allocationRangeIsSet(bitmap.segments[index].bitmapBits(), bit, count)
	}
	if head/bitmap.pagesPerGroup() != end/bitmap.pagesPerGroup() {
		return false
	}
	for count > 0 {
		index := int(head / capacity)
		if index >= len(bitmap.segments) {
			return false
		}
		bit := head % capacity
		n := min(count, int(capacity-bit))
		if !allocationRangeIsSet(bitmap.segments[index].bitmapBits(), bit, n) {
			return false
		}
		head += page.ID(n)
		count -= n
	}
	return true
}

// allocationRangeIsSet checks a bounded run of bits in one bitmap frame.
func allocationRangeIsSet(data []byte, bit page.ID, count int) bool {
	at, shift := int(bit/8), uint(bit%8)
	if shift != 0 {
		n := min(count, 8-int(shift))
		mask := byte(((1 << n) - 1) << shift)
		if data[at]&mask != mask {
			return false
		}
		count -= n
		at++
	}
	for count >= 64 {
		if binary.LittleEndian.Uint64(data[at:at+8]) != ^uint64(0) {
			return false
		}
		at += 8
		count -= 64
	}
	for count >= 8 {
		if data[at] != 0xff {
			return false
		}
		at++
		count -= 8
	}
	if count > 0 {
		mask := byte((1 << count) - 1)
		if data[at]&mask != mask {
			return false
		}
	}
	return true
}

// allocatedPageCount returns the number of set bits across all allocation segments.
func (bitmap *allocationBitmap) allocatedPageCount() uint64 {
	var count uint64
	for index := range bitmap.segments {
		for _, value := range bitmap.segments[index].bitmapBits() {
			count += uint64(bits.OnesCount8(value))
		}
	}
	return count
}

// validate checks the allocation layout against metadata and rebuilds the memory-only free-page cursors.
// It checks segment ownership, reserved pages, the root page, and the highest allocated page. It does not walk the tree.
func (bitmap *allocationBitmap) validate(meta *page.Meta) error {
	if len(bitmap.segments) == 0 || bitmap.pagesPerSegment() == 0 {
		return fmt.Errorf("allocation bitmap has no segments: %w", page.ErrInvalid)
	}
	wantSegments := int(meta.LastPage()/bitmap.pagesPerGroup()+1) * bitmap.segmentsPerGroup()
	if len(bitmap.segments) != wantSegments {
		return fmt.Errorf("allocation bitmap has %d segments, want %d: %w", len(bitmap.segments), wantSegments, page.ErrInvalid)
	}
	for index := range bitmap.segments {
		segmentPageID := bitmap.segmentPageID(index)
		if !bitmap.allocated(segmentPageID) {
			return fmt.Errorf("allocation segment page %d is free: %w", segmentPageID, page.ErrInvalid)
		}
		bitmap.segments[index].firstFreeBit = bitmap.findFirstFreeBit(index, 0)
	}
	if meta.Root() < firstTreePageID || bitmap.isSegmentPage(meta.Root()) {
		return fmt.Errorf("root uses a reserved page: %w", page.ErrInvalid)
	}
	for _, pageID := range []page.ID{page.Meta0ID, page.Meta1ID, meta.Root()} {
		if !bitmap.allocated(pageID) {
			return fmt.Errorf("allocation bitmap marks live page %d free: %w", pageID, page.ErrInvalid)
		}
	}
	if highest := bitmap.highestAllocated(); highest != meta.LastPage() {
		return fmt.Errorf("allocation high page %d does not match metadata page %d: %w", highest, meta.LastPage(), page.ErrInvalid)
	}
	bitmap.firstFreeSegment = 0
	bitmap.advanceFirstFree()
	return nil
}
