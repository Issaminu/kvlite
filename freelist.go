package kvlite

import (
	"fmt"
	"maps"
	"math/bits"
	"slices"

	"github.com/Issaminu/kvlite/internal/page"
)

const (
	// The two metadata pages are followed by the first allocation page and the first tree page.
	firstAllocationSegmentID page.ID = page.Meta1ID + 1
	firstTreePageID          page.ID = firstAllocationSegmentID + 1

	allocationSegmentMagic uint32 = 0x5246564B // The little-endian bytes spell "KVFR".
)

// An allocation segment uses one database page. Its header has this layout:
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

// allocationSegment is one encoded allocation page and its memory-only free-page cursor.
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
	segments         []allocationSegment
	firstFreeSegment int
}

// allocationChanges overlays one committed bitmap with uncommitted allocation work.
// A missing dirty segment is read from base. A dirty segment is a private complete page.
// Retired pages remain allocated until finalize prepares the commit.
type allocationChanges struct {
	base             *allocationBitmap          // committed read-only segments
	dirtySegments    map[int]*allocationSegment // private segments, keyed by segment index
	segmentCount     int                        // private count after segment additions or removals
	firstFreeSegment int                        // private search cursor
	retired          map[page.ID]struct{}       // tree pages that become free during commit preparation
	obsoleteWALPages map[page.ID]struct{}       // allocation pages removed from the file tail
}

// newAllocationChanges starts an empty transaction overlay on bitmap.
// It does not copy an allocation page until the transaction changes that page.
func newAllocationChanges(bitmap *allocationBitmap) *allocationChanges {
	return &allocationChanges{
		base:             bitmap,
		segmentCount:     len(bitmap.segments),
		firstFreeSegment: bitmap.firstFreeSegment,
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
		retired:          maps.Clone(changes.retired),
		obsoleteWALPages: maps.Clone(changes.obsoleteWALPages),
	}
}

func cloneAllocationSegment(segment *allocationSegment) *allocationSegment {
	return &allocationSegment{
		data:         slices.Clone(segment.data),
		firstFreeBit: segment.firstFreeBit,
	}
}

// allocate returns the lowest reusable tree page at or below lastPage.
// It extends the logical file when no reusable page exists. If the extension starts a new segment, that first page stores the segment and the following page is returned.
func (changes *allocationChanges) allocate(lastPage page.ID) (page.ID, error) {
	pagesPerSegment := changes.base.pagesPerSegment()
	for index := changes.firstFreeSegment; index < changes.segmentCount; index++ {
		segment := changes.segment(index)
		firstPage := page.ID(index) * pagesPerSegment
		candidate := firstPage + segment.firstFreeBit
		// Clear bits after lastPage describe pages that are not in the logical file. They are not reusable pages.
		if candidate >= firstTreePageID && candidate <= lastPage {
			changes.markAllocated(candidate)
			return candidate, nil
		}
	}

	if lastPage == ^page.ID(0) {
		return 0, fmt.Errorf("allocate after page %d: %w", lastPage, ErrInvalid)
	}
	nextPage := lastPage + 1
	segmentIndex := changes.base.segmentIndex(nextPage)
	if segmentIndex == changes.segmentCount {
		// The first page in each later range stores the allocation segment for that range.
		changes.addSegment()
		nextPage++
		segmentIndex = changes.base.segmentIndex(nextPage)
	}
	if segmentIndex >= changes.segmentCount {
		return 0, fmt.Errorf("allocation segment %d is not present: %w", segmentIndex, ErrInvalid)
	}
	changes.markAllocated(nextPage)
	return nextPage, nil
}

// retire delays reuse of an allocated tree page until finalize prepares the commit.
// It rejects reserved pages, free pages, allocation pages, and repeated retirement.
func (changes *allocationChanges) retire(pageID page.ID) error {
	if pageID < firstTreePageID || changes.isSegmentPage(pageID) {
		return fmt.Errorf("retire reserved page %d: %w", pageID, ErrInvalid)
	}
	if !changes.allocated(pageID) {
		return fmt.Errorf("retire free page %d: %w", pageID, ErrInvalid)
	}
	if changes.retired == nil {
		changes.retired = make(map[page.ID]struct{})
	}
	if _, exists := changes.retired[pageID]; exists {
		return fmt.Errorf("retire page %d twice: %w", pageID, ErrInvalid)
	}
	changes.retired[pageID] = struct{}{}
	return nil
}

func (changes *allocationChanges) changed() bool {
	return len(changes.dirtySegments) != 0 || len(changes.retired) != 0
}

// finalize releases retired pages and lowers metadata when the allocated tail shrinks.
// It returns true when it changes metadata's last page ID.
func (changes *allocationChanges) finalize(meta *page.Meta) bool {
	for pageID := range changes.retired {
		changes.release(pageID)
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
	meta.SetLastPage(highest)
	return true
}

// walRetiredPages names tree and allocation pages that must leave the WAL view at commit.
// A removed tail segment can still have an older WAL image even though it is no longer in the bitmap.
func (changes *allocationChanges) walRetiredPages() map[page.ID]struct{} {
	if len(changes.obsoleteWALPages) == 0 {
		return changes.retired
	}
	retired := maps.Clone(changes.retired)
	if retired == nil {
		retired = make(map[page.ID]struct{}, len(changes.obsoleteWALPages))
	}
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
// The first write copies the segment visible through segment.
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
	return isAllocationSegmentPage(pageID, changes.base.pagesPerSegment(), page.ID(changes.segmentCount-1))
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
	if bit < segment.firstFreeBit {
		segment.firstFreeBit = bit
	}
	if index < changes.firstFreeSegment {
		changes.firstFreeSegment = index
	}
}

func (changes *allocationChanges) segmentFull(index int) bool {
	return changes.segment(index).firstFreeBit >= changes.base.pagesPerSegment()
}

func (changes *allocationChanges) findFirstFreeBit(index int, start page.ID) page.ID {
	// Segment zero also describes the reserved metadata and allocation pages. A tree allocation starts at page 3.
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

// addSegment creates a private segment and reserves its own database page.
func (changes *allocationChanges) addSegment() {
	index := changes.segmentCount
	firstFreeBit := page.ID(0)
	if index == 0 {
		firstFreeBit = firstTreePageID
	}
	if changes.dirtySegments == nil {
		changes.dirtySegments = make(map[int]*allocationSegment)
	}
	changes.dirtySegments[index] = newAllocationSegment(changes.base.pageSize, changes.base.segmentPageID(index), firstFreeBit)
	changes.segmentCount++
	changes.markAllocated(changes.base.segmentPageID(index))
}

// highestAllocated returns the highest page ID whose allocation bit is set.
// It searches backward because tail reclamation needs only the final allocated page.
func (changes *allocationChanges) highestAllocated() page.ID {
	pagesPerSegment := changes.base.pagesPerSegment()
	for index := changes.segmentCount - 1; index >= 0; index-- {
		firstPage := page.ID(index) * pagesPerSegment
		data := changes.segment(index).bitmapBits()
		for byteIndex := len(data) - 1; byteIndex >= 0; byteIndex-- {
			value := data[byteIndex]
			if value != 0 {
				bit := page.ID(byteIndex*8 + bits.Len8(value) - 1)
				return firstPage + bit
			}
		}
	}
	return page.Meta1ID
}

// dropEmptyTailSegments removes later segments that contain only their own allocation page.
// The first segment always remains because it describes the reserved pages and the first tree range.
func (changes *allocationChanges) dropEmptyTailSegments() {
	for changes.segmentCount > 1 {
		last := changes.segmentCount - 1
		if changes.highestAllocated() != changes.base.segmentPageID(last) {
			return
		}
		changes.segmentCount--
		delete(changes.dirtySegments, last)
		if changes.firstFreeSegment > last {
			changes.firstFreeSegment = last
		}
	}
}

// publish makes private segment changes visible after the WAL commit succeeds.
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
	return bitmap
}

// newAllocationBitmap creates the first segment and reserves both metadata pages, the segment page, and the first tree page.
func newAllocationBitmap(pageSize int64) *allocationBitmap {
	bitmap := &allocationBitmap{pageSize: pageSize}
	bitmap.addSegment()
	for _, pageID := range []page.ID{page.Meta0ID, page.Meta1ID, firstTreePageID} {
		bitmap.markAllocated(pageID)
	}
	return bitmap
}

// pagesPerSegment returns the number of database pages described by one allocation page.
func (bitmap *allocationBitmap) pagesPerSegment() page.ID {
	return page.ID((bitmap.pageSize - allocationHeaderSize) * 8)
}

// segmentIndex returns the allocation-segment index that describes pageID.
func (bitmap *allocationBitmap) segmentIndex(pageID page.ID) int {
	return int(pageID / bitmap.pagesPerSegment())
}

// segmentPageID returns the database page that stores one allocation segment.
// The first segment follows metadata. Each later segment starts the range that it describes.
func (bitmap *allocationBitmap) segmentPageID(index int) page.ID {
	if index == 0 {
		return firstAllocationSegmentID
	}
	return page.ID(index) * bitmap.pagesPerSegment()
}

// addSegment appends one encoded segment and reserves its own database page.
func (bitmap *allocationBitmap) addSegment() {
	index := len(bitmap.segments)
	firstFreeBit := page.ID(0)
	if index == 0 {
		firstFreeBit = firstTreePageID
	}
	bitmap.segments = append(bitmap.segments, *newAllocationSegment(bitmap.pageSize, bitmap.segmentPageID(index), firstFreeBit))
	bitmap.markAllocated(bitmap.segmentPageID(index))
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
	// Segment zero also describes the reserved metadata and allocation pages. A tree allocation starts at page 3.
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
	return isAllocationSegmentPage(pageID, bitmap.pagesPerSegment(), page.ID(len(bitmap.segments)-1))
}

// isAllocationSegmentPage reports whether pageID stores an existing allocation segment.
// Page 2 is the special first segment. Each later segment is stored at the start of its described range.
func isAllocationSegmentPage(pageID, pagesPerSegment, lastSegment page.ID) bool {
	if pageID == firstAllocationSegmentID {
		return true
	}
	index := pageID / pagesPerSegment
	return index > 0 && index <= lastSegment && pageID%pagesPerSegment == 0
}

// highestAllocated returns the highest page ID whose allocation bit is set.
func (bitmap *allocationBitmap) highestAllocated() page.ID {
	pagesPerSegment := bitmap.pagesPerSegment()
	for index := len(bitmap.segments) - 1; index >= 0; index-- {
		firstPage := page.ID(index) * pagesPerSegment
		data := bitmap.segments[index].bitmapBits()
		for byteIndex := len(data) - 1; byteIndex >= 0; byteIndex-- {
			value := data[byteIndex]
			if value != 0 {
				bit := page.ID(byteIndex*8 + bits.Len8(value) - 1)
				return firstPage + bit
			}
		}
	}
	return page.Meta1ID
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
		return fmt.Errorf("allocation bitmap has no segments: %w", ErrInvalid)
	}
	wantSegments := bitmap.segmentIndex(meta.LastPage()) + 1
	if len(bitmap.segments) != wantSegments {
		return fmt.Errorf("allocation bitmap has %d segments, want %d: %w", len(bitmap.segments), wantSegments, ErrInvalid)
	}
	for index := range bitmap.segments {
		segmentPageID := bitmap.segmentPageID(index)
		if !bitmap.allocated(segmentPageID) {
			return fmt.Errorf("allocation segment page %d is free: %w", segmentPageID, ErrInvalid)
		}
		bitmap.segments[index].firstFreeBit = bitmap.findFirstFreeBit(index, 0)
	}
	for _, pageID := range []page.ID{page.Meta0ID, page.Meta1ID, meta.Root()} {
		if !bitmap.allocated(pageID) {
			return fmt.Errorf("allocation bitmap marks live page %d free: %w", pageID, ErrInvalid)
		}
	}
	if highest := bitmap.highestAllocated(); highest != meta.LastPage() {
		return fmt.Errorf("allocation high page %d does not match metadata page %d: %w", highest, meta.LastPage(), ErrInvalid)
	}
	bitmap.firstFreeSegment = 0
	bitmap.advanceFirstFree()
	return nil
}
