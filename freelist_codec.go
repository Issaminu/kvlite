package kvlite

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/Issaminu/kvlite/internal/checksum"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

// pageRecords returns the changed allocation pages that remain after finalization.
// Each payload aliases a private segment. The caller must not change it after commit.
func (changes *allocationChanges) pageRecords() []wal.AllocationRecord {
	indexes := make([]int, 0, len(changes.dirtySegments))
	for index := range changes.dirtySegments {
		if index < changes.segmentCount {
			indexes = append(indexes, index)
		}
	}
	slices.Sort(indexes)

	records := make([]wal.AllocationRecord, 0, len(indexes))
	for _, index := range indexes {
		final := allocationSegmentImage(changes.segment(index))
		// The committed page can have an old checksum in memory. Compare its bits
		// without changing the committed page or treating a checksum update as a change.
		if index < len(changes.base.segments) && bytes.Equal(final[allocationHeaderSize:], changes.base.segments[index].bitmapBits()) {
			continue
		}
		records = append(records, wal.AllocationRecord{PageID: changes.base.segmentPageID(index), Payload: final})
	}
	return records
}

// encodeSegment refreshes the checksum and returns the segment's owned page data.
// The returned slice aliases the bitmap and must not be changed by the caller.
func (bitmap *allocationBitmap) encodeSegment(index int) []byte {
	return allocationSegmentImage(&bitmap.segments[index])
}

// newAllocationSegment creates one zeroed encoded page and sets its memory-only free-bit cursor.
func newAllocationSegment(pageSize int64, pageID page.ID, firstFreeBit page.ID) *allocationSegment {
	data := encodeAllocationSegment(pageSize, pageID, nil)
	return &allocationSegment{
		data:         data,
		firstFreeBit: firstFreeBit,
	}
}

// allocationSegmentImage refreshes the checksum and returns the segment's owned page data.
// The returned slice aliases the segment and must not be changed by the caller.
func allocationSegmentImage(segment *allocationSegment) []byte {
	sealAllocationPage(segment.data)
	return segment.data
}

// encodeAllocationSegment returns one complete allocation page for pageID.
// Extra bitmap bytes are discarded when bits is larger than the page payload.
func encodeAllocationSegment(pageSize int64, pageID page.ID, bits []byte) []byte {
	data := make([]byte, pageSize)
	binary.LittleEndian.PutUint32(data[allocationMagicOffset:allocationMagicOffset+allocationMagicSize], allocationSegmentMagic)
	binary.LittleEndian.PutUint64(data[allocationPageIDOffset:allocationPageIDOffset+allocationPageIDSize], uint64(pageID))
	copy(data[allocationHeaderSize:], bits)
	sealAllocationPage(data)
	return data
}

// decodeAllocationSegment validates data and transfers ownership of it to the returned segment.
// The caller must not change or reuse data after a successful call.
func decodeAllocationSegment(data []byte, pageSize int64, pageID page.ID) (allocationSegment, error) {
	if err := verifyAllocationPage(data, pageSize, pageID); err != nil {
		return allocationSegment{}, err
	}
	return allocationSegment{
		data: data,
	}, nil
}

// readAllocationBitmap reads the allocation pages named by meta.
// A committed WAL image replaces the main-file image of the same page.
// It reads one page per segment and does not scan tree pages.
// The records must contain only committed WAL records in file order.
// The caller must not change records or their payloads while it uses the returned bitmap.
func (db *DB) readAllocationBitmap(meta *page.Meta, records []wal.WALRecord) (*allocationBitmap, error) {
	if err := validateMeta(meta); err != nil {
		return nil, err
	}
	info, err := db.file.Stat()
	if err != nil {
		return nil, err
	}
	mainPages := uint64(info.Size() / meta.PageSize())
	var availableLastPage page.ID
	if mainPages != 0 {
		availableLastPage = page.ID(mainPages - 1)
	}
	var latest map[page.ID][]byte
	for _, record := range records {
		switch record.Header.Type {
		case wal.RecordTypeNode, wal.RecordTypeAllocation:
			availableLastPage = max(availableLastPage, record.Header.PageID)
		}
		if record.Header.Type == wal.RecordTypeAllocation {
			if latest == nil {
				latest = make(map[page.ID][]byte)
			}
			latest[record.Header.PageID] = record.Payload
		}
	}
	if meta.LastPage() > availableLastPage {
		return nil, fmt.Errorf("metadata last page %d exceeds main-file and WAL page IDs: %w", meta.LastPage(), ErrInvalid)
	}

	bitmap := &allocationBitmap{pageSize: meta.PageSize()}
	segmentCount := bitmap.segmentIndex(meta.LastPage()) + 1
	bitmap.segments = make([]allocationSegment, segmentCount)
	for index := range bitmap.segments {
		pageID := bitmap.segmentPageID(index)
		data, inWAL := latest[pageID]
		if !inWAL {
			data, err = db.readMainPage(pageID)
			if err != nil {
				return nil, fmt.Errorf("read allocation page %d: %w", pageID, err)
			}
		}
		segment, err := decodeAllocationSegment(data, bitmap.pageSize, pageID)
		if err != nil {
			return nil, fmt.Errorf("decode allocation page %d: %w", pageID, err)
		}
		bitmap.segments[index] = segment
	}
	if err := bitmap.validate(meta); err != nil {
		return nil, err
	}
	return bitmap, nil
}

// sealAllocationPage stores a CRC32C checksum over the header fields and bitmap.
// The checksum bytes are excluded from the checksum input.
func sealAllocationPage(data []byte) {
	sum := checksum.Sum32(data[:allocationChecksumOffset], data[allocationPageIDOffset:])
	binary.LittleEndian.PutUint32(data[allocationChecksumOffset:allocationChecksumOffset+allocationChecksumSize], sum)
}

// verifyAllocationPage checks the exact page size, checksum, magic value, and expected physical page ID.
func verifyAllocationPage(data []byte, pageSize int64, pageID page.ID) error {
	if int64(len(data)) != pageSize || len(data) < allocationHeaderSize {
		return fmt.Errorf("decode allocation page size: %w", ErrInvalid)
	}
	storedChecksum := binary.LittleEndian.Uint32(data[allocationChecksumOffset : allocationChecksumOffset+allocationChecksumSize])
	if storedChecksum != checksum.Sum32(data[:allocationChecksumOffset], data[allocationPageIDOffset:]) {
		return fmt.Errorf("verify allocation page: %w", ErrChecksum)
	}
	magic := binary.LittleEndian.Uint32(data[allocationMagicOffset : allocationMagicOffset+allocationMagicSize])
	if magic != allocationSegmentMagic {
		return fmt.Errorf("decode allocation page magic: %w", ErrInvalid)
	}
	storedPageID := page.ID(binary.LittleEndian.Uint64(data[allocationPageIDOffset : allocationPageIDOffset+allocationPageIDSize]))
	if storedPageID != pageID {
		return fmt.Errorf("allocation page stores page %d, want %d: %w", storedPageID, pageID, ErrInvalid)
	}
	return nil
}
