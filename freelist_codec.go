package kvlite

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"slices"

	"github.com/Issaminu/kvlite/internal/checksum"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

// pageRecords returns one full image per changed allocation segment after finalization.
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

// encodeSegment refreshes the checksum and returns one owned bitmap page.
// The returned slice aliases the bitmap and must not be changed by the caller.
func (bitmap *allocationBitmap) encodeSegment(index int) []byte {
	return allocationSegmentImage(&bitmap.segments[index])
}

// newAllocationSegment creates one zeroed bitmap page and its free-bit cursor.
func newAllocationSegment(pageSize int64, pageID page.ID, firstFreeBit page.ID) *allocationSegment {
	data := encodeAllocationSegment(pageSize, pageID, nil)
	return &allocationSegment{
		data:         data,
		firstFreeBit: firstFreeBit,
	}
}

// allocationSegmentImage refreshes the checksum and returns one owned bitmap page.
// The returned slice aliases the segment and must not be changed by the caller.
func allocationSegmentImage(segment *allocationSegment) []byte {
	sealAllocationPage(segment.data)
	return segment.data
}

// encodeAllocationSegment returns one complete bitmap page for pageID.
// Extra bitmap bytes are discarded when bits exceeds the page payload.
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
// A committed WAL image, changed by the byte patches after it, replaces the main-file image of the same page.
// It reads every bitmap frame in the allocated groups and does not scan tree pages.
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
	for _, record := range records {
		if isWALPageRecord(record.Header.Type) {
			last := record.Header.PageID
			if record.Header.Type == wal.RecordTypeAllocation || record.Header.Type == wal.RecordTypeNode {
				count := (int64(len(record.Payload)) + meta.PageSize() - 1) / meta.PageSize()
				if count > 0 && page.ID(count)-1 <= ^page.ID(0)-last {
					last += page.ID(count) - 1
				}
			}
			availableLastPage = max(availableLastPage, last)
		}
	}
	chains := walPageChains(records)
	if meta.LastPage() > availableLastPage {
		return nil, fmt.Errorf("metadata last page %d exceeds main-file and WAL page IDs: %w", meta.LastPage(), page.ErrInvalid)
	}

	bitmap := &allocationBitmap{pageSize: meta.PageSize(), groupSegments: int(page.AllocationSegmentPages(meta.PageSize()))}
	segmentCount := int(meta.LastPage()/bitmap.pagesPerGroup()+1) * bitmap.segmentsPerGroup()
	bitmap.segments = make([]allocationSegment, segmentCount)
	var groupData []byte
	for index := range bitmap.segments {
		pageID := bitmap.segmentPageID(index)
		if index%bitmap.segmentsPerGroup() == 0 {
			groupData = nil
			overlay := false
			for frame := index; frame < index+bitmap.segmentsPerGroup(); frame++ {
				if _, found := chains[bitmap.segmentPageID(frame)]; found {
					overlay = true
					break
				}
			}
			if !overlay {
				groupData = make([]byte, int64(bitmap.segmentsPerGroup())*meta.PageSize())
				if err := fileio.ReadFull(io.NewSectionReader(db.file, int64(pageID)*meta.PageSize(), int64(len(groupData))), groupData); err != nil {
					return nil, fmt.Errorf("read allocation group at page %d: %w", pageID, err)
				}
			}
		}
		var data []byte
		if chain, inWAL := chains[pageID]; inWAL {
			record, err := db.materializeWALPage(records, chain)
			if err != nil {
				return nil, err
			}
			if record.Header.Type != wal.RecordTypeAllocation {
				return nil, fmt.Errorf("WAL record type %d for allocation page %d: %w", record.Header.Type, pageID, page.ErrInvalid)
			}
			data = record.Payload
		} else if groupData != nil {
			start := int64(index%bitmap.segmentsPerGroup()) * meta.PageSize()
			end := start + meta.PageSize()
			data = groupData[start:end:end]
		} else {
			data = make([]byte, meta.PageSize())
			err = fileio.ReadFull(io.NewSectionReader(db.file, int64(pageID)*meta.PageSize(), int64(len(data))), data)
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

// verifyAllocationPage checks one page's exact size, checksum, magic, and physical ID.
func verifyAllocationPage(data []byte, pageSize int64, pageID page.ID) error {
	if int64(len(data)) != pageSize || len(data) < allocationHeaderSize {
		return fmt.Errorf("decode allocation page size: %w", page.ErrInvalid)
	}
	storedChecksum := binary.LittleEndian.Uint32(data[allocationChecksumOffset : allocationChecksumOffset+allocationChecksumSize])
	if storedChecksum != checksum.Sum32(data[:allocationChecksumOffset], data[allocationPageIDOffset:]) {
		return fmt.Errorf("verify allocation page: %w", page.ErrChecksum)
	}
	magic := binary.LittleEndian.Uint32(data[allocationMagicOffset : allocationMagicOffset+allocationMagicSize])
	if magic != allocationSegmentMagic {
		return fmt.Errorf("decode allocation page magic: %w", page.ErrInvalid)
	}
	storedPageID := page.ID(binary.LittleEndian.Uint64(data[allocationPageIDOffset : allocationPageIDOffset+allocationPageIDSize]))
	if storedPageID != pageID {
		return fmt.Errorf("allocation page stores page %d, want %d: %w", storedPageID, pageID, page.ErrInvalid)
	}
	return nil
}
