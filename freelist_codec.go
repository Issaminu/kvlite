package kvlite

import (
	"encoding/binary"
	"fmt"

	"github.com/Issaminu/kvlite/internal/checksum"
	"github.com/Issaminu/kvlite/internal/page"
)

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
