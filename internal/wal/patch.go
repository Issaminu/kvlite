package wal

import (
	"encoding/binary"
	"fmt"

	"github.com/Issaminu/kvlite/internal/page"
)

// Each encoded range starts with a four-byte offset and a four-byte size.
const pagePatchRangeHeaderSize = 8

// PagePatch contains the encoded byte ranges that change one existing data node.
// One [RecordTypePatch] record contains one PagePatch. The record header identifies the page.
// A patch does not change the node image size and does not contain page padding.
//
// Each range in the record payload has this layout:
//
//	Offset  Size  Field
//	0       4     Absolute offset in the compact node image as a uint32
//	4       4     Replacement byte count as a uint32
//	8       size  Final bytes to write
//
// All integers use little-endian byte order. The payload has no range count.
// Recovery reads ranges until it reaches the payload size in the WAL record header.
// Ranges must be ordered, must not overlap, and must fit in the compact base image.
//
// Recovery copies the base image and writes each replacement range into the copy.
// Bytes outside the ranges stay unchanged. The base comes from an earlier WAL record or the main database file.
// The zero value is ready to use.
type PagePatch struct {
	ranges []byte // Encoded range headers and replacement bytes in record order.
}

// reset removes the previous ranges and keeps their storage for the next node.
func (patch *PagePatch) reset() {
	patch.ranges = patch.ranges[:0]
}

// appendChangedRanges compares one same-size field and appends the ranges that change base into target.
// The field can be a leaf value, a branch separator key, or a child page ID.
// base and target must have the same length.
// baseOffset is the absolute offset of that field in the compact node image.
// Existing ranges remain in patch.
func (patch *PagePatch) appendChangedRanges(baseOffset int, base, target []byte) {
	patch.ranges = appendChangedPagePatchRanges(patch.ranges, baseOffset, base, target)
}

// encodedSize returns the number of bytes used by all range headers and replacement data.
func (patch *PagePatch) encodedSize() int {
	return len(patch.ranges)
}

// appendEncoded appends the complete patch payload to data.
func (patch *PagePatch) appendEncoded(data []byte) []byte {
	return append(data, patch.ranges...)
}

// appendChangedPagePatchRanges appends encoded replacement ranges for one same-size field.
// base and target must have the same length.
// Here's how it works:
// 1. Skip bytes that are equal.
// 2. Mark the first different byte.
// 3. Continue until a sufficiently long equal section appears.
// 4. Store one replacement range.
// 5. Repeat until the target ends.
func appendChangedPagePatchRanges(patch []byte, baseOffset int, base, target []byte) []byte {

	// Each outer-loop pass emits one replacement range.
	for index := 0; index < len(target); {
		// Skip bytes that already match.
		// When this loop stops, index is at either:
		//   1. the next different byte, or
		//   2. the end of target.
		for index < len(target) && index < len(base) && base[index] == target[index] {
			index++
		}
		if index == len(target) {
			break
		}

		// The current byte differs.
		// Save its position as the start of the next replacement range.
		start := index

		// Now find where this replacement range should end.
		for index < len(target) {
			// A different byte belongs to this replacement range.
			// index >= len(base) is defensive. Current callers pass same-length base and target fields.
			if index >= len(base) || base[index] != target[index] {
				index++
				continue
			}

			// We found equal bytes after at least one different byte.
			// Save the start and scan the complete equal run.
			equalStart := index
			for index < len(target) && index < len(base) && base[index] == target[index] {
				index++
			}

			// Every new range needs an eight-byte header.
			//
			// If this equal run is longer than eight bytes, excluding it is smaller than keeping it in the current range.
			// Rewind to the start of the equal run and finish the current range.
			//
			// The next outer-loop pass will skip this equal run.

			if index-equalStart > pagePatchRangeHeaderSize {
				index = equalStart
				break
			}
		}

		// Store an absolute node offset, the replacement size, and the final bytes.
		patch = binary.LittleEndian.AppendUint32(patch, uint32(baseOffset+start))
		patch = binary.LittleEndian.AppendUint32(patch, uint32(index-start))
		patch = append(patch, target[start:index]...)
	}
	return patch
}

// ApplyPagePatch applies an encoded patch to a new same-size copy of base.
// It does not change or retain base or patch data.
//
// An empty patch returns an unchanged copy of base. Bytes outside replacement ranges also come from base.
// ApplyPagePatch validates the base size, range headers, range order, and range bounds.
// It returns an error that wraps [page.ErrInvalid] when the patch is invalid.
// It does not validate the rebuilt node format or checksum.
func ApplyPagePatch(base, patch []byte, pageSize int64) ([]byte, error) {
	if int64(len(base)) > pageSize {
		return nil, fmt.Errorf("read page patch base size %d: %w", len(base), page.ErrInvalid)
	}
	decoded := PagePatch{ranges: patch}
	return decoded.apply(base)
}

// apply parses the encoded ranges and writes them into a new copy of base.
func (patch *PagePatch) apply(base []byte) ([]byte, error) {
	// Start with the base so bytes outside the encoded ranges stay unchanged.
	target := make([]byte, len(base))
	copy(target, base)

	ranges := patch.ranges
	// previousEnd rejects ranges that overlap or arrive out of order.
	previousEnd := uint64(0)
	for len(ranges) > 0 {
		if len(ranges) < pagePatchRangeHeaderSize {
			return nil, fmt.Errorf("read page patch range header: %w", page.ErrInvalid)
		}
		rangeOffset := uint64(binary.LittleEndian.Uint32(ranges[:4]))
		rangeSize := uint64(binary.LittleEndian.Uint32(ranges[4:8]))
		// Remove the header so ranges starts with this range's replacement data.
		ranges = ranges[pagePatchRangeHeaderSize:]
		rangeEnd := rangeOffset + rangeSize
		empty := rangeSize == 0
		overlapsPrevious := rangeOffset < previousEnd
		outsideBase := rangeEnd > uint64(len(target))
		missingData := rangeSize > uint64(len(ranges))
		if empty || overlapsPrevious || outsideBase || missingData {
			return nil, fmt.Errorf("read page patch range: %w", page.ErrInvalid)
		}
		copy(target[int(rangeOffset):int(rangeEnd)], ranges[:int(rangeSize)])
		// Remove the replacement data so ranges starts with the next range header.
		ranges = ranges[int(rangeSize):]
		previousEnd = rangeEnd
	}
	return target, nil
}
