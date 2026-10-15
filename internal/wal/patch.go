package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"

	"github.com/Issaminu/kvlite/internal/btree"
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
//
// Both scans compare eight bytes in each step. A range ends before the first
// equal section that is longer than one range header, or at the end of target.
// Shorter equal sections stay in the range, because a new range header costs more.
func appendChangedPagePatchRanges(patch []byte, baseOffset int, base, target []byte) []byte {
	// Bytes after the end of base count as different. Current callers pass same-length fields.
	compared := min(len(base), len(target))
	base, comparedTarget := base[:compared], target[:compared]

	// Each outer-loop pass emits one replacement range.
	for index := 0; index < len(target); {
		// Skip bytes that already match.
		index = nextDifferentByte(base, comparedTarget, index)
		if index == len(target) {
			break
		}

		// The current byte differs. It is the start of the next replacement range.
		// The range ends before the next equal section that is longer than a range header.
		// The next outer-loop pass skips that equal section.
		start := index
		index = nextLongEqualRun(base, comparedTarget, start, len(target))

		// Store an absolute node offset, the replacement size, and the final bytes.
		patch = binary.LittleEndian.AppendUint32(patch, uint32(baseOffset+start))
		patch = binary.LittleEndian.AppendUint32(patch, uint32(index-start))
		patch = append(patch, target[start:index]...)
	}
	return patch
}

// nextDifferentByte returns the first index at or after index where base and target differ.
// base and target must have the same length. It returns len(base) when the rest is equal.
func nextDifferentByte(base, target []byte, index int) int {
	for ; index+8 <= len(base) && index+8 <= len(target); index += 8 {
		if difference := binary.LittleEndian.Uint64(base[index:]) ^ binary.LittleEndian.Uint64(target[index:]); difference != 0 {
			return index + bits.TrailingZeros64(difference)/8
		}
	}
	for index < len(base) && index < len(target) && base[index] == target[index] {
		index++
	}
	return index
}

// nextLongEqualRun returns the start of the first equal section at or after index
// that is longer than [pagePatchRangeHeaderSize] bytes. base and target must have
// the same length. Bytes from len(base) to end count as different.
// It returns end when no such section exists.
func nextLongEqualRun(base, target []byte, index, end int) int {
	const (
		lowBits  = 0x0101010101010101
		highBits = 0x8080808080808080
	)
	b, t := base[index:], target[index:]
	run := 0 // The number of equal bytes just before the start of b.
	for ; len(b) >= 8 && len(t) >= 8; b, t = b[8:], t[8:] {
		difference := binary.LittleEndian.Uint64(b) ^ binary.LittleEndian.Uint64(t)
		if difference == 0 {
			run += 8
			if run > pagePatchRangeHeaderSize {
				return len(base) - len(b) + 8 - run
			}
			continue
		}
		// This expression is nonzero exactly when difference has a zero byte, which is an equal byte.
		if (difference-lowBits)&^difference&highBits == 0 {
			run = 0
			continue
		}
		for at := range 8 {
			if byte(difference>>(8*at)) != 0 {
				run = 0
				continue
			}
			run++
			if run > pagePatchRangeHeaderSize {
				return len(base) - len(b) + at + 1 - run
			}
		}
	}
	for ; len(b) > 0 && len(t) > 0; b, t = b[1:], t[1:] {
		if b[0] != t[0] {
			run = 0
			continue
		}
		run++
		if run > pagePatchRangeHeaderSize {
			return len(base) - len(b) + 1 - run
		}
	}
	return end
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

// LeafDeleteEntryCounts reads the entry counts from one leaf-delete payload.
func LeafDeleteEntryCounts(keys []byte) (uint32, uint32, error) {
	if len(keys) < 11 {
		return 0, 0, page.ErrInvalid
	}
	baseCount := binary.LittleEndian.Uint32(keys[:4])
	targetCount := binary.LittleEndian.Uint32(keys[4:8])
	if targetCount >= baseCount {
		return 0, 0, page.ErrInvalid
	}
	var previous []byte
	var removed uint32
	for offset := 8; offset < len(keys); {
		if len(keys)-offset < 2 {
			return 0, 0, page.ErrInvalid
		}
		keySize := int(binary.LittleEndian.Uint16(keys[offset : offset+2]))
		offset += 2
		if keySize == 0 || keySize > btree.MaxKeySize || len(keys)-offset < keySize {
			return 0, 0, page.ErrInvalid
		}
		key := keys[offset : offset+keySize]
		if previous != nil && bytes.Compare(previous, key) >= 0 {
			return 0, 0, page.ErrInvalid
		}
		previous = key
		offset += keySize
		removed++
	}
	if removed != baseCount-targetCount {
		return 0, 0, page.ErrInvalid
	}
	return baseCount, targetCount, nil
}

// ApplyLeafDeletes removes named entries from a verified compact leaf image.
// The payload holds base and final entry counts followed by byte-ordered keys.
// It also accepts a main-file leaf that already has the final entry count.
func ApplyLeafDeletes(base, keys []byte, pageID page.ID) ([]byte, error) {
	baseCount, targetCount, err := LeafDeleteEntryCounts(keys)
	if err != nil {
		return nil, err
	}
	node, err := btree.DecodeWALNode(base, pageID, int64(len(base)), nil)
	if err != nil {
		return nil, err
	}
	if !node.IsLeaf() {
		return nil, page.ErrInvalid
	}
	if count := uint32(node.EntryCount()); count != baseCount && count != targetCount {
		return nil, page.ErrInvalid
	}
	for offset := 8; offset < len(keys); {
		keySize := int(binary.LittleEndian.Uint16(keys[offset : offset+2]))
		offset += 2
		key := keys[offset : offset+keySize]
		if err := node.DeleteKeyIfPresent(key); err != nil {
			return nil, err
		}
		offset += keySize
	}
	if uint32(node.EntryCount()) != targetCount {
		return nil, page.ErrInvalid
	}
	return btree.EncodeWALNode(node), nil
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
