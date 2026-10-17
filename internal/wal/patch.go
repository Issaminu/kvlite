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
const bytePatchRangeHeaderSize = 8

// BytePatch contains the encoded byte ranges that change one existing page image.
// The image can be a leaf or branch, one page of a continued node, or an allocation page.
// One [RecordTypeBytePatch] record contains one BytePatch. The record header identifies the page.
// A patch does not change the image size. A node image is compact: it has no page padding.
//
// Each range in the record payload has this layout:
//
//	Offset  Size  Field
//	0       4     Absolute offset in the image as a uint32
//	4       4     Replacement byte count as a uint32
//	8       size  Final bytes to write
//
// All integers use little-endian byte order. The payload has no range count.
// Recovery reads ranges until it reaches the payload size in the WAL record header.
// Ranges must be ordered, must not overlap, and must fit in the base image.
//
// Recovery copies the base image and writes each replacement range into the copy.
// Bytes outside the ranges stay unchanged.
// The base is the image that the earlier WAL records of the page give. It is never a main-file page: see [WAL.Commit].
// The zero value is ready to use.
type BytePatch struct {
	ranges []byte // Encoded range headers and replacement bytes in record order.
}

// reset removes the previous ranges and keeps their storage for the next node.
func (patch *BytePatch) reset() {
	patch.ranges = patch.ranges[:0]
}

// appendChangedRanges compares one same-size field and appends the ranges that change base into target.
// The field can be a leaf value, a branch separator key, or a child page ID.
// base and target must have the same length.
// baseOffset is the absolute offset of that field in the compact node image.
// Existing ranges remain in patch.
func (patch *BytePatch) appendChangedRanges(baseOffset int, base, target []byte) {
	patch.ranges = appendChangedBytePatchRanges(patch.ranges, baseOffset, base, target)
}

// encodedSize returns the number of bytes used by all range headers and replacement data.
func (patch *BytePatch) encodedSize() int {
	return len(patch.ranges)
}

// appendEncoded appends the complete patch payload to data.
func (patch *BytePatch) appendEncoded(data []byte) []byte {
	return append(data, patch.ranges...)
}

// appendChangedBytePatchRanges appends encoded replacement ranges for one same-size field.
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
func appendChangedBytePatchRanges(patch []byte, baseOffset int, base, target []byte) []byte {
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
// that is longer than [bytePatchRangeHeaderSize] bytes. base and target must have
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
			if run > bytePatchRangeHeaderSize {
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
			if run > bytePatchRangeHeaderSize {
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
		if run > bytePatchRangeHeaderSize {
			return len(base) - len(b) + 1 - run
		}
	}
	return end
}

// ApplyBytePatch applies an encoded patch to a new same-size copy of base.
// It does not change or retain base or patch data.
//
// An empty patch returns an unchanged copy of base. Bytes outside replacement ranges also come from base.
// ApplyBytePatch validates the base size, range headers, range order, and range bounds.
// It returns an error that wraps [page.ErrInvalid] when the patch is invalid.
// It does not validate the rebuilt node format or checksum.
func ApplyBytePatch(base, patch []byte, pageSize int64) ([]byte, error) {
	if int64(len(base)) > pageSize {
		return nil, fmt.Errorf("read byte patch base size %d: %w", len(base), page.ErrInvalid)
	}
	decoded := BytePatch{ranges: patch}
	return decoded.apply(base)
}

// apply parses the encoded ranges and writes them into a new copy of base.
func (patch *BytePatch) apply(base []byte) ([]byte, error) {
	// Start with the base so bytes outside the encoded ranges stay unchanged.
	target := make([]byte, len(base))
	copy(target, base)

	ranges := patch.ranges
	// previousEnd rejects ranges that overlap or arrive out of order.
	previousEnd := uint64(0)
	for len(ranges) > 0 {
		if len(ranges) < bytePatchRangeHeaderSize {
			return nil, fmt.Errorf("read byte patch range header: %w", page.ErrInvalid)
		}
		rangeOffset := uint64(binary.LittleEndian.Uint32(ranges[:4]))
		rangeSize := uint64(binary.LittleEndian.Uint32(ranges[4:8]))
		// Remove the header so ranges starts with this range's replacement data.
		ranges = ranges[bytePatchRangeHeaderSize:]
		rangeEnd := rangeOffset + rangeSize
		empty := rangeSize == 0
		overlapsPrevious := rangeOffset < previousEnd
		outsideBase := rangeEnd > uint64(len(target))
		missingData := rangeSize > uint64(len(ranges))
		if empty || overlapsPrevious || outsideBase || missingData {
			return nil, fmt.Errorf("read byte patch range: %w", page.ErrInvalid)
		}
		copy(target[int(rangeOffset):int(rangeEnd)], ranges[:int(rangeSize)])
		// Remove the replacement data so ranges starts with the next range header.
		ranges = ranges[int(rangeSize):]
		previousEnd = rangeEnd
	}
	return target, nil
}

// Entry patch operation codes and sizes.
const (
	entryPatchSet    = 1
	entryPatchDelete = 2

	// A delete operation has a code byte and a two-byte key size before the key.
	entryPatchDeleteHeaderSize = 3
	// A set operation also has a four-byte flags field and a four-byte value size.
	entryPatchSetHeaderSize = entryPatchDeleteHeaderSize + 8
)

// appendEntryPatchSet appends one set operation.
func appendEntryPatchSet(patch []byte, flags uint32, key, value []byte) []byte {
	patch = append(patch, entryPatchSet)
	patch = binary.LittleEndian.AppendUint16(patch, uint16(len(key)))
	patch = append(patch, key...)
	patch = binary.LittleEndian.AppendUint32(patch, flags)
	patch = binary.LittleEndian.AppendUint32(patch, uint32(len(value)))
	return append(patch, value...)
}

// appendEntryPatchDelete appends one delete operation.
func appendEntryPatchDelete(patch []byte, key []byte) []byte {
	patch = append(patch, entryPatchDelete)
	patch = binary.LittleEndian.AppendUint16(patch, uint16(len(key)))
	return append(patch, key...)
}

// appendLeafEntryPatch appends the set and delete operations that change base into target.
// It merges both sorted entry lists once. An entry with equal flags, key, and value makes no operation.
// It returns false without new bytes when the payload would not be smaller than limit.
func appendLeafEntryPatch(patch []byte, base, target *btree.Node, limit int) ([]byte, bool) {
	if !base.IsLeaf() || !target.IsLeaf() {
		return patch, false
	}
	start := len(patch)
	baseIndex, targetIndex := 0, 0
	for baseIndex < base.EntryCount() || targetIndex < target.EntryCount() {
		// A negative comparison deletes a base entry. A positive comparison sets a new target entry.
		var comparison int
		switch {
		case baseIndex == base.EntryCount():
			comparison = 1
		case targetIndex == target.EntryCount():
			comparison = -1
		default:
			comparison = bytes.Compare(base.EntryAt(baseIndex).Key(), target.EntryAt(targetIndex).Key())
		}
		switch {
		case comparison < 0:
			key := base.EntryAt(baseIndex).Key()
			if len(patch)-start+entryPatchDeleteHeaderSize+len(key) >= limit {
				return patch[:start], false
			}
			patch = appendEntryPatchDelete(patch, key)
			baseIndex++
			continue
		case comparison == 0:
			before, after := base.EntryAt(baseIndex), target.EntryAt(targetIndex)
			baseIndex++
			if before.Flags() == after.Flags() && bytes.Equal(before.Value(), after.Value()) {
				targetIndex++
				continue
			}
		}
		entry := target.EntryAt(targetIndex)
		if len(patch)-start+entryPatchSetHeaderSize+len(entry.Key())+len(entry.Value()) >= limit {
			return patch[:start], false
		}
		patch = appendEntryPatchSet(patch, entry.Flags(), entry.Key(), entry.Value())
		targetIndex++
	}
	return patch, true
}

// appendBranchEntryPatch appends the set and delete operations that change base into target.
// Each separator key owns the child page ID on its right side.
// It returns false without new bytes when the first child changes or the payload would not be smaller than limit.
func appendBranchEntryPatch(patch []byte, base, target *btree.Node, limit int) ([]byte, bool) {
	if base.IsLeaf() || target.IsLeaf() || len(base.Children) != base.EntryCount()+1 || len(target.Children) != target.EntryCount()+1 {
		return patch, false
	}
	if base.Children[0] != target.Children[0] {
		return patch, false
	}
	start := len(patch)
	var child [page.IDSize]byte
	baseIndex, targetIndex := 0, 0
	for baseIndex < base.EntryCount() || targetIndex < target.EntryCount() {
		// A negative comparison deletes a base separator. A positive comparison sets a new target separator.
		var comparison int
		switch {
		case baseIndex == base.EntryCount():
			comparison = 1
		case targetIndex == target.EntryCount():
			comparison = -1
		default:
			comparison = bytes.Compare(base.EntryAt(baseIndex).Key(), target.EntryAt(targetIndex).Key())
		}
		switch {
		case comparison < 0:
			key := base.EntryAt(baseIndex).Key()
			if len(patch)-start+entryPatchDeleteHeaderSize+len(key) >= limit {
				return patch[:start], false
			}
			patch = appendEntryPatchDelete(patch, key)
			baseIndex++
			continue
		case comparison == 0:
			sameChild := base.Children[baseIndex+1] == target.Children[targetIndex+1]
			baseIndex++
			if sameChild {
				targetIndex++
				continue
			}
		}
		key := target.EntryAt(targetIndex).Key()
		if len(patch)-start+entryPatchSetHeaderSize+len(key)+page.IDSize >= limit {
			return patch[:start], false
		}
		binary.LittleEndian.PutUint64(child[:], uint64(target.Children[targetIndex+1]))
		patch = appendEntryPatchSet(patch, 0, key, child[:])
		targetIndex++
	}
	return patch, true
}

// ApplyEntryPatch applies the operations of an encoded entry patch to node.
// A [RecordTypeEntryPatch] payload changes the entries of one leaf or branch that fits in one page.
// Each operation names an entry by its key. The payload contains operations in ascending key order.
// Each operation has this layout:
//
//	Offset  Size  Field
//	0       1     Operation: 1 sets an entry, 2 deletes an entry
//	1       2     Key size K as a uint16
//	3       K     Key
//
// A set operation continues with these fields:
//
//	3+K     4     Leaf flags as a uint32. A branch uses zero.
//	7+K     4     Value size V as a uint32. A branch uses eight.
//	11+K    V     Leaf value, or the branch child page ID on the right side of the key
//
// All integers use little-endian byte order.
// Set inserts the entry, or it replaces the entry that has the same key.
// Delete removes the entry that has the key. That entry must exist.
// A branch patch cannot change the first child page ID. That change needs a complete image.
//
// node must be the image that the earlier WAL records of the page give, because Commit compares that image with the final node.
// node must be a decoded leaf or branch without continuation pages.
// Set operations copy their key and value bytes. An empty patch makes no change.
// It does not limit the size of the result. The caller checks it.
// It returns an error that wraps [page.ErrInvalid] when the patch is invalid for node. Then node can be partly changed.
func ApplyEntryPatch(node *btree.Node, patch []byte) error {
	if len(node.OverflowPages()) != 0 {
		return fmt.Errorf("apply entry patch to continued page %d: %w", node.PageID(), page.ErrInvalid)
	}
	var previous []byte
	for offset := 0; offset < len(patch); {
		if len(patch)-offset < entryPatchDeleteHeaderSize {
			return fmt.Errorf("read entry patch operation header: %w", page.ErrInvalid)
		}
		operation := patch[offset]
		keySize := int(binary.LittleEndian.Uint16(patch[offset+1 : offset+3]))
		offset += entryPatchDeleteHeaderSize
		if keySize == 0 || keySize > btree.MaxKeySize || len(patch)-offset < keySize {
			return fmt.Errorf("read entry patch key: %w", page.ErrInvalid)
		}
		key := patch[offset : offset+keySize]
		offset += keySize
		if previous != nil && bytes.Compare(previous, key) >= 0 {
			return fmt.Errorf("read entry patch key order: %w", page.ErrInvalid)
		}
		previous = key

		var err error
		switch operation {
		case entryPatchDelete:
			if node.IsLeaf() {
				err = node.DeleteKey(key)
			} else {
				err = node.DeleteSeparator(key)
			}
		case entryPatchSet:
			if len(patch)-offset < entryPatchSetHeaderSize-entryPatchDeleteHeaderSize {
				return fmt.Errorf("read entry patch value header: %w", page.ErrInvalid)
			}
			flags := binary.LittleEndian.Uint32(patch[offset : offset+4])
			valueSize := uint64(binary.LittleEndian.Uint32(patch[offset+4 : offset+8]))
			offset += entryPatchSetHeaderSize - entryPatchDeleteHeaderSize
			if valueSize > uint64(len(patch)-offset) {
				return fmt.Errorf("read entry patch value: %w", page.ErrInvalid)
			}
			value := patch[offset : offset+int(valueSize)]
			offset += int(valueSize)
			if node.IsLeaf() {
				if flags&^btree.BucketLeafFlag != 0 || flags&btree.BucketLeafFlag != 0 && len(value) != page.IDSize {
					return fmt.Errorf("read entry patch leaf flags: %w", page.ErrInvalid)
				}
				err = node.InsertOrReplaceLeafEntry(btree.NewEntry(flags, key, value))
			} else {
				if flags != 0 || len(value) != page.IDSize {
					return fmt.Errorf("read entry patch branch child: %w", page.ErrInvalid)
				}
				child := page.ID(binary.LittleEndian.Uint64(value))
				if page.IsMetaID(child) {
					return fmt.Errorf("read entry patch branch child %d: %w", child, page.ErrInvalid)
				}
				err = node.InsertOrReplaceSeparator(key, child)
			}
		default:
			return fmt.Errorf("read entry patch operation %d: %w", operation, page.ErrInvalid)
		}
		if err != nil {
			return fmt.Errorf("apply entry patch to page %d: %w: %w", node.PageID(), page.ErrInvalid, err)
		}
	}
	return nil
}
