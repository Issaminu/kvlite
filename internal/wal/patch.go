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

// appendChangedBytePatchRanges compares one same-size region and appends the byte-patch ranges that change base into target.
// The region can be a field of a node image, such as a leaf value, a branch separator key, or a child page ID, or a whole page image.
// baseOffset is the absolute offset of the region in the image. Existing ranges remain in patch.
// base and target must have the same length. See [ApplyBytePatch] for the range format.
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

		// Store an absolute image offset, the replacement size, and the final bytes.
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

// ApplyBytePatch applies the payload of a [RecordTypeBytePatch] record to a new same-size copy of base.
// The patch changes one existing page image: a leaf or branch, one page of a continued node, or an allocation page.
// The base is the image that the earlier WAL records of the page give. It is never a main-file page: see [WAL.Commit].
// A patch does not change the image size. A node image is compact: it has no page padding.
//
// The payload is a list of ranges. Each range has this layout:
//
//	Offset  Size  Field
//	0       4     Absolute offset in the image as a uint32
//	4       4     Replacement byte count as a uint32
//	8       size  Final bytes to write
//
// All integers use little-endian byte order.
// The payload has no range count: recovery reads ranges until it reaches the payload size in the WAL record header.
// Ranges must be ordered, must not overlap, and must fit in the base image.
// Bytes outside the ranges stay unchanged. An empty patch returns an unchanged copy of base.
//
// ApplyBytePatch does not change or retain base or patch data.
// It validates the base size, range headers, range order, and range bounds.
// It returns an error that wraps [page.ErrInvalid] when the patch is invalid.
// It does not validate the format or checksum of the result.
func ApplyBytePatch(base, patch []byte, pageSize int64) ([]byte, error) {
	if int64(len(base)) > pageSize {
		return nil, fmt.Errorf("read byte patch base size %d: %w", len(base), page.ErrInvalid)
	}
	// Start with the base so bytes outside the encoded ranges stay unchanged.
	target := make([]byte, len(base))
	copy(target, base)

	// previousEnd rejects ranges that overlap or arrive out of order.
	previousEnd := uint64(0)
	for len(patch) > 0 {
		if len(patch) < bytePatchRangeHeaderSize {
			return nil, fmt.Errorf("read byte patch range header: %w", page.ErrInvalid)
		}
		rangeOffset := uint64(binary.LittleEndian.Uint32(patch[:4]))
		rangeSize := uint64(binary.LittleEndian.Uint32(patch[4:8]))
		// Remove the header so patch starts with this range's replacement data.
		patch = patch[bytePatchRangeHeaderSize:]
		rangeEnd := rangeOffset + rangeSize
		empty := rangeSize == 0
		overlapsPrevious := rangeOffset < previousEnd
		outsideBase := rangeEnd > uint64(len(target))
		missingData := rangeSize > uint64(len(patch))
		if empty || overlapsPrevious || outsideBase || missingData {
			return nil, fmt.Errorf("read byte patch range: %w", page.ErrInvalid)
		}
		copy(target[int(rangeOffset):int(rangeEnd)], patch[:int(rangeSize)])
		// Remove the replacement data so patch starts with the next range header.
		patch = patch[int(rangeSize):]
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

// appendEntryPatch appends the set and delete operations that change base into target, two leaves or two branches.
// It merges both sorted entry lists once. An entry with equal key, flags, and value makes no operation.
// A branch entry is a separator key with zero flags, and its value is the 8-byte ID of the child on its right side (see entryPatchEntry).
// It returns false without new bytes when the node types differ, when a branch changes its first child, or when the payload would not be smaller than limit.
func appendEntryPatch(patch []byte, base, target *btree.Node, limit int) ([]byte, bool) {
	if base.IsLeaf() != target.IsLeaf() {
		return patch, false
	}
	// No separator owns the first child, so an entry patch cannot change it.
	if !base.IsLeaf() && (len(base.Children) != base.EntryCount()+1 || len(target.Children) != target.EntryCount()+1 || base.Children[0] != target.Children[0]) {
		return patch, false
	}
	start := len(patch)
	var baseChild, targetChild [page.IDSize]byte
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
			_, beforeFlags, beforeValue := entryPatchEntry(base, baseIndex, &baseChild)
			_, afterFlags, afterValue := entryPatchEntry(target, targetIndex, &targetChild)
			baseIndex++
			if beforeFlags == afterFlags && bytes.Equal(beforeValue, afterValue) {
				targetIndex++
				continue
			}
		}
		key, flags, value := entryPatchEntry(target, targetIndex, &targetChild)
		if len(patch)-start+entryPatchSetHeaderSize+len(key)+len(value) >= limit {
			return patch[:start], false
		}
		patch = appendEntryPatchSet(patch, flags, key, value)
		targetIndex++
	}
	return patch, true
}

// entryPatchEntry returns the key, flags, and value that an entry patch stores for entry index of node.
// A leaf returns its stored entry.
// A branch returns separator index with zero flags, and the ID of the child on its right side, encoded into child.
func entryPatchEntry(node *btree.Node, index int, child *[page.IDSize]byte) ([]byte, uint32, []byte) {
	entry := node.EntryAt(index)
	if node.IsLeaf() {
		return entry.Key(), entry.Flags(), entry.Value()
	}
	binary.LittleEndian.PutUint64(child[:], uint64(node.Children[index+1]))
	return entry.Key(), 0, child[:]
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
