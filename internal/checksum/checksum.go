// Package checksum provides the checksum used by every KVLite storage format.
package checksum

import "hash/crc32"

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// Sum32 returns the CRC32C checksum of parts in order.
func Sum32(parts ...[]byte) uint32 {
	return Update32(0, parts...)
}

// Update32 adds parts to a CRC32C checksum and returns the new checksum.
func Update32(sum uint32, parts ...[]byte) uint32 {
	for _, part := range parts {
		sum = crc32.Update(sum, crc32cTable, part)
	}
	return sum
}
