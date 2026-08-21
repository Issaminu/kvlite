package checksum

import (
	"hash/crc32"
	"testing"
)

func TestSum32_UsesCastagnoliAndPreservesPartOrder(t *testing.T) {
	header := []byte{
		0,
		1, 0, 0, 0, 0, 0, 0, 0,
		2, 0, 0, 0, 0, 0, 0, 0,
		2, 0, 0, 0,
	}
	content := []byte{'x', 'y'}

	want := crc32.Checksum(append(append([]byte{}, header...), content...), crc32.MakeTable(crc32.Castagnoli))
	if got := Sum32(header, content); got != want {
		t.Fatalf("CRC32C checksum: got %x, want %x", got, want)
	}
}
