package internal

import (
	"hash/crc32"
)

func getKVChecksum(key, value string) uint32 {
	keyValString := key + ";" + value

	return calculateChecksum(keyValString)
}

func calculateChecksum(s string) uint32 {
	data := []byte(s)
	checksum := crc32.ChecksumIEEE(data)
	return checksum
}
