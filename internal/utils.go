package internal

import (
	"crypto/rand"
	"fmt"
	"io"
	"strconv"
)

func StringToUint32(s string) (uint32, error) {
	i, err := strconv.Atoi(s)
	return uint32(i), err
}

func GenerateUUIDv4() ([16]byte, error) {
	uuid := [16]byte{}

	// Read random bytes
	_, err := io.ReadFull(rand.Reader, uuid[:])
	if err != nil {
		return [16]byte{}, err
	}

	// Set version (4) and variant bits according to RFC 4122
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // Version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // Variant is 10

	return uuid, nil
}
func GenerateUUIDv4String() (string, error) {
	uuid, err := GenerateUUIDv4()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%x-%x-%x-%x-%x",
		uuid[0:4],
		uuid[4:6],
		uuid[6:8],
		uuid[8:10],
		uuid[10:16]), nil
}
