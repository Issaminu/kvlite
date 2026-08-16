package page

import (
	"bytes"
	"errors"
	"testing"
)

func TestIDCodec_UsesExactlyEightLittleEndianBytes(t *testing.T) {
	// The byte pattern makes the uint64 byte order visible in the assertion.
	const id ID = 0x0102030405060708
	want := []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}

	encoded := EncodeID(id)
	if !bytes.Equal(encoded, want) {
		t.Fatalf("encode page ID: got %x, want %x", encoded, want)
	}

	decoded, err := DecodeID(encoded)
	if err != nil {
		t.Fatalf("decode page ID: %v", err)
	}
	if decoded != id {
		t.Fatalf("decode page ID: got %x, want %x", decoded, id)
	}

	// A page ID is a uint64, so its encoding must contain exactly eight bytes.
	for _, size := range []int{7, 9} {
		if _, err := DecodeID(make([]byte, size)); !errors.Is(err, ErrInvalid) {
			t.Errorf("decode %d bytes: got %v, want ErrInvalid", size, err)
		}
	}
}
