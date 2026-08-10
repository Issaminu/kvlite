package kvlite

import (
	"encoding/binary"
	"io"
)

// TODO: generic utility func, move to some utils file
func readLengthPrefixedBytes[T ~uint8 | ~uint16 | ~uint32 | ~uint64](r io.Reader) ([]byte, error) {
	var length T
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}

// TODO: generic utility func, move to some utils file
func writeLengthPrefixedBytes[T ~uint8 | ~uint16 | ~uint32 | ~uint64](w io.Writer, data []byte) error {
	if err := binary.Write(w, binary.LittleEndian, T(len(data))); err != nil {
		return err
	}
	return writeFull(w, data)
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
