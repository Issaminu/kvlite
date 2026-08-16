package fileio

import (
	"bytes"
	"testing"
	"testing/iotest"
)

// oneByteWriter accepts one byte from each Write call without returning an
// error. It verifies that WriteFull handles valid partial writes.
type oneByteWriter struct {
	bytes.Buffer
}

func (w *oneByteWriter) Write(data []byte) (int, error) {
	if len(data) > 1 {
		data = data[:1]
	}
	return w.Buffer.Write(data)
}

func TestWriteFull_CompletesPartialWrites(t *testing.T) {
	writer := new(oneByteWriter)
	data := []byte("partial write")
	if err := WriteFull(writer, data); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(writer.Bytes(), data) {
		t.Fatalf("wrote %q, want %q", writer.Bytes(), data)
	}
}

func TestReadFull_CompletesPartialReads(t *testing.T) {
	want := []byte("partial read")
	data := make([]byte, len(want))
	reader := iotest.OneByteReader(bytes.NewReader(want))

	if err := ReadFull(reader, data); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, want) {
		t.Fatalf("read %q, want %q", data, want)
	}
}
