package fileio

import (
	"bytes"
	"io"
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

type oneByteWriterAt struct {
	data []byte
}

func (w *oneByteWriterAt) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, io.EOF
	}
	end := off + int64(len(p))
	if end > int64(len(w.data)) {
		w.data = append(w.data, make([]byte, end-int64(len(w.data)))...)
	}
	if len(p) > 1 {
		p = p[:1]
	}
	n := copy(w.data[off:], p)
	return n, nil
}

func TestWriteFullAt_CompletesPartialWrites(t *testing.T) {
	writer := &oneByteWriterAt{}
	data := []byte("partial write at offset")
	if err := WriteFullAt(writer, data, 4); err != nil {
		t.Fatal(err)
	}

	want := append(make([]byte, 4), data...)
	if !bytes.Equal(writer.data, want) {
		t.Fatalf("wrote %q, want %q", writer.data, want)
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
