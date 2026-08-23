package fileio

import "io"

func ReadFull(reader io.Reader, data []byte) error {
	_, err := io.ReadFull(reader, data)
	return err
}

func WriteFull(writer io.Writer, data []byte) error {
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

// WriteFullAt writes data to w starting at offset, retrying until all bytes are written or an error occurs.
// It does not move the read/write cursor of a file opened for sequential access.
func WriteFullAt(w io.WriterAt, data []byte, offset int64) error {
	for len(data) > 0 {
		n, err := w.WriteAt(data, offset)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
		offset += int64(n)
	}
	return nil
}
