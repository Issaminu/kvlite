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
