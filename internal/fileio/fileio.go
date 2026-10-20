package fileio

import (
	"io"
	"os"
	"runtime"
)

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

// SkipDriveCacheFlushForTesting makes [Sync] and [SyncData] use fsync(2) for an [*os.File] on macOS, and not F_FULLFSYNC.
// F_FULLFSYNC also flushes the cache of the drive, so the data survives a power failure, but each call takes some milliseconds.
// fsync(2) on macOS does not flush that cache, so a power failure can lose the synced data.
// Only a test binary sets it, before it opens a database.
// Other platforms ignore it.
var SkipDriveCacheFlushForTesting bool

// Sync flushes file data and metadata to storage.
// It skips directory sync on Windows.
func Sync(file *os.File) error {
	if runtime.GOOS == "windows" {
		info, err := file.Stat()
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
	}
	if runtime.GOOS == "darwin" && SkipDriveCacheFlushForTesting {
		// SyncData on macOS uses fsync(2) for this case.
		return SyncData(file)
	}
	return file.Sync()
}
