//go:build darwin

package fileio

import (
	"os"

	"golang.org/x/sys/unix"
)

// SyncData writes file data to stable storage.
// An [*os.File] uses F_FULLFSYNC, or fsync(2) when [SkipDriveCacheFlushForTesting] is set.
// A file of another [FS] uses its Sync method.
func SyncData(file File) error {
	if osFile, ok := file.(*os.File); ok && SkipDriveCacheFlushForTesting {
		return unix.Fsync(int(osFile.Fd()))
	}
	return file.Sync()
}
