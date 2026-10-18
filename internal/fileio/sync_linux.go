//go:build linux

package fileio

import (
	"os"

	"golang.org/x/sys/unix"
)

// SyncData writes file data and required size changes to stable storage.
// A file of another [FS] uses its Sync method.
func SyncData(file File) error {
	if osFile, ok := file.(*os.File); ok {
		return unix.Fdatasync(int(osFile.Fd()))
	}
	return file.Sync()
}
