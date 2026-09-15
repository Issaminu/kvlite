//go:build linux

package fileio

import (
	"os"

	"golang.org/x/sys/unix"
)

// SyncData writes file data and required size changes to stable storage.
func SyncData(file *os.File) error {
	return unix.Fdatasync(int(file.Fd()))
}
