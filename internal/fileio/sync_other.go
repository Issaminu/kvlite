//go:build !linux

package fileio

import "os"

// SyncData writes file data to stable storage.
func SyncData(file *os.File) error {
	return file.Sync()
}
