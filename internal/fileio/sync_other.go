//go:build !linux

package fileio

// SyncData writes file data to stable storage.
func SyncData(file File) error {
	return file.Sync()
}
