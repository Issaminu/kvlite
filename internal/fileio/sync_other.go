//go:build !linux && !darwin

package fileio

// SyncData writes file data to stable storage.
func SyncData(file File) error {
	return file.Sync()
}
