package fileio

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

// File is the part of an open file that KVLite uses.
// [*os.File] implements it.
// A test file system can supply another implementation, for example one that simulates a power failure.
type File interface {
	io.ReaderAt
	io.WriterAt
	Stat() (os.FileInfo, error)
	Truncate(size int64) error
	Sync() error
	Close() error
}

// FS opens, removes, and syncs the files of a database.
// [OSFS] uses the operating system.
// Memory mapping and file locks need an [*os.File], so KVLite skips them for files of another FS.
type FS interface {
	// OpenFile opens name like [os.OpenFile].
	// It returns a nil File when it returns an error.
	OpenFile(name string, flag int, perm os.FileMode) (File, error)

	// Remove removes the file name like [os.Remove].
	Remove(name string) error

	// EvalSymlinks returns name after it resolves symbolic links, like [filepath.EvalSymlinks].
	EvalSymlinks(name string) (string, error)

	// SyncDir makes the file names in directory dir durable.
	SyncDir(dir string) error
}

// OSFS is the [FS] of the operating system.
type OSFS struct {
	// SyncDirectory syncs the open directory that SyncDir opens.
	// Nil uses [Sync].
	// Tests use it to count or fail directory syncs.
	SyncDirectory func(*os.File) error
}

// OpenFile opens name with [os.OpenFile].
func (OSFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		// Return an untyped nil, so the caller's nil check works.
		return nil, err
	}
	return file, nil
}

// Remove removes name with [os.Remove].
func (OSFS) Remove(name string) error {
	return os.Remove(name)
}

// EvalSymlinks resolves name with [filepath.EvalSymlinks].
func (OSFS) EvalSymlinks(name string) (string, error) {
	return filepath.EvalSymlinks(name)
}

// SyncDir opens dir, syncs it, and closes it.
// [Sync] skips directory sync on Windows.
func (fs OSFS) SyncDir(dir string) error {
	parent, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncDirectory := fs.SyncDirectory
	if syncDirectory == nil {
		syncDirectory = Sync
	}
	return errors.Join(syncDirectory(parent), parent.Close())
}
