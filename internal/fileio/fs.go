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

// Mapper is a [File] that KVLite can map into memory for reads.
// KVLite maps an [*os.File] with the operating system, so only a File of another FS implements Mapper.
// A test file system uses it to check the mapped read path.
type Mapper interface {
	// Map maps the first length bytes of the file for reads.
	Map(length int) ([]byte, error)

	// Unmap releases a mapping that Map returned.
	// The caller must not read the mapping after Unmap.
	Unmap(data []byte) error

	// MappingFollowsWrites reports whether a mapping shows later writes to the file.
	// It is true for a shared mapping on Unix, and false on Windows.
	MappingFollowsWrites() bool
}

// FS opens, removes, and syncs the files of a database.
// [OSFS] uses the operating system.
// File locks need an [*os.File], so KVLite skips them for files of another FS.
// A file of another FS is mapped into memory only when it implements [Mapper].
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

	// Abs returns an absolute form of name, like [filepath.Abs].
	Abs(name string) (string, error)

	// Lstat returns the file information of name without following a final symbolic link, like [os.Lstat].
	Lstat(name string) (os.FileInfo, error)

	// CreateTemp creates a new file in dir, like [os.CreateTemp], and returns the file and its name.
	// It returns a nil File when it returns an error.
	CreateTemp(dir, pattern string) (File, string, error)

	// Link creates newname as a hard link to oldname, like [os.Link].
	Link(oldname, newname string) error

	// Chmod changes the permission bits of name, like [os.Chmod].
	Chmod(name string, mode os.FileMode) error
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

// Abs returns an absolute form of name with [filepath.Abs].
func (OSFS) Abs(name string) (string, error) {
	return filepath.Abs(name)
}

// Lstat returns the file information of name with [os.Lstat].
func (OSFS) Lstat(name string) (os.FileInfo, error) {
	return os.Lstat(name)
}

// CreateTemp creates a new file in dir with [os.CreateTemp].
func (OSFS) CreateTemp(dir, pattern string) (File, string, error) {
	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		// Return an untyped nil, so the caller's nil check works.
		return nil, "", err
	}
	return file, file.Name(), nil
}

// Link creates newname as a hard link to oldname with [os.Link].
func (OSFS) Link(oldname, newname string) error {
	return os.Link(oldname, newname)
}

// Chmod changes the permission bits of name with [os.Chmod].
func (OSFS) Chmod(name string, mode os.FileMode) error {
	return os.Chmod(name, mode)
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
