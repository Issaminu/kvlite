package kvlite

import (
	"os"
)

// Options represents the options that can be set when opening a database.
// This is a bbolt-compatible subset; fields are added as components land.
type Options struct {
	// ReadOnly opens the database in read-only mode.
	ReadOnly bool
}

// DefaultOptions is used when nil options are passed to Open.
var DefaultOptions = &Options{}

// DB represents a collection of buckets persisted to a single file on disk.
// All data access is performed through transactions obtained from the DB.
type DB struct {
	path string
	file *os.File

	// TODO: meta pages, pager, freelist, mmap, rwlock, ... added per component.
}

// Open creates and opens a database at the given path. If the file does not
// exist it is created automatically.
func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	var dbFile *os.File
	var err error

	if options != nil && options.ReadOnly {
		dbFile, err = os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0444)
	} else {
		dbFile, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	}

	if err != nil {
		return nil, err
	}
	db := &DB{
		path: path,
		file: dbFile,
	}
	return db, nil
}

// Close releases all database resources. All transactions must be closed before
// closing the database.
func (db *DB) Close() error {
	err := db.file.Close()
	return err
}

// Path returns the path to the currently open database file.
func (db *DB) Path() string {
	return db.path
}
