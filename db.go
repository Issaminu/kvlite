package kvlite

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const ITEM_HEADER_SIZE = 8

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

func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}

	header := make([]byte, ITEM_HEADER_SIZE)
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(value)))

	if err := writeFull(db.file, header); err != nil {

		return fmt.Errorf("write record header: %w", err)
	}
	if err := writeFull(db.file, key); err != nil {
		return fmt.Errorf("write key: %w", err)
	}
	if err := writeFull(db.file, value); err != nil {
		return fmt.Errorf("write value: %w", err)
	}

	if err := db.file.Sync(); err != nil {
		return fmt.Errorf("sync database: %w", err)
	}

	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	var result []byte

	_, err := db.file.Seek(0, io.SeekStart)

	if err != nil {
		return nil, err
	}

	header := make([]byte, ITEM_HEADER_SIZE)
	for {
		_, err := io.ReadFull(db.file, header)
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errors.New("database contains a truncated record header")
		}
		if err != nil {
			return nil, fmt.Errorf("read record header: %w", err)
		}
		keyLength := binary.LittleEndian.Uint32(header[0:4])
		valueLength := binary.LittleEndian.Uint32(header[4:8])
		storedKey := make([]byte, keyLength)
		if _, err := io.ReadFull(db.file, storedKey); err != nil {
			return nil, fmt.Errorf("read stored key: %w", err)
		}
		value := make([]byte, valueLength)
		if _, err := io.ReadFull(db.file, value); err != nil {
			return nil, fmt.Errorf("read stored value: %w", err)
		}
		if string(storedKey) == string(key) {
			result = value
		}
	}

	if len(result) == 0 {
		return nil, ErrKeyNotFound
	}
	return result, nil

}

func writeFull(writer io.Writer, data []byte) error {
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
