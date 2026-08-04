package kvlite

import (
	"errors"
	"fmt"
	"io"
	"os"
)

var NODE_SIZE = os.Getpagesize()

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
	path     string
	file     *os.File
	rootNode *Node
}

// Open creates and opens a database at the given path. If the file does not
// exist it is created automatically.
func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	if path == "" {
		return nil, errors.New("path required")
	}

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

	if db.hasRootNode() {
		rootNode, err := readNode(db.file)
		if err != nil {
			_ = db.file.Close()
			return nil, fmt.Errorf("read root node: %w", err)
		}
		db.rootNode = rootNode
	} else {
		db.rootNode = newLeafNode()
	}

	return db, nil
}

// Close releases all database resources. All transactions must be closed before
// closing the database.
func (db *DB) Close() error {
	return db.file.Close()
}

// Path returns the path to the currently open database file.
func (db *DB) Path() string {
	return db.path
}

func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > MaxValueSize {
		return ErrValueTooLarge
	}

	node := db.rootNode
	for !node.IsLeaf {
		childIndex := node.findChildIndex(key)
		node = db.readNode(int(node.Children[childIndex]))
		if node == nil {
			return fmt.Errorf("read child node: %w", errNotImplemented)
		}
	}

	if err := node.insert(key, value); err != nil {
		return err
	}

	// Rung 2: single root leaf; deeper nodes persist when tree splits land.
	if node != db.rootNode {
		return errNotImplemented
	}
	return db.persistRootNode()
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyEmpty
	}
	if len(key) > MaxKeySize {
		return nil, ErrKeyTooLarge
	}

	node := db.rootNode
	for !node.IsLeaf {
		childIndex := node.findChildIndex(key)
		node = db.readNode(int(node.Children[childIndex]))
		if node == nil {
			return nil, ErrKeyNotFound
		}
	}

	value, found, err := node.get(key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrKeyNotFound
	}
	return value, nil
}

func (db *DB) persistRootNode() error {
	if _, err := db.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek root node: %w", err)
	}
	if err := db.file.Truncate(0); err != nil {
		return fmt.Errorf("truncate root node: %w", err)
	}
	if err := writeNode(db.file, db.rootNode); err != nil {
		return fmt.Errorf("write root node: %w", err)
	}
	return db.file.Sync()
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

func (db *DB) hasRootNode() bool {
	fi, err := db.file.Stat()
	if err != nil {
		return false
	}
	return fi.Size() > 0
}

func (db *DB) readNode(pgid int) *Node {
	offset := int64(pgid) * int64(NODE_SIZE)
	if _, err := db.file.Seek(offset, io.SeekStart); err != nil {
		return nil
	}
	node, err := readNode(db.file)
	if err != nil {
		return nil
	}
	return node
}
