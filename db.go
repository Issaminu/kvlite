package kvlite

import (
	"errors"
	"fmt"
	"io"
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
	path     string
	file     *os.File
	meta     *Meta
	rootNode *Node
}

// Open creates and opens a database at the given path.
// If the file does not exist it is created automatically.
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

	// if the file is empty, create the meta, otherwise read it
	if db.hasMeta() {
		db.meta, err = db.readMeta()
		if err != nil {
			if closeErr := db.Close(); closeErr != nil {
				return nil, errors.Join(err, fmt.Errorf("failed to close database: %w", closeErr))
			}
			return nil, fmt.Errorf("read db meta: %w", err)
		}
		err := db.meta.Validate()
		if err != nil {
			if closeErr := db.Close(); closeErr != nil {
				return nil, errors.Join(err, fmt.Errorf("failed to close database: %w", closeErr))
			}
			return nil, fmt.Errorf("db version is unsupported: %w", err)
		}
		rootNode := db.readNode(db.meta.root)
		if rootNode == nil {
			if closeErr := db.Close(); closeErr != nil {
				return nil, errors.Join(err, fmt.Errorf("failed to close database: %w", closeErr))
			}
			return nil, fmt.Errorf("read root node: %w", err)
		}
		db.rootNode = rootNode
	} else {
		db.meta = NewMeta()
		db.persistMeta()
		db.rootNode = db.newLeafNode(db.meta.pgid)
		db.persistNode(db.rootNode)
	}

	return db, nil
}

// Close releases all database resources.
// All transactions must be closed before closing the database.
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
		childIndex, err := node.findChildIndex(key)
		if err != nil {
			return err
		}
		childNode := db.readNode(node.Children[childIndex])
		if childNode == nil {
			return ErrKeyNotFound
		}

		childNode.parent = node
		childNode.Index = childIndex

		node = childNode
	}

	if err := node.insert(key, value); err != nil {
		return err
	}

	if !node.needsSplit() {
		db.persistNode(node)
		return db.persistMeta()
	}

	for node.needsSplit() {
		rightPgid := db.allocate()
		rightNode, _, keyAtSeperatorIndex, err := node.split(rightPgid)
		if err != nil {
			return err
		}

		if node == db.rootNode {
			var newEntries []Entry

			if node.IsLeaf {
				newEntries = []Entry{{key: rightNode.entries[0].key}}
			} else {
				newEntries = []Entry{{key: keyAtSeperatorIndex}}
			}

			newRoot := &Node{
				db:       db,
				IsLeaf:   false,
				entries:  newEntries,                        // separator: first key of right (copied up)
				Children: []Pgid{node.pgid, rightNode.pgid}, // left, right
				pgid:     db.allocate(),
			}

			node.parent = newRoot
			rightNode.parent = newRoot

			db.persistNode(newRoot)

			db.rootNode = newRoot
			db.meta.root = newRoot.pgid
		} else { // parent is not a root node
			parent := node.parent
			rightNode.parent = parent

			var newEntry Entry
			if node.IsLeaf {
				newEntry = Entry{key: rightNode.entries[0].key}
			} else {
				newEntry = Entry{key: keyAtSeperatorIndex}
			}

			// add seperator to the parent's entries
			parent.entries = append(parent.entries, Entry{})
			copy(parent.entries[node.Index+1:], parent.entries[node.Index:])

			parent.entries[node.Index] = newEntry

			// add the new right node `pgid` to the parent's Children
			parent.Children = append(parent.Children, 0)
			copy(parent.Children[rightNode.Index+1:], parent.Children[rightNode.Index:])
			parent.Children[rightNode.Index] = rightNode.pgid

			db.persistNode(parent)
		}
		db.persistNode(node)
		db.persistNode(rightNode)

		node = node.parent
	}
	return db.persistMeta()
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
		childIndex, err := node.findChildIndex(key)
		if err != nil {
			return nil, err
		}
		childNode := db.readNode(node.Children[childIndex])
		if childNode == nil {
			return nil, ErrKeyNotFound
		}

		childNode.parent = node
		childNode.Index = childIndex

		node = childNode
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

func (db *DB) persistNode(node *Node) error {
	offset := int64(node.pgid) * db.meta.pageSize
	if _, err := db.file.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek node: %w", err)
	}
	if err := writeNode(db.file, node, true); err != nil {
		return fmt.Errorf("write node: %w", err)
	}
	// return db.file.Sync()
	return nil
}

func (db *DB) hasMeta() bool {
	fi, err := db.file.Stat()
	if err != nil {
		return false
	}
	return fi.Size() > 0
}

func (db *DB) readNode(pgid Pgid) *Node {
	offset := int64(pgid) * db.meta.pageSize
	if _, err := db.file.Seek(offset, io.SeekStart); err != nil {
		return nil
	}
	node, err := readNode(db.file)
	if err != nil {
		return nil
	}

	node.db = db
	node.pgid = pgid
	return node
}

func (db *DB) readMeta() (*Meta, error) {
	if _, err := db.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	meta, err := readMeta(db.file)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func (db *DB) persistMeta() error {
	// recompute checksum
	db.meta.checksum = db.meta.GenerateChecksum()

	if _, err := db.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek node: %w", err)
	}
	if err := writeMeta(db.file, db.meta); err != nil {
		return fmt.Errorf("write node: %w", err)
	}
	// return db.file.Sync()
	return nil
}

func (db *DB) newLeafNode(pgid Pgid) *Node {
	return &Node{db: db, IsLeaf: true, pgid: pgid, Children: []Pgid{}, entries: []Entry{}}
}

func (db *DB) allocate() Pgid {
	db.meta.pgid++
	return db.meta.pgid
}
