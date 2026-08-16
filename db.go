package kvlite

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

type Node = btree.Node
type Entry = btree.Entry

const (
	MaxKeySize   = btree.MaxKeySize
	MaxValueSize = btree.MaxValueSize
)

// DB represents a collection of buckets persisted to a single file on disk.
// All data access is performed through transactions obtained from the DB.
type DB struct {
	path     string
	file     *os.File
	meta     *page.Meta
	rootNode *Node
	options  *Options
	wal      *wal.WAL
	closed   bool
}

func (db *DB) ensureOpen() error {
	if db.closed {
		return ErrDatabaseNotOpen
	}
	return nil
}

// Open creates and opens a database at the given path.
// If the file does not exist it is created automatically.
func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	if path == "" {
		return nil, errors.New("path required")
	}

	resolvedOptions, err := resolveOptions(options)
	if err != nil {
		return nil, err
	}

	var dbFile *os.File

	if resolvedOptions.ReadOnly {
		dbFile, err = os.OpenFile(path, os.O_RDONLY, mode)
	} else {
		dbFile, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, mode)
	}

	if err != nil {
		return nil, err
	}

	db := &DB{
		path:    path,
		file:    dbFile,
		options: resolvedOptions,
	}

	// if the file is empty, create the meta, otherwise read it
	isNew := !db.hasMeta()
	var mainMetaErr error
	if isNew {
		db.meta = page.NewMeta(int64(db.options.PageSize))
	} else {
		db.meta, err = db.readMeta()
		if err != nil {
			return db.failOpen(fmt.Errorf("read db meta: %w", err))
		}
		if err := db.meta.Validate(); err != nil {
			mainMetaErr = fmt.Errorf("db version is unsupported: %w", err)
			if !errors.Is(err, ErrChecksum) {
				return db.failOpen(mainMetaErr)
			}
		}
		if db.meta.PageSize() < page.MetaSize || db.meta.PageSize() > MaxValueSize {
			if mainMetaErr != nil {
				return db.failOpen(mainMetaErr)
			}
			return db.failOpen(fmt.Errorf("invalid database page size %d: %w", db.meta.PageSize(), ErrInvalid))
		}
	}

	// if there's no WAL, create it. Otherwise, read it
	wal, records, err := db.readOrCreateWal()
	if err != nil {
		return db.failOpen(err)
	}

	db.wal = wal
	if mainMetaErr != nil {
		if len(records) == 0 {
			return db.failOpen(mainMetaErr)
		}
		meta, err := metaFromCommittedWAL(records)
		if err != nil {
			return db.failOpen(mainMetaErr)
		}
		db.meta = meta
	}

	if isNew {
		if err := db.initializeNewDatabase(); err != nil {
			return db.failOpen(err)
		}
	}

	// If the WAL has records, the previous run crashed before checkpointing.
	if err := db.replayWAL(records); err != nil {
		return db.failOpen(err)
	}

	// A writable open re-reads meta from the main file. A
	// read-only open already holds the right meta: from the file when there is no
	// WAL, or from the committed overlay when a WAL was replayed above.
	if !db.options.ReadOnly {
		db.meta, err = db.readMeta()
		if err != nil {
			return db.failOpen(err)
		}
	}

	if err := db.loadRootNode(); err != nil {
		return db.failOpen(err)
	}

	if len(records) > 0 && !db.options.ReadOnly {
		if err := db.wal.Truncate(); err != nil {
			return db.failOpen(err)
		}
	}

	return db, nil
}

func (db *DB) initializeNewDatabase() error {
	if err := db.persistMeta(); err != nil {
		return fmt.Errorf("init meta: %w", err)
	}
	db.rootNode = btree.NewLeafNode(db.meta.Root())
	if err := db.persistNode(db.rootNode); err != nil {
		return fmt.Errorf("init root node: %w", err)
	}
	if err := db.file.Sync(); err != nil {
		return fmt.Errorf("sync new database: %w", err)
	}
	return nil
}

func (db *DB) loadRootNode() error {
	if db.rootNode != nil {
		return nil
	}
	rootNode, err := db.readNode(db.meta.Root())
	if err != nil {
		return err
	}
	if rootNode == nil {
		return fmt.Errorf("read root node: got nil")
	}
	db.rootNode = rootNode
	return nil
}

func (db *DB) closeFiles() error {
	var walErr error
	if db.wal != nil {
		walErr = db.wal.Close()
	}
	var dbErr error
	if db.file != nil {
		dbErr = db.file.Close()
	}
	return errors.Join(walErr, dbErr)
}

func (db *DB) failOpen(err error) (*DB, error) {
	if closeErr := db.closeFiles(); closeErr != nil {
		return nil, errors.Join(err, fmt.Errorf("failed to close database files: %w", closeErr))
	}
	return nil, err
}

// Close releases all database resources.
// All transactions must be closed before closing the database.
func (db *DB) Close() error {
	if db.options.ReadOnly {
		if err := db.closeFiles(); err != nil {
			return err
		}
		db.closed = true
		return nil
	}

	if db.wal != nil {
		if err := db.checkpointWAL(); err != nil {
			return err
		}
		if err := db.wal.Delete(); err != nil {
			return err
		}
	} else {
		err := db.file.Sync()
		if err != nil {
			return err
		}
	}
	if err := db.file.Close(); err != nil {
		return err
	}
	db.closed = true
	return nil
}

// Path returns the path to the currently open database file.
func (db *DB) Path() string {
	return db.path
}

func (db *DB) Put(key []byte, value []byte) error {
	return db.Update(func(tx *Tx) error {
		return tx.Put(key, value)
	})
}

func (db *DB) findLeafNode(rootNode *Node, key []byte) (*Node, error) {
	node := rootNode
	for !node.IsLeaf {
		childIndex, err := node.FindChildIndex(key)
		if err != nil {
			return nil, err
		}
		childNode, err := db.readNode(node.Children[childIndex])
		if err != nil {
			return nil, err
		}
		if childNode == nil {
			return nil, ErrKeyNotFound
		}

		childNode.SetParent(node)
		childNode.Index = childIndex
		node = childNode
	}
	return node, nil
}

func (db *DB) validateTreeEntry(entry Entry) error {
	if len(entry.Key()) == 0 {
		return ErrKeyEmpty
	}
	if len(entry.Key()) > MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(entry.Value()) > MaxValueSize {
		return ErrValueTooLarge
	}
	const encodedLeafHeaderSize = 1 + 4 // IsLeaf byte + uint32 entry count.
	if encodedLeafHeaderSize+entry.EncodedSize(true) > int(db.meta.PageSize()) {
		return fmt.Errorf("%w: key %q, page size %d bytes", ErrEntryTooLargeForPage, entry.Key(), db.meta.PageSize())
	}
	return nil
}

// putTreeEntry inserts entry into the tree that starts at rootNode. It returns
// the current root because a split can create a replacement root. The caller
// owns that root and must adopt it after this function succeeds.
func (db *DB) putTreeEntry(rootNode *Node, entry Entry) (*Node, error) {
	if db.options.ReadOnly {
		return nil, ErrDatabaseReadOnly
	}
	if err := db.validateTreeEntry(entry); err != nil {
		return nil, err
	}

	node, err := db.findLeafNode(rootNode, entry.Key())
	if err != nil {
		return nil, err
	}

	if err := node.InsertEntry(entry); err != nil {
		return nil, err
	}

	if !node.NeedsSplit(db.meta.PageSize()) {
		db.wal.InsertNodeRecord(node)
		return rootNode, nil
	}

	for node.NeedsSplit(db.meta.PageSize()) {
		rightPgid := db.allocate()
		rightNode, _, keyAtSeperatorIndex, err := node.Split(rightPgid, db.meta.PageSize())
		if err != nil {
			if errors.Is(err, ErrNodeNotSaturated) {
				// split() only fails this way when a single entry (or, for a branch,
				// too few entries) already overflows a page on its own: there is no
				// way to divide it into two non-empty halves. Surface a clear error
				// instead of the internal split-precondition failure.
				return nil, fmt.Errorf("%w: key %q, page size %d bytes", ErrEntryTooLargeForPage, entry.Key(), db.meta.PageSize())
			}
			return nil, err
		}

		if node == rootNode {
			newRoot := db.newRootAfterSplit(node, rightNode, keyAtSeperatorIndex)
			db.wal.InsertNodeRecord(newRoot)
			rootNode = newRoot
		} else { // parent is not a root node
			parent := node.Parent()
			parent.InsertSplitChild(node, rightNode, keyAtSeperatorIndex)
			db.wal.InsertNodeRecord(parent)
		}

		db.wal.InsertNodeRecord(node)
		db.wal.InsertNodeRecord(rightNode)

		// The right sibling can itself still overflow a page.
		// So keep splitting it before climbing, so no node is left over a page.
		if rightNode.NeedsSplit(db.meta.PageSize()) {
			node = rightNode
			continue
		}
		node = node.Parent()
	}

	return rootNode, nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	var value []byte
	err := db.View(func(tx *Tx) error {
		var err error
		value, err = tx.Get(key)
		return err
	})
	return value, err
}

func (db *DB) findTreeEntry(rootNode *Node, key []byte) (Entry, bool, error) {
	if len(key) == 0 {
		return Entry{}, false, ErrKeyEmpty
	}
	if len(key) > MaxKeySize {
		return Entry{}, false, ErrKeyTooLarge
	}

	node, err := db.findLeafNode(rootNode, key)
	if err != nil {
		return Entry{}, false, err
	}
	return node.FindEntry(key)
}

func (db *DB) persistNode(node *Node) error {
	offset := int64(node.PageID()) * db.meta.PageSize()
	if _, err := db.file.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek node: %w", err)
	}
	if err := btree.WriteNode(db.file, node, db.meta.PageSize(), true); err != nil {
		return fmt.Errorf("write node: %w", err)
	}
	return nil
}

func (db *DB) hasMeta() bool {
	fi, err := db.file.Stat()
	if err != nil {
		return false
	}
	return fi.Size() > 0
}

func (db *DB) readNode(pgid page.ID) (*Node, error) {
	// check if Node exists in current transaction

	record, ok := db.wal.Lookup(pgid)
	if ok {
		node, err := wal.RecordToNode(&record)
		if err != nil {
			return nil, err
		}
		node.SetPageID(pgid)
		return node, nil
	}

	// Node not found in-memory, so we have to read its full page from the database file.

	offset := int64(pgid) * db.meta.PageSize()
	if _, err := db.file.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	page := make([]byte, db.meta.PageSize())
	if err := fileio.ReadFull(db.file, page); err != nil {
		return nil, err
	}
	node, err := btree.DecodeNode(page)
	if err != nil {
		return nil, err
	}

	node.SetPageID(pgid)
	return node, nil
}

func (db *DB) readMeta() (*page.Meta, error) {
	if _, err := db.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	data := make([]byte, page.MetaSize)
	if err := fileio.ReadFull(db.file, data); err != nil {
		return nil, errors.Join(ErrInvalid, err)
	}
	return page.DecodeMeta(data)
}

func (db *DB) persistMeta() error {
	// recompute checksum
	db.meta.RefreshChecksum()

	if _, err := db.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek meta: %w", err)
	}
	if err := fileio.WriteFull(db.file, page.EncodeMeta(db.meta)); err != nil {
		return fmt.Errorf("write meta: %w", err)
	}
	return nil
}

func (db *DB) newRootAfterSplit(leftNode, rightNode *Node, separator []byte) *Node {
	return btree.NewRootNode(db.allocate(), leftNode, rightNode, separator)
}

func (db *DB) allocate() page.ID {
	return db.meta.Allocate()
}
