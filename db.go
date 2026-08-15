package kvlite

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
)

type Sync uint8

const (
	SYNCHRONOUS_DEFAULT Sync = 0 // Default value when `synchronous` is not defined. It uses `DefaultOptions.synchronous`
	SYNCHRONOUS_FULL    Sync = 1 // Always sync every commit to disk
	SYNCHRONOUS_NORMAL  Sync = 2 // Sync to disk only once we reach checkpointThresholdBytes
)

// Options represents the options that can be set when opening a database.
type Options struct {
	// ReadOnly opens the database in read-only mode.
	ReadOnly bool

	// syncMode defines when we do fsync, every commit (SYNCRONOUS_FULL) vs at checkpoint (SYNCRONOUS_NORMAL)
	synchronous Sync

	// checkpointThresholdBytes defines the threshold for applying the in-memory changes to disk.
	// It controls how often the WAL is checkpointed into the main file, in both sync modes; larger = fewer checkpoints, bigger WAL, longer recovery.
	checkpointThresholdBytes uint32
}

// DefaultOptions is used when nil options are passed to Open.
var DefaultOptions = &Options{
	ReadOnly:                 false,
	synchronous:              SYNCHRONOUS_FULL,
	checkpointThresholdBytes: 1000 * uint32(os.Getpagesize()), // same as SQLite
}

// DB represents a collection of buckets persisted to a single file on disk.
// All data access is performed through transactions obtained from the DB.
type DB struct {
	path     string
	file     *os.File
	meta     *Meta
	rootNode *Node
	options  *Options
	wal      *WAL
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
		dbFile, err = os.OpenFile(path, os.O_RDONLY, 0444)
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

	db.applyOptions(options)

	// if the file is empty, create the meta, otherwise read it
	isNew := !db.hasMeta()
	if isNew {
		db.meta = NewMeta()
	} else {
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
	}

	// if there's no WAL, create it. Otherwise, read it
	wal, records, err := db.readOrCreateWal()
	if err != nil {
		if closeErr := db.Close(); closeErr != nil {
			return nil, errors.Join(err, fmt.Errorf("failed to close database: %w", closeErr))
		}
		return nil, err
	}

	db.wal = wal

	if isNew {
		// A brand-new database writes its meta and empty root straight to the main
		// file, so there is no WAL record to collect here. Any failure below leaves a
		// half-written file, so we surface it instead of returning a broken handle.
		fail := func(err error) (*DB, error) {
			if closeErr := db.Close(); closeErr != nil {
				return nil, errors.Join(err, fmt.Errorf("failed to close database: %w", closeErr))
			}
			return nil, err
		}
		if err := db.persistMeta(); err != nil {
			return fail(fmt.Errorf("init meta: %w", err))
		}
		db.rootNode = db.newLeafNode(db.meta.pgid)
		if err := db.persistNode(db.rootNode); err != nil {
			return fail(fmt.Errorf("init root node: %w", err))
		}
		if err := db.file.Sync(); err != nil {
			return fail(fmt.Errorf("sync new database: %w", err))
		}
	}

	// if the wal has records already, there has been a crash and we must read and parse it's records into our main db file
	if records != nil && !db.options.ReadOnly {
		err = db.ingestWalRecords(records)
		if err != nil {
			if closeErr := db.Close(); closeErr != nil {
				return nil, errors.Join(err, fmt.Errorf("failed to close database: %w", closeErr))
			}
			return nil, err
		}

		err = db.wal.Truncate()
		if err != nil {
			if closeErr := db.Close(); closeErr != nil {
				return nil, errors.Join(err, fmt.Errorf("failed to close database: %w", closeErr))
			}
			return nil, err
		}
	}

	db.meta, err = db.readMeta()
	if err != nil {
		return nil, err
	}

	// Load the root node from the (now WAL-recovered) main file. A freshly created
	// DB already has its root in memory.
	if db.rootNode == nil {
		rootNode, err := db.readNode(db.meta.root)
		if err != nil {
			return nil, err
		}
		if rootNode == nil {
			if closeErr := db.Close(); closeErr != nil {
				return nil, errors.Join(fmt.Errorf("read root node: got nil"), fmt.Errorf("failed to close database: %w", closeErr))
			}
			return nil, fmt.Errorf("read root node: got nil")
		}
		db.rootNode = rootNode
	}

	return db, nil
}

// Close releases all database resources.
// All transactions must be closed before closing the database.
func (db *DB) Close() error {
	if db.options.ReadOnly {
		// A read-only open may still hold a WAL handle (opened O_RDONLY when a "-wal"
		// file was present). Close it too, but always close the main file as well.
		var walErr error
		if db.wal != nil && db.wal.file != nil {
			walErr = db.wal.file.Close()
		}
		return errors.Join(walErr, db.file.Close())
	}

	if db.wal != nil {
		if err := db.wal.checkpoint(); err != nil { // implicitely also fsyncs db.file
			return err
		}
		if err := db.wal.delete(); err != nil {
			return err
		}
	} else {
		err := db.file.Sync()
		if err != nil {
			return err
		}
	}
	return db.file.Close()
}

// Path returns the path to the currently open database file.
func (db *DB) Path() string {
	return db.path
}

func (db *DB) Put(key []byte, value []byte) error {
	node, err := db._put(db.rootNode, key, value, 0, true) // forceCommit is true for single Put() operation
	if err != nil {
		return err
	}
	if node != nil {
		db.rootNode = node
		db.meta.root = node.pgid
	}

	return nil
}

// _put() places a key in it's correct place starting from a root *Node.
// Due to node splitting, it's possible that the the new root (starting from the provided rootNode) is not actually the root of that tree.
// returns (*NewRootNode, error), since it's possible that the root was split within the process.
// if `rootNode != NewRootNode`, please assign it as the new root node of the tree.
func (db *DB) _put(rootNode *Node, key []byte, value []byte, flags uint32, forceCommit bool) (*Node, error) {
	if db.options.ReadOnly {
		return nil, ErrDatabaseReadOnly
	}

	if len(key) == 0 {
		return nil, ErrKeyEmpty
	}
	if len(key) > MaxKeySize {
		return nil, ErrKeyTooLarge
	}
	if len(value) > MaxValueSize {
		return nil, ErrValueTooLarge
	}

	node := rootNode
	for !node.IsLeaf {
		childIndex, err := node.findChildIndex(key)
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

		childNode.parent = node
		childNode.Index = childIndex

		node = childNode
	}

	if err := node.insert(key, value, flags); err != nil {
		return nil, err
	}

	if !node.needsSplit() {
		db.wal.insertNodeRecord(node)
		db.wal.insertMetaRecord(db.meta)

		if !forceCommit {
			return rootNode, nil
		}

		return rootNode, db.wal.persistCollectedRecords()
	}

	for node.needsSplit() {
		rightPgid := db.allocate()
		rightNode, _, keyAtSeperatorIndex, err := node.split(rightPgid)
		if err != nil {
			return nil, err
		}

		if node == rootNode {
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

			db.wal.insertNodeRecord(newRoot)

			rootNode = newRoot
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

			db.wal.insertNodeRecord(parent)
		}

		db.wal.insertNodeRecord(node)
		db.wal.insertNodeRecord(rightNode)

		node = node.parent
	}

	db.wal.insertMetaRecord(db.meta)

	// forceCommit is true for single Put operations, and false for multi Put operations (transaction)
	// for transactions, the persistCollectedRecords() happens elsewhere
	if !forceCommit {
		return rootNode, nil
	}
	return rootNode, db.wal.persistCollectedRecords()
}

func (db *DB) Get(key []byte) ([]byte, error) {
	value, _, err := db._get(db.rootNode, key)
	return value, err
}

func (db *DB) _get(rootNode *Node, key []byte) ([]byte, uint32, error) {
	if len(key) == 0 {
		return nil, 0, ErrKeyEmpty
	}
	if len(key) > MaxKeySize {
		return nil, 0, ErrKeyTooLarge
	}

	node := rootNode
	for !node.IsLeaf {
		childIndex, err := node.findChildIndex(key)
		if err != nil {
			return nil, 0, err
		}
		childNode, err := db.readNode(node.Children[childIndex])
		if err != nil {
			return nil, 0, err
		}
		if childNode == nil {
			return nil, 0, ErrKeyNotFound
		}

		childNode.parent = node
		childNode.Index = childIndex

		node = childNode
	}

	value, flags, err := node.get(key)
	if err != nil {
		return nil, 0, err
	}
	if value == nil {
		return nil, 0, ErrKeyNotFound
	}
	return value, flags, nil
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

func (db *DB) readNode(pgid Pgid) (*Node, error) {
	offset := int64(pgid) * db.meta.pageSize

	// check if Node exists in current transaction
	if record, ok := db.wal.collectedRecords[pgid]; ok {
		node, err := record.toNode()
		if err != nil {
			return nil, err
		}
		node.db = db
		node.pgid = pgid
		return node, nil
	}

	// check if Node has been commited (in an earlier transaction) but not yet checkpointed
	if record, ok := db.wal.overlay[pgid]; ok {
		node, err := record.toNode()
		if err != nil {
			return nil, err
		}
		node.db = db
		node.pgid = pgid
		return node, nil
	}

	// Node not found in-memory, so we have to search in db file

	if _, err := db.file.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	node, err := readNode(db.file)
	if err != nil {
		return nil, err
	}

	node.db = db
	node.pgid = pgid
	return node, nil
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

func (db *DB) readOrCreateWal() (*WAL, *[]Record, error) {
	walPath := db.path + "-wal"
	var walFile *os.File
	var err error

	if db.options != nil && db.options.ReadOnly {
		walFile, err = os.OpenFile(walPath, os.O_RDONLY, 0444)
	} else {
		walFile, err = os.OpenFile(walPath, os.O_RDWR|os.O_CREATE, 0644)
	}

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			walFile = nil
		} else {
			return nil, nil, err
		}
	}

	wal := &WAL{
		db: db, path: walPath, file: walFile, collectedRecords: make(map[Pgid]Record), overlay: make(map[Pgid]Record), nextTxid: 1, checkpointThresholdBytes: db.options.checkpointThresholdBytes, bytesSinceCheckpoint: 0,
	}

	records, err := wal.readRecords()
	if err != nil {
		return nil, nil, err
	}

	if records != nil {
		for _, record := range *records {
			if record.header.txid >= wal.nextTxid {
				wal.nextTxid = record.header.txid + 1
			}
		}
	}

	return wal, records, nil
}

func (db *DB) ingestWalRecords(records *[]Record) error {
	recordsInCommit := make([]Record, 0)
	for _, record := range *records {
		if isRecordCommitMarker(&record) {
			for _, record := range recordsInCommit {
				err := db.wal.applyRecordToDatabase(&record, db.meta.pageSize)
				if err != nil {
					return err
				}
			}
			//clean out commit Records
			recordsInCommit = recordsInCommit[:0]
			continue
		}
		// record is not a commit marker
		recordsInCommit = append(recordsInCommit, record)
	}

	return db.file.Sync()
}

func (db *DB) applyOptions(options *Options) {
	// starting with default options
	opts := *DefaultOptions
	db.options = &opts

	if options == nil {
		return
	}

	// then overriding with any explicitely provided options

	db.options.ReadOnly = options.ReadOnly

	if options.checkpointThresholdBytes > 0 {
		db.options.checkpointThresholdBytes = options.checkpointThresholdBytes
	}
	if options.synchronous != SYNCHRONOUS_DEFAULT {
		db.options.synchronous = options.synchronous
	}
}

func (db *DB) Update(transaction func(tx *Tx) error) error {
	if db.options.ReadOnly {
		return ErrDatabaseReadOnly
	}

	tx := &Tx{db: db, readOnly: false}

	// take snapshot

	bytesSinceCheckpoint := db.wal.bytesSinceCheckpoint
	records := maps.Clone(db.wal.collectedRecords)
	metaSnapshot := *db.meta

	if err := transaction(tx); err != nil {
		// transaction failed, revert back to snapshot
		if rbErr := db.rollbackTransaction(bytesSinceCheckpoint, records, &metaSnapshot); rbErr != nil {
			return errors.Join(err, rbErr)
		}
		return err
	}

	// transaction succeeded
	if err := db.wal.persistCollectedRecords(); err != nil {
		if rbErr := db.rollbackTransaction(bytesSinceCheckpoint, records, &metaSnapshot); rbErr != nil {
			return errors.Join(err, rbErr)
		}
		return err
	}

	return nil
}

func (db *DB) rollbackTransaction(bytesSinceCheckpoint uint32, records map[Pgid]Record, metaSnapshot *Meta) error {
	db.wal.bytesSinceCheckpoint = bytesSinceCheckpoint
	db.wal.collectedRecords = records
	db.meta = metaSnapshot
	root, err := db.readNode(metaSnapshot.root)
	if err != nil {
		return fmt.Errorf("reload root node: %w", err)
	}
	db.rootNode = root
	return nil
}

func (db *DB) View(transaction func(tx *Tx) error) error {
	tx := &Tx{db: db, readOnly: true}

	// take snapshot

	bytesSinceCheckpoint := db.wal.bytesSinceCheckpoint
	records := maps.Clone(db.wal.collectedRecords)
	metaSnapshot := *db.meta

	if err := transaction(tx); err != nil {
		// transaction failed, revert back to snapshot
		if rbErr := db.rollbackTransaction(bytesSinceCheckpoint, records, &metaSnapshot); rbErr != nil {
			return errors.Join(err, rbErr)
		}
		return err
	}

	// transaction succeeded

	return nil
}
