package kvlite

import (
	"bytes"
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
	var mainMetaErr error
	if isNew {
		db.meta = NewMeta()
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
		if db.meta.pageSize <= 0 || db.meta.pageSize > MaxValueSize {
			if mainMetaErr != nil {
				return db.failOpen(mainMetaErr)
			}
			return db.failOpen(fmt.Errorf("invalid database page size %d: %w", db.meta.pageSize, ErrInvalid))
		}
	}

	// if there's no WAL, create it. Otherwise, read it
	wal, records, err := db.readOrCreateWal()
	if err != nil {
		return db.failOpen(err)
	}

	db.wal = wal
	if mainMetaErr != nil {
		if records == nil {
			return db.failOpen(mainMetaErr)
		}
		meta, err := metaFromCommittedWAL(*records)
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

	if records != nil && !db.options.ReadOnly {
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
	db.rootNode = db.newLeafNode(db.meta.pgid)
	if err := db.persistNode(db.rootNode); err != nil {
		return fmt.Errorf("init root node: %w", err)
	}
	if err := db.file.Sync(); err != nil {
		return fmt.Errorf("sync new database: %w", err)
	}
	return nil
}

func (db *DB) replayWAL(records *[]Record) error {
	if records == nil {
		return nil
	}
	if db.options.ReadOnly {
		return db.loadCommittedIntoOverlay(records)
	}
	return db.ingestWalRecords(records)
}

func (db *DB) loadRootNode() error {
	if db.rootNode != nil {
		return nil
	}
	rootNode, err := db.readNode(db.meta.root)
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
	if db.wal != nil && db.wal.file != nil {
		walErr = db.wal.file.Close()
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
		// A read-only open may still hold a WAL handle (opened O_RDONLY when a "-wal"
		// file was present). Close it too, but always close the main file as well.
		var walErr error
		if db.wal != nil && db.wal.file != nil {
			walErr = db.wal.file.Close()
		}
		if err := errors.Join(walErr, db.file.Close()); err != nil {
			return err
		}
		db.closed = true
		return nil
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
	if err := db.ensureOpen(); err != nil {
		return err
	}
	return db.Update(func(tx *Tx) error {
		return tx.Put(key, value)
	})
}

func (db *DB) findLeafNode(rootNode *Node, key []byte) (*Node, error) {
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
	return node, nil
}

func (db *DB) validatePutEntry(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > MaxValueSize {
		return ErrValueTooLarge
	}
	entry := Entry{key: key, value: value}
	const encodedLeafHeaderSize = 1 + 4 // IsLeaf byte + uint32 entry count.
	if encodedLeafHeaderSize+entry.encodedSize(true) > int(db.meta.pageSize) {
		return fmt.Errorf("%w: key %q, page size %d bytes", ErrEntryTooLargeForPage, key, db.meta.pageSize)
	}
	return nil
}

// _put() places a key in it's correct place starting from a root *Node.
// Due to node splitting, it's possible that the the new root (starting from the provided rootNode) is not actually the root of that tree.
// returns (*NewRootNode, error), since it's possible that the root was split within the process.
// if `rootNode != NewRootNode`, please assign it as the new root node of the tree.
func (db *DB) _put(rootNode *Node, key []byte, value []byte, flags uint32) (*Node, error) {
	if db.options.ReadOnly {
		return nil, ErrDatabaseReadOnly
	}
	if err := db.validatePutEntry(key, value); err != nil {
		return nil, err
	}

	// When we operate on the database's own tree (rather than a bucket sub-tree),
	// a root split must update db.meta.root *before* we collect the meta record
	// below. Otherwise the record captures the pre-split root, and a commit that
	// ends on a root split persists a meta pointing at the old root — reopen then
	// loads a root that holds only the left half of the split.
	isTopLevel := rootNode == db.rootNode

	node, err := db.findLeafNode(rootNode, key)
	if err != nil {
		return nil, err
	}

	if err := node.insert(key, value, flags); err != nil {
		return nil, err
	}

	if !node.needsSplit() {
		db.wal.insertNodeRecord(node)
		db.wal.insertMetaRecord(db.meta)
		return rootNode, nil
	}

	for node.needsSplit() {
		rightPgid := db.allocate()
		rightNode, _, keyAtSeperatorIndex, err := node.split(rightPgid)
		if err != nil {
			if errors.Is(err, ErrNodeNotSaturated) {
				// split() only fails this way when a single entry (or, for a branch,
				// too few entries) already overflows a page on its own: there is no
				// way to divide it into two non-empty halves. Surface a clear error
				// instead of the internal split-precondition failure.
				return nil, fmt.Errorf("%w: key %q, page size %d bytes", ErrEntryTooLargeForPage, key, db.meta.pageSize)
			}
			return nil, err
		}

		if node == rootNode {
			newRoot := db.newRootAfterSplit(node, rightNode, keyAtSeperatorIndex)
			db.wal.insertNodeRecord(newRoot)
			rootNode = newRoot

			// Adopt the new root now, so the meta record collected below records it.
			if isTopLevel {
				db.rootNode = newRoot
				db.meta.root = newRoot.pgid
			}
		} else { // parent is not a root node
			parent := node.parent
			parent.insertSplitChild(node, rightNode, keyAtSeperatorIndex)
			db.wal.insertNodeRecord(parent)
		}

		db.wal.insertNodeRecord(node)
		db.wal.insertNodeRecord(rightNode)

		// The right sibling can itself still overflow a page.
		// So keep splitting it before climbing, so no node is left over a page.
		if rightNode.needsSplit() {
			node = rightNode
			continue
		}
		node = node.parent
	}

	db.wal.insertMetaRecord(db.meta)
	return rootNode, nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	var value []byte
	err := db.View(func(tx *Tx) error {
		var err error
		value, err = tx.Get(key)
		return err
	})
	return value, err
}

func (db *DB) _get(rootNode *Node, key []byte) ([]byte, uint32, error) {
	if len(key) == 0 {
		return nil, 0, ErrKeyEmpty
	}
	if len(key) > MaxKeySize {
		return nil, 0, ErrKeyTooLarge
	}

	node, err := db.findLeafNode(rootNode, key)
	if err != nil {
		return nil, 0, err
	}

	value, flags, found, err := node.get(key)
	if err != nil {
		return nil, 0, err
	}
	if !found {
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
	// check if Node exists in current transaction

	record, ok := db.wal.collectedRecords[pgid]
	if !ok {
		// Node is not in current transaction.
		// let's check if Node has been commited (in an earlier transaction) but not yet checkpointed
		record, ok = db.wal.overlay[pgid]
	}
	if ok {
		node, err := record.toNode()
		if err != nil {
			return nil, err
		}
		node.db = db
		node.pgid = pgid
		return node, nil
	}

	// Node not found in-memory, so we have to read its full page from the database file.

	offset := int64(pgid) * db.meta.pageSize
	if _, err := db.file.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	page := make([]byte, db.meta.pageSize)
	if _, err := io.ReadFull(db.file, page); err != nil {
		return nil, err
	}
	node, err := readNode(bytes.NewReader(page))
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

func (db *DB) newRootAfterSplit(leftNode, rightNode *Node, separator []byte) *Node {
	rootNode := &Node{
		db:       db,
		IsLeaf:   false,
		entries:  []Entry{{key: separator}},
		Children: []Pgid{leftNode.pgid, rightNode.pgid},
		pgid:     db.allocate(),
	}
	leftNode.parent = rootNode
	rightNode.parent = rootNode
	return rootNode
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
		return nil, nil, errors.Join(err, wal.file.Close())
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
	committed, err := committedWALRecords(*records)
	if err != nil {
		return err
	}
	for _, record := range committed {
		if err := db.wal.applyRecordToDatabase(&record, db.meta.pageSize); err != nil {
			return err
		}
	}

	return db.file.Sync()
}

func committedWALRecords(records []Record) ([]Record, error) {
	committed := make([]Record, 0, len(records))
	pending := make([]Record, 0)

	for index, record := range records {
		if !isRecordCommitMarker(&record) {
			pending = append(pending, record)
			continue
		}

		matches := len(pending) > 0
		for _, candidate := range pending {
			if candidate.header.txid != record.header.txid {
				matches = false
				break
			}
		}
		if !matches {
			if index == len(records)-1 {
				return committed, nil
			}
			return nil, ErrInvalid
		}

		committed = append(committed, pending...)
		pending = pending[:0]
	}

	return committed, nil
}

func metaFromCommittedWAL(records []Record) (*Meta, error) {
	committed, err := committedWALRecords(records)
	if err != nil {
		return nil, err
	}
	// starting from the last record downwards to get the most recent version of the meta, then early break then
	for index := len(committed) - 1; index >= 0; index-- {
		record := committed[index]
		if record.header.recordType != recordTypeMeta || record.header.pgid != metaPgid {
			continue
		}
		meta, err := readMeta(bytes.NewReader(record.pageContent))
		if err != nil {
			return nil, err
		}
		if err := meta.Validate(); err != nil {
			return nil, err
		}
		return meta, nil
	}
	return nil, ErrInvalid
}

// loadCommittedIntoOverlay replays a crashed WAL into the in-memory overlay instead
// of the main file. It is the read-only counterpart to ingestWalRecords: a read-only
// handle may not write the main file, yet it must still expose every committed page.
// It groups records by commit marker (so a torn, uncommitted tail is dropped), keeps
// the latest page per pgid, and adopts the committed meta (page 0) so reads resolve
// the recovered root.
func (db *DB) loadCommittedIntoOverlay(records *[]Record) error {
	committed, err := committedWALRecords(*records)
	if err != nil {
		return err
	}
	for _, record := range committed {
		db.wal.overlay[record.header.pgid] = record
	}

	if metaRecord, ok := db.wal.overlay[metaPgid]; ok {
		meta, err := readMeta(bytes.NewReader(metaRecord.pageContent))
		if err != nil {
			return fmt.Errorf("read committed meta from WAL: %w", err)
		}
		db.meta = meta
	}
	return nil
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
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.options.ReadOnly {
		return ErrDatabaseReadOnly
	}

	tx := &Tx{db: db, readOnly: false}
	defer func() { tx.closed = true }()

	snapshot, err := db.snapshotWriteTransaction()
	if err != nil {
		return err
	}
	callbackReturned := false
	defer func() {
		if callbackReturned {
			return
		}

		panicValue := recover() // nil when there's no panic
		rollbackErr := db.rollbackWriteTransaction(&snapshot)
		if rollbackErr != nil {
			if panicValue != nil {
				panic(errors.Join(fmt.Errorf("transaction panic: %v", panicValue), rollbackErr))
			}
			panic(rollbackErr)
		}
		if panicValue != nil {
			panic(panicValue)
		}
	}()

	transactionErr := transaction(tx)
	callbackReturned = true
	if transactionErr != nil {
		// transaction failed, revert back to snapshot
		if rbErr := db.rollbackWriteTransaction(&snapshot); rbErr != nil {
			return errors.Join(transactionErr, rbErr)
		}
		return transactionErr
	}

	// transaction succeeded
	if err := db.wal.persistCollectedRecords(); err != nil {
		if rbErr := db.rollbackWriteTransaction(&snapshot); rbErr != nil {
			return errors.Join(err, rbErr)
		}
		return err
	}

	return nil
}

func (db *DB) snapshotWriteTransaction() (writeTransactionSnapshot, error) {
	walOffset, err := db.wal.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return writeTransactionSnapshot{}, err
	}
	return writeTransactionSnapshot{
		bytesSinceCheckpoint: db.wal.bytesSinceCheckpoint,
		collectedRecords:     maps.Clone(db.wal.collectedRecords),
		overlay:              maps.Clone(db.wal.overlay),
		meta:                 *db.meta,
		walOffset:            walOffset,
		nextTxid:             db.wal.nextTxid,
		hasUnsyncedWrites:    db.wal.hasUnsyncedWrites,
	}, nil
}

func (db *DB) rollbackTransaction(snapshot *writeTransactionSnapshot) error {
	db.wal.bytesSinceCheckpoint = snapshot.bytesSinceCheckpoint
	db.wal.collectedRecords = snapshot.collectedRecords
	db.wal.overlay = snapshot.overlay
	db.meta = &snapshot.meta
	root, err := db.readNode(snapshot.meta.root)
	if err != nil {
		return fmt.Errorf("reload root node: %w", err)
	}
	db.rootNode = root
	return nil
}

func (db *DB) rollbackWriteTransaction(snapshot *writeTransactionSnapshot) error {
	memoryErr := db.rollbackTransaction(snapshot)
	db.wal.nextTxid = snapshot.nextTxid
	if err := db.wal.file.Truncate(snapshot.walOffset); err != nil {
		return errors.Join(memoryErr, fmt.Errorf("truncate failed WAL transaction: %w", err))
	}
	if _, err := db.wal.file.Seek(snapshot.walOffset, io.SeekStart); err != nil {
		return errors.Join(memoryErr, fmt.Errorf("rewind after failed WAL transaction: %w", err))
	}
	db.wal.hasUnsyncedWrites = snapshot.hasUnsyncedWrites
	return memoryErr
}

func (db *DB) View(transaction func(tx *Tx) error) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	tx := &Tx{db: db, readOnly: true}
	defer func() { tx.closed = true }()
	return transaction(tx)
}
