package kvlite

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"os"

	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
)

type Sync uint8

const (
	SyncDefault Sync = iota
	SyncFull
	SyncNormal

	// SQLite checkpoints its WAL after 1,000 pages by default. KVLite uses the
	// same page count to calculate its default byte threshold.
	defaultCheckpointPageCount = 1000
)

// Options represents the options that can be set when opening a database.
type Options struct {
	// ReadOnly opens the database in read-only mode.
	ReadOnly bool

	// PageSize sets the page size for a new database. A zero value uses the
	// operating system page size. An existing database always uses its stored page
	PageSize int

	// Synchronous controls when KVLite syncs the WAL.
	// SyncFull syncs every commit.
	// SyncNormal syncs at a checkpoint or close.
	// SyncDefault uses the value from DefaultOptions.
	Synchronous Sync

	// CheckpointThresholdBytes sets the WAL size that starts a checkpoint.
	// A zero value uses the value from DefaultOptions.
	CheckpointThresholdBytes int64
}

// DefaultOptions is used when nil options are passed to Open.
var DefaultOptions = &Options{
	ReadOnly:                 false,
	PageSize:                 0,
	Synchronous:              SyncFull,
	CheckpointThresholdBytes: defaultCheckpointPageCount * int64(os.Getpagesize()),
}

// DB represents a collection of buckets persisted to a single file on disk.
// All data access is performed through transactions obtained from the DB.
type DB struct {
	path     string
	file     *os.File
	meta     *page.Meta
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
	db.rootNode = newLeafNode(db.meta.Root())
	if err := db.persistNode(db.rootNode); err != nil {
		return fmt.Errorf("init root node: %w", err)
	}
	if err := db.file.Sync(); err != nil {
		return fmt.Errorf("sync new database: %w", err)
	}
	return nil
}

func (db *DB) replayWAL(records []Record) error {
	if len(records) == 0 {
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
		if err := db.closeFiles(); err != nil {
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

func (db *DB) validateTreeEntry(entry Entry) error {
	if len(entry.key) == 0 {
		return ErrKeyEmpty
	}
	if len(entry.key) > MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(entry.value) > MaxValueSize {
		return ErrValueTooLarge
	}
	const encodedLeafHeaderSize = 1 + 4 // IsLeaf byte + uint32 entry count.
	if encodedLeafHeaderSize+entry.encodedSize(true) > int(db.meta.PageSize()) {
		return fmt.Errorf("%w: key %q, page size %d bytes", ErrEntryTooLargeForPage, entry.key, db.meta.PageSize())
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

	node, err := db.findLeafNode(rootNode, entry.key)
	if err != nil {
		return nil, err
	}

	if err := node.insertEntry(entry); err != nil {
		return nil, err
	}

	if !node.needsSplit(db.meta.PageSize()) {
		db.wal.insertNodeRecord(node)
		return rootNode, nil
	}

	for node.needsSplit(db.meta.PageSize()) {
		rightPgid := db.allocate()
		rightNode, _, keyAtSeperatorIndex, err := node.split(rightPgid, db.meta.PageSize())
		if err != nil {
			if errors.Is(err, ErrNodeNotSaturated) {
				// split() only fails this way when a single entry (or, for a branch,
				// too few entries) already overflows a page on its own: there is no
				// way to divide it into two non-empty halves. Surface a clear error
				// instead of the internal split-precondition failure.
				return nil, fmt.Errorf("%w: key %q, page size %d bytes", ErrEntryTooLargeForPage, entry.key, db.meta.PageSize())
			}
			return nil, err
		}

		if node == rootNode {
			newRoot := db.newRootAfterSplit(node, rightNode, keyAtSeperatorIndex)
			db.wal.insertNodeRecord(newRoot)
			rootNode = newRoot
		} else { // parent is not a root node
			parent := node.parent
			parent.insertSplitChild(node, rightNode, keyAtSeperatorIndex)
			db.wal.insertNodeRecord(parent)
		}

		db.wal.insertNodeRecord(node)
		db.wal.insertNodeRecord(rightNode)

		// The right sibling can itself still overflow a page.
		// So keep splitting it before climbing, so no node is left over a page.
		if rightNode.needsSplit(db.meta.PageSize()) {
			node = rightNode
			continue
		}
		node = node.parent
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
	return node.findEntry(key)
}

func (db *DB) persistNode(node *Node) error {
	offset := int64(node.pgid) * db.meta.PageSize()
	if _, err := db.file.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek node: %w", err)
	}
	if err := writeNode(db.file, node, db.meta.PageSize(), true); err != nil {
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
		node.pgid = pgid
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
	node, err := decodeNode(page)
	if err != nil {
		return nil, err
	}

	node.pgid = pgid
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
	rootNode := &Node{
		IsLeaf:   false,
		entries:  []Entry{{key: separator}},
		Children: []page.ID{leftNode.pgid, rightNode.pgid},
		pgid:     db.allocate(),
	}
	leftNode.parent = rootNode
	rightNode.parent = rootNode
	return rootNode
}

func (db *DB) allocate() page.ID {
	return db.meta.Allocate()
}

func (db *DB) readOrCreateWal() (*WAL, []Record, error) {
	walPath := db.path + "-wal"
	var walFile *os.File
	var err error

	if db.options.ReadOnly {
		walFile, err = os.OpenFile(walPath, os.O_RDONLY, 0)
	} else {
		info, statErr := db.file.Stat()
		if statErr != nil {
			return nil, nil, statErr
		}
		walFile, err = os.OpenFile(walPath, os.O_RDWR|os.O_CREATE, info.Mode().Perm())
	}

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			walFile = nil
		} else {
			return nil, nil, err
		}
	}

	wal := &WAL{
		db:                       db,
		path:                     walPath,
		file:                     walFile,
		collectedRecords:         make(map[page.ID]Record),
		overlay:                  make(map[page.ID]Record),
		nextTxid:                 1,
		checkpointThresholdBytes: db.options.CheckpointThresholdBytes,
		bytesSinceCheckpoint:     0,
	}

	records, err := wal.readRecords()
	if err != nil {
		return nil, nil, errors.Join(err, wal.file.Close())
	}

	if len(records) > 0 {
		for _, record := range records {
			if record.header.txid >= wal.nextTxid {
				wal.nextTxid = record.header.txid + 1
			}
		}
	}

	return wal, records, nil
}

func (db *DB) ingestWalRecords(records []Record) error {
	committed, err := committedWALRecords(records)
	if err != nil {
		return err
	}
	for _, record := range committed {
		if err := db.wal.applyRecordToDatabase(&record, db.meta.PageSize()); err != nil {
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

func metaFromCommittedWAL(records []Record) (*page.Meta, error) {
	committed, err := committedWALRecords(records)
	if err != nil {
		return nil, err
	}
	// starting from the last record downwards to get the most recent version of the meta, then early break then
	for index := len(committed) - 1; index >= 0; index-- {
		record := committed[index]
		if record.header.recordType != recordTypeMeta || record.header.pgid != page.MetaID {
			continue
		}
		meta, err := page.DecodeMeta(record.pageContent)
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
func (db *DB) loadCommittedIntoOverlay(records []Record) error {
	committed, err := committedWALRecords(records)
	if err != nil {
		return err
	}
	for _, record := range committed {
		db.wal.overlay[record.header.pgid] = record
	}

	if metaRecord, ok := db.wal.overlay[page.MetaID]; ok {
		meta, err := page.DecodeMeta(metaRecord.pageContent)
		if err != nil {
			return fmt.Errorf("read committed meta from WAL: %w", err)
		}
		db.meta = meta
	}
	return nil
}

func resolveOptions(options *Options) (*Options, error) {
	resolved := *DefaultOptions
	if options != nil {
		resolved.ReadOnly = options.ReadOnly
		resolved.PageSize = options.PageSize
		if options.Synchronous != SyncDefault {
			resolved.Synchronous = options.Synchronous
		}
		if options.CheckpointThresholdBytes != 0 {
			resolved.CheckpointThresholdBytes = options.CheckpointThresholdBytes
		}
	}

	if resolved.PageSize == 0 {
		resolved.PageSize = os.Getpagesize()
	}
	if resolved.PageSize < page.MetaSize || resolved.PageSize > MaxValueSize {
		return nil, fmt.Errorf("invalid database page size %d: %w", resolved.PageSize, ErrInvalid)
	}

	if resolved.Synchronous == SyncDefault {
		resolved.Synchronous = SyncFull
	}
	if resolved.Synchronous != SyncFull && resolved.Synchronous != SyncNormal {
		return nil, fmt.Errorf("invalid synchronous mode %d: %w", resolved.Synchronous, ErrInvalid)
	}
	if resolved.CheckpointThresholdBytes <= 0 {
		return nil, fmt.Errorf("invalid checkpoint threshold %d: %w", resolved.CheckpointThresholdBytes, ErrInvalid)
	}

	return &resolved, nil
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
	root, err := db.readNode(snapshot.meta.Root())
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
