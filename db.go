package kvlite

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

const (
	MaxKeySize              = btree.MaxKeySize
	MaxValueSize            = btree.MaxValueSize
	maxCachedWriteNodePages = 1024
)

type writeNodeCacheEntry struct {
	node *btree.Node // Nil means the decoded image was invalidated.
	// referenced is the CLOCK bit. Read-only lookups set it while they hold only the read lock of writeNodesMu, so it is atomic.
	referenced atomic.Bool
}

// DB is an open handle to a KVLite database in a file or in memory. Create it with [Open] or [OpenMemory]. Its zero value is not usable, and it must not be copied. Its methods accept concurrent calls. Read-only transaction callbacks can run together, but a write callback runs alone. Callers access named buckets through managed transactions or the [DB.Put], [DB.Get], [DB.Delete], and [DB.DeleteBucket] methods.
type DB struct {
	path              string
	fs                fileio.FS // the file system of the database files; nil for a memory database
	file              fileio.File
	meta              *page.Meta
	rootNode          *btree.Node
	memoryNodes       map[page.ID]*btree.Node          // A non-nil map owns all committed nodes of a memory database. It never evicts a live node.
	writeNodes        map[page.ID]*writeNodeCacheEntry // Each committed node refers only to stable heap bytes that later write clones can share.
	writeNodesMu      sync.RWMutex                     // Protects cache access by concurrent readers. Writers already hold operationMu exclusively.
	writeNodeSlots    []page.ID
	nextWriteNodeSlot int
	mappedFile        []byte
	mappedBacking     []byte // full writable mapping; mappedFile exposes only the real file length
	mappedWritable    bool
	directWriter      directMappedFile
	mainWriteFailure  error // A failed main-file write or explicit sync stops later data operations.
	options           *Options
	wal               *wal.WAL
	operationMu       sync.RWMutex
	activeBackups     int           // Protected by operationMu. A backup keeps the main file unchanged.
	backupsDone       chan struct{} // Closed under operationMu when the last active backup finishes.
	lifecycleMu       sync.RWMutex
	activeOperations  sync.WaitGroup
	writeRequests     chan *writeRequest
	stopWriteBatcher  chan struct{}
	writeBatcherDone  chan struct{}
	closing           bool
	closed            bool
	stats             Stats
	allocation        *allocationBitmap
}

func (db *DB) ensureOpen() error {
	if db.closed || db.closing {
		return ErrDatabaseNotOpen
	}
	return nil
}

func (db *DB) beginOperation() error {
	db.lifecycleMu.RLock()
	defer db.lifecycleMu.RUnlock()
	if err := db.ensureOpen(); err != nil {
		return err
	}
	db.activeOperations.Add(1)
	return nil
}

// beginWriteOperation starts an operation and rejects read-only handles.
// A successful caller must call endOperation.
func (db *DB) beginWriteOperation() error {
	if err := db.beginOperation(); err != nil {
		return err
	}
	if db.options.ReadOnly {
		db.endOperation()
		return ErrDatabaseReadOnly
	}
	return nil
}

func (db *DB) endOperation() {
	db.activeOperations.Done()
}

// writeFailure returns the WAL or main-file error that stopped this handle, or nil.
// After that failure, data operations must stop because the saved state is unknown.
// The caller must hold operationMu.
func (db *DB) writeFailure() error {
	if db.mainWriteFailure != nil {
		return db.mainWriteFailure
	}
	if db.wal == nil {
		return nil
	}
	return db.wal.AppendFailure()
}

// Open opens the database at path. It returns [ErrPathRequired] for an empty path. It selects the valid metadata copy with the highest generation. It also recovers committed write-ahead log data before it returns. If options is nil, Open uses KVLite's fixed defaults.
//
// A writable Open creates the database when it does not exist. The new file uses mode subject to the process umask, and a new write-ahead log uses the resulting database permissions.
// With the WAL enabled, Open commits a new database to the WAL and checkpoints it. In [SyncFull] and [SyncNormal], a crash during creation leaves an empty file or a database that the next Open recovers.
// With [SyncNone] and [Options.DisableWAL] enabled, Open writes the new database directly. A process crash during creation can leave a corrupt file. Existing WAL data is recovered before direct writes start.
// A read-only Open requires an existing database and does not create either file. An empty file returns an error that matches [os.ErrNotExist].
// With [SyncFull] or [SyncNormal], the first write-ahead log sync of a writable handle also syncs the parent directory.
// That sync runs before a commit or a checkpoint depends on the file names, so a handle that does not write does not sync the directory.
// The file system must support directory sync.
// Windows skips directory sync.
// Open follows symbolic links and keeps the write-ahead log beside the target file.
//
// WAL recovery stops at the first invalid record or transaction.
// It uses only complete transactions before that point.
// With [SyncFull] or [SyncNormal], writable recovery syncs the WAL and main file before it clears the WAL.
// With [SyncNone], recovery does not issue storage sync calls.
//
// A new database uses the operating system page size. Open uses the stored page size when it reopens that database.
//
// A read-only Open takes a shared database-file lock, so several read-only handles can open the database together. A writable Open takes an exclusive lock. Open waits for a conflicting handle to close unless a positive [Options.LockTimeout] expires.
//
// The caller must call [DB.Close] when the database is no longer needed and must check its error.
// Use [OpenMemory] to create a database with no file operations.
func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	return open(path, mode, options, fileio.OSFS{})
}

// open opens the database files through fsys.
// Tests use a simulated FS to inject storage faults and power failures.
func open(path string, mode os.FileMode, options *Options, fsys fileio.FS) (*DB, error) {
	if path == "" {
		return nil, ErrPathRequired
	}

	resolvedOptions, err := resolveOptions(options, int64(fsys.PageSize()))
	if err != nil {
		return nil, err
	}
	filePath, err := fsys.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve database path: %w", err)
	}

	var dbFile fileio.File

	if resolvedOptions.ReadOnly {
		dbFile, err = fsys.OpenFile(filePath, os.O_RDONLY, mode)
	} else {
		dbFile, err = fsys.OpenFile(filePath, os.O_RDWR|os.O_CREATE, mode)
	}

	if err != nil {
		return nil, err
	}

	db := &DB{
		path:       path,
		fs:         fsys,
		file:       dbFile,
		options:    resolvedOptions,
		writeNodes: make(map[page.ID]*writeNodeCacheEntry),
	}
	// The main file descriptor owns the process lock. failOpen and Close release the lock when they close this file.
	// A file of a simulated test FS has no descriptor and no lock.
	if osFile, ok := db.file.(*os.File); ok {
		if err := lockDatabaseFile(osFile, db.options.ReadOnly, db.options.LockTimeout); err != nil {
			return db.failOpen(err)
		}
	}
	filePath, err = fsys.EvalSymlinks(filePath)
	if err != nil {
		return db.failOpen(fmt.Errorf("resolve database links: %w", err))
	}
	hasMeta, err := db.hasMeta()
	if err != nil {
		return db.failOpen(fmt.Errorf("read database file size: %w", err))
	}
	isNew := !hasMeta
	// An empty main file holds no database, so a WAL next to it comes from a deleted database or from a creation that did not finish.
	// The creating Open checkpoints and syncs the main file before it returns, so that WAL holds no acknowledged commit.
	// So Open ignores that WAL, as SQLite deletes the WAL of an empty database file (pagerOpenWalIfPresent).
	if isNew && db.options.ReadOnly {
		// A crash while Open creates the database can leave an empty file, and a read-only handle cannot create the database.
		return db.failOpen(fmt.Errorf("database file is empty: %w", os.ErrNotExist))
	}
	var mainMetaErr error
	var walPageSize int64
	if isNew {
		db.meta = page.NewMeta(db.options.pageSize)
		db.meta.SetDataPageChecksums(db.options.DataPageChecksums)
		walPageSize = db.meta.PageSize()
	} else {
		db.meta, walPageSize, mainMetaErr = db.readValidMainMeta()
		if mainMetaErr != nil {
			mainMetaErr = fmt.Errorf("read db meta: %w", mainMetaErr)
		}
		// An unsupported main-file format can use another WAL layout. Current-format WAL data must not replace it.
		if errors.Is(mainMetaErr, page.ErrVersionMismatch) {
			return db.failOpen(mainMetaErr)
		}
		if walPageSize == 0 {
			return db.failOpen(mainMetaErr)
		}
	}

	var syncWALDirectory func() error
	if !db.options.ReadOnly && db.options.Synchronous != SyncNone {
		// Close removes the WAL, so a writable Open can create its name again. A file sync does not persist a new file name.
		// The first WAL sync calls this function, so both names reach storage before a commit or a checkpoint depends on them.
		syncWALDirectory = func() error {
			if err := fsys.SyncDir(filepath.Dir(filePath)); err != nil {
				return fmt.Errorf("sync database directory: %w", err)
			}
			return nil
		}
	}
	wal, records, walFound, err := db.readOrCreateWal(fsys, filePath, walPageSize, syncWALDirectory, isNew)
	if err != nil {
		return db.failOpen(err)
	}

	db.wal = wal
	committed := committedWALRecords(records)
	committedMeta, committedMetaErr := metaFromCommittedRecords(committed)
	if mainMetaErr != nil && (len(records) == 0 || committedMetaErr != nil || committedMeta == nil) {
		// Only validated metadata from a complete WAL transaction can replace invalid main-file metadata.
		return db.failOpen(mainMetaErr)
	}
	if committedMetaErr != nil {
		return db.failOpen(fmt.Errorf("read committed meta from WAL: %w", committedMetaErr))
	}
	if committedMeta != nil {
		// The WAL decoded its records with walPageSize, and its checkpoint places pages with it.
		// walPageSize can come from a metadata copy that failed its checksum, so validated metadata must confirm it before recovery writes.
		if committedMeta.PageSize() != walPageSize {
			return db.failOpen(fmt.Errorf("WAL metadata page size %d does not match the page size %d that Open used for the WAL: %w", committedMeta.PageSize(), walPageSize, page.ErrInvalid))
		}
		db.meta = committedMeta
	}

	if isNew {
		if err := db.initializeNewDatabase(); err != nil {
			return db.failOpen(err)
		}
	}
	// Check the final allocation view before writable recovery can copy WAL pages into the main file and clear the WAL.
	db.allocation, err = db.readAllocationBitmap(db.meta, committed)
	if err != nil {
		return db.failOpen(err)
	}

	// A failed sync of an earlier process can drop WAL writes from storage while the cache keeps them.
	// Write the records again, so the sync before the checkpoint stores them, and the WAL can repair a torn main-file write of recovery.
	if !db.options.ReadOnly && db.options.Synchronous != SyncNone && len(committed) > 0 {
		if err := db.wal.RewriteRecords(records); err != nil {
			return db.failOpen(err)
		}
	}
	// A remaining WAL can contain committed data from an unclean close, an interrupted checkpoint, or failed cleanup. Writable recovery copies committed records to the main file. Read-only recovery keeps them in the WAL overlay.
	if err := db.replayWAL(committed); err != nil {
		return db.failOpen(err)
	}
	// Writable recovery can replace either main-file metadata copy. Select and validate the copies again before a root page is read. Read-only recovery already selected main-file metadata or adopted validated WAL metadata.
	if !db.options.ReadOnly {
		db.meta, _, err = db.readValidMainMeta()
		if err != nil {
			return db.failOpen(err)
		}
	}
	if err := db.loadRootNode(); err != nil {
		return db.failOpen(err)
	}
	if err := db.mapMainFile(); err != nil {
		return db.failOpen(err)
	}

	if !db.options.ReadOnly {
		if err := db.wal.Truncate(); err != nil {
			return db.failOpen(err)
		}
		// Close removes the WAL, so a WAL that Open finds comes from a process that did not close the database.
		// That process can have changed the WAL in the cache only, for example with the rollback truncate of a failed commit before the process crashed.
		// Storage can then still hold records that this Open did not apply, and a power failure can bring them back after readers saw the state without them.
		// So Open syncs the cleared WAL before it returns, and its answer about a commit with an unknown result is final.
		// PostgreSQL syncs the data directory after a crash for the same reason (SyncDataDirectory in StartupXLOG).
		if walFound {
			if err := db.wal.SyncTruncate(); err != nil {
				return db.failOpen(err)
			}
		}
	}
	db.startWriteBatcher()

	return db, nil
}

// initializeNewDatabase writes the metadata, bitmap, and root of an empty database.
// With the WAL enabled, it commits them to the WAL before the main file.
// Otherwise it writes the main file directly and can leave partial data after a crash.
func (db *DB) initializeNewDatabase() error {
	allocation := newAllocationBitmap(db.meta.PageSize())
	db.rootNode = btree.NewLeafNode(db.meta.Root())
	db.meta.RefreshChecksum()
	nodes := []wal.NodeRecord{{Final: db.rootNode}}
	pages := make([]wal.AllocationRecord, len(allocation.segments))
	for i := range allocation.segments {
		pages[i] = wal.AllocationRecord{PageID: allocation.segmentPageID(i), Payload: allocation.encodeSegment(i)}
	}
	if db.options.DisableWAL {
		size, err := db.logicalFileSize()
		if err != nil {
			return err
		}
		if err := db.wal.WriteDirect(db.file, size, db.meta.DataPageChecksums(), nodes, pages, page.EncodeMeta(db.meta)); err != nil {
			return fmt.Errorf("%w: initialize database: %w", ErrDatabaseWriteFailed, err)
		}
		return nil
	}
	if _, err := db.wal.Commit(nodes, pages, page.EncodeMeta(db.meta), nil); err != nil {
		return fmt.Errorf("commit new database: %w", err)
	}
	size, err := db.logicalFileSize()
	if err != nil {
		return err
	}
	if err := db.wal.Checkpoint(db.file, size, db.meta.DataPageChecksums()); err != nil {
		return fmt.Errorf("checkpoint new database: %w", err)
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
		return fmt.Errorf("read root node: got nil: %w", page.ErrInvalid)
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
	mappingErr := db.unmapMainFile()
	closeErr := db.closeFiles()
	if mappingErr != nil || closeErr != nil {
		if mappingErr != nil {
			mappingErr = fmt.Errorf("failed to unmap database: %w", mappingErr)
		}
		if closeErr != nil {
			closeErr = fmt.Errorf("failed to close database files: %w", closeErr)
		}
		return nil, errors.Join(err, mappingErr, closeErr)
	}
	return nil, err
}

// Sync flushes committed database and WAL data to storage without closing the database or forcing a checkpoint. It also syncs their directory, except on Windows.
// Sync works in [SyncFull], [SyncNormal], and [SyncNone], regardless of when the configured mode normally synchronizes writes.
// It does not change the synchronization mode for later writes. In SyncNone it makes completed writes durable, but a later unsynchronized checkpoint or direct write can still corrupt earlier data if it crashes halfway through.
// A memory database has nothing to flush and returns nil.
//
// Sync waits for active transaction callbacks. Do not call it from a View or Update callback.
// Pending writes that have not committed are not covered. A read-only handle returns [ErrDatabaseReadOnly], and a closed handle returns [ErrDatabaseNotOpen].
// A storage synchronization failure stops later data operations with [ErrWALWriteFailed] or [ErrDatabaseWriteFailed]. Close and reopen the database before using it again.
func (db *DB) Sync() error {
	if err := db.beginWriteOperation(); err != nil {
		return err
	}
	defer db.endOperation()
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	if err := db.writeFailure(); err != nil {
		return err
	}
	if db.memoryNodes != nil {
		return nil
	}
	// Persist an empty recovery truncate too, so an older WAL cannot return after main-file writes reach storage.
	if err := db.wal.ForceSync(); err != nil {
		return err
	}
	if err := db.syncMainFile(); err != nil {
		db.mainWriteFailure = fmt.Errorf("%w: %w", ErrDatabaseWriteFailed, err)
		return db.mainWriteFailure
	}
	return nil
}

// Close releases the database resources. For a writable file database, it first checkpoints committed WAL data and removes the WAL. SyncNone adds no storage synchronization at close. After [ErrWALWriteFailed] or [ErrDatabaseWriteFailed], Close only releases resources. A memory database discards all data without file operations.
//
// Close prevents new data operations and waits for active operations to finish. After Close succeeds, data operations return [ErrDatabaseNotOpen], but [DB.Path] remains available. If Close returns an error, the database remains open so the caller can retry, but some cleanup can remain incomplete.
func (db *DB) Close() error {
	db.lifecycleMu.Lock()
	if err := db.ensureOpen(); err != nil {
		db.lifecycleMu.Unlock()
		return err
	}
	db.closing = true
	db.lifecycleMu.Unlock()

	// closing prevents beginOperation from adding another operation while Wait is in progress. All queued updates finish before close touches the files.
	db.activeOperations.Wait()
	db.operationMu.Lock()
	err := db.close()
	db.operationMu.Unlock()

	db.lifecycleMu.Lock()
	if err != nil {
		db.closing = false
		db.lifecycleMu.Unlock()
		return err
	}
	db.closed = true
	db.lifecycleMu.Unlock()
	db.stopWriteBatcherAndWait()
	return nil
}

func (db *DB) close() error {
	if db.memoryNodes != nil {
		db.memoryNodes = nil
		db.rootNode = nil
		db.meta = nil
		db.allocation = nil
		return nil
	}
	// A read-only handle only closes its files.
	// After a failed WAL or direct main-file write, the saved state is unknown, so Close must not write more pages.
	// A retained WAL can be recovered on the next Open. A direct write has no repair log.
	if db.options.ReadOnly || db.writeFailure() != nil {
		if err := db.unmapMainFile(); err != nil {
			return err
		}
		if err := db.closeFiles(); err != nil {
			return err
		}
		return nil
	}

	if db.wal != nil && db.wal.Stats().CommittedRecordCount != 0 {
		if err := db.checkpointWAL(); err != nil {
			return err
		}
	} else if db.options.Synchronous != SyncNone {
		// An unchanged put can leave no WAL records to checkpoint. Sync data inherited from an earlier SyncNone handle too.
		if err := db.syncMainFile(); err != nil {
			return err
		}
	}
	if err := db.unmapMainFile(); err != nil {
		return err
	}
	if db.wal != nil {
		if err := db.wal.Delete(); err != nil {
			return err
		}
	}
	if err := db.file.Close(); err != nil {
		return err
	}
	return nil
}

// syncMainFile flushes a writable mapping before syncing the file and its directory.
// The caller holds operationMu and handles failures according to its operation.
func (db *DB) syncMainFile() error {
	if db.mappedWritable {
		if err := systemSyncMainFile(db.mappedFile); err != nil {
			return fmt.Errorf("sync mapped database pages: %w", err)
		}
	}
	if err := fileio.SyncData(db.file); err != nil {
		return fmt.Errorf("sync database file: %w", err)
	}
	// Persist the current names so a removed older WAL cannot return after a power failure.
	if err := db.fs.SyncDir(filepath.Dir(db.path)); err != nil {
		return fmt.Errorf("sync database directory: %w", err)
	}
	return nil
}

// Path returns the database path passed to [Open]. It returns an empty string for a database created by [OpenMemory]. It does not clean the path or convert it to an absolute path, and it remains available after [DB.Close].
func (db *DB) Path() string {
	return db.path
}

// Put stores value under key in the top-level bucket named bucketName.
// It is equivalent to resolving the bucket and calling [Bucket.Put] inside
// [DB.Update], so it replaces an existing value atomically and returns any
// lookup, write, or commit error. See [Bucket.Put] for entry-size and
// bucket-conflict errors.
//
// The bucket must already exist. An empty bucket name returns
// [ErrBucketNameRequired], and a missing bucket returns [ErrBucketNotFound].
// Put copies key and value before it returns, and a nil value is stored as an
// empty value. If key names a nested bucket, Put returns
// [ErrIncompatibleValue] instead of replacing the bucket.
func (db *DB) Put(bucketName, key, value []byte) error {
	if db.options != nil && db.options.DisableWAL && db.memoryNodes == nil {
		return db.putMapped(bucketName, []KeyValue{{Key: key, Value: value}})
	}
	return db.Update(func(tx *Tx) error { return tx.putManaged(bucketName, key, value) })
}

// PutBatch stores entries in an existing top-level bucket in one write transaction.
// For repeated keys, the last entry supplies the stored value.
// A nil value stores an empty value, and an empty batch has no effect.
// The bucket must exist even when entries is empty.
//
// PutBatch does not change the input order or retain caller bytes after it returns.
// The caller must keep bucketName, entries and their bytes unchanged until it returns.
// Bucket, key and value errors match [DB.Put].
// An error discards the transaction, subject to the commit-failure rules of [DB.Update].
// Use [Bucket.PutBatch] inside a transaction callback.
func (db *DB) PutBatch(bucketName []byte, entries []KeyValue) error {
	if len(entries) == 1 {
		return db.Put(bucketName, entries[0].Key, entries[0].Value)
	}
	if db.options != nil && db.options.DisableWAL && db.memoryNodes == nil {
		return db.putMapped(bucketName, entries)
	}
	return db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(bucketName)
		if err != nil {
			return err
		}
		return bucket.putBatch(entries, true)
	})
}

// putMapped owns the operation lifetime, write lock and backup exclusion for scalar and batch writes.
// Scalar writes retain their existing mapped insert support; batches preflight every replacement before changing bytes.
func (db *DB) putMapped(bucketName []byte, entries []KeyValue) error {
	if err := db.beginWriteOperation(); err != nil {
		return err
	}
	defer db.endOperation()
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	if err := db.prepareDirectWrite(); err != nil {
		return err
	}
	if _, osFile := db.file.(*os.File); len(entries) != 0 && osFile && db.mappedWritable && db.wal.Stats().CommittedRecordCount == 0 {
		var handled bool
		var err error
		if len(entries) == 1 {
			entry := entries[0]
			if len(entry.Key) != 0 && len(entry.Key) <= MaxKeySize && len(entry.Value) <= MaxValueSize {
				handled, err = db.putMappedEntry(bucketName, entry.Key, entry.Value)
			}
		} else {
			handled, err = db.replaceMappedBatch(bucketName, entries)
		}
		if handled || err != nil {
			return err
		}
	}
	return db.updateDirect(func(tx *Tx) error {
		if len(entries) == 1 {
			return tx.putManaged(bucketName, entries[0].Key, entries[0].Value)
		}
		bucket, err := tx.Bucket(bucketName)
		if err != nil {
			return err
		}
		return bucket.putBatch(entries, true)
	})
}

// replaceMappedBatch checks every request before the first mapped value changes.
// Same-size replacements keep every key, descriptor, separator and page span in place.
// The copy pass uses the checked destinations in caller order, so repeated keys keep their last value.
func (db *DB) replaceMappedBatch(bucketName []byte, entries []KeyValue) (bool, error) {
	root, err := db.mappedBucketRoot(bucketName)
	if err != nil {
		return true, err
	}
	if root == 0 {
		return false, nil
	}
	for _, entry := range entries {
		if len(entry.Key) == 0 {
			return true, ErrKeyRequired
		}
		if len(entry.Key) > MaxKeySize {
			return true, ErrKeyTooLarge
		}
		if len(entry.Value) > MaxValueSize {
			return true, ErrValueTooLarge
		}
	}
	// Group nearby destinations without changing the order in which caller values are applied.
	order := make([]int, len(entries))
	for index := range order {
		order[index] = index
	}
	if !slices.IsSortedFunc(entries, func(a, b KeyValue) int { return bytes.Compare(a.Key, b.Key) }) {
		slices.SortFunc(order, func(a, b int) int { return bytes.Compare(entries[a].Key, entries[b].Key) })
	}
	pageSize, checksums := db.meta.PageSize(), db.meta.DataPageChecksums()
	// Retain checked destinations, not decoded nodes. The operation lock keeps every span stable.
	values := make([][]byte, len(entries))
	spans := make([][]byte, len(entries))
	ids := make([]page.ID, len(entries))
	var lastSpan []byte
	var lastID page.ID
	for _, index := range order {
		entry := entries[index]
		if lastSpan != nil {
			// Nearby requests often share a leaf. Its complete body and checksum are already checked.
			current, found, _, err := btree.LookupCheckedNodeSpan(lastSpan, entry.Key, false, false)
			if err != nil {
				return true, err
			}
			if found {
				if current.Flags() != 0 || len(current.Value()) != len(entry.Value) {
					return false, nil
				}
				values[index], spans[index], ids[index] = current.Value(), lastSpan, lastID
				continue
			}
		}
		id := root
		for {
			data, err := db.readMainNodePage(id)
			if err != nil {
				return true, err
			}
			if !db.nodeIsMapped(data, id) {
				return false, nil
			}
			var current btree.Entry
			_, found, child, err := btree.PrepareMappedPut(data, pageSize, entry.Key, len(entry.Value), &current)
			if err != nil {
				return true, err
			}
			if child != 0 {
				id = child
				continue
			}
			if !found || current.Flags() != 0 || len(current.Value()) != len(entry.Value) {
				return false, nil
			}
			values[index], spans[index], ids[index] = current.Value(), data, id
			lastSpan, lastID = data, id
			break
		}
	}
	// Every request is valid before the first copy. No tree lookup, allocation or mapping change follows.
	for index, entry := range entries {
		if !bytes.Equal(values[index], entry.Value) {
			copy(values[index], entry.Value)
		}
	}
	// Sorted key ranges put each leaf's requests together, so seal and invalidate it once.
	for position, index := range order {
		if position+1 < len(order) && ids[index] == ids[order[position+1]] {
			continue
		}
		if err := btree.VerifyNodeIDAndSetChecksum(spans[index], ids[index], checksums); err != nil {
			// Preflight checked the unchanged header. Treat an unexpected failure after copying as a write failure.
			db.mainWriteFailure = fmt.Errorf("%w: %w", ErrDatabaseWriteFailed, err)
			return true, db.mainWriteFailure
		}
		db.invalidateWriteNode(ids[index])
	}
	return true, nil
}

// mappedBucketRoot returns the root of a bucket backed by mapped bytes.
// A zero root requests the normal transaction path when the bucket value is owned memory.
func (db *DB) mappedBucketRoot(bucketName []byte) (page.ID, error) {
	if len(bucketName) == 0 {
		return 0, ErrBucketNameRequired
	}
	entry, found, err := db.findCommittedEntry(db.meta.Root(), bucketName)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, ErrBucketNotFound
	}
	if entry.ValueIsOwned() {
		return 0, nil
	}
	return decodeBucketRootPageID(entry)
}

// nodeIsMapped reports whether checked node bytes alias the main-file mapping.
func (db *DB) nodeIsMapped(data []byte, id page.ID) bool {
	offset := int64(id) * db.meta.PageSize()
	return offset <= int64(len(db.mappedFile)) && int64(len(data)) <= int64(len(db.mappedFile))-offset && &data[0] == &db.mappedFile[offset]
}

// putMappedEntry changes one entry without changing parent separators or node spans.
// The caller excludes readers and active backups with the operation write lock.
func (db *DB) putMappedEntry(bucketName, key, value []byte) (bool, error) {
	id, err := db.mappedBucketRoot(bucketName)
	if err != nil {
		return true, err
	}
	if id == 0 {
		return false, nil
	}
	for {
		data, err := db.readMainNodePage(id)
		if err != nil {
			return true, err
		}
		if !db.nodeIsMapped(data, id) {
			return false, nil
		}
		handled, child, err := btree.PutMappedEntry(data, id, db.meta.PageSize(), db.meta.DataPageChecksums(), key, value)
		if err != nil {
			return true, err
		}
		if child != 0 {
			id = child
			continue
		}
		if handled {
			db.invalidateWriteNode(id)
		}
		return handled, nil
	}
}

// Delete removes one plain key from an existing top-level bucket in one write transaction. Other database operations see the change after this method returns successfully.
//
// A missing key returns nil. A missing bucket returns [ErrBucketNotFound]. A key that names a nested bucket returns [ErrIncompatibleValue]. An empty bucket name returns [ErrBucketNameRequired]. See [Bucket.Delete] for key errors. A read-only database returns [ErrDatabaseReadOnly].
func (db *DB) Delete(bucketName, key []byte) error {
	return db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(bucketName)
		if err != nil {
			return err
		}
		return bucket.Delete(key)
	})
}

// DeleteBatch removes plain keys from an existing top-level bucket in one write transaction.
// Missing and repeated keys have no additional effect, and an empty batch has no effect.
// The bucket must exist even when keys is empty.
//
// DeleteBatch does not change the input order or retain caller bytes after it returns.
// The caller must keep bucketName, keys and their bytes unchanged until it returns.
// Bucket and key errors match [DB.Delete].
// An error discards the transaction, subject to the commit-failure rules of [DB.Update].
// Use [Bucket.DeleteBatch] inside a transaction callback.
func (db *DB) DeleteBatch(bucketName []byte, keys [][]byte) error {
	return db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(bucketName)
		if err != nil {
			return err
		}
		return bucket.DeleteBatch(keys)
	})
}

// DeleteBucket removes an existing top-level bucket and all of its contents in one write transaction.
// It removes nested buckets and releases their pages after the transaction commits.
// A missing bucket returns [ErrBucketNotFound]. An empty name returns [ErrBucketNameRequired]. A read-only database returns [ErrDatabaseReadOnly].
func (db *DB) DeleteBucket(bucketName []byte) error {
	return db.Update(func(tx *Tx) error {
		return tx.DeleteBucket(bucketName)
	})
}

// Get returns an owned copy of the value stored under key in the top-level bucket named bucketName.
// It reads one committed database state without creating a transaction.
//
// The bucket must already exist.
// An empty bucket name returns [ErrBucketNameRequired].
// A missing bucket returns [ErrBucketNotFound].
// Get returns [ErrKeyNotFound] when the key is absent.
// It returns [ErrIncompatibleValue] when the key names a nested bucket.
// The caller can retain and change the returned slice.
// A stored empty value returns a non-nil slice with length zero.
func (db *DB) Get(bucketName, key []byte) ([]byte, error) {
	// Get has no callback: this lock keeps the handle open until its owned result is ready.
	db.lifecycleMu.RLock()
	defer db.lifecycleMu.RUnlock()
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}

	// Keep the root, WAL pages, and mapped bytes unchanged during both lookups.
	db.operationMu.RLock()
	defer db.operationMu.RUnlock()

	return db.getCommittedValue(bucketName, key)
}

// getCommittedValue returns an owned value while the caller holds operationMu
// for reading and keeps the database open through the result copy.
func (db *DB) getCommittedValue(bucketName, key []byte) ([]byte, error) {
	if err := db.writeFailure(); err != nil {
		return nil, err
	}
	if len(bucketName) == 0 {
		return nil, ErrBucketNameRequired
	}
	bucketEntry, found, err := db.findCommittedEntry(db.meta.Root(), bucketName)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrBucketNotFound
	}
	bucketRootPageID, err := decodeBucketRootPageID(bucketEntry)
	if err != nil {
		return nil, err
	}

	entry, found, err := db.findCommittedEntry(bucketRootPageID, key)
	if err != nil {
		return nil, err
	}
	value, err := valueFromEntry(entry, found)
	if err != nil {
		return nil, err
	}
	if entry.ValueIsOwned() {
		return value, nil
	}
	return slices.Clone(value), nil
}

// GetBatch returns copies of the values for keys from one committed state of the named top-level bucket.
// Results follow the input order. Missing keys have nil results; stored empty values have non-nil results of length zero.
// Repeated keys have independent copies, so the caller can retain and change every returned value.
//
// The bucket must exist even when keys is empty. Empty input returns an empty result slice.
// Bucket and key errors match [DB.Get], except that missing keys cause no error. Any error returns no results.
// The caller must keep bucketName, keys and their bytes unchanged until GetBatch returns; their order is not modified.
// Reads wait for active writes. Do not call GetBatch inside a transaction callback; use [Bucket.GetBatch] there.
func (db *DB) GetBatch(bucketName []byte, keys [][]byte) ([][]byte, error) {
	if len(keys) == 1 {
		value, err := db.Get(bucketName, keys[0])
		if err != nil && err != ErrKeyNotFound {
			return nil, err
		}
		return [][]byte{value}, nil
	}
	var values [][]byte
	err := db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(bucketName)
		if err != nil {
			return err
		}
		values, err = bucket.GetBatch(keys)
		if err != nil {
			return err
		}
		for i := range values {
			values[i] = bytes.Clone(values[i])
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return values, nil
}

// readMainBatch reads sorted request positions from the main-file tree without decoding leaf entry lists.
// Each leaf checksum is checked once for its request group. Branch separators partition the requests before recursion.
// The caller holds the operation lock and has excluded WAL overlays and private transaction changes.
// Result bytes borrow the page; DB.GetBatch copies them before ending its View.
func (db *DB) readMainBatch(id page.ID, keys [][]byte, order []int, values [][]byte) error {
	data, err := db.readMainNodePage(id)
	if err != nil {
		return err
	}
	entry, found, child, err := btree.LookupCheckedNodeSpan(data, keys[order[0]], false, db.meta.DataPageChecksums())
	if err != nil {
		return err
	}
	// checking if this node is a leaf
	if child == 0 {
		if found {
			values[order[0]], err = valueFromEntry(entry, true)
			if err != nil {
				return err
			}
		}
		for position := 1; position < len(order); position++ {
			index, previous := order[position], order[position-1]
			if bytes.Equal(keys[index], keys[previous]) {
				values[index] = values[previous]
				continue
			}
			entry, found, _, err := btree.LookupCheckedNodeSpan(data, keys[index], false, false)
			if err != nil {
				return err
			}
			if found {
				values[index], err = valueFromEntry(entry, true)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}
	branch, err := db.readOnlyNode(id)
	if err != nil {
		return err
	}
	for start := 0; start < len(order); {
		index, err := branch.FindChildIndex(keys[order[start]])
		if err != nil {
			return err
		}
		end := len(order)
		if index < branch.EntryCount() {
			separator := branch.EntryAt(index).Key()
			end = start + 1
			for end < len(order) && bytes.Compare(keys[order[end]], separator) < 0 {
				end++
			}
		}
		if err := db.readMainBatch(branch.Children[index], keys, order[start:end], values); err != nil {
			return err
		}
		start = end
	}
	return nil
}

// hasMeta reports whether the main file holds data.
// A Stat error is not an empty file: Open would then write a new database over the old one.
func (db *DB) hasMeta() (bool, error) {
	fi, err := db.file.Stat()
	if err != nil {
		return false, err
	}
	return fi.Size() > 0, nil
}

// readMainPage returns one complete main-file page.
// Mapped bytes stay valid while the caller holds the database operation lock.
func (db *DB) readMainPage(pageID page.ID) ([]byte, error) {
	pageSize := db.meta.PageSize()
	if pageSize <= 0 || uint64(pageID) > uint64(math.MaxInt64)/uint64(pageSize) {
		return nil, fmt.Errorf("read page %d offset: %w", pageID, page.ErrInvalid)
	}
	offset := int64(pageID) * pageSize
	mappedSize := int64(len(db.mappedFile))
	if offset <= mappedSize && pageSize <= mappedSize-offset {
		start := int(offset)
		end := start + int(pageSize)
		return db.mappedFile[start:end:end], nil
	}

	data := make([]byte, int(pageSize))
	n, err := db.file.ReadAt(data, offset)
	if err != nil {
		if errors.Is(err, io.EOF) && n > 0 {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if n != len(data) {
		return nil, io.ErrUnexpectedEOF
	}
	return data, nil
}

// readMainNodePage returns the complete consecutive span of one node.
// Mapped spans borrow the main-file mapping. A large unmapped node needs a head read, then one full-span read.
func (db *DB) readMainNodePage(pageID page.ID) ([]byte, error) {
	data, err := db.readMainPage(pageID)
	if err != nil {
		return nil, err
	}
	count, err := btree.NodeSpan(data, pageID, db.meta.PageSize())
	if err != nil {
		return nil, fmt.Errorf("read node %d: %w", pageID, err)
	}
	if pageID > db.meta.LastPage() || uint64(count) > uint64(db.meta.LastPage())+1-uint64(pageID) {
		return nil, ErrInvalid
	}
	if !db.allocation.ownsNodeSpan(pageID, count) {
		return nil, fmt.Errorf("node %d has free or reserved storage: %w", pageID, ErrInvalid)
	}
	if count > 1 {
		size := int64(count) * db.meta.PageSize()
		offset := int64(pageID) * db.meta.PageSize()
		if offset <= int64(len(db.mappedFile)) && size <= int64(len(db.mappedFile))-offset {
			data = db.mappedFile[offset : offset+size : offset+size]
		} else {
			data = make([]byte, int(size))
			n, e := db.file.ReadAt(data, offset)
			if e != nil {
				if errors.Is(e, io.EOF) && n > 0 {
					return nil, io.ErrUnexpectedEOF
				}
				return nil, e
			}
			if n != len(data) {
				return nil, io.ErrUnexpectedEOF
			}
			// Check the returned header too. This read can return different bytes from the head read.
			actualCount, err := btree.NodeSpan(data, pageID, db.meta.PageSize())
			if err != nil {
				return nil, fmt.Errorf("read node %d: %w", pageID, err)
			}
			if actualCount != count {
				return nil, fmt.Errorf("node %d span changed during read: %w", pageID, ErrInvalid)
			}
		}
	}
	if err := btree.CheckNodeChecksumMode(data, db.meta.DataPageChecksums()); err != nil {
		return nil, fmt.Errorf("read data node %d: %w", pageID, err)
	}
	return data, nil
}

// readNode returns one committed node. A file database gives the WAL image priority over the main-file image.
func (db *DB) readNode(pgid page.ID) (*btree.Node, error) {
	if db.memoryNodes != nil {
		node, ok := db.memoryNodes[pgid]
		if !ok {
			return nil, fmt.Errorf("read memory page %d: %w", pgid, page.ErrInvalid)
		}
		return node, nil
	}
	// A committed WAL image overrides the main-file image until checkpoint cleanup clears the overlay.
	record, ok := db.wal.Lookup(pgid)
	if ok {
		// An encoded record, such as the first page of a continued node, needs a full decode.
		// Each successful commit caches its final nodes, and a failed commit changes neither the WAL nor the cache.
		// So a cached node for this page is the committed version that the WAL record holds.
		if record.Node == nil {
			if node, ok := db.cachedWriteNode(pgid); ok {
				return node, nil
			}
		}
		node, err := db.wal.CommittedPageToNode(&record)
		if err != nil {
			return nil, err
		}
		db.cacheWriteNode(node)
		return node, nil
	}
	if node, ok := db.cachedWriteNode(pgid); ok {
		return node, nil
	}

	data, err := db.readMainNodePage(pgid)
	if err != nil {
		return nil, err
	}
	node, err := btree.DecodeNode(slices.Clone(data), pgid, db.meta.PageSize(), db.readMainNodePage, db.meta.DataPageChecksums())
	if err != nil {
		return nil, err
	}
	db.cacheWriteNode(node)
	return node, nil
}

// readOnlyNode reads a committed node and keeps decoded main-file nodes in the shared cache.
// The caller must hold the database operation read lock or write lock.
func (db *DB) readOnlyNode(pageID page.ID) (*btree.Node, error) {
	if db.memoryNodes != nil {
		return db.readNode(pageID)
	}
	if record, ok := db.wal.Lookup(pageID); ok {
		return db.wal.CommittedPageToNode(&record)
	}
	db.writeNodesMu.RLock()
	if entry, ok := db.writeNodes[pageID]; ok && entry.node != nil {
		node := entry.node
		// Mark the hit so eviction skips this node once. A load first keeps hits from writing a shared cache line again and again.
		if !entry.referenced.Load() {
			entry.referenced.Store(true)
		}
		db.writeNodesMu.RUnlock()
		return node, nil
	}
	db.writeNodesMu.RUnlock()

	// Decode without the cache lock, so concurrent misses do not wait for each other.
	// Mapped pages stay unchanged while the caller holds the operation lock.
	// Two readers can decode the same page. The first insert wins, and the other copy is dropped.
	data, err := db.readMainNodePage(pageID)
	if err != nil {
		return nil, err
	}
	node, err := btree.DecodeNode(slices.Clone(data), pageID, db.meta.PageSize(), db.readMainNodePage, db.meta.DataPageChecksums())
	if err != nil {
		return nil, err
	}
	db.writeNodesMu.Lock()
	defer db.writeNodesMu.Unlock()
	if cached, ok := db.cachedWriteNode(pageID); ok {
		return cached, nil
	}
	db.cacheWriteNode(node)
	return node, nil
}

// visitCommittedNodeReferences checks one node and visits its child and storage pages.
// It does not make a private node copy or copy large values.
func (db *DB) visitCommittedNodeReferences(pageID page.ID, visit, visitContinuation func(page.ID) error) error {
	if db.memoryNodes != nil {
		node, err := db.readNode(pageID)
		if err != nil {
			return err
		}
		return btree.VisitNodeReferences(node, visit, visitContinuation)
	}
	if record, ok := db.wal.Lookup(pageID); ok {
		return db.wal.VisitCommittedPageReferences(&record, visit, visitContinuation)
	}
	if node, ok := db.cachedWriteNode(pageID); ok {
		return btree.VisitNodeReferences(node, visit, visitContinuation)
	}
	data, err := db.readMainNodePage(pageID)
	if err != nil {
		return err
	}
	return btree.VisitMappedNodeReferences(data, pageID, db.meta.PageSize(), db.readMainNodePage, visit, visitContinuation, db.meta.DataPageChecksums())
}

// cachedWriteNode returns an immutable committed node. It marks the node as recently used so eviction skips it once.
// The caller must hold the database operation write lock, or its read lock with writeNodesMu locked for writing. Open can call this before other goroutines can use the handle.
func (db *DB) cachedWriteNode(pageID page.ID) (*btree.Node, bool) {
	entry, ok := db.writeNodes[pageID]
	if !ok || entry.node == nil {
		return nil, false
	}
	entry.referenced.Store(true)
	return entry.node, true
}

// invalidateWriteNode discards the decoded image and keeps its existing clock slot.
// The caller must hold the database operation write lock.
func (db *DB) invalidateWriteNode(id page.ID) {
	if cached := db.writeNodes[id]; cached != nil {
		cached.node = nil
		cached.referenced.Store(false)
	}
}

// cacheWriteNode keeps an immutable committed node for later write transactions. It keeps at most [maxCachedWriteNodePages] nodes.
// The caller must hold the database operation write lock, or its read lock with writeNodesMu locked for writing. Open can call this before other goroutines can use the handle.
func (db *DB) cacheWriteNode(node *btree.Node) {
	pageID := node.PageID()
	if entry, ok := db.writeNodes[pageID]; ok {
		entry.node = node
		entry.referenced.Store(true)
		return
	}

	if len(db.writeNodeSlots) < maxCachedWriteNodePages {
		db.writeNodeSlots = append(db.writeNodeSlots, pageID)
	} else {
		// Give recently used nodes one more pass. Remove the first node that no read used after the last pass.
		for {
			victimPageID := db.writeNodeSlots[db.nextWriteNodeSlot]
			victim := db.writeNodes[victimPageID]
			if !victim.referenced.Load() {
				delete(db.writeNodes, victimPageID)
				db.writeNodeSlots[db.nextWriteNodeSlot] = pageID
				db.nextWriteNodeSlot = (db.nextWriteNodeSlot + 1) % maxCachedWriteNodePages
				break
			}
			victim.referenced.Store(false)
			db.nextWriteNodeSlot = (db.nextWriteNodeSlot + 1) % maxCachedWriteNodePages
		}
	}
	entry := &writeNodeCacheEntry{node: node}
	entry.referenced.Store(true)
	db.writeNodes[pageID] = entry
}

// lookupCommittedPage searches one committed page.
// In a file database, a WAL page has priority over the main-file page with the same ID.
// includeKey controls whether an encoded lookup returns the matched key.
func (db *DB) lookupCommittedPage(pageID page.ID, key []byte, includeKey bool) (btree.Entry, bool, page.ID, error) {
	if db.memoryNodes != nil {
		node, err := db.readNode(pageID)
		if err != nil {
			return btree.Entry{}, false, 0, err
		}
		return btree.LookupDecodedNode(node, key)
	}
	if record, ok := db.wal.Lookup(pageID); ok {
		return db.wal.LookupCommittedPage(&record, key, includeKey)
	}
	if db.meta.DataPageChecksums() {
		return db.lookupCheckedMainPage(pageID, key, includeKey)
	}
	// readMainNodePage checks the complete span before the lookup reads its body.
	data, err := db.readMainNodePage(pageID)
	if err != nil {
		return btree.Entry{}, false, 0, err
	}
	return btree.LookupCheckedNodeSpan(data, key, includeKey, false)
}

// lookupCheckedMainPage searches one main-file page in a file with data-page checksums.
// A separate function keeps the mode check and its error formatting out of lookupCommittedPage, which runs for each page of a point read.
func (db *DB) lookupCheckedMainPage(pageID page.ID, key []byte, includeKey bool) (btree.Entry, bool, page.ID, error) {
	data, err := db.readMainNodePage(pageID)
	if err != nil {
		return btree.Entry{}, false, 0, err
	}
	return btree.LookupCheckedNodeSpan(data, key, includeKey, true)
}

// findCommittedEntry searches one committed tree for Get.
// It omits encoded keys because Get needs only the value and flags.
// Decoded and ordinary mapped values borrow database storage.
// Continued encoded leaf values own their bytes.
func (db *DB) findCommittedEntry(pageID page.ID, key []byte) (btree.Entry, bool, error) {
	for {
		entry, found, childPageID, err := db.lookupCommittedPage(pageID, key, false)
		if err != nil {
			return btree.Entry{}, false, err
		}
		if childPageID == 0 {
			return entry, found, nil
		}
		pageID = childPageID
	}
}

// errMetaRead marks a failed read of a metadata page.
// The read says nothing about the page, so Open fails instead of selecting the other copy, and the caller can try again.
var errMetaRead = errors.New("read metadata page")

func (db *DB) readMetaAt(offset int64) (*page.Meta, error) {
	data := make([]byte, page.MetaSize)
	reader := io.NewSectionReader(db.file, offset, page.MetaSize)
	if err := fileio.ReadFull(reader, data); err != nil {
		// A short file has no metadata page at offset.
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errors.Join(page.ErrInvalid, err)
		}
		return nil, fmt.Errorf("%w at offset %d: %w", errMetaRead, offset, err)
	}
	return page.DecodeMeta(data)
}

// validMetaPageSize reports whether pageSize is a power of two from the metadata size to MaxValueSize.
// One changed bit turns a power of two into a value that is not a power of two, so this check also finds that damage.
func validMetaPageSize(pageSize int64) bool {
	return pageSize >= page.MetaSize && pageSize <= MaxValueSize && pageSize&(pageSize-1) == 0
}

// validateMeta checks the file marker, format version, checksum, and supported page size.
func validateMeta(meta *page.Meta) error {
	if meta == nil {
		return page.ErrInvalid
	}
	if err := meta.Validate(); err != nil {
		return err
	}
	if !validMetaPageSize(meta.PageSize()) {
		return fmt.Errorf("invalid database page size %d: %w", meta.PageSize(), page.ErrInvalid)
	}
	return nil
}

// readValidMainMeta returns selected main-file metadata and the page size for WAL decoding. The selected metadata always passes full validation. If selection fails, the WAL page size can come from a supported field in a decoded metadata copy. Open does not treat that copy as valid.
func (db *DB) readValidMainMeta() (selected *page.Meta, walPageSize int64, err error) {
	// Metadata page 0 starts at byte zero. Open can read it before it knows the database page size.
	meta0, meta0Err := db.readMetaAt(0)
	if errors.Is(meta0Err, errMetaRead) {
		return nil, 0, meta0Err
	}
	if meta0Err == nil {
		meta0Err = validateMeta(meta0)
	}

	// Metadata page 1 starts at the database page size, so a valid page 0 gives its offset.
	// After page 0 fails validation, its page-size field can be damaged too.
	// So Open then tries the common operating-system page sizes, most common first: 4 KiB (Linux and Windows), 16 KiB (macOS on Apple silicon), and 64 KiB (some Linux on ARM and POWER).
	// Then it tries every other supported page size in increasing order, as bbolt does.
	var pageSizes []int64
	if meta0Err == nil {
		pageSizes = append(pageSizes, meta0.PageSize())
	} else {
		pageSizes = append(pageSizes, 4096, 16384, 65536)
		for pageSize := int64(1); pageSize <= MaxValueSize; pageSize *= 2 {
			if validMetaPageSize(pageSize) && !slices.Contains(pageSizes, pageSize) {
				pageSizes = append(pageSizes, pageSize)
			}
		}
	}

	var meta1 *page.Meta
	meta1Err := error(page.ErrInvalid)
	fileEnd := int64(math.MaxInt64) // no metadata copy starts at or after this offset
	for _, pageSize := range pageSizes {
		if pageSize >= fileEnd {
			continue
		}
		candidate, readErr := db.readMetaAt(pageSize)
		if errors.Is(readErr, errMetaRead) {
			return nil, 0, readErr
		}
		if readErr != nil {
			// The file ends before this offset, so it also ends before every larger offset.
			meta1Err = errors.Join(meta1Err, readErr)
			fileEnd = pageSize
			continue
		}
		// A copy counts only at the offset that its page-size field gives.
		// The other offsets hold the zero padding of page 0 or the start of a tree or allocation page.
		if candidate.PageSize() != pageSize {
			continue
		}

		candidateErr := validateMeta(candidate)
		if candidateErr == nil {
			meta1 = candidate
			meta1Err = nil
			break
		}
		if walPageSize == 0 {
			walPageSize = pageSize
		}
		meta1Err = errors.Join(meta1Err, candidateErr)
	}

	// One valid copy is sufficient. If both copies are valid, select the highest generation. Equal generations must contain the same checkpoint values.
	switch {
	case meta0Err == nil && meta1Err == nil:
		if meta1.Generation() == meta0.Generation() && !bytes.Equal(page.EncodeMeta(meta0), page.EncodeMeta(meta1)) {
			return nil, meta0.PageSize(), fmt.Errorf("metadata pages have different contents at generation %d: %w", meta0.Generation(), page.ErrInvalid)
		}
		if meta1.Generation() > meta0.Generation() {
			return meta1, meta1.PageSize(), nil
		}
		return meta0, meta0.PageSize(), nil
	case meta0Err == nil:
		return meta0, meta0.PageSize(), nil
	case meta1Err == nil:
		return meta1, meta1.PageSize(), nil
	}

	// WAL decoding needs a page size before it can recover metadata. A supported field from an invalid metadata copy can set this size. It never makes that copy valid or assigns it to db.meta.
	// A copy of page 1 at the offset that its own field gives is the better source, because two facts agree there.
	if walPageSize == 0 && meta0 != nil && validMetaPageSize(meta0.PageSize()) {
		walPageSize = meta0.PageSize()
	}
	return nil, walPageSize, errors.Join(
		fmt.Errorf("metadata page %d: %w", page.Meta0ID, meta0Err),
		fmt.Errorf("metadata page %d: %w", page.Meta1ID, meta1Err),
	)
}

// putManaged borrows large input values until the main-file write or WAL commit.
// It runs without caller callbacks. A WAL commit copies values that it retains.
// A direct commit keeps borrowed values out of the shared cache.
func (tx *Tx) putManaged(bucketName, key, value []byte) error {
	db := tx.db
	borrowValue := db.memoryNodes == nil && int64(len(value)) > tx.meta.PageSize()
	if borrowValue && len(bucketName) != 0 && len(key) != 0 && len(key) <= MaxKeySize && len(value) <= MaxValueSize {
		catalogEntry, found, err := tx.tree.FindEntryRef(tx.rootNode, bucketName, true)
		if err != nil {
			return err
		}
		if !found {
			return ErrBucketNotFound
		}
		rootID, err := decodeBucketRootPageID(catalogEntry)
		if err != nil {
			return err
		}
		replaced, err := tx.store.ReplaceLeafEntry(rootID, btree.NewEntryBorrowingValue(key, value), tx.tree.SkipIdenticalPuts)
		if err != nil {
			return err
		}
		if replaced {
			tx.borrowedValue = true
			if node := tx.store.nodes[rootID].node; node != nil {
				tx.cacheBucket(&Bucket{tx: tx, name: catalogEntry.Key(), rootNode: node})
			}
			return nil
		}
	}
	bucket, err := tx.Bucket(bucketName)
	if err != nil {
		return err
	}
	if borrowValue {
		tx.borrowedValue = true
		return bucket.putBucketEntry(btree.NewEntryBorrowingValue(key, value))
	}
	return bucket.Put(key, value)
}
