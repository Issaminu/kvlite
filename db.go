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

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

const (
	MaxKeySize              = btree.MaxKeySize   // MaxKeySize is the largest key length in bytes. A longer key returns ErrKeyTooLarge.
	MaxValueSize            = btree.MaxValueSize // MaxValueSize is the largest value length in bytes. A longer value returns ErrValueTooLarge.
	maxCachedWriteNodePages = 1024
)

// DB is an open handle to a KVLite database in a file or in memory. Create it with [Open] or [OpenMemory]. Its zero value is not usable, and it must not be copied. Its methods accept concurrent calls. Read-only transaction callbacks can run together, but a write callback runs alone. Callers access named buckets through managed transactions or the [DB.Put], [DB.Get], [DB.Delete], and [DB.DeleteBucket] methods.
type DB struct {
	path string
	storage
	operationMu      sync.RWMutex
	lifecycleMu      sync.RWMutex
	activeOperations sync.WaitGroup
	writeRequests    chan *writeRequest
	stopWriteBatcher chan struct{}
	writeBatcherDone chan struct{}
	closing          bool
	closed           bool
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
		path: path,
		storage: storage{
			fs:         fsys,
			file:       dbFile,
			options:    resolvedOptions,
			writeNodes: make(map[page.ID]*writeNodeCacheEntry),
		},
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
	// Check the complete catalog root, so a damaged root fails Open and not a later transaction.
	if _, err := db.checkNode(db.meta.Root()); err != nil {
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
	root := btree.NewLeafNode(db.meta.Root())
	db.meta.RefreshChecksum()
	nodes := []wal.NodeRecord{{Final: root}}
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
			// Check the whole span now.
			// A bad checksum in a later leaf must stop the batch before it changes an earlier leaf.
			if _, err := btree.ReadCheckedNodeSpan(data, checksums); err != nil {
				return true, err
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

// hasMeta reports whether the main file holds data.
// A Stat error is not an empty file: Open would then write a new database over the old one.
func (db *DB) hasMeta() (bool, error) {
	fi, err := db.file.Stat()
	if err != nil {
		return false, err
	}
	return fi.Size() > 0, nil
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
	bucket, err := tx.Bucket(bucketName)
	if err != nil {
		return err
	}
	if tx.db.memoryNodes == nil && int64(len(value)) > tx.meta.PageSize() {
		tx.borrowedValue = true
		return bucket.putBucketEntry(btree.NewEntryBorrowingValue(key, value))
	}
	return bucket.Put(key, value)
}
