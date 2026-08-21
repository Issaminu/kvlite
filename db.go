package kvlite

import (
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

const (
	MaxKeySize   = btree.MaxKeySize
	MaxValueSize = btree.MaxValueSize
)

// DB is an open handle to a KVLite database and its write-ahead log. A DB must be created by [Open] because its zero value is not usable, and it must not be copied. Its methods accept concurrent calls. Read-only transaction callbacks can run together, but a write callback runs alone. Callers access named buckets through managed transactions or the [DB.Put] and [DB.Get] convenience methods.
type DB struct {
	path             string
	file             *os.File
	meta             *page.Meta
	rootNode         *btree.Node
	pageCache        *nodeCache
	options          *Options
	wal              *wal.WAL
	operationMu      sync.RWMutex
	lifecycleMu      sync.Mutex
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
	db.lifecycleMu.Lock()
	defer db.lifecycleMu.Unlock()
	if err := db.ensureOpen(); err != nil {
		return err
	}
	db.activeOperations.Add(1)
	return nil
}

func (db *DB) endOperation() {
	db.activeOperations.Done()
}

// Open opens the database at path and recovers any committed write-ahead log data before it returns. If options is nil, Open uses KVLite's fixed defaults.
//
// A writable Open creates the database when it does not exist. The new file uses mode subject to the process umask, and a new write-ahead log uses the resulting database permissions. A read-only Open requires an existing database and does not create either file.
//
// A read-only Open takes a shared database-file lock, so several read-only handles can open the database together. A writable Open takes an exclusive lock. Open waits for a conflicting handle to close unless a positive [Options.LockTimeout] expires.
//
// The caller must call [DB.Close] when the database is no longer needed and must check its error.
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
	// The main file descriptor owns the process lock, so failOpen and Close release the lock through their existing file-close paths.
	if err := lockDatabaseFile(db.file, db.options.ReadOnly, db.options.LockTimeout); err != nil {
		return db.failOpen(err)
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

	db.pageCache = newNodeCache(pageCacheCapacity(db.options.PageCacheBytes, db.meta.PageSize(), db.options.DisablePageCache))
	if err := db.loadRootNode(); err != nil {
		return db.failOpen(err)
	}

	if len(records) > 0 && !db.options.ReadOnly {
		if err := db.wal.Truncate(); err != nil {
			return db.failOpen(err)
		}
	}
	db.startWriteBatcher()

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
	rootNode, err := db.loadNode(db.meta.Root())
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

// Close releases the database resources. For a writable database, it first checkpoints committed data into the database file and removes the write-ahead log, while a read-only database only closes its files.
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
	if db.options.ReadOnly {
		if err := db.closeFiles(); err != nil {
			return err
		}
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
	return nil
}

// Path returns the database path passed to [Open]. It does not clean the path or convert it to an absolute path, and it remains available after [DB.Close].
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
	return db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(bucketName)
		if err != nil {
			return err
		}
		return bucket.Put(key, value)
	})
}

// Get returns the value stored under key in the top-level bucket named
// bucketName. It performs the lookup in its own read-only transaction.
//
// The bucket must already exist. An empty bucket name returns
// [ErrBucketNameRequired], and a missing bucket returns [ErrBucketNotFound].
// Get returns [ErrKeyNotFound] when the key is absent and
// [ErrIncompatibleValue] when the key names a nested bucket. It returns a new
// slice that the caller can retain and modify. A stored empty value returns a
// non-nil slice with length zero.
func (db *DB) Get(bucketName, key []byte) ([]byte, error) {
	var value []byte
	err := db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(bucketName)
		if err != nil {
			return err
		}
		value, err = bucket.Get(key)
		if err == nil {
			value = slices.Clone(value)
			if value == nil {
				value = []byte{}
			}
		}
		return err
	})
	return value, err
}

func (db *DB) persistNode(node *btree.Node) error {
	offset := int64(node.PageID()) * db.meta.PageSize()
	writer := io.NewOffsetWriter(db.file, offset)
	if err := btree.WriteNode(writer, node, db.meta.PageSize(), true); err != nil {
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

// readNode returns a read-only committed node. A cache miss reads the WAL before the main file so the cache never publishes an older page image.
func (db *DB) readNode(pgid page.ID) (*btree.Node, error) {
	if node, ok := db.pageCache.Get(pgid); ok {
		return node, nil
	}
	node, err := db.loadNode(pgid)
	if err != nil {
		return nil, err
	}
	db.pageCache.Put(node)
	return node, nil
}

func (db *DB) loadNode(pgid page.ID) (*btree.Node, error) {
	// check if Node exists exists in [wal.WAL.overlay]
	record, ok := db.wal.Lookup(pgid)
	if ok {
		node, err := wal.RecordToNode(&record)
		if err != nil {
			return nil, err
		}
		return node, nil
	}

	// Node not found in-memory, so we have to read its full page from the database file.

	offset := int64(pgid) * db.meta.PageSize()
	data := make([]byte, db.meta.PageSize())
	reader := io.NewSectionReader(db.file, offset, db.meta.PageSize())
	if err := fileio.ReadFull(reader, data); err != nil {
		return nil, err
	}
	node, err := btree.DecodeNode(data, pgid, db.meta.PageSize())
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (db *DB) readMeta() (*page.Meta, error) {
	data := make([]byte, page.MetaSize)
	reader := io.NewSectionReader(db.file, 0, page.MetaSize)
	if err := fileio.ReadFull(reader, data); err != nil {
		return nil, errors.Join(ErrInvalid, err)
	}
	return page.DecodeMeta(data)
}

func (db *DB) persistMeta() error {
	// recompute checksum
	db.meta.RefreshChecksum()

	writer := io.NewOffsetWriter(db.file, 0)
	if err := fileio.WriteFull(writer, page.EncodeMeta(db.meta)); err != nil {
		return fmt.Errorf("write meta: %w", err)
	}
	return nil
}
