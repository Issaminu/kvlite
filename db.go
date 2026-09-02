package kvlite

import (
	"bytes"
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
	mappedFile       []byte
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

// Open opens the database at path. It selects the valid metadata copy with the highest generation. It also recovers committed write-ahead log data before it returns. If options is nil, Open uses KVLite's fixed defaults.
//
// A writable Open creates the database when it does not exist. The new file uses mode subject to the process umask, and a new write-ahead log uses the resulting database permissions. A read-only Open requires an existing database and does not create either file.
//
// A new database uses the operating system page size. Open uses the stored page size when it reopens that database.
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
	// The main file descriptor owns the process lock. failOpen and Close release the lock when they close this file.
	if err := lockDatabaseFile(db.file, db.options.ReadOnly, db.options.LockTimeout); err != nil {
		return db.failOpen(err)
	}
	isNew := !db.hasMeta()
	var mainMetaErr error
	var walPageSize int64
	if isNew {
		db.meta = page.NewMeta(int64(os.Getpagesize()))
		walPageSize = db.meta.PageSize()
	} else {
		db.meta, walPageSize, mainMetaErr = db.readValidMainMeta()
		if mainMetaErr != nil {
			mainMetaErr = fmt.Errorf("read db meta: %w", mainMetaErr)
		}
		// An unsupported main-file format can use another WAL layout. Current-format WAL data must not replace it.
		if errors.Is(mainMetaErr, ErrVersionMismatch) {
			return db.failOpen(mainMetaErr)
		}
		if walPageSize == 0 {
			return db.failOpen(mainMetaErr)
		}
	}

	wal, records, err := db.readOrCreateWal(walPageSize)
	if err != nil {
		return db.failOpen(err)
	}

	db.wal = wal
	if mainMetaErr != nil {
		// Main-file metadata is not trusted here. Only validated metadata from a complete WAL transaction can establish db.meta.
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

	// A remaining WAL can contain committed data from an unclean close, an interrupted checkpoint, or failed cleanup. Writable recovery copies committed records to the main file. Read-only recovery keeps them in the WAL overlay.
	if err := db.replayWAL(records); err != nil {
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
		if err := db.unmapMainFile(); err != nil {
			return err
		}
		if err := db.closeFiles(); err != nil {
			return err
		}
		return nil
	}

	if db.wal != nil {
		if err := db.checkpointWAL(); err != nil {
			return err
		}
	} else {
		if err := db.file.Sync(); err != nil {
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

// readNode decodes one committed node. The WAL image has priority over the main-file image.
func (db *DB) readNode(pgid page.ID) (*btree.Node, error) {
	// A committed WAL image overrides the main-file image until checkpoint cleanup clears the overlay.
	record, ok := db.wal.Lookup(pgid)
	if ok {
		node, err := wal.RecordToNode(&record)
		if err != nil {
			return nil, err
		}
		return node, nil
	}

	data, err := db.readMainPage(pgid)
	if err != nil {
		return nil, err
	}
	return btree.DecodeNode(data, pgid, db.meta.PageSize())
}

func (db *DB) readMetaAt(offset int64) (*page.Meta, error) {
	data := make([]byte, page.MetaSize)
	reader := io.NewSectionReader(db.file, offset, page.MetaSize)
	if err := fileio.ReadFull(reader, data); err != nil {
		return nil, errors.Join(ErrInvalid, err)
	}
	return page.DecodeMeta(data)
}

func validMetaPageSize(pageSize int64) bool {
	return pageSize >= page.MetaSize && pageSize <= MaxValueSize
}

// validateMeta checks the file marker, format version, checksum, and supported page size.
func validateMeta(meta *page.Meta) error {
	if meta == nil {
		return ErrInvalid
	}
	if err := meta.Validate(); err != nil {
		return err
	}
	if !validMetaPageSize(meta.PageSize()) {
		return fmt.Errorf("invalid database page size %d: %w", meta.PageSize(), ErrInvalid)
	}
	return nil
}

// readValidMainMeta returns selected main-file metadata and the page size for WAL decoding. The selected metadata always passes full validation. If selection fails, the WAL page size can come from a supported field in a decoded metadata copy. Open does not treat that copy as valid.
func (db *DB) readValidMainMeta() (selected *page.Meta, walPageSize int64, err error) {
	// Metadata page 0 starts at byte zero. Open can read it before it knows the database page size.
	meta0, meta0Err := db.readMetaAt(0)
	if meta0Err == nil {
		meta0Err = validateMeta(meta0)
	}

	// Metadata page 1 starts at the database page size. A supported page-size field from page 0 can locate it after page 0 fails validation. The operating system page size is the second choice because every new database uses that size.
	pageSizes := make([]int64, 0, 2)
	if meta0 != nil && validMetaPageSize(meta0.PageSize()) {
		pageSizes = append(pageSizes, meta0.PageSize())
	}
	operatingSystemPageSize := int64(os.Getpagesize())
	if validMetaPageSize(operatingSystemPageSize) && (len(pageSizes) == 0 || pageSizes[0] != operatingSystemPageSize) {
		pageSizes = append(pageSizes, operatingSystemPageSize)
	}

	var meta1 *page.Meta
	meta1Err := error(ErrInvalid)
	for _, pageSize := range pageSizes {
		candidate, readErr := db.readMetaAt(pageSize)
		if readErr != nil {
			meta1Err = errors.Join(meta1Err, readErr)
			continue
		}

		candidateErr := validateMeta(candidate)
		if candidateErr == nil && candidate.PageSize() != pageSize {
			candidateErr = fmt.Errorf("metadata page size %d does not match page offset %d: %w", candidate.PageSize(), pageSize, ErrInvalid)
		}
		if candidateErr == nil {
			meta1 = candidate
			meta1Err = nil
			break
		}
		if walPageSize == 0 && validMetaPageSize(candidate.PageSize()) {
			walPageSize = candidate.PageSize()
		}
		meta1Err = errors.Join(meta1Err, candidateErr)
	}

	// One valid copy is sufficient. If both copies are valid, select the highest generation. Equal generations must contain the same checkpoint values.
	switch {
	case meta0Err == nil && meta1Err == nil:
		if meta1.Generation() == meta0.Generation() && !bytes.Equal(page.EncodeMeta(meta0), page.EncodeMeta(meta1)) {
			return nil, meta0.PageSize(), fmt.Errorf("metadata pages have different contents at generation %d: %w", meta0.Generation(), ErrInvalid)
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
	if meta0 != nil && validMetaPageSize(meta0.PageSize()) {
		walPageSize = meta0.PageSize()
	}
	return nil, walPageSize, errors.Join(
		fmt.Errorf("metadata page %d: %w", page.Meta0ID, meta0Err),
		fmt.Errorf("metadata page %d: %w", page.Meta1ID, meta1Err),
	)
}

// persistMeta initializes both metadata pages with the same checksummed values. Checkpoints update both copies through WAL metadata records.
func (db *DB) persistMeta() error {
	db.meta.RefreshChecksum()
	encoded := page.EncodeMeta(db.meta)
	for _, pageID := range [...]page.ID{page.Meta0ID, page.Meta1ID} {
		offset := int64(pageID) * db.meta.PageSize()
		writer := io.NewOffsetWriter(db.file, offset)
		if err := fileio.WriteFull(writer, encoded); err != nil {
			return fmt.Errorf("write meta page %d: %w", pageID, err)
		}
	}
	return nil
}
