package kvlite

import (
	"errors"
	"fmt"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

type Tx struct {
	db       *DB
	readOnly bool
	closed   bool
	buckets  map[string]*Bucket // per-tx cache: one *Bucket handle per top-level name, so every caller in this tx observes the same in-memory state
}

type writeTransactionSnapshot struct {
	meta page.Meta
	wal  wal.Snapshot
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
	needsCheckpoint, err := db.wal.PersistCollectedRecords()
	if err != nil {
		if rbErr := db.rollbackWriteTransaction(&snapshot); rbErr != nil {
			return errors.Join(err, rbErr)
		}
		return err
	}
	if needsCheckpoint {
		// The WAL is already durable. A checkpoint failure must not turn this
		// committed transaction into a reported failure.
		_ = db.checkpointWAL()
	}

	return nil
}

func (db *DB) snapshotWriteTransaction() (writeTransactionSnapshot, error) {
	walSnapshot, err := db.wal.Snapshot()
	if err != nil {
		return writeTransactionSnapshot{}, err
	}
	return writeTransactionSnapshot{
		meta: *db.meta,
		wal:  walSnapshot,
	}, nil
}

func (db *DB) rollbackWriteTransaction(snapshot *writeTransactionSnapshot) error {
	walErr := db.wal.Restore(snapshot.wal)
	db.meta = &snapshot.meta
	root, err := db.readNode(snapshot.meta.Root())
	if err != nil {
		return errors.Join(walErr, fmt.Errorf("reload root node: %w", err))
	}
	db.rootNode = root
	return walErr
}

func (db *DB) View(transaction func(tx *Tx) error) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	tx := &Tx{db: db, readOnly: true}
	defer func() { tx.closed = true }()
	return transaction(tx)
}

func (tx *Tx) Writable() bool {
	return !tx.readOnly && !tx.closed
}

func (tx *Tx) writableError() error {
	if tx.closed {
		return ErrTxClosed
	}
	if tx.readOnly {
		return ErrTxNotWritable
	}
	return nil
}

// putCatalogEntry writes entry to the database's top-level tree. putTreeEntry
// can return a replacement root after a split, but it does not install that
// root. This method installs it, updates meta.root, and then stages the meta
// record so recovery uses the root that contains the entry.
func (tx *Tx) putCatalogEntry(entry Entry) error {
	newRoot, err := tx.db.putTreeEntry(tx.db.rootNode, entry)
	if err != nil {
		return err
	}
	tx.db.rootNode = newRoot
	tx.db.meta.SetRoot(newRoot.PageID())
	tx.db.wal.InsertMetaRecord(tx.db.meta)
	return nil
}

func (tx *Tx) Put(key, value []byte) error {
	if err := tx.writableError(); err != nil {
		return err
	}
	return tx.putCatalogEntry(btree.NewEntry(0, key, value))
}

func (tx *Tx) Get(key []byte) ([]byte, error) {
	if tx.closed {
		return nil, ErrTxClosed
	}

	entry, found, err := tx.db.findTreeEntry(tx.db.rootNode, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrKeyNotFound
	}
	if entry.Flags()&BucketLeafFlag != 0 {
		return nil, ErrIncompatibleValue
	}
	return entry.Value(), nil
}
