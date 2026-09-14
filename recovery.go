package kvlite

import (
	"errors"
	"fmt"
	"os"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

func (db *DB) replayWAL(records []wal.Record) error {
	if len(records) == 0 {
		return nil
	}
	if db.options.ReadOnly {
		return db.loadCommittedIntoOverlay(records)
	}
	return db.ingestWalRecords(records)
}

func (db *DB) readOrCreateWal(pageSize int64) (*wal.WAL, []wal.Record, error) {
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

	log := wal.New(wal.Config{
		Path:                     walPath,
		File:                     walFile,
		PageSize:                 pageSize,
		SyncOnCommit:             db.options.Synchronous == SyncFull,
		SyncOnCheckpoint:         db.options.Synchronous != SyncNone,
		CheckpointThresholdBytes: db.options.CheckpointThresholdBytes,
	})

	records, err := log.ReadRecords()
	if err != nil {
		return nil, nil, errors.Join(err, log.Close())
	}

	return log, records, nil
}

func (db *DB) ingestWalRecords(records []wal.Record) error {
	committed, err := committedWALRecords(records)
	if err != nil {
		return err
	}
	for _, record := range committed {
		db.wal.LoadCommittedRecord(record)
	}
	return db.wal.Checkpoint(db.file)
}

func committedWALRecords(records []wal.Record) ([]wal.Record, error) {
	committed := make([]wal.Record, 0, len(records))
	pending := make([]wal.Record, 0)

	for index, record := range records {
		if !wal.IsCommitMarker(&record) {
			pending = append(pending, record)
			continue
		}

		matches := len(pending) > 0
		for _, candidate := range pending {
			if candidate.Header.TxID != record.Header.TxID {
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

// metaFromCommittedWAL returns the newest valid metadata from a complete WAL transaction.
func metaFromCommittedWAL(records []wal.Record) (*page.Meta, error) {
	committed, err := committedWALRecords(records)
	if err != nil {
		return nil, err
	}
	// Scan backward because the newest committed transaction contains the latest metadata generation.
	for index := len(committed) - 1; index >= 0; index-- {
		record := committed[index]
		if record.Header.Type != wal.RecordTypeMeta || !page.IsMetaID(record.Header.PageID) {
			continue
		}
		meta, err := page.DecodeMeta(record.PageContent)
		if err != nil {
			return nil, err
		}
		if err := validateMeta(meta); err != nil {
			return nil, err
		}
		return meta, nil
	}
	return nil, ErrInvalid
}

// loadCommittedIntoOverlay makes committed WAL pages visible to a read-only database. It does not change the main file. Validated WAL metadata replaces db.meta when a committed transaction includes metadata.
func (db *DB) loadCommittedIntoOverlay(records []wal.Record) error {
	committed, err := committedWALRecords(records)
	if err != nil {
		return err
	}
	// Check all data before any record becomes visible.
	for _, record := range committed {
		if record.Header.Type != wal.RecordTypeData {
			continue
		}
		if err := btree.ValidateWALNode(record.PageContent, record.Header.PageID); err != nil {
			return err
		}
	}
	for _, record := range committed {
		db.wal.LoadCommittedRecord(record)
	}

	hasMetaRecord := false
	for _, record := range committed {
		if record.Header.Type == wal.RecordTypeMeta && page.IsMetaID(record.Header.PageID) {
			hasMetaRecord = true
			break
		}
	}
	if !hasMetaRecord {
		return nil
	}
	meta, metaErr := metaFromCommittedWAL(records)
	if metaErr != nil {
		return fmt.Errorf("read committed meta from WAL: %w", metaErr)
	}
	db.meta = meta
	return nil
}
