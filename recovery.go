package kvlite

import (
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

func (db *DB) replayWAL(committed []wal.WALRecord) error {
	if len(committed) == 0 {
		return nil
	}
	if db.options.ReadOnly {
		return db.loadCommittedIntoOverlay(committed)
	}
	return db.ingestWalRecords(committed)
}

func (db *DB) readOrCreateWal(pageSize int64) (*wal.WAL, []wal.WALRecord, error) {
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

func (db *DB) ingestWalRecords(committed []wal.WALRecord) error {
	committed, err := db.materializeWALPagePatches(committed)
	if err != nil {
		return err
	}
	for _, record := range committed {
		db.wal.LoadCommittedRecord(record)
	}
	return db.wal.Checkpoint(db.file)
}

func committedWALRecords(records []wal.WALRecord) ([]wal.WALRecord, error) {
	committed := make([]wal.WALRecord, 0, len(records))
	pending := make([]wal.WALRecord, 0)

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

// metaFromCommittedRecords returns the newest valid metadata from committed WAL records. It returns nil when the records do not contain metadata.
func metaFromCommittedRecords(committed []wal.WALRecord) (*page.Meta, error) {
	// Scan backward because the newest committed transaction contains the latest metadata generation.
	for index := len(committed) - 1; index >= 0; index-- {
		record := committed[index]
		if record.Header.Type != wal.RecordTypeMeta || !page.IsMetaID(record.Header.PageID) {
			continue
		}
		meta, err := page.DecodeMeta(record.Payload)
		if err != nil {
			return nil, err
		}
		if err := validateMeta(meta); err != nil {
			return nil, err
		}
		return meta, nil
	}
	return nil, nil
}

// loadCommittedIntoOverlay makes committed WAL pages visible to a read-only database. It does not change the main file.
func (db *DB) loadCommittedIntoOverlay(committed []wal.WALRecord) error {
	committed, err := db.materializeWALPagePatches(committed)
	if err != nil {
		return err
	}
	// Check all data before any record becomes visible.
	for _, record := range committed {
		if record.Header.Type != wal.RecordTypeNode {
			continue
		}
		if err := btree.ValidateWALNode(record.Payload, record.Header.PageID); err != nil {
			return err
		}
	}
	for _, record := range committed {
		db.wal.LoadCommittedRecord(record)
	}
	return nil
}

// materializeWALPagePatches rebuilds the latest complete image for each data page.
// Absolute patch ranges can run again after an interrupted checkpoint because later ranges restore the latest committed bytes.
func (db *DB) materializeWALPagePatches(records []wal.WALRecord) ([]wal.WALRecord, error) {
	pageImages := make(map[page.ID][]byte)
	lastRecord := make(map[page.ID]int)
	for index, record := range records {
		var image []byte
		switch record.Header.Type {
		case wal.RecordTypeNode:
			image = record.Payload
		case wal.RecordTypePatch:
			base, ok := pageImages[record.Header.PageID]
			if !ok {
				mainPage, err := db.readMainPage(record.Header.PageID)
				if err != nil {
					return nil, fmt.Errorf("read page %d for WAL patch: %w", record.Header.PageID, err)
				}
				node, err := btree.DecodeNode(slices.Clone(mainPage), record.Header.PageID, db.meta.PageSize())
				if err != nil {
					return nil, fmt.Errorf("decode page %d for WAL patch: %w", record.Header.PageID, err)
				}
				base = btree.EncodeWALNode(node)
			}
			var err error
			image, err = wal.ApplyPagePatch(base, record.Payload, db.meta.PageSize())
			if err != nil {
				return nil, fmt.Errorf("apply WAL patch for page %d: %w", record.Header.PageID, err)
			}
		default:
			continue
		}
		pageImages[record.Header.PageID] = image
		lastRecord[record.Header.PageID] = index
	}

	materialized := make([]wal.WALRecord, 0, len(records))
	for index, record := range records {
		if record.Header.Type == wal.RecordTypeNode || record.Header.Type == wal.RecordTypePatch {
			if lastRecord[record.Header.PageID] != index {
				continue
			}
			record.Header.Type = wal.RecordTypeNode
			record.Payload = pageImages[record.Header.PageID]
		}
		materialized = append(materialized, record)
	}
	return materialized, nil
}
