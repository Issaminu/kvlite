package kvlite

import (
	"errors"
	"fmt"
	"os"

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

func (db *DB) readOrCreateWal() (*wal.WAL, []wal.Record, error) {
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
		PageSize:                 db.meta.PageSize(),
		SyncOnCommit:             db.options.Synchronous == SyncFull,
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
		if err := db.applyWALRecord(&record); err != nil {
			return err
		}
	}

	return db.file.Sync()
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

func metaFromCommittedWAL(records []wal.Record) (*page.Meta, error) {
	committed, err := committedWALRecords(records)
	if err != nil {
		return nil, err
	}
	// starting from the last record downwards to get the most recent version of the meta, then early break then
	for index := len(committed) - 1; index >= 0; index-- {
		record := committed[index]
		if record.Header.Type != wal.RecordTypeMeta || record.Header.PageID != page.MetaID {
			continue
		}
		meta, err := page.DecodeMeta(record.PageContent)
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
func (db *DB) loadCommittedIntoOverlay(records []wal.Record) error {
	committed, err := committedWALRecords(records)
	if err != nil {
		return err
	}
	for _, record := range committed {
		db.wal.LoadCommittedRecord(record)
	}

	if metaRecord, ok := db.wal.CommittedRecord(page.MetaID); ok {
		meta, err := page.DecodeMeta(metaRecord.PageContent)
		if err != nil {
			return fmt.Errorf("read committed meta from WAL: %w", err)
		}
		db.meta = meta
	}
	return nil
}
