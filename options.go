package kvlite

import (
	"fmt"
	"os"

	"github.com/Issaminu/kvlite/internal/page"
)

// Sync controls when committed write-ahead log data is synchronized to storage.
type Sync uint8

const (
	// SyncDefault selects KVLite's default mode, SyncFull.
	SyncDefault Sync = iota

	// SyncFull favors durability over write speed. It waits for each non-empty write transaction to reach storage before [DB.Update] returns success, which adds synchronization work to every commit.
	SyncFull

	// SyncNormal favors write speed over the durability of recent updates. It waits until a checkpoint or close to synchronize the write-ahead log, so [DB.Update] can return success while recent writes remain only in the operating system cache. A system failure can lose those writes.
	SyncNormal

	// defaultCheckpointPageCount keeps about 1,000 operating-system pages of data in the write-ahead log before KVLite starts a checkpoint. This limits log growth without running a checkpoint after every update.
	defaultCheckpointPageCount = 1000
)

// Options configures [Open]. Open resolves the zero values described below and then copies the result, so callers can reuse or change their Options after Open returns.
type Options struct {
	// ReadOnly opens an existing database without creating or modifying its database or write-ahead log files. Because this mode cannot write, [DB.Update] and [DB.Put] return [ErrDatabaseReadOnly].
	ReadOnly bool

	// PageSize sets the page size in bytes for a new database. It must be between 40 bytes, the encoded metadata size, and [MaxValueSize]. A zero value uses the operating system page size, while an existing database always uses the page size stored in its file.
	PageSize int

	// Synchronous controls when KVLite synchronizes committed write-ahead log data. A zero value uses SyncFull.
	Synchronous Sync

	// CheckpointThresholdBytes is the number of committed write-ahead log bytes that starts a checkpoint. A zero value uses about 1,000 operating-system pages, while a smaller value keeps the log smaller at the cost of more frequent checkpoints.
	CheckpointThresholdBytes uint64
}

func defaultOptions() Options {
	return Options{
		ReadOnly:                 false,
		PageSize:                 0,
		Synchronous:              SyncFull,
		CheckpointThresholdBytes: defaultCheckpointPageCount * uint64(os.Getpagesize()),
	}
}

func resolveOptions(options *Options) (*Options, error) {
	defaults := defaultOptions()
	resolved := defaults
	if options != nil {
		resolved = *options
		if resolved.Synchronous == SyncDefault {
			resolved.Synchronous = defaults.Synchronous
		}
		if resolved.CheckpointThresholdBytes == 0 {
			resolved.CheckpointThresholdBytes = defaults.CheckpointThresholdBytes
		}
	}

	if resolved.PageSize == 0 {
		resolved.PageSize = os.Getpagesize()
	}
	if resolved.PageSize < page.MetaSize || resolved.PageSize > MaxValueSize {
		return nil, fmt.Errorf("invalid database page size %d: %w", resolved.PageSize, ErrInvalid)
	}

	if resolved.Synchronous != SyncFull && resolved.Synchronous != SyncNormal {
		return nil, fmt.Errorf("invalid synchronous mode %d: %w", resolved.Synchronous, ErrInvalid)
	}
	return &resolved, nil
}
