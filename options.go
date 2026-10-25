package kvlite

import (
	"fmt"
	"time"

	"github.com/Issaminu/kvlite/internal/page"
)

// Sync controls storage synchronization for file database writes, checkpoints, and backups.
// It does not select memory storage. Use [OpenMemory] for a database with no file operations.
type Sync uint8

const (
	// SyncDefault selects KVLite's default mode, SyncFull.
	SyncDefault Sync = iota

	// SyncFull favors durability over write speed. It waits for each non-empty write transaction to reach storage before [DB.Update] returns success. Concurrent updates can share one write-ahead log append and storage synchronization. Backup syncs its file and, except on Windows, its parent directory before it returns success.
	SyncFull

	// SyncNormal is a middle ground between write speed and durability. It waits until a checkpoint or close to synchronize the write-ahead log, so [DB.Update] can return success while recent writes remain only in the operating system cache. A system failure can lose those writes. Backup syncs its file and, except on Windows, its parent directory before it returns success.
	SyncNormal

	// SyncNone writes database, log, and backup files without synchronizing them to storage. It does not synchronize a new database or synchronize after commits, checkpoints, recovery, close, or backup. A system failure can lose committed updates or corrupt the database. Use this mode only when the data can be rebuilt. Use [OpenMemory] to avoid file operations.
	SyncNone

	// defaultCheckpointPageCount keeps about 1,000 operating-system pages of data in the write-ahead log before KVLite starts a checkpoint. This limits log growth without running a checkpoint after every update.
	defaultCheckpointPageCount = 1000
)

// Options configures [Open]. Open resolves the zero values described below and then copies the result, so callers can reuse or change their Options after Open returns.
type Options struct {
	// ReadOnly opens an existing database without creating or modifying its database or write-ahead log files. Because this mode cannot write, [DB.Update], [DB.Put], [DB.Delete], and [DB.DeleteBucket] return [ErrDatabaseReadOnly].
	ReadOnly bool

	// DataPageChecksums enables a checksum over each complete leaf or branch node, including its tail pages, in a new file.
	// The default is false. The DB file stores this choice.
	// Open uses the stored choice for an existing file and ignores this option.
	// Use [DB.SetDataPageChecksums] to change an existing file.
	// Without these checks, damaged key and value bytes can pass unnoticed. Metadata, allocation pages, and WAL records always retain their checksums.
	DataPageChecksums bool

	// LockTimeout limits how long [Open] waits for a conflicting database-file lock. A zero value waits until the other handle closes, a positive value returns an error that matches [ErrDatabaseLocked] after that duration, and a negative value is invalid. KVLite holds the lock for the full [DB] lifetime, not for one transaction.
	LockTimeout time.Duration

	// Synchronous controls when KVLite synchronizes committed write-ahead log data and backup files. A zero value uses SyncFull.
	Synchronous Sync

	// MaxWriteBatchSize limits how many SyncFull write callbacks can share one WAL commit. A zero value uses 100. A value of 1 prevents callbacks from sharing a commit.
	MaxWriteBatchSize int

	// CheckpointThresholdBytes is the number of committed write-ahead log bytes that starts a checkpoint. A zero value uses about 1,000 operating-system pages, while a smaller value keeps the log smaller at the cost of more frequent checkpoints.
	CheckpointThresholdBytes uint64

	// pageSize is the page size of a new file. Zero means the operating-system page size.
	// Only tests set it, so that a simulation can select the page size of each run.
	pageSize int64
}

// defaultOptions returns the defaults on an operating system with osPageSize pages.
func defaultOptions(osPageSize int64) Options {
	return Options{
		ReadOnly:                 false,
		Synchronous:              SyncFull,
		MaxWriteBatchSize:        defaultWriteBatchSize,
		CheckpointThresholdBytes: defaultCheckpointPageCount * uint64(osPageSize),
		pageSize:                 osPageSize,
	}
}

// resolveOptions replaces the zero values of options with the defaults on an operating system with osPageSize pages.
// Open takes osPageSize from its file system, so a simulated disk can select it.
func resolveOptions(options *Options, osPageSize int64) (*Options, error) {
	defaults := defaultOptions(osPageSize)
	resolved := defaults
	if options != nil {
		resolved = *options
		if resolved.Synchronous == SyncDefault {
			resolved.Synchronous = defaults.Synchronous
		}
		if resolved.MaxWriteBatchSize == 0 {
			resolved.MaxWriteBatchSize = defaults.MaxWriteBatchSize
		}
		if resolved.CheckpointThresholdBytes == 0 {
			resolved.CheckpointThresholdBytes = defaults.CheckpointThresholdBytes
		}
		if resolved.pageSize == 0 {
			resolved.pageSize = defaults.pageSize
		}
	}

	if resolved.LockTimeout < 0 {
		return nil, fmt.Errorf("invalid database lock timeout %s: %w", resolved.LockTimeout, page.ErrInvalid)
	}

	if resolved.Synchronous != SyncFull && resolved.Synchronous != SyncNormal && resolved.Synchronous != SyncNone {
		return nil, fmt.Errorf("invalid synchronous mode %d: %w", resolved.Synchronous, page.ErrInvalid)
	}
	if resolved.MaxWriteBatchSize < 0 {
		return nil, fmt.Errorf("invalid maximum write batch size %d: %w", resolved.MaxWriteBatchSize, page.ErrInvalid)
	}
	return &resolved, nil
}
