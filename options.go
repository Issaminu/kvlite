package kvlite

import (
	"fmt"
	"os"
	"time"
)

// Sync controls when committed write-ahead log data is synchronized to storage.
type Sync uint8

const (
	// SyncDefault selects KVLite's default mode, SyncFull.
	SyncDefault Sync = iota

	// SyncFull favors durability over write speed. It waits for each non-empty write transaction to reach storage before [DB.Update] returns success. Concurrent updates can share one write-ahead log append and storage synchronization.
	SyncFull

	// SyncNormal favors write speed over the durability of recent updates. It waits until a checkpoint or close to synchronize the write-ahead log, so [DB.Update] can return success while recent writes remain only in the operating system cache. A system failure can lose those writes.
	SyncNormal

	// defaultCheckpointPageCount keeps about 1,000 operating-system pages of data in the write-ahead log before KVLite starts a checkpoint. This limits log growth without running a checkpoint after every update.
	defaultCheckpointPageCount = 1000

	defaultPageCacheBytes uint64 = 16 << 20 // 16 MiB
)

// Options configures [Open]. Open resolves the zero values described below and then copies the result, so callers can reuse or change their Options after Open returns.
type Options struct {
	// ReadOnly opens an existing database without creating or modifying its database or write-ahead log files. Because this mode cannot write, [DB.Update] and [DB.Put] return [ErrDatabaseReadOnly].
	ReadOnly bool

	// LockTimeout limits how long [Open] waits for a conflicting database-file lock. A zero value waits until the other handle closes, a positive value returns an error that matches [ErrDatabaseLocked] after that duration, and a negative value is invalid. KVLite holds the lock for the full [DB] lifetime, not for one transaction.
	LockTimeout time.Duration

	// Synchronous controls when KVLite synchronizes committed write-ahead log data. A zero value uses SyncFull.
	Synchronous Sync

	// CheckpointThresholdBytes is the number of committed write-ahead log bytes that starts a checkpoint. A zero value uses about 1,000 operating-system pages, while a smaller value keeps the log smaller at the cost of more frequent checkpoints.
	CheckpointThresholdBytes uint64

	// PageCacheBytes limits the encoded size of committed child pages that KVLite keeps decoded between transactions. A zero value uses 16 MiB. The cache grows as pages are read and decoded Go values can use more memory than this limit.
	PageCacheBytes uint64

	// DisablePageCache prevents KVLite from keeping decoded child pages between transactions. The catalog root remains in memory while the database is open.
	DisablePageCache bool
}

func defaultOptions() Options {
	return Options{
		ReadOnly:                 false,
		Synchronous:              SyncFull,
		CheckpointThresholdBytes: defaultCheckpointPageCount * uint64(os.Getpagesize()),
		PageCacheBytes:           defaultPageCacheBytes,
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
		if resolved.PageCacheBytes == 0 {
			resolved.PageCacheBytes = defaults.PageCacheBytes
		}
	}

	if resolved.LockTimeout < 0 {
		return nil, fmt.Errorf("invalid database lock timeout %s: %w", resolved.LockTimeout, ErrInvalid)
	}

	if resolved.Synchronous != SyncFull && resolved.Synchronous != SyncNormal {
		return nil, fmt.Errorf("invalid synchronous mode %d: %w", resolved.Synchronous, ErrInvalid)
	}
	return &resolved, nil
}
