package kvlite

import (
	"fmt"
	"os"

	"github.com/Issaminu/kvlite/internal/page"
)

type Sync uint8

const (
	SyncDefault Sync = iota
	SyncFull
	SyncNormal

	// SQLite checkpoints its WAL after 1,000 pages by default. KVLite uses the
	// same page count to calculate its default byte threshold.
	defaultCheckpointPageCount = 1000
)

// Options represents the options that can be set when opening a database.
type Options struct {
	// ReadOnly opens the database in read-only mode.
	ReadOnly bool

	// PageSize sets the page size for a new database. A zero value uses the
	// operating system page size. An existing database always uses its stored page
	PageSize int

	// Synchronous controls when KVLite syncs the WAL.
	// SyncFull syncs every commit.
	// SyncNormal syncs at a checkpoint or close.
	// SyncDefault uses the value from DefaultOptions.
	Synchronous Sync

	// CheckpointThresholdBytes sets the WAL size that starts a checkpoint.
	// A zero value uses the value from DefaultOptions.
	CheckpointThresholdBytes int64
}

// DefaultOptions is used when nil options are passed to Open.
var DefaultOptions = &Options{
	ReadOnly:                 false,
	PageSize:                 0,
	Synchronous:              SyncFull,
	CheckpointThresholdBytes: defaultCheckpointPageCount * int64(os.Getpagesize()),
}

func resolveOptions(options *Options) (*Options, error) {
	resolved := *DefaultOptions
	if options != nil {
		resolved.ReadOnly = options.ReadOnly
		resolved.PageSize = options.PageSize
		if options.Synchronous != SyncDefault {
			resolved.Synchronous = options.Synchronous
		}
		if options.CheckpointThresholdBytes != 0 {
			resolved.CheckpointThresholdBytes = options.CheckpointThresholdBytes
		}
	}

	if resolved.PageSize == 0 {
		resolved.PageSize = os.Getpagesize()
	}
	if resolved.PageSize < page.MetaSize || resolved.PageSize > MaxValueSize {
		return nil, fmt.Errorf("invalid database page size %d: %w", resolved.PageSize, ErrInvalid)
	}

	if resolved.Synchronous == SyncDefault {
		resolved.Synchronous = SyncFull
	}
	if resolved.Synchronous != SyncFull && resolved.Synchronous != SyncNormal {
		return nil, fmt.Errorf("invalid synchronous mode %d: %w", resolved.Synchronous, ErrInvalid)
	}
	if resolved.CheckpointThresholdBytes <= 0 {
		return nil, fmt.Errorf("invalid checkpoint threshold %d: %w", resolved.CheckpointThresholdBytes, ErrInvalid)
	}

	return &resolved, nil
}
