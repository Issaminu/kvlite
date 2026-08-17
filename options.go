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

	defaultCheckpointPageCount = 1000
)

type Options struct {
	ReadOnly bool

	PageSize int

	Synchronous Sync

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
