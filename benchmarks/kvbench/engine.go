package kvbench

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrKeyNotFound means that a read key does not exist.
var ErrKeyNotFound = errors.New("benchmark key not found")

// DurabilityMode selects the write acknowledgement rule for an engine.
type DurabilityMode string

const (
	// DurabilityDurable waits for the engine's required storage sync before a successful write returns.
	DurabilityDurable DurabilityMode = "durable"
	// DurabilityNoCommitSync lets a successful write return without waiting for an engine-requested storage sync.
	DurabilityNoCommitSync DurabilityMode = "no-commit-sync"
)

// EngineKind identifies one database adapter.
type EngineKind string

const (
	EngineKVLite EngineKind = "kvlite"
	EngineBBolt  EngineKind = "bbolt"
	EngineRedis  EngineKind = "redis"
)

var benchmarkBucketName = []byte("kvbench")

// Engine runs operations that have a matched contract across all three databases.
type Engine interface {
	Get(context.Context, []byte) ([]byte, error)
	GetBatch(context.Context, [][]byte) ([][]byte, error)
	Put(context.Context, []byte, []byte) error
	PutBatch(context.Context, []Pair) error
	MixedBatch(context.Context, [][]byte, []Pair) ([][]byte, error)
	ScanPrefix(context.Context, []byte, func([]byte, []byte) error) error
	Count(context.Context) (int, error)
	StorageStats(context.Context) (storageStats, error)
	Validate(context.Context) error
	Close() error
}

type storageStats struct {
	primaryBytes int64
	logBytes     int64
	memoryBytes  int64
}

type orderedEngine interface {
	VisitOrdered(context.Context, []byte, []byte, bool, int, func([]byte, []byte) error) error
}

type collectionEngine interface {
	PrepareCollections(context.Context, [][][]byte) error
	PutCollectionBatch(context.Context, [][]byte, []Pair) error
	GetCollection(context.Context, [][]byte, []byte) ([]byte, error)
}

type engineOpenOptions struct {
	Kind         EngineKind
	Mode         DurabilityMode
	DataDir      string
	RedisAddr    string
	RedisFlushDB bool
	ClientCount  int
}

func openEngine(ctx context.Context, options engineOpenOptions) (Engine, error) {
	switch options.Kind {
	case EngineKVLite:
		return openKVLiteEngine(filepath.Join(options.DataDir, "kvlite.db"), options.Mode)
	case EngineBBolt:
		return openBBoltEngine(filepath.Join(options.DataDir, "bbolt.db"), options.Mode, options.ClientCount)
	case EngineRedis:
		return openRedisEngine(ctx, options.RedisAddr, options.Mode, options.ClientCount, options.RedisFlushDB)
	default:
		return nil, fmt.Errorf("unknown benchmark engine %q", options.Kind)
	}
}

func loadPairs(ctx context.Context, engine Engine, pairs []Pair, batchSize int) error {
	if batchSize < 1 {
		return fmt.Errorf("load batch size must be positive: %d", batchSize)
	}
	for start := 0; start < len(pairs); start += batchSize {
		end := min(start+batchSize, len(pairs))
		if err := engine.PutBatch(ctx, pairs[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func fileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
