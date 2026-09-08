package kvbench

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
)

// ErrKeyNotFound means that a read key does not exist.
var ErrKeyNotFound = errors.New("benchmark key not found")

// DurabilityMode selects the write acknowledgement rule for an engine.
type DurabilityMode string

const (
	// DurabilityDurable waits for the engine's required storage sync before a successful write returns.
	DurabilityDurable DurabilityMode = "durable"
	// DurabilityNoCommitSync lets a successful write return without a storage sync.
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

// Engine runs matched point operations against one logical collection.
type Engine interface {
	Get(context.Context, []byte) ([]byte, error)
	Put(context.Context, []byte, []byte) error
	PutBatch(context.Context, []Pair) error
	Count(context.Context) (int, error)
	Close() error
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
		return openBBoltEngine(filepath.Join(options.DataDir, "bbolt.db"), options.Mode)
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
