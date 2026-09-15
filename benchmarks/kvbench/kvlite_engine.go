package kvbench

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/Issaminu/kvlite"
)

type kvliteEngine struct {
	db          *kvlite.DB
	mode        DurabilityMode
	synchronous kvlite.Sync
}

func openKVLiteEngine(path string, mode DurabilityMode) (Engine, error) {
	synchronous, err := kvliteSync(mode)
	if err != nil {
		return nil, err
	}
	db, err := kvlite.Open(path, 0600, &kvlite.Options{Synchronous: synchronous})
	if err != nil {
		return nil, err
	}
	engine := &kvliteEngine{db: db, mode: mode, synchronous: synchronous}
	if err := engine.ensureBucket(); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return engine, nil
}

func kvliteSync(mode DurabilityMode) (kvlite.Sync, error) {
	switch mode {
	case DurabilityDurable:
		return kvlite.SyncFull, nil
	case DurabilityNoCommitSync:
		return kvlite.SyncNone, nil
	default:
		return kvlite.SyncDefault, fmt.Errorf("unsupported KVLite durability mode %q", mode)
	}
}

func (engine *kvliteEngine) ensureBucket() error {
	return engine.db.Update(func(tx *kvlite.Tx) error {
		_, err := tx.Bucket(benchmarkBucketName)
		if err == nil {
			return nil
		}
		if !errors.Is(err, kvlite.ErrBucketNotFound) {
			return err
		}
		_, err = tx.CreateBucket(benchmarkBucketName)
		return err
	})
}

func (engine *kvliteEngine) Get(_ context.Context, key []byte) ([]byte, error) {
	value, err := engine.db.Get(benchmarkBucketName, key)
	if errors.Is(err, kvlite.ErrKeyNotFound) {
		return nil, ErrKeyNotFound
	}
	return value, err
}

func (engine *kvliteEngine) GetBatch(_ context.Context, keys [][]byte) ([][]byte, error) {
	values := make([][]byte, len(keys))
	err := engine.db.View(func(tx *kvlite.Tx) error {
		bucket, err := tx.Bucket(benchmarkBucketName)
		if err != nil {
			return err
		}
		for index, key := range keys {
			value, err := bucket.Get(key)
			if errors.Is(err, kvlite.ErrKeyNotFound) {
				return ErrKeyNotFound
			}
			if err != nil {
				return err
			}
			values[index] = bytes.Clone(value)
		}
		return nil
	})
	return values, err
}

func (engine *kvliteEngine) Put(_ context.Context, key, value []byte) error {
	return engine.db.Put(benchmarkBucketName, key, value)
}

func (engine *kvliteEngine) PutBatch(_ context.Context, pairs []Pair) error {
	return engine.db.Update(func(tx *kvlite.Tx) error {
		bucket, err := tx.Bucket(benchmarkBucketName)
		if err != nil {
			return err
		}
		for _, pair := range pairs {
			if err := bucket.Put(pair.Key, pair.Value); err != nil {
				return err
			}
		}
		return nil
	})
}

func (engine *kvliteEngine) MixedBatch(_ context.Context, keys [][]byte, pairs []Pair) ([][]byte, error) {
	values := make([][]byte, len(keys))
	err := engine.db.Update(func(tx *kvlite.Tx) error {
		bucket, err := tx.Bucket(benchmarkBucketName)
		if err != nil {
			return err
		}
		for index, key := range keys {
			value, err := bucket.Get(key)
			if err != nil {
				return err
			}
			values[index] = bytes.Clone(value)
		}
		for _, pair := range pairs {
			if err := bucket.Put(pair.Key, pair.Value); err != nil {
				return err
			}
		}
		return nil
	})
	return values, err
}

func (engine *kvliteEngine) ScanPrefix(_ context.Context, prefix []byte, visit func([]byte, []byte) error) error {
	return engine.db.View(func(tx *kvlite.Tx) error {
		bucket, err := tx.Bucket(benchmarkBucketName)
		if err != nil {
			return err
		}
		return bucket.ScanPrefix(prefix, visit)
	})
}

func (engine *kvliteEngine) VisitOrdered(_ context.Context, start, end []byte, reverse bool, limit int, visit func([]byte, []byte) error) error {
	return engine.db.View(func(tx *kvlite.Tx) error {
		bucket, err := tx.Bucket(benchmarkBucketName)
		if err != nil {
			return err
		}
		cursor, err := bucket.Cursor()
		if err != nil {
			return err
		}
		var key, value []byte
		if reverse {
			key, value, err = cursor.Last()
		} else {
			key, value, err = cursor.Seek(start)
		}
		for visited := 0; err == nil && key != nil; visited++ {
			if limit > 0 && visited >= limit {
				break
			}
			if !reverse && len(end) > 0 && bytes.Compare(key, end) >= 0 {
				break
			}
			if reverse && len(start) > 0 && bytes.Compare(key, start) < 0 {
				break
			}
			if err := visit(key, value); err != nil {
				return err
			}
			if reverse {
				key, value, err = cursor.Prev()
			} else {
				key, value, err = cursor.Next()
			}
		}
		return err
	})
}

func (engine *kvliteEngine) Count(_ context.Context) (int, error) {
	count := 0
	err := engine.db.View(func(tx *kvlite.Tx) error {
		bucket, err := tx.Bucket(benchmarkBucketName)
		if err != nil {
			return err
		}
		cursor, err := bucket.Cursor()
		if err != nil {
			return err
		}
		for key, _, err := cursor.First(); key != nil || err != nil; key, _, err = cursor.Next() {
			if err != nil {
				return err
			}
			count++
		}
		return nil
	})
	return count, err
}

func (engine *kvliteEngine) Validate(_ context.Context) error {
	want, err := kvliteSync(engine.mode)
	if err != nil {
		return err
	}
	if engine.synchronous != want {
		return fmt.Errorf("KVLite sync mode is %d, want %d", engine.synchronous, want)
	}
	return nil
}

func (engine *kvliteEngine) StorageStats(_ context.Context) (storageStats, error) {
	primaryBytes, err := fileSize(engine.db.Path())
	if err != nil {
		return storageStats{}, err
	}
	logBytes, err := fileSize(engine.db.Path() + "-wal")
	if err != nil {
		return storageStats{}, err
	}
	runtimeStats, err := engine.db.Stats()
	if err != nil {
		return storageStats{}, err
	}
	return storageStats{
		primaryBytes:      primaryBytes,
		logBytes:          logBytes,
		walBytesWritten:   runtimeStats.WALBytesWritten,
		checkpointCount:   runtimeStats.CheckpointCount,
		hasKVLiteWALStats: true,
	}, nil
}

func (engine *kvliteEngine) PrepareCollections(_ context.Context, paths [][][]byte) error {
	return engine.db.Update(func(tx *kvlite.Tx) error {
		for _, path := range paths {
			bucket, err := tx.Bucket(benchmarkBucketName)
			if err != nil {
				return err
			}
			for _, name := range path {
				child, err := bucket.Bucket(name)
				if errors.Is(err, kvlite.ErrBucketNotFound) {
					child, err = bucket.CreateBucket(name)
				}
				if err != nil {
					return err
				}
				bucket = child
			}
		}
		return nil
	})
}

func (engine *kvliteEngine) PutCollectionBatch(_ context.Context, path [][]byte, pairs []Pair) error {
	return engine.db.Update(func(tx *kvlite.Tx) error {
		bucket, err := kvliteCollection(tx, path)
		if err != nil {
			return err
		}
		for _, pair := range pairs {
			if err := bucket.Put(pair.Key, pair.Value); err != nil {
				return err
			}
		}
		return nil
	})
}

func (engine *kvliteEngine) GetCollection(_ context.Context, path [][]byte, key []byte) ([]byte, error) {
	var value []byte
	err := engine.db.View(func(tx *kvlite.Tx) error {
		bucket, err := kvliteCollection(tx, path)
		if err != nil {
			return err
		}
		found, err := bucket.Get(key)
		if errors.Is(err, kvlite.ErrKeyNotFound) {
			return ErrKeyNotFound
		}
		value = bytes.Clone(found)
		return err
	})
	return value, err
}

func kvliteCollection(tx *kvlite.Tx, path [][]byte) (*kvlite.Bucket, error) {
	bucket, err := tx.Bucket(benchmarkBucketName)
	if err != nil {
		return nil, err
	}
	for _, name := range path {
		bucket, err = bucket.Bucket(name)
		if err != nil {
			return nil, err
		}
	}
	return bucket, nil
}

func (engine *kvliteEngine) Close() error { return engine.db.Close() }
