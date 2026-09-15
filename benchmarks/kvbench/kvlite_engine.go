package kvbench

import (
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

func (engine *kvliteEngine) Close() error { return engine.db.Close() }
