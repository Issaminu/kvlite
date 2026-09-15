package kvbench

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

type bboltEngine struct {
	db            *bolt.DB
	mode          DurabilityMode
	clients       int
	useWriteBatch bool
}

func openBBoltEngine(path string, mode DurabilityMode, clients int) (Engine, error) {
	options := *bolt.DefaultOptions
	switch mode {
	case DurabilityDurable:
	case DurabilityNoCommitSync:
		options.NoSync = true
		// The benchmark uses disposable files. This setting prevents bbolt from issuing a file-growth sync in the no-commit-sync mode.
		options.NoGrowSync = true
	default:
		return nil, fmt.Errorf("unsupported bbolt durability mode %q", mode)
	}
	db, err := bolt.Open(path, 0600, &options)
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(benchmarkBucketName)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	engine := &bboltEngine{
		db:            db,
		mode:          mode,
		clients:       clients,
		useWriteBatch: mode == DurabilityDurable && clients > 1,
	}
	if err := engine.Validate(context.Background()); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return engine, nil
}

func (engine *bboltEngine) Get(_ context.Context, key []byte) ([]byte, error) {
	var value []byte
	err := engine.db.View(func(tx *bolt.Tx) error {
		found := tx.Bucket(benchmarkBucketName).Get(key)
		if found == nil {
			return ErrKeyNotFound
		}
		value = bytes.Clone(found)
		return nil
	})
	return value, err
}

func (engine *bboltEngine) Put(_ context.Context, key, value []byte) error {
	if engine.useWriteBatch {
		// Batch can run this callback more than once. Put is safe to repeat with the same key and value.
		return engine.db.Batch(func(tx *bolt.Tx) error {
			return tx.Bucket(benchmarkBucketName).Put(key, value)
		})
	}
	return engine.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(benchmarkBucketName).Put(key, value)
	})
}

func (engine *bboltEngine) Validate(_ context.Context) error {
	wantNoSync := engine.mode == DurabilityNoCommitSync
	if engine.db.NoSync != wantNoSync {
		return fmt.Errorf("bbolt NoSync is %t, want %t", engine.db.NoSync, wantNoSync)
	}
	if engine.db.NoGrowSync != wantNoSync {
		return fmt.Errorf("bbolt NoGrowSync is %t, want %t", engine.db.NoGrowSync, wantNoSync)
	}
	if engine.db.NoFreelistSync {
		return errors.New("bbolt NoFreelistSync is enabled")
	}
	if engine.db.FreelistType != bolt.FreelistArrayType {
		return fmt.Errorf("bbolt freelist type is %q, want %q", engine.db.FreelistType, bolt.FreelistArrayType)
	}
	wantWriteBatch := engine.mode == DurabilityDurable && engine.clients > 1
	if engine.useWriteBatch != wantWriteBatch {
		return fmt.Errorf("bbolt DB.Batch use is %t, want %t", engine.useWriteBatch, wantWriteBatch)
	}
	if engine.useWriteBatch && (engine.db.MaxBatchSize != 1_000 || engine.db.MaxBatchDelay != 10*time.Millisecond) {
		return fmt.Errorf("bbolt batch limit is %d calls and %s, want 1000 calls and 10ms", engine.db.MaxBatchSize, engine.db.MaxBatchDelay)
	}
	return nil
}

func (engine *bboltEngine) PutBatch(_ context.Context, pairs []Pair) error {
	return engine.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(benchmarkBucketName)
		for _, pair := range pairs {
			if err := bucket.Put(pair.Key, pair.Value); err != nil {
				return err
			}
		}
		return nil
	})
}

func (engine *bboltEngine) Count(_ context.Context) (int, error) {
	count := 0
	err := engine.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(benchmarkBucketName).Cursor()
		for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
			count++
		}
		return nil
	})
	return count, err
}

func (engine *bboltEngine) Close() error { return engine.db.Close() }
