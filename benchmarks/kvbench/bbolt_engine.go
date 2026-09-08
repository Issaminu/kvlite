package kvbench

import (
	"bytes"
	"context"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

type bboltEngine struct {
	db *bolt.DB
}

func openBBoltEngine(path string, mode DurabilityMode) (Engine, error) {
	options := &bolt.Options{}
	switch mode {
	case DurabilityDurable:
	case DurabilityNoCommitSync:
		options.NoSync = true
	default:
		return nil, fmt.Errorf("unsupported bbolt durability mode %q", mode)
	}
	db, err := bolt.Open(path, 0600, options)
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
	return &bboltEngine{db: db}, nil
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
	return engine.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(benchmarkBucketName).Put(key, value)
	})
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
