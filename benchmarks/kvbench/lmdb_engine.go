package kvbench

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/PowerDNS/lmdb-go/lmdb"
)

const (
	lmdbMapSize    = 1 << 30
	lmdbMaxKeySize = 511
)

type lmdbEngine struct {
	env         *lmdb.Env
	db          lmdb.DBI
	path        string
	mode        DurabilityMode
	extraFlags  uint
	collections map[string]lmdb.DBI
}

func openLMDBEngine(path string, mode DurabilityMode) (Engine, error) {
	var flags uint
	if mode == DurabilityNoCommitSync {
		// Disposable no-sync files use LMDB's mapped write path; durable commits keep its default write path.
		flags = lmdb.WriteMap
	}
	return openLMDBEngineWithFlags(path, mode, flags)
}

func openLMDBEngineWithFlags(path string, mode DurabilityMode, extraFlags uint) (Engine, error) {
	if extraFlags&^uint(lmdb.WriteMap) != 0 || (extraFlags != 0 && mode != DurabilityNoCommitSync) {
		return nil, fmt.Errorf("unsupported LMDB extra flags %#x for mode %q", extraFlags, mode)
	}
	flags := uint(lmdb.NoSubdir) | extraFlags
	switch mode {
	case DurabilityDurable:
	case DurabilityNoCommitSync:
		flags |= lmdb.NoSync
	default:
		return nil, fmt.Errorf("unsupported LMDB durability mode %q", mode)
	}
	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}
	fail := func(err error) (Engine, error) { return nil, errors.Join(err, env.Close()) }
	// Reserve address space before measurement. The map size is not a stored-data size.
	if err := env.SetMapSize(lmdbMapSize); err != nil {
		return fail(err)
	}
	if err := env.SetMaxDBs(512); err != nil {
		return fail(err)
	}
	if err := env.Open(path, flags, 0600); err != nil {
		return fail(err)
	}
	engine := &lmdbEngine{env: env, path: path, mode: mode, extraFlags: extraFlags, collections: make(map[string]lmdb.DBI)}
	if err := env.Update(func(tx *lmdb.Txn) error {
		var err error
		engine.db, err = tx.OpenDBI(string(benchmarkBucketName), lmdb.Create)
		if err != nil {
			return err
		}
		root, err := tx.OpenRoot(0)
		if err != nil {
			return err
		}
		cursor, err := tx.OpenCursor(root)
		if err != nil {
			return err
		}
		defer cursor.Close()
		// Open collection handles before clients start. LMDB cannot open handles concurrently.
		for key, _, err := cursor.Get(nil, nil, lmdb.First); ; key, _, err = cursor.Get(nil, nil, lmdb.Next) {
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}
			name := string(key)
			if strings.HasPrefix(name, "collection/") {
				db, err := tx.OpenDBI(name, 0)
				if err != nil {
					return err
				}
				engine.collections[name] = db
			}
		}
	}); err != nil {
		return fail(err)
	}
	if err := engine.Validate(context.Background()); err != nil {
		return fail(err)
	}
	return engine, nil
}

func (engine *lmdbEngine) Get(_ context.Context, key []byte) ([]byte, error) {
	var value []byte
	err := engine.env.View(func(tx *lmdb.Txn) error {
		// RawRead stays false so the binding copies the result before the transaction ends.
		var err error
		value, err = tx.Get(engine.db, key)
		return err
	})
	return value, lmdbReadError(err)
}

func (engine *lmdbEngine) GetBatch(_ context.Context, keys [][]byte) ([][]byte, error) {
	values := make([][]byte, len(keys))
	err := engine.env.View(func(tx *lmdb.Txn) error {
		for index, key := range keys {
			var err error
			values[index], err = tx.Get(engine.db, key)
			if lmdb.IsNotFound(err) {
				continue
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, lmdbReadError(err)
	}
	return values, nil
}

func lmdbReadError(err error) error {
	if lmdb.IsNotFound(err) {
		return ErrKeyNotFound
	}
	return err
}

func (engine *lmdbEngine) Put(_ context.Context, key, value []byte) error {
	return engine.env.Update(func(tx *lmdb.Txn) error { return tx.Put(engine.db, key, value, 0) })
}

func (engine *lmdbEngine) PutBatch(_ context.Context, pairs []Pair) error {
	return engine.env.Update(func(tx *lmdb.Txn) error {
		cursor, err := tx.OpenCursor(engine.db)
		if err != nil {
			return err
		}
		defer cursor.Close()
		for _, pair := range pairs {
			if err := cursor.Put(pair.Key, pair.Value, 0); err != nil {
				return err
			}
		}
		return nil
	})
}

func (engine *lmdbEngine) Sync(_ context.Context) error {
	return engine.env.Sync(true)
}

func (engine *lmdbEngine) Delete(ctx context.Context, key []byte) error {
	return engine.DeleteBatch(ctx, [][]byte{key})
}

func (engine *lmdbEngine) DeleteBatch(_ context.Context, keys [][]byte) error {
	return engine.env.Update(func(tx *lmdb.Txn) error {
		for _, key := range keys {
			if err := tx.Del(engine.db, key, nil); err != nil && !lmdb.IsNotFound(err) {
				return err
			}
		}
		return nil
	})
}

func (engine *lmdbEngine) MixedBatch(_ context.Context, keys [][]byte, pairs []Pair) ([][]byte, error) {
	values := make([][]byte, len(keys))
	err := engine.env.Update(func(tx *lmdb.Txn) error {
		for index, key := range keys {
			var err error
			values[index], err = tx.Get(engine.db, key)
			if err != nil {
				return err
			}
		}
		for _, pair := range pairs {
			if err := tx.Put(engine.db, pair.Key, pair.Value, 0); err != nil {
				return err
			}
		}
		return nil
	})
	return values, lmdbReadError(err)
}

func (engine *lmdbEngine) ScanPrefix(_ context.Context, prefix []byte, visit func([]byte, []byte) error) error {
	return engine.env.View(func(tx *lmdb.Txn) error {
		// Scan callbacks consume the mapped bytes before this transaction ends.
		tx.RawRead = true
		cursor, err := tx.OpenCursor(engine.db)
		if err != nil {
			return err
		}
		defer cursor.Close()
		op := uint(lmdb.First)
		if len(prefix) > 0 {
			op = lmdb.SetRange
		}
		for key, value, err := cursor.Get(prefix, nil, op); ; key, value, err = cursor.Get(nil, nil, lmdb.Next) {
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if !bytes.HasPrefix(key, prefix) {
				return nil
			}
			if err := visit(key, value); err != nil {
				return err
			}
		}
	})
}

func (engine *lmdbEngine) VisitOrdered(_ context.Context, start, end []byte, reverse bool, limit int, visit func([]byte, []byte) error) error {
	return engine.env.View(func(tx *lmdb.Txn) error {
		tx.RawRead = true
		cursor, err := tx.OpenCursor(engine.db)
		if err != nil {
			return err
		}
		defer cursor.Close()
		op, step := uint(lmdb.First), uint(lmdb.Next)
		if reverse {
			op, step = lmdb.Last, lmdb.Prev
		} else if len(start) > 0 {
			op = lmdb.SetRange
		}
		for key, value, err := cursor.Get(start, nil, op); ; key, value, err = cursor.Get(nil, nil, step) {
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if (!reverse && len(end) > 0 && bytes.Compare(key, end) >= 0) || (reverse && len(start) > 0 && bytes.Compare(key, start) < 0) {
				return nil
			}
			if err := visit(key, value); err != nil {
				return err
			}
			if limit > 0 {
				limit--
				if limit == 0 {
					return nil
				}
			}
		}
	})
}

func (engine *lmdbEngine) Count(ctx context.Context) (int, error) {
	count := 0
	err := engine.ScanPrefix(ctx, nil, func(_, _ []byte) error { count++; return nil })
	return count, err
}

func (engine *lmdbEngine) StorageStats(_ context.Context) (storageStats, error) {
	stats := storageStats{hasAllocatedBytes: true}
	for _, path := range []string{engine.path, engine.path + "-lock"} {
		size, err := fileSize(path)
		if err != nil {
			return storageStats{}, err
		}
		allocated, available, err := fileAllocatedBytes(path)
		if err != nil {
			return storageStats{}, err
		}
		stats.primaryBytes += size
		stats.allocatedBytes += allocated
		stats.hasAllocatedBytes = stats.hasAllocatedBytes && available
	}
	return stats, nil
}

func (engine *lmdbEngine) Validate(_ context.Context) error {
	flags, err := engine.env.Flags()
	if err != nil {
		return err
	}
	want := uint(lmdb.NoSubdir|lmdb.NoTLS) | engine.extraFlags
	if engine.mode == DurabilityNoCommitSync {
		want |= lmdb.NoSync
	}
	if flags != want {
		return fmt.Errorf("LMDB flags are %#x, want %#x", flags, want)
	}
	if size := engine.env.MaxKeySize(); size != lmdbMaxKeySize {
		return fmt.Errorf("LMDB maximum key size is %d, want %d", size, lmdbMaxKeySize)
	}
	info, err := engine.env.Info()
	if err != nil {
		return err
	}
	if info.MapSize != lmdbMapSize {
		return fmt.Errorf("LMDB map size is %d, want %d", info.MapSize, lmdbMapSize)
	}
	return nil
}

func (engine *lmdbEngine) PrepareCollections(_ context.Context, paths [][][]byte) error {
	handles := make(map[string]lmdb.DBI, len(paths))
	err := engine.env.Update(func(tx *lmdb.Txn) error {
		for _, path := range paths {
			name := lmdbCollectionName(path)
			db, err := tx.OpenDBI(name, lmdb.Create)
			if err != nil {
				return err
			}
			handles[name] = db
		}
		return nil
	})
	if err == nil {
		maps.Copy(engine.collections, handles)
	}
	return err
}

func (engine *lmdbEngine) PutCollectionBatch(_ context.Context, path [][]byte, pairs []Pair) error {
	db, ok := engine.collections[lmdbCollectionName(path)]
	if !ok {
		return ErrKeyNotFound
	}
	return engine.env.Update(func(tx *lmdb.Txn) error {
		for _, pair := range pairs {
			if err := tx.Put(db, pair.Key, pair.Value, 0); err != nil {
				return err
			}
		}
		return nil
	})
}

func (engine *lmdbEngine) GetCollection(_ context.Context, path [][]byte, key []byte) ([]byte, error) {
	db, ok := engine.collections[lmdbCollectionName(path)]
	if !ok {
		return nil, ErrKeyNotFound
	}
	var value []byte
	err := engine.env.View(func(tx *lmdb.Txn) error {
		var err error
		value, err = tx.Get(db, key)
		return err
	})
	return value, lmdbReadError(err)
}

func lmdbCollectionName(path [][]byte) string {
	name := []byte("collection/")
	for _, part := range path {
		name = hex.AppendEncode(name, part)
		name = append(name, '/')
	}
	return string(name)
}

func (engine *lmdbEngine) PrepareBucket(_ context.Context, name []byte, pairs []Pair) error {
	return engine.env.Update(func(tx *lmdb.Txn) error {
		db, err := tx.OpenDBI("bucket/"+hex.EncodeToString(name), lmdb.Create)
		if err != nil {
			return err
		}
		for _, pair := range pairs {
			if err := tx.Put(db, pair.Key, pair.Value, 0); err != nil {
				return err
			}
		}
		return nil
	})
}

func (engine *lmdbEngine) DeleteBucket(_ context.Context, name []byte) error {
	return engine.env.Update(func(tx *lmdb.Txn) error {
		db, err := tx.OpenDBI("bucket/"+hex.EncodeToString(name), 0)
		if err != nil {
			return err
		}
		return tx.Drop(db, true)
	})
}

func (engine *lmdbEngine) BucketExists(_ context.Context, name []byte) (bool, error) {
	exists := false
	err := engine.env.View(func(tx *lmdb.Txn) error {
		_, err := tx.OpenDBI("bucket/"+hex.EncodeToString(name), 0)
		if lmdb.IsNotFound(err) {
			return nil
		}
		exists = err == nil
		return err
	})
	return exists, err
}

func (engine *lmdbEngine) Close() error { return engine.env.Close() }
