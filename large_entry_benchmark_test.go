package kvlite

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
)

var largeEntryBucket = []byte("large")

// openLargeEntryBenchmark creates a database with count entries of valueSize bytes.
// Each key has keySize bytes. The database is not closed.
func openLargeEntryBenchmark(b *testing.B, options *Options, count, keySize, valueSize int) (string, *DB, [][]byte) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "database")
	db, err := Open(path, 0600, options)
	if err != nil {
		b.Fatal(err)
	}
	keys := make([][]byte, count)
	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket(largeEntryBucket)
		if err != nil {
			return err
		}
		for index := range keys {
			key := fmt.Appendf(nil, "key-%08d", index)
			keys[index] = append(key, bytes.Repeat([]byte{'k'}, max(0, keySize-len(key)))...)
			if err := bucket.Put(keys[index], bytes.Repeat([]byte{byte(index)}, valueSize)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	return path, db, keys
}

// BenchmarkPut_LargeValueWAL measures repeated replacement of one 64 KiB value while the WAL keeps the last committed leaf.
// Each Put reads that continued leaf before it changes it.
func BenchmarkPut_LargeValueWAL(b *testing.B) {
	_, db, keys := openLargeEntryBenchmark(b, &Options{Synchronous: SyncNone, CheckpointThresholdBytes: 64 << 20}, 1, 16, 64<<10)
	defer db.Close()
	value := bytes.Repeat([]byte{1}, 64<<10)
	b.SetBytes(int64(len(value)))
	b.ReportAllocs()
	for b.Loop() {
		value[0]++
		if err := db.Put(largeEntryBucket, keys[0], value); err != nil {
			b.Fatal(err)
		}
	}
}
