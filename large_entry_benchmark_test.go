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

// reopenReadOnly closes db and opens path again in read-only mode, so reads use the mapped main file.
func reopenReadOnly(b *testing.B, path string, db *DB) *DB {
	b.Helper()
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	db, err := Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db
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

// BenchmarkViewGet_LargeKey measures Bucket.Get in a read transaction with keys that do not fit in one page.
func BenchmarkViewGet_LargeKey(b *testing.B) {
	path, db, keys := openLargeEntryBenchmark(b, nil, 16, MaxKeySize, 128)
	db = reopenReadOnly(b, path, db)
	b.ReportAllocs()
	index := 0
	for b.Loop() {
		err := db.View(func(tx *Tx) error {
			bucket, err := tx.Bucket(largeEntryBucket)
			if err != nil {
				return err
			}
			_, err = bucket.Get(keys[index%len(keys)])
			return err
		})
		if err != nil {
			b.Fatal(err)
		}
		index++
	}
}

// BenchmarkScanRange_LargeValues measures a full scan of continued 64 KiB values in the mapped main file.
func BenchmarkScanRange_LargeValues(b *testing.B) {
	const count, size = 64, 64 << 10
	path, db, _ := openLargeEntryBenchmark(b, nil, count, 16, size)
	db = reopenReadOnly(b, path, db)
	b.SetBytes(count * size)
	b.ReportAllocs()
	for b.Loop() {
		visited := 0
		err := db.View(func(tx *Tx) error {
			bucket, err := tx.Bucket(largeEntryBucket)
			if err != nil {
				return err
			}
			return bucket.ScanRange(nil, nil, func(key, value []byte) error {
				visited += len(value)
				return nil
			})
		})
		if err != nil || visited != count*size {
			b.Fatalf("scan visited %d bytes, error %v", visited, err)
		}
	}
}
