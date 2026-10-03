package kvlite

import (
	"fmt"
	"path/filepath"
	"testing"
)

const operationsBenchmarkEntries = 16 * 1024

var operationsBenchmarkBucket = []byte("bench")

// prepareOperationsBenchmark opens a SyncNone database with one bucket that
// holds operationsBenchmarkEntries keys in ascending order.
func prepareOperationsBenchmark(b *testing.B) (*DB, [][]byte) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })

	keys := make([][]byte, operationsBenchmarkEntries)
	value := make([]byte, 128)
	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket(operationsBenchmarkBucket)
		if err != nil {
			return err
		}
		for index := range keys {
			keys[index] = fmt.Appendf(nil, "key-%08d", index)
			if err := bucket.Put(keys[index], value); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	return db, keys
}

// BenchmarkCursor_ForwardScan measures a full ordered iteration with a cursor.
func BenchmarkCursor_ForwardScan(b *testing.B) {
	db, _ := prepareOperationsBenchmark(b)
	b.ReportAllocs()
	for b.Loop() {
		err := db.View(func(tx *Tx) error {
			bucket, err := tx.Bucket(operationsBenchmarkBucket)
			if err != nil {
				return err
			}
			cursor, err := bucket.Cursor()
			if err != nil {
				return err
			}
			count := 0
			key, _, err := cursor.First()
			for ; err == nil && key != nil; key, _, err = cursor.Next() {
				count++
			}
			if err != nil {
				return err
			}
			if count != operationsBenchmarkEntries {
				return fmt.Errorf("scanned %d keys, want %d", count, operationsBenchmarkEntries)
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCursor_Seek measures cursor positioning on existing keys.
func BenchmarkCursor_Seek(b *testing.B) {
	db, keys := prepareOperationsBenchmark(b)
	b.ReportAllocs()
	index := 0
	err := db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(operationsBenchmarkBucket)
		if err != nil {
			return err
		}
		cursor, err := bucket.Cursor()
		if err != nil {
			return err
		}
		for b.Loop() {
			if _, _, err := cursor.Seek(keys[(index*7919)%len(keys)]); err != nil {
				return err
			}
			index++
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
}

// BenchmarkScanPrefix measures a prefix scan that visits 1,000 keys.
func BenchmarkScanPrefix(b *testing.B) {
	db, _ := prepareOperationsBenchmark(b)
	prefix := []byte("key-00001")
	b.ReportAllocs()
	for b.Loop() {
		err := db.View(func(tx *Tx) error {
			bucket, err := tx.Bucket(operationsBenchmarkBucket)
			if err != nil {
				return err
			}
			return bucket.ScanPrefix(prefix, func(key, value []byte) error { return nil })
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkScanRange measures a range scan that visits 4,096 keys.
func BenchmarkScanRange(b *testing.B) {
	db, keys := prepareOperationsBenchmark(b)
	start, end := keys[4096], keys[8192]
	b.ReportAllocs()
	for b.Loop() {
		err := db.View(func(tx *Tx) error {
			bucket, err := tx.Bucket(operationsBenchmarkBucket)
			if err != nil {
				return err
			}
			return bucket.ScanRange(start, end, func(key, value []byte) error { return nil })
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUpdate_Batch100 measures one write transaction that updates 100
// existing keys.
func BenchmarkUpdate_Batch100(b *testing.B) {
	db, keys := prepareOperationsBenchmark(b)
	value := make([]byte, 128)
	b.ReportAllocs()
	iteration := 0
	for b.Loop() {
		err := db.Update(func(tx *Tx) error {
			bucket, err := tx.Bucket(operationsBenchmarkBucket)
			if err != nil {
				return err
			}
			offset := (iteration * 100) % (len(keys) - 100)
			for _, key := range keys[offset : offset+100] {
				if err := bucket.Put(key, value); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		iteration++
	}
}

// BenchmarkDelete_ThenReinsert measures deleting a key and writing it back.
func BenchmarkDelete_ThenReinsert(b *testing.B) {
	db, keys := prepareOperationsBenchmark(b)
	value := make([]byte, 128)
	b.ReportAllocs()
	iteration := 0
	for b.Loop() {
		key := keys[(iteration*7919)%len(keys)]
		if err := db.Delete(operationsBenchmarkBucket, key); err != nil {
			b.Fatal(err)
		}
		if err := db.Put(operationsBenchmarkBucket, key, value); err != nil {
			b.Fatal(err)
		}
		iteration++
	}
}
