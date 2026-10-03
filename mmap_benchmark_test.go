package kvlite

import (
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"runtime"
	"testing"
)

// BenchmarkGet_LargeUniform measures point reads across a database that is
// larger than the former decoded page cache.
func BenchmarkGet_LargeUniform(b *testing.B) {
	path, db, keys := prepareLargeGetBenchmark(b)
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}

	db, err := Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	benchmarkLargeUniformGets(b, db, keys)
}

// BenchmarkGet_LargeUniformParallelWarm measures parallel point reads after every key has been read once.
func BenchmarkGet_LargeUniformParallelWarm(b *testing.B) {
	path, db, keys := prepareLargeGetBenchmark(b)
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}

	db, err := Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	for _, key := range keys {
		if _, err := db.Get([]byte("bench"), key); err != nil {
			b.Fatal(err)
		}
	}

	const readsPerWorker = 100
	workers := runtime.GOMAXPROCS(0)
	requests := make([]chan struct{}, workers)
	results := make(chan error, workers)
	for worker := range requests {
		requests[worker] = make(chan struct{})
		go func(worker int, request <-chan struct{}) {
			random := rand.New(rand.NewSource(int64(worker + 1)))
			for range request {
				var readError error
				for range readsPerWorker {
					if _, err := db.Get([]byte("bench"), keys[random.Intn(len(keys))]); err != nil {
						readError = err
						break
					}
				}
				results <- readError
			}
		}(worker, requests[worker])
	}
	defer func() {
		for _, request := range requests {
			close(request)
		}
	}()
	b.SetBytes(int64(workers * readsPerWorker * 256))
	for b.Loop() {
		for _, request := range requests {
			request <- struct{}{}
		}
		for range requests {
			if err := <-results; err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*workers*readsPerWorker), "ns/op")
}

// BenchmarkGet_LargeUniformWAL measures point reads from the WAL overlay.
func BenchmarkGet_LargeUniformWAL(b *testing.B) {
	_, db, keys := prepareLargeGetBenchmark(b)
	defer db.Close()

	benchmarkLargeUniformGets(b, db, keys)
}

func prepareLargeGetBenchmark(b *testing.B) (string, *DB, [][]byte) {
	b.Helper()
	const entryCount = 64 * 1024

	path := filepath.Join(b.TempDir(), "database")
	db, err := Open(path, 0600, &Options{
		Synchronous:              SyncNone,
		CheckpointThresholdBytes: math.MaxUint64,
	})
	if err != nil {
		b.Fatal(err)
	}

	keys := make([][]byte, entryCount)
	value := make([]byte, 256)
	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("bench"))
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

	return path, db, keys
}

func benchmarkLargeUniformGets(b *testing.B, db *DB, keys [][]byte) {
	b.Helper()
	random := rand.New(rand.NewSource(1))
	b.ReportAllocs()
	for b.Loop() {
		keyIndex := random.Intn(len(keys))
		if _, err := db.Get([]byte("bench"), keys[keyIndex]); err != nil {
			b.Fatal(err)
		}
	}
}
