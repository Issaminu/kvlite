package kvlite

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestOpenMemory_TransactionsAndClose(t *testing.T) {
	db := OpenMemory()
	if db.Path() != "" || db.file != nil || db.wal != nil || db.mappedFile != nil || db.writeRequests != nil {
		t.Fatal("memory database has file resources or a write worker")
	}
	t.Cleanup(func() {
		if !db.closed {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("data"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte("key"), []byte("old"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket([]byte("data"))
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte("key"), []byte("new")); err != nil {
			return err
		}
		return errDiscardTx
	}); !errors.Is(err, errDiscardTx) {
		t.Fatalf("rollback: %v", err)
	}
	value, err := db.Get([]byte("data"), []byte("key"))
	if err != nil || string(value) != "old" {
		t.Fatalf("committed value: %q, %v", value, err)
	}
	stats, err := db.Stats()
	if err != nil || stats.WALBytesWritten != 0 || stats.CheckpointCount != 0 {
		t.Fatalf("memory stats: %+v, %v", stats, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("data"), []byte("key")); !errors.Is(err, ErrDatabaseNotOpen) {
		t.Fatalf("get after close: %v", err)
	}
	if err := db.Close(); !errors.Is(err, ErrDatabaseNotOpen) {
		t.Fatalf("second close: %v", err)
	}
	other := OpenMemory()
	defer other.Close()
	if _, err := other.Get([]byte("data"), []byte("key")); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("new memory database retained data: %v", err)
	}
}

func TestStorageModes_LargeEntriesAndStats(t *testing.T) {
	for _, mode := range []string{"file", "memory"} {
		t.Run(mode, func(t *testing.T) {
			var db *DB
			if mode == "memory" {
				db = OpenMemory()
			} else {
				var err error
				db, err = Open(filepath.Join(t.TempDir(), "database"), 0o600, &Options{Synchronous: SyncNone})
				if err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			keys := make([][]byte, 8)
			values := make([][]byte, len(keys))
			for i := range keys {
				keys[i] = bytes.Repeat([]byte{byte('a' + i)}, MaxKeySize)
				values[i] = bytes.Repeat([]byte{byte(i)}, int(db.meta.PageSize())*3+37)
			}
			var pending BucketStats
			if err := db.Update(func(tx *Tx) error {
				parent, err := tx.CreateBucket(testBucketName)
				if err != nil {
					return err
				}
				nested, err := parent.CreateBucket([]byte("nested"))
				if err != nil {
					return err
				}
				for i, key := range keys {
					if err := nested.Put(key, values[i]); err != nil {
						return err
					}
				}
				pending, err = parent.Stats()
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if pending.KeyN != len(keys) || pending.BucketN != 2 || pending.BranchPageN == 0 || pending.LeafAlloc <= pending.LeafPageN*int(db.meta.PageSize()) {
				t.Fatalf("large entry stats: %+v", pending)
			}
			if err := db.View(func(tx *Tx) error {
				parent, err := tx.Bucket(testBucketName)
				if err != nil {
					return err
				}
				committed, err := parent.Stats()
				if err != nil {
					return err
				}
				if committed != pending {
					return fmt.Errorf("bucket stats changed at commit: before %+v, after %+v", pending, committed)
				}
				nested, err := parent.Bucket([]byte("nested"))
				if err != nil {
					return err
				}
				for i, key := range keys {
					value, err := nested.Get(key)
					if err != nil || !bytes.Equal(value, values[i]) {
						return fmt.Errorf("large entry %d: %v", i, err)
					}
				}
				cursor, err := nested.Cursor()
				if err != nil {
					return err
				}
				key, value, err := cursor.Seek(keys[4])
				if err != nil || !bytes.Equal(key, keys[4]) || !bytes.Equal(value, values[4]) {
					return fmt.Errorf("large entry seek: %v", err)
				}
				index := len(keys) - 1
				if err := nested.ScanReverse(func(key, value []byte) error {
					if index < 0 || !bytes.Equal(key, keys[index]) || !bytes.Equal(value, values[index]) {
						return fmt.Errorf("large reverse scan at %d", index)
					}
					index--
					return nil
				}); err != nil {
					return err
				}
				if index != -1 {
					return fmt.Errorf("large reverse scan ended at %d", index)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			before := readStats(t, db)
			if before.PageSize != int(db.meta.PageSize()) || before.DataSize != int64(before.LastPageID+1)*int64(before.PageSize) {
				t.Fatalf("logical sizes: %+v", before)
			}
			if mode == "memory" && (before.FileSize != 0 || before.WALSize != 0 || before.WALPageCount != 0 || before.WALBytesSinceCheckpoint != 0 || before.CheckpointThresholdBytes != 0 || before.WALBytesWritten != 0 || before.WALCommitCount != 0 || before.CheckpointCount != 0 || before.CheckpointFailureCount != 0 || before.LastCheckpointError != nil) {
				t.Fatalf("memory file counters: %+v", before)
			}
			if err := db.Update(func(tx *Tx) error {
				parent, err := tx.Bucket(testBucketName)
				if err != nil {
					return err
				}
				if err := parent.DeleteBucket([]byte("nested")); err != nil {
					return err
				}
				return errDiscardTx
			}); !errors.Is(err, errDiscardTx) {
				t.Fatal(err)
			}
			if after := readStats(t, db); after != before {
				t.Fatalf("large deletion rollback changed stats: %+v", after)
			}
			if err := db.Update(func(tx *Tx) error {
				parent, err := tx.Bucket(testBucketName)
				if err != nil {
					return err
				}
				nested, err := parent.Bucket([]byte("nested"))
				if err != nil {
					return err
				}
				return nested.Put(keys[0], nil)
			}); err != nil {
				t.Fatal(err)
			}
			if after := readStats(t, db); after.PagesRetired <= before.PagesRetired {
				t.Fatalf("large overwrite did not retire pages: %+v", after)
			}
			if err := db.DeleteBucket(testBucketName); err != nil {
				t.Fatal(err)
			}
			if after := readStats(t, db); after.AllocatedPageCount != 4 {
				t.Fatalf("large bucket deletion kept pages: %+v", after)
			}
			if mode == "memory" && len(db.memoryNodes) != 1 {
				t.Fatalf("large bucket deletion kept %d nodes", len(db.memoryNodes))
			}
		})
	}
}

func TestOpenMemory_BackupDoesNotAccessFiles(t *testing.T) {
	db := newMemoryTestDB(t)
	path := filepath.Join(t.TempDir(), "existing")
	if err := os.WriteFile(path, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{path, path + "-new", filepath.Join(path, "not-a-directory")} {
		if err := db.Backup(target); !errors.Is(err, errors.ErrUnsupported) {
			t.Fatalf("memory backup to %q: %v", target, err)
		}
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "unchanged" {
		t.Fatalf("memory backup changed a file: %q, %v", got, err)
	}
	if _, err := os.Stat(path + "-new"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("memory backup created a file: %v", err)
	}
}

func newMemoryTestDB(t *testing.T) *DB {
	t.Helper()
	db := OpenMemory()
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket(testBucketName)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestOpenMemory_TreeGrowthScansAndPageReuse(t *testing.T) {
	db := newMemoryTestDB(t)
	const count = maxCachedWriteNodePages + 100
	value := bytes.Repeat([]byte("x"), int(db.meta.PageSize()/2))
	key := func(index int) []byte { return fmt.Appendf(nil, "%06d", index) }
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		for index := range count {
			if err := bucket.Put(key(index), value); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(db.memoryNodes) <= maxCachedWriteNodePages {
		t.Fatalf("tree has only %d nodes", len(db.memoryNodes))
	}
	for index := range count {
		got, err := db.Get(testBucketName, key(index))
		if err != nil || !bytes.Equal(got, value) {
			t.Fatalf("get %d: %v", index, err)
		}
	}
	if err := db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		next := 0
		if err := bucket.ScanPrefix(nil, func(k, v []byte) error {
			if !bytes.Equal(k, key(next)) || !bytes.Equal(v, value) {
				return fmt.Errorf("forward scan at %d: key %q", next, k)
			}
			next++
			return nil
		}); err != nil {
			return err
		}
		if next != count {
			return fmt.Errorf("forward scan count: %d", next)
		}
		next = count - 1
		if err := bucket.ScanReverse(func(k, v []byte) error {
			if !bytes.Equal(k, key(next)) || !bytes.Equal(v, value) {
				return fmt.Errorf("reverse scan at %d: key %q", next, k)
			}
			next--
			return nil
		}); err != nil {
			return err
		}
		if next != -1 {
			return fmt.Errorf("reverse scan ended at %d", next)
		}
		next = 100
		if err := bucket.ScanRange(key(100), key(200), func(k, v []byte) error {
			if !bytes.Equal(k, key(next)) {
				return fmt.Errorf("range scan at %d: key %q", next, k)
			}
			next++
			return nil
		}); err != nil {
			return err
		}
		if next != 200 {
			return fmt.Errorf("range scan ended at %d", next)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		keys := make([][]byte, count/2)
		for index := range keys {
			keys[index] = key(index)
		}
		return bucket.DeleteBatch(keys)
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		for index := range count / 2 {
			if err := bucket.Put(key(index), value); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	stats, err := db.Stats()
	if err != nil || stats.PagesReused == 0 || stats.PagesRetired == 0 {
		t.Fatalf("page reuse: %+v, %v", stats, err)
	}
	for index := range count {
		got, err := db.Get(testBucketName, key(index))
		if err != nil || !bytes.Equal(got, value) {
			t.Fatalf("get after reuse %d: %v", index, err)
		}
	}
	if err := db.DeleteBucket(testBucketName); err != nil {
		t.Fatal(err)
	}
	if len(db.memoryNodes) != 1 {
		t.Fatalf("bucket deletion kept %d nodes", len(db.memoryNodes))
	}
	stats, err = db.Stats()
	if err != nil || stats.AllocatedPageCount != 4 || stats.TailPagesReclaimed == 0 || stats.WALBytesWritten != 0 || stats.CheckpointCount != 0 {
		t.Fatalf("stats after deletion: %+v, %v", stats, err)
	}
}

func TestOpenMemory_NestedBucketRollback(t *testing.T) {
	db := newMemoryTestDB(t)
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		nested, err := bucket.CreateBucket([]byte("nested"))
		if err != nil {
			return err
		}
		child, err := nested.CreateBucket([]byte("child"))
		if err != nil {
			return err
		}
		return child.Put([]byte("key"), []byte("old"))
	}); err != nil {
		t.Fatal(err)
	}
	before, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	for _, panics := range []bool{false, true} {
		func() {
			defer func() {
				if got := recover(); got != nil {
					if !panics || got != "discard" {
						t.Fatalf("unexpected panic: %v", got)
					}
				} else if panics {
					t.Fatal("transaction did not panic")
				}
			}()
			err := db.Update(func(tx *Tx) error {
				bucket, err := tx.Bucket(testBucketName)
				if err != nil {
					return err
				}
				if err := bucket.DeleteBucket([]byte("nested")); err != nil {
					return err
				}
				if _, err := tx.CreateBucket([]byte("temporary")); err != nil {
					return err
				}
				if panics {
					panic("discard")
				}
				return errDiscardTx
			})
			if !errors.Is(err, errDiscardTx) {
				t.Fatalf("rollback: %v", err)
			}
		}()
	}
	if err := db.View(func(tx *Tx) error {
		if _, err := tx.Bucket([]byte("temporary")); !errors.Is(err, ErrBucketNotFound) {
			return fmt.Errorf("temporary bucket after rollback: %v", err)
		}
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		nested, err := bucket.Bucket([]byte("nested"))
		if err != nil {
			return err
		}
		child, err := nested.Bucket([]byte("child"))
		if err != nil {
			return err
		}
		value, err := child.Get([]byte("key"))
		if err != nil || string(value) != "old" {
			return fmt.Errorf("nested value after rollback: %q, %v", value, err)
		}
		return bucket.ScanPrefix(nil, func(key, value []byte) error {
			if string(key) != "nested" || value != nil {
				return fmt.Errorf("nested scan: %q, %q", key, value)
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	after, err := db.Stats()
	if err != nil || after != before {
		t.Fatalf("rollback changed stats: before %+v, after %+v, %v", before, after, err)
	}
	if err := db.DeleteBucket(testBucketName); err != nil {
		t.Fatal(err)
	}
	if len(db.memoryNodes) != 1 {
		t.Fatalf("nested deletion kept %d nodes", len(db.memoryNodes))
	}
}

func TestOpenMemory_ByteOwnership(t *testing.T) {
	db := newMemoryTestDB(t)
	key, value := []byte("key"), []byte("value")
	if err := db.Put(testBucketName, key, value); err != nil {
		t.Fatal(err)
	}
	key[0], value[0] = 'x', 'x'
	got, err := db.Get(testBucketName, []byte("key"))
	if err != nil || string(got) != "value" {
		t.Fatalf("input ownership: %q, %v", got, err)
	}
	got[0] = 'x'
	got, err = db.Get(testBucketName, []byte("key"))
	if err != nil || string(got) != "value" {
		t.Fatalf("output ownership: %q, %v", got, err)
	}
	if err := db.Put(testBucketName, []byte("empty"), nil); err != nil {
		t.Fatal(err)
	}
	got, err = db.Get(testBucketName, []byte("empty"))
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty value: %v, %v", got, err)
	}
	if err := db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte("key"), []byte("bad")); !errors.Is(err, ErrTxNotWritable) {
			return fmt.Errorf("write in view: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestOpenMemory_ConcurrentTransactions(t *testing.T) {
	db := newMemoryTestDB(t)
	if err := db.Put(testBucketName, []byte("counter"), []byte("0")); err != nil {
		t.Fatal(err)
	}
	const workers, updates = 8, 50
	results := make(chan error, workers*2)
	for range workers {
		go func() {
			for range updates {
				if err := db.Update(func(tx *Tx) error {
					bucket, err := tx.Bucket(testBucketName)
					if err != nil {
						return err
					}
					value, err := bucket.Get([]byte("counter"))
					if err != nil {
						return err
					}
					number, err := strconv.Atoi(string(value))
					if err != nil {
						return err
					}
					return bucket.Put([]byte("counter"), strconv.AppendInt(nil, int64(number+1), 10))
				}); err != nil {
					results <- err
					return
				}
			}
			results <- nil
		}()
		go func() {
			for range updates {
				if _, err := db.Get(testBucketName, []byte("counter")); err != nil {
					results <- err
					return
				}
			}
			results <- nil
		}()
	}
	for range workers * 2 {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
	value, err := db.Get(testBucketName, []byte("counter"))
	if err != nil || string(value) != strconv.Itoa(workers*updates) {
		t.Fatalf("counter: %q, %v", value, err)
	}
}

func ExampleOpenMemory() {
	if err := func() (err error) {
		db := OpenMemory()
		defer func() { err = errors.Join(err, db.Close()) }()
		if err := db.Update(func(tx *Tx) error {
			users, err := tx.CreateBucket([]byte("users"))
			if err != nil {
				return err
			}
			return users.Put([]byte("42"), []byte("Ada"))
		}); err != nil {
			return err
		}
		value, err := db.Get([]byte("users"), []byte("42"))
		if err != nil {
			return err
		}
		fmt.Println(string(value))
		return nil
	}(); err != nil {
		fmt.Println(err)
	}
	// Output: Ada
}

// BenchmarkStorageComparison compares memory storage with file storage in SyncNone mode.
// Both modes use 10,000 keys, 128-byte values, and one caller.
// Each timed iteration does the same fixed work. Setup, warm reads, validation, and close are excluded.
// File storage starts with a completed checkpoint and keeps its default checkpoint threshold during writes.
func BenchmarkStorageComparison(b *testing.B) {
	const records, valueSize = 10_000, 128
	keys := make([][]byte, records)
	for index := range keys {
		keys[index] = fmt.Appendf(nil, "key-%08d", index)
	}
	order := rand.New(rand.NewSource(1)).Perm(records)
	initial := bytes.Repeat([]byte{0x5a}, valueSize)
	updated := bytes.Repeat([]byte{0xa5}, valueSize)
	modes := []string{"file-sync-none", "memory"}
	if os.Getenv("KVLITE_STORAGE_BENCH_ORDER") == "memory-first" {
		modes[0], modes[1] = modes[1], modes[0]
	}
	for _, workload := range []string{"read", "update", "update-batch=100", "mixed-read=95", "scan"} {
		b.Run(workload, func(b *testing.B) {
			for _, mode := range modes {
				b.Run(mode, func(b *testing.B) {
					var db *DB
					if mode == "memory" {
						db = OpenMemory()
					} else {
						var err error
						db, err = Open(filepath.Join(b.TempDir(), "database"), 0o600, &Options{Synchronous: SyncNone})
						if err != nil {
							b.Fatal(err)
						}
					}
					b.Cleanup(func() {
						if err := db.Close(); err != nil {
							b.Error(err)
						}
					})
					bucketName := []byte("bench")
					if err := db.Update(func(tx *Tx) error {
						bucket, err := tx.CreateBucket(bucketName)
						if err != nil {
							return err
						}
						for _, key := range keys {
							if err := bucket.Put(key, initial); err != nil {
								return err
							}
						}
						return nil
					}); err != nil {
						b.Fatal(err)
					}
					if mode == "file-sync-none" {
						if err := db.checkpointWAL(); err != nil {
							b.Fatal(err)
						}
					}
					for _, key := range keys {
						if value, err := db.Get(bucketName, key); err != nil || !bytes.Equal(value, initial) {
							b.Fatalf("warm read: %v", err)
						}
					}
					operations := records
					if workload == "read" || workload == "mixed-read=95" || workload == "scan" {
						operations *= 10
					}
					b.ReportAllocs()
					for b.Loop() {
						switch workload {
						case "read", "update", "mixed-read=95":
							for index := range operations {
								key := keys[order[index%records]]
								if workload == "update" || (workload == "mixed-read=95" && index%20 == 0) {
									if err := db.Put(bucketName, key, updated); err != nil {
										b.Fatal(err)
									}
								} else if value, err := db.Get(bucketName, key); err != nil || len(value) != valueSize {
									b.Fatalf("read: %v", err)
								}
							}
						case "update-batch=100":
							for start := 0; start < records; start += 100 {
								if err := db.Update(func(tx *Tx) error {
									bucket, err := tx.Bucket(bucketName)
									if err != nil {
										return err
									}
									for _, index := range order[start : start+100] {
										if err := bucket.Put(keys[index], updated); err != nil {
											return err
										}
									}
									return nil
								}); err != nil {
									b.Fatal(err)
								}
							}
						case "scan":
							for range 10 {
								visited := 0
								if err := db.View(func(tx *Tx) error {
									bucket, err := tx.Bucket(bucketName)
									if err != nil {
										return err
									}
									return bucket.ScanPrefix(nil, func(key, value []byte) error {
										if len(key) != len(keys[0]) || len(value) != valueSize {
											return fmt.Errorf("scan returned invalid data")
										}
										visited++
										return nil
									})
								}); err != nil {
									b.Fatal(err)
								}
								if visited != records {
									b.Fatalf("scan count: %d", visited)
								}
							}
						}
					}
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*operations), "ns/key")
					b.ReportMetric(float64(b.N*operations)/b.Elapsed().Seconds(), "keys/s")
					changed := make([]bool, records)
					for position, index := range order {
						changed[index] = workload == "update" || workload == "update-batch=100" || (workload == "mixed-read=95" && position%20 == 0)
					}
					for index, key := range keys {
						want := initial
						if changed[index] {
							want = updated
						}
						value, err := db.Get(bucketName, key)
						if err != nil || !bytes.Equal(value, want) {
							b.Fatalf("validate key %q: %v", key, err)
						}
					}
				})
			}
		})
	}
}
