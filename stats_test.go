package kvlite

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

// openStatsDB opens an existing database, so the counters of the handle start at zero.
// The handle that creates a database counts the WAL commit and the checkpoint of the creation.
func openStatsDB(t *testing.T, options *Options) *DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "stats.db")
	db, err := Open(path, 0600, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, 0600, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !db.closed {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	return db
}

func readStats(t *testing.T, db *DB) Stats {
	t.Helper()
	stats, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	return stats
}

func TestStatsStorageAndWriteCounters(t *testing.T) {
	db := openStatsDB(t, &Options{Synchronous: SyncNormal})
	initial := readStats(t, db)
	if initial.WALCommitCount != 0 || initial.WALBytesWritten != 0 || initial.CheckpointFailureCount != 0 || initial.LastCheckpointError != nil {
		t.Fatalf("initial counters: %+v", initial)
	}
	wantAllocationPages := uint64(page.AllocationSegmentPages(int64(initial.PageSize)))
	wantLastPage := uint64(2) + wantAllocationPages
	wantDataSize := int64(wantLastPage+1) * int64(initial.PageSize)
	if initial.PageSize != int(db.meta.PageSize()) || initial.LastPageID != wantLastPage || initial.AllocatedPageCount != wantLastPage+1 || initial.DataSize != wantDataSize || initial.FileSize != wantDataSize || initial.AllocationMetadataPageCount != wantAllocationPages {
		t.Fatalf("initial sizes: %+v", initial)
	}
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("data"))
		if err != nil {
			return err
		}
		for i := 0; i < 200; i++ {
			if err := bucket.Put([]byte(fmt.Sprintf("key-%04d", i)), bytes.Repeat([]byte("v"), 128)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	after := readStats(t, db)
	if after.WALCommitCount != 1 || after.WALSize != fileSize(t, db.Path()+"-wal") || after.WALBytesWritten != uint64(after.WALSize) || after.WALBytesSinceCheckpoint != after.WALBytesWritten || after.WALPageCount == 0 || after.CheckpointThresholdBytes != db.options.CheckpointThresholdBytes {
		t.Fatalf("WAL state: %+v", after)
	}
	if err := db.Put([]byte("data"), []byte("key-0000"), bytes.Repeat([]byte("w"), 128)); err != nil {
		t.Fatal(err)
	}
	updated := readStats(t, db)
	if updated.WALCommitCount != 2 || updated.WALBytesWritten <= after.WALBytesWritten || updated.WALPageCount != after.WALPageCount {
		t.Fatalf("repeated page write: before %+v, after %+v", after, updated)
	}
	if err := db.Update(func(*Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got := readStats(t, db); got != updated {
		t.Fatalf("empty update changed statistics: %+v", got)
	}
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket([]byte("data"))
		if err != nil {
			return err
		}
		keys := make([][]byte, 200)
		for i := range keys {
			keys[i] = []byte(fmt.Sprintf("key-%04d", i))
		}
		return bucket.DeleteBatch(keys)
	}); err != nil {
		t.Fatal(err)
	}
	deleted := readStats(t, db)
	if deleted.PagesRetired == 0 || deleted.WALCommitCount != 3 {
		t.Fatalf("deletion counters: %+v", deleted)
	}
	db.operationMu.Lock()
	err := db.checkpointWAL()
	db.operationMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	checkpointed := readStats(t, db)
	delta := checkpointed.Sub(&deleted)
	if delta.CheckpointCount != 1 || delta.WALSize != 0 || delta.WALPageCount != 0 || delta.WALBytesSinceCheckpoint != 0 || delta.FileSize != delta.DataSize || delta.WALCommitCount != 0 || delta.WALBytesWritten != 0 {
		t.Fatalf("checkpoint state: %+v", delta)
	}
	path := db.Path()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Stats(); !errors.Is(err, ErrDatabaseNotOpen) {
		t.Fatalf("closed Stats: %v", err)
	}
	reopened, err := Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	current := readStats(t, reopened)
	if current.WALCommitCount != 0 || current.WALBytesWritten != 0 || current.CheckpointCount != 0 || current.WALSize != 0 || current.AllocatedPageCount != checkpointed.AllocatedPageCount {
		t.Fatalf("reopened Stats: %+v", current)
	}
}

func TestStatsWriteBatchCountsOneCommit(t *testing.T) {
	db := openStatsDB(t, nil)
	createBucket(t, db, []byte("data"))
	before := readStats(t, db)
	callbackError := errors.New("discard callback")
	requests := make([]*writeRequest, 3)
	for i := range requests {
		requests[i] = &writeRequest{result: make(chan writeResult, 1), transaction: func(tx *Tx) error {
			b, err := tx.Bucket([]byte("data"))
			if err != nil {
				return err
			}
			if err := b.Put([]byte(fmt.Sprintf("key-%d", i)), []byte("value")); err != nil {
				return err
			}
			if i == 1 {
				return callbackError
			}
			return nil
		}}
	}
	db.operationMu.Lock()
	db.executeWriteBatch(requests)
	db.operationMu.Unlock()
	for i, request := range requests {
		result := <-request.result
		if i == 1 {
			if !errors.Is(result.err, callbackError) {
				t.Fatal(result.err)
			}
		} else if result.err != nil {
			t.Fatal(result.err)
		}
	}
	delta := readStats(t, db).Sub(&before)
	if delta.WALCommitCount != 1 || delta.WALBytesWritten == 0 {
		t.Fatalf("batch counters: %+v", delta)
	}
	if _, err := db.Get([]byte("data"), []byte("key-1")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("failed callback key: %v", err)
	}
}

func TestStatsFailedCommit(t *testing.T) {
	options := &Options{Synchronous: SyncFull, MaxWriteBatchSize: 1}
	db := openStatsDB(t, options)
	createBucket(t, db, []byte("data"))
	before := readStats(t, db)
	syncError := errors.New("sync failed")
	db.wal.SetSyncFileForTesting(func() error { return syncError })
	err := db.Put([]byte("data"), []byte("failed"), []byte("value"))
	if !errors.Is(err, syncError) {
		t.Fatalf("commit error: %v", err)
	}
	db.wal.SetSyncFileForTesting(nil)
	// Rollback removes the failed append and any unused preallocated suffix.
	// Logical state and counters stay unchanged; WALSize reports physical bytes.
	afterFailure := before
	afterFailure.WALSize = int64(before.WALBytesSinceCheckpoint)
	if got := readStats(t, db); got != afterFailure {
		t.Fatalf("failed commit changed statistics: %+v", got)
	}
	if err := db.Put([]byte("data"), []byte("failed"), []byte("value")); !errors.Is(err, ErrWALWriteFailed) {
		t.Fatalf("Put on the stopped handle: got %v, want ErrWALWriteFailed", err)
	}
	if got := readStats(t, db); got != afterFailure {
		t.Fatalf("rejected Put changed statistics: %+v", got)
	}

	reopened := reopenAfterWALWriteFailure(t, db, db.Path(), options)
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := reopened.Get([]byte("data"), []byte("failed")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("failed commit published data: %v", err)
	}
	reopenedBefore := readStats(t, reopened)
	if err := reopened.Put([]byte("data"), []byte("failed"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	retried := readStats(t, reopened).Sub(&reopenedBefore)
	if retried.WALCommitCount != 1 || retried.WALBytesWritten == 0 || retried.AllocatedPageCount != before.AllocatedPageCount {
		t.Fatalf("retry counters: %+v", retried)
	}
}

func TestStatsSubKeepsCurrentState(t *testing.T) {
	var before, after Stats
	old, current := reflect.ValueOf(&before).Elem(), reflect.ValueOf(&after).Elem()
	counters := map[string]bool{
		"WALBytesWritten": true, "WALCommitCount": true, "CheckpointCount": true,
		"CheckpointFailureCount": true, "PagesReused": true, "PagesRetired": true, "TailPagesReclaimed": true,
	}
	for i := 0; i < old.NumField(); i++ {
		switch old.Field(i).Kind() {
		case reflect.Uint64:
			old.Field(i).SetUint(2)
			current.Field(i).SetUint(5)
		case reflect.Int, reflect.Int64:
			old.Field(i).SetInt(2)
			current.Field(i).SetInt(5)
		}
	}
	after.LastCheckpointError = errors.New("checkpoint failed")
	result := after.Sub(&before)
	value := reflect.ValueOf(result)
	for i := 0; i < value.NumField(); i++ {
		name := value.Type().Field(i).Name
		if name == "LastCheckpointError" {
			if result.LastCheckpointError != after.LastCheckpointError {
				t.Fatal("Sub changed the checkpoint error")
			}
			continue
		}
		want := int64(5)
		if counters[name] {
			want = 3
		}
		var got int64
		if value.Field(i).Kind() == reflect.Uint64 {
			got = int64(value.Field(i).Uint())
		} else {
			got = value.Field(i).Int()
		}
		if got != want {
			t.Fatalf("%s = %d, want %d", name, got, want)
		}
	}
	if after.Sub(nil) != after {
		t.Fatal("nil baseline changed the reading")
	}
}

func TestStatsAutomaticCheckpointFailure(t *testing.T) {
	for _, mode := range []Sync{SyncNormal, SyncFull} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			db := openStatsDB(t, &Options{Synchronous: mode, CheckpointThresholdBytes: 1})
			createBucket(t, db, []byte("data"))
			before := readStats(t, db)
			writableMain := db.file
			readOnlyMain, err := os.Open(db.Path())
			if err != nil {
				t.Fatal(err)
			}
			defer readOnlyMain.Close()
			db.file = readOnlyMain
			defer func() { db.file = writableMain }()
			if err := db.Put([]byte("data"), []byte("key"), []byte("value")); err != nil {
				t.Fatalf("checkpoint failure failed the committed write: %v", err)
			}
			failed := readStats(t, db)
			delta := failed.Sub(&before)
			if delta.WALCommitCount != 1 || delta.CheckpointCount != 0 || delta.CheckpointFailureCount != 1 || failed.LastCheckpointError == nil || failed.WALPageCount == 0 {
				t.Fatalf("automatic checkpoint failure: %+v", failed)
			}
			if got, err := db.Get([]byte("data"), []byte("key")); err != nil || !bytes.Equal(got, []byte("value")) {
				t.Fatalf("committed value: %q, %v", got, err)
			}
			if err := db.Close(); err == nil {
				t.Fatal("Close did not report the checkpoint failure")
			}
			failed = readStats(t, db)
			if failed.CheckpointFailureCount != before.CheckpointFailureCount+2 {
				t.Fatalf("Close failure count: %+v", failed)
			}
			db.file = writableMain
			db.operationMu.Lock()
			err = db.checkpointWAL()
			db.operationMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			retried := readStats(t, db)
			if retried.LastCheckpointError != nil || retried.CheckpointFailureCount != failed.CheckpointFailureCount || retried.CheckpointCount != before.CheckpointCount+1 || retried.WALPageCount != 0 || retried.WALBytesSinceCheckpoint != 0 || retried.WALSize != 0 {
				t.Fatalf("checkpoint retry state: %+v", retried)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("Close retry: %v", err)
			}
		})
	}
}

func TestStatsCheckpointSyncFailure(t *testing.T) {
	db := openStatsDB(t, &Options{Synchronous: SyncNormal, CheckpointThresholdBytes: ^uint64(0)})
	createBucket(t, db, []byte("data"))
	before := readStats(t, db)
	failure := errors.New("checkpoint sync failed")
	db.wal.SetSyncFileForTesting(func() error { return failure })
	defer db.wal.SetSyncFileForTesting(nil)
	if err := db.Put([]byte("data"), []byte("key"), []byte("value")); err != nil {
		t.Fatalf("committed write failed: %v", err)
	}
	db.operationMu.Lock()
	err := db.checkpointWAL()
	db.operationMu.Unlock()
	if !errors.Is(err, failure) {
		t.Fatalf("checkpoint sync error: %v", err)
	}
	failed := readStats(t, db)
	if !errors.Is(failed.LastCheckpointError, failure) || failed.CheckpointFailureCount != 1 || failed.CheckpointCount != before.CheckpointCount || failed.WALPageCount == 0 {
		t.Fatalf("sync failure state: %+v", failed)
	}
	// After a failed WAL sync, the WAL can lose unsynced records, so the handle stops as after a failed commit.
	if err := db.Put([]byte("data"), []byte("next"), []byte("value")); !errors.Is(err, ErrWALWriteFailed) {
		t.Fatalf("Put after a failed checkpoint sync: got %v, want ErrWALWriteFailed", err)
	}
	reopened := reopenAfterWALWriteFailure(t, db, db.Path(), &Options{Synchronous: SyncNormal})
	defer reopened.Close()
	if value, err := reopened.Get([]byte("data"), []byte("key")); err != nil || string(value) != "value" {
		t.Fatalf("committed write after the reopen: got %q, %v", value, err)
	}
}

// TestStatsConcurrentReadersKeepCacheUnchanged checks that concurrent readers keep the cache size and statistics unchanged.
// Readers mark each cached node that they use, so CLOCK eviction keeps the nodes that only readers use.
func TestStatsConcurrentReadersKeepCacheUnchanged(t *testing.T) {
	db := openStatsDB(t, &Options{Synchronous: SyncNone, CheckpointThresholdBytes: 1})
	createBucket(t, db, []byte("data"))
	recordCount := int(db.meta.PageSize() / 64)
	if err := db.Update(func(tx *Tx) error {
		b, err := tx.Bucket([]byte("data"))
		if err != nil {
			return err
		}
		for i := 0; i < recordCount; i++ {
			if err := b.Put([]byte(fmt.Sprintf("%04d", i)), bytes.Repeat([]byte("v"), 128)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before := readStats(t, db)
	// Encoded traversal must not populate a decoded cache, even under concurrent reads.
	clear(db.writeNodes)
	db.writeNodeSlots = nil
	db.nextWriteNodeSlot = 0
	for range 2 {
		for _, entry := range db.writeNodes {
			entry.referenced.Store(false)
		}
		cacheSize := len(db.writeNodes)
		results := make(chan error, 8)
		for range 8 {
			go func() {
				results <- db.View(func(tx *Tx) error {
					b, err := tx.Bucket([]byte("data"))
					if err != nil {
						return err
					}
					stats, err := b.Stats()
					if err != nil || stats.KeyN != recordCount {
						return fmt.Errorf("bucket statistics: %+v, %v", stats, err)
					}
					cursor, err := b.Cursor()
					if err != nil {
						return err
					}
					if _, _, err := cursor.First(); err != nil {
						return err
					}
					if _, _, err := cursor.Last(); err != nil {
						return err
					}
					visit := func(_, _ []byte) error { return nil }
					if err := b.ScanRange(nil, nil, visit); err != nil {
						return err
					}
					return b.ScanReverse(visit)
				})
			}()
		}
		for range 8 {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		if after := readStats(t, db); after != before {
			t.Fatalf("readers changed statistics: %+v", after)
		}
		if len(db.writeNodes) != cacheSize {
			t.Fatal("readers changed cache size")
		}
		for pageID, entry := range db.writeNodes {
			if !entry.referenced.Load() {
				t.Fatalf("readers did not mark cached page %d as used", pageID)
			}
		}
	}
}

func TestBucketStatsIncludesNestedAndUncommittedData(t *testing.T) {
	for _, mode := range []Sync{SyncNone, SyncFull} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			db := openStatsDB(t, &Options{Synchronous: mode})
			valueSize := int(db.meta.PageSize() / 32)
			var expected BucketStats
			var closed *Bucket
			if err := db.Update(func(tx *Tx) error {
				parent, err := tx.CreateBucket([]byte("parent"))
				if err != nil {
					return err
				}
				empty, err := parent.Stats()
				if err != nil {
					return err
				}
				if empty.LeafPageN != 1 || empty.BucketN != 1 || empty.Depth != 1 || empty.KeyN != 0 || empty.LeafInuse != btree.NodeHeaderSize {
					return fmt.Errorf("empty bucket: %+v", empty)
				}
				if err := parent.Put([]byte("key"), []byte("value")); err != nil {
					return err
				}
				child, err := parent.CreateBucket([]byte("child"))
				if err != nil {
					return err
				}
				for i := 0; i < 100; i++ {
					if err := child.Put([]byte(fmt.Sprintf("%04d", i)), bytes.Repeat([]byte("v"), valueSize)); err != nil {
						return err
					}
				}
				grandchild, err := child.CreateBucket([]byte("grandchild"))
				if err != nil {
					return err
				}
				if err := grandchild.Put([]byte("g"), nil); err != nil {
					return err
				}
				expected, err = parent.Stats()
				if err != nil {
					return err
				}
				if expected.KeyN != 102 || expected.BucketN != 3 || expected.Depth != 2 || expected.BranchPageN == 0 || expected.LeafPageN <= 3 || expected.KeyBytes != 419 || expected.ValueBytes != 100*valueSize+5 {
					return fmt.Errorf("nested bucket: %+v", expected)
				}
				if expected.BranchInuse >= expected.BranchAlloc || expected.LeafInuse >= expected.LeafAlloc {
					return fmt.Errorf("page use: %+v", expected)
				}
				closed = parent
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := closed.Stats(); !errors.Is(err, ErrTxClosed) {
				t.Fatalf("closed bucket: %v", err)
			}
			check := func(db *DB) {
				t.Helper()
				if err := db.View(func(tx *Tx) error {
					parent, err := tx.Bucket([]byte("parent"))
					if err != nil {
						return err
					}
					got, err := parent.Stats()
					if err != nil {
						return err
					}
					if got != expected {
						return fmt.Errorf("bucket counts: got %+v, want %+v", got, expected)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			check(db)
			path := db.Path()
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			ro, err := Open(path, 0600, &Options{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer ro.Close()
			check(ro)
		})
	}
}

func TestBucketStatsDeletedHandle(t *testing.T) {
	db := openStatsDB(t, &Options{Synchronous: SyncNone})
	if err := db.Update(func(tx *Tx) error {
		parent, err := tx.CreateBucket([]byte("parent"))
		if err != nil {
			return err
		}
		child, err := parent.CreateBucket([]byte("child"))
		if err != nil {
			return err
		}
		if err := tx.DeleteBucket([]byte("parent")); err != nil {
			return err
		}
		if _, err := child.Stats(); !errors.Is(err, ErrBucketNotFound) {
			return fmt.Errorf("deleted child: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestStatsReadOnlyRecoveredWAL(t *testing.T) {
	source := openStatsDB(t, &Options{Synchronous: SyncNormal})
	createBucket(t, source, []byte("data"))
	value := bytes.Repeat([]byte("v"), 3*int(source.meta.PageSize()))
	if err := source.Put([]byte("data"), []byte("key"), value); err != nil {
		t.Fatal(err)
	}
	// The WAL covers a four-page leaf, its one-page catalog, one bitmap page, and two metadata pages.
	if stats := readStats(t, source); stats.WALPageCount != 8 {
		t.Fatalf("live WAL physical pages: got %d, want 8", stats.WALPageCount)
	}
	path := filepath.Join(t.TempDir(), "recovered.db")
	for _, suffix := range []string{"", "-wal"} {
		data, err := os.ReadFile(source.Path() + suffix)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path+suffix, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	db, err := Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	current := readStats(t, db)
	if current.WALSize != fileSize(t, path+"-wal") || current.WALSize == 0 || current.WALPageCount != 8 || current.WALBytesWritten != 0 || current.WALCommitCount != 0 || current.WALBytesSinceCheckpoint != 0 {
		t.Fatalf("recovered WAL statistics: %+v", current)
	}
	if err := db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket([]byte("data"))
		if err != nil {
			return err
		}
		counts, err := bucket.Stats()
		if err != nil {
			return err
		}
		if counts.KeyN != 1 || counts.KeyBytes != 3 || counts.ValueBytes != len(value) {
			return fmt.Errorf("recovered bucket: %+v", counts)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucketStatsIncludesContinuationStorage(t *testing.T) {
	for _, largeKeys := range []bool{false, true} {
		t.Run(fmt.Sprintf("large-keys=%t", largeKeys), func(t *testing.T) {
			db := openStatsDB(t, &Options{Synchronous: SyncNone})
			pageSize := int(db.meta.PageSize())
			var expected BucketStats
			if err := db.Update(func(tx *Tx) error {
				bucket, err := tx.CreateBucket([]byte("data"))
				if err != nil {
					return err
				}
				count := 1
				if largeKeys {
					count = 24
				}
				for i := range count {
					key := []byte("key")
					value := bytes.Repeat([]byte("v"), 3*pageSize)
					if largeKeys {
						key = append(bytes.Repeat([]byte("k"), pageSize), []byte(fmt.Sprintf("%08d", i))...)
						value = []byte("v")
					}
					if err := bucket.Put(key, value); err != nil {
						return err
					}
				}
				expected, err = bucket.Stats()
				return err
			}); err != nil {
				t.Fatal(err)
			}
			check := func(db *DB) {
				t.Helper()
				if err := db.View(func(tx *Tx) error {
					bucket, err := tx.Bucket([]byte("data"))
					if err != nil {
						return err
					}
					got, err := bucket.Stats()
					if err != nil {
						return err
					}
					if got != expected {
						return fmt.Errorf("continued bucket: got %+v, want %+v", got, expected)
					}
					var branchBytes, leafBytes, branchPages, leafPages int
					var walk func(*btree.MutableNode) error
					walk = func(node *btree.MutableNode) error {
						used := len(btree.EncodeWALNode(node))
						pages := (used + pageSize - 1) / pageSize
						if node.IsLeaf() {
							leafBytes += used
							leafPages += pages
						} else {
							branchBytes += used
							branchPages += pages
							for _, id := range node.Children {
								child, err := tx.store.decodedNode(id)
								if err != nil {
									return err
								}
								if err := walk(child); err != nil {
									return err
								}
							}
						}
						return nil
					}
					if err := walk(testBucketRoot(t, bucket)); err != nil {
						return err
					}
					if got.BranchInuse != branchBytes || got.LeafInuse != leafBytes || got.BranchAlloc != branchPages*pageSize || got.LeafAlloc != leafPages*pageSize {
						return fmt.Errorf("continued storage: %+v, encoded branch %d/%d, leaf %d/%d", got, branchBytes, branchPages*pageSize, leafBytes, leafPages*pageSize)
					}
					if leafPages <= got.LeafPageN || largeKeys && branchPages <= got.BranchPageN {
						return fmt.Errorf("test needs continued storage: %+v", got)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			check(db)
			path := db.Path()
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			ro, err := Open(path, 0600, &Options{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer ro.Close()
			check(ro)
		})
	}
}
