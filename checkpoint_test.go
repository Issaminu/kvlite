package kvlite

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Issaminu/kvlite/internal/blackout/disk"
	"github.com/Issaminu/kvlite/internal/fileio"
)

func TestDBCheckpointMovesCommittedWAL(t *testing.T) {
	for _, mode := range []Sync{SyncFull, SyncNormal, SyncNone} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			db, err := Open(path, 0600, &Options{Synchronous: mode, CheckpointThresholdBytes: 1 << 30, DataPageChecksums: true})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			createBucket(t, db, testBucketName)
			value := bytes.Repeat([]byte("v"), 64<<10)
			if err := db.Put(testBucketName, []byte("key"), value); err != nil {
				t.Fatal(err)
			}
			before := db.wal.Stats()
			if before.CommittedRecordCount == 0 {
				t.Fatal("fixture has no committed WAL data")
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			after := db.wal.Stats()
			if after.CommittedRecordCount != 0 || after.CheckpointCount != before.CheckpointCount+1 || after.CommitCount != before.CommitCount {
				t.Fatalf("checkpoint state: before=%+v after=%+v", before, after)
			}
			if err := db.Checkpoint(); err != nil || db.wal.Stats().CheckpointCount != after.CheckpointCount {
				t.Fatalf("empty checkpoint did extra work: %v", err)
			}
			// Open the main file alone, without closing or recovering the source.
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			copyPath := filepath.Join(t.TempDir(), "main-only")
			if err := os.WriteFile(copyPath, data, 0600); err != nil {
				t.Fatal(err)
			}
			copyDB, err := Open(copyPath, 0600, &Options{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer copyDB.Close()
			if got, err := copyDB.Get(testBucketName, []byte("key")); err != nil || !bytes.Equal(got, value) {
				t.Fatalf("main file needs WAL recovery: len=%d error=%v", len(got), err)
			}
			if err := db.Put(testBucketName, []byte("key"), []byte("later")); err != nil {
				t.Fatalf("checkpoint closed the source: %v", err)
			}
		})
	}
}

func TestSyncNoneSkipsAutomaticSync(t *testing.T) {
	for _, threshold := range []uint64{1, 1 << 30} {
		t.Run(fmt.Sprint(threshold), func(t *testing.T) {
			syncCalls := 0
			fsys := disk.New(1, disk.Config{OnEvent: func(event disk.Event) {
				if event.Op == "sync" || event.Op == "syncdir" {
					syncCalls++
				}
			}})
			options := &Options{Synchronous: SyncNone, CheckpointThresholdBytes: threshold}
			db, err := open(simulatedPath, 0600, options, fsys)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			simulatedPut(t, db, "key", "value")
			if threshold == 1 && db.wal.Stats().CommittedRecordCount != 0 {
				t.Fatal("automatic checkpoint left committed WAL data")
			}
			closeWithoutCheckpoint(t, db)
			db, err = open(simulatedPath, 0600, options, fsys)
			if err != nil {
				t.Fatal(err)
			}
			if got, err := db.Get(testBucketName, []byte("key")); err != nil || string(got) != "value" {
				t.Fatalf("recovered value: %q, %v", got, err)
			}
			simulatedPut(t, db, "key", "checkpoint")
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			simulatedPut(t, db, "key", "close")
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if syncCalls != 0 {
				t.Fatalf("SyncNone issued %d storage syncs", syncCalls)
			}
		})
	}
}

func TestDBCheckpointPersistsMainFile(t *testing.T) {
	for _, mode := range []Sync{SyncFull, SyncNormal} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			fsys := disk.New(1, disk.Config{KeepChange: disk.Ratio{Num: 0, Den: 1}})
			db, err := open(simulatedPath, 0600, &Options{Synchronous: mode}, fsys)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			simulatedPut(t, db, "key", "value")
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			simulatedCrash(fsys, db)
			fsys.Restart(true)
			// Only the main file may supply the value after losing unsynced writes.
			if err := fsys.Remove(simulatedPath + "-wal"); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			if got, err := simulatedGet(t, fsys, mode, "key"); err != nil || got != "value" {
				t.Fatalf("checkpoint did not persist the main file: %q, %v", got, err)
			}
		})
	}
}

func TestDBCheckpointNoOpAndHandleErrors(t *testing.T) {
	memory := OpenMemory()
	if err := memory.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := memory.Close(); err != nil {
		t.Fatal(err)
	}
	if err := memory.Checkpoint(); !errors.Is(err, ErrDatabaseNotOpen) {
		t.Fatalf("closed memory checkpoint: %v", err)
	}
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone, DisableWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	createBucket(t, db, testBucketName)
	before := db.wal.Stats()
	db.wal.SetSyncFileForTesting(func() error { t.Error("DisableWAL checkpoint synchronized the WAL"); return nil })
	if err := db.Checkpoint(); err != nil || db.wal.Stats() != before {
		t.Fatalf("DisableWAL checkpoint was not a no-op: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); !errors.Is(err, ErrDatabaseNotOpen) {
		t.Fatalf("closed file checkpoint: %v", err)
	}
	db, err = Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Checkpoint(); !errors.Is(err, ErrDatabaseReadOnly) {
		t.Fatalf("read-only checkpoint: %v", err)
	}
}

func TestDBCheckpointFailures(t *testing.T) {
	for _, step := range []string{"main", "wal"} {
		t.Run(step, func(t *testing.T) {
			fsys := disk.New(1, disk.Config{SyncFault: disk.Ratio{Num: 1, Den: 1}})
			fsys.SetFaults(false)
			db, err := open(simulatedPath, 0600, &Options{Synchronous: SyncNormal}, fsys)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			simulatedPut(t, db, "key", "value")
			failure := errors.New("checkpoint WAL sync failure")
			db.wal.SetSyncFileForTesting(func() error {
				if step == "wal" {
					return failure
				}
				return nil
			})
			fsys.SetFaults(true)
			err = db.Checkpoint()
			fsys.SetFaults(false)
			if err == nil {
				t.Fatal("checkpoint hid a synchronization failure")
			}
			stats, statsErr := db.Stats()
			if statsErr != nil || stats.CheckpointFailureCount != 1 || stats.LastCheckpointError == nil || stats.WALPageCount == 0 {
				t.Fatalf("failed checkpoint lost its retry state: %+v, %v", stats, statsErr)
			}
			if step == "wal" {
				if !errors.Is(err, failure) || !errors.Is(err, ErrWALWriteFailed) {
					t.Fatalf("lost WAL failure: %v", err)
				}
				if got := db.Checkpoint(); !errors.Is(got, failure) || !errors.Is(got, ErrWALWriteFailed) {
					t.Fatalf("stopped checkpoint changed failure: %v", got)
				}
				return
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("main sync failure could not be retried: %v", err)
			}
			if got, err := db.Get(testBucketName, []byte("key")); err != nil || string(got) != "value" {
				t.Fatalf("retry lost committed value: %q, %v", got, err)
			}
		})
	}
}

func TestDBCheckpointRetriesWALCleanup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	createBucket(t, db, testBucketName)
	if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.wal.Close(); err != nil {
		t.Fatal(err)
	}
	readOnly, err := os.Open(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	db.wal.ReplaceFileForTesting(readOnly)
	defer readOnly.Close()
	if err := db.Checkpoint(); err == nil {
		t.Fatal("checkpoint hid failed WAL cleanup")
	}
	if db.wal.Stats().CommittedRecordCount != 0 {
		t.Fatal("fixture did not reach WAL cleanup after writing the main file")
	}
	if err := db.Checkpoint(); err == nil {
		t.Fatal("checkpoint skipped failed cleanup because the overlay was empty")
	}
	writable, err := os.OpenFile(path+"-wal", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	db.wal.ReplaceFileForTesting(writable)
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("could not retry WAL cleanup: %v", err)
	}
	info, err := os.Stat(path + "-wal")
	if err != nil || info.Size() != 0 {
		t.Fatalf("successful retry left WAL contents: %v, %v", info, err)
	}
	stats, err := db.Stats()
	if err != nil || stats.CheckpointFailureCount != 2 || stats.LastCheckpointError != nil {
		t.Fatalf("cleanup retry stats: %+v, %v", stats, err)
	}
}

func TestDBCheckpointWaitsForBackup(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "db"), 0600, &Options{Synchronous: SyncNormal})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	createBucket(t, db, testBucketName)
	if err := db.Put(testBucketName, []byte("key"), []byte("before")); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	backupDone := make(chan error, 1)
	go func() {
		backupDone <- db.backup(filepath.Join(dir, "backup"), func(dst io.Writer, src io.Reader) (int64, error) {
			close(entered)
			<-release
			return io.Copy(dst, src)
		}, fileio.Sync)
	}()
	<-entered
	done := make(chan error, 1)
	go func() { done <- db.Checkpoint() }()
	select {
	case err := <-done:
		t.Fatalf("checkpoint returned during backup: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	// Waiting must release the operation lock so the backup and other writes can finish.
	if err := db.Put(testBucketName, []byte("key"), []byte("during")); err != nil {
		t.Fatal(err)
	}
	unblock()
	if err := <-backupDone; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if db.wal.Stats().CommittedRecordCount != 0 {
		t.Fatal("checkpoint skipped the WAL after waiting for backup")
	}
}

func TestDBCheckpointWaitsForTransaction(t *testing.T) {
	db := OpenMemory()
	t.Cleanup(func() { db.Close() })
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	viewDone := make(chan error, 1)
	go func() { viewDone <- db.View(func(*Tx) error { close(entered); <-release; return nil }) }()
	<-entered
	done := make(chan error, 1)
	go func() { done <- db.Checkpoint() }()
	select {
	case err := <-done:
		t.Fatalf("checkpoint returned during View: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	if err := <-viewDone; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
