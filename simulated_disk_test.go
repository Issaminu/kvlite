package kvlite

import (
	"errors"
	"testing"

	"github.com/Issaminu/kvlite/internal/blackout/disk"
)

// These tests run KVLite on the simulated disk of package internal/blackout/disk.
// KVLite reaches every file through fileio.FS, so the disk can crash the process and cut the power under it.

const simulatedPath = "/sim/database"

// simulatedPut commits key and value to the test bucket, and creates the bucket first when it is missing.
func simulatedPut(t *testing.T, db *DB, key, value string) {
	t.Helper()
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if errors.Is(err, ErrBucketNotFound) {
			bucket, err = tx.CreateBucket(testBucketName)
		}
		if err != nil {
			return err
		}
		return bucket.Put([]byte(key), []byte(value))
	}); err != nil {
		t.Fatal(err)
	}
}

// simulatedCrash stops the process: the disk refuses every later call of db, and db stops without Close.
func simulatedCrash(fsys *disk.FS, db *DB) {
	fsys.Crash()
	db.lifecycleMu.Lock()
	db.closed = true
	db.lifecycleMu.Unlock()
	db.stopWriteBatcherAndWait()
}

// simulatedGet opens the database again and returns the value of key.
func simulatedGet(t *testing.T, fsys *disk.FS, sync Sync, key string) (string, error) {
	t.Helper()
	db, err := open(simulatedPath, 0o600, &Options{Synchronous: sync}, fsys)
	if err != nil {
		t.Fatalf("Open after the restart: %v", err)
	}
	defer db.Close()
	value, err := db.Get(testBucketName, []byte(key))
	return string(value), err
}

// A SyncFull commit is durable when Update returns. A SyncNormal commit is durable after Close.
// Each seed makes other power-failure decisions about the unsynced changes.
func TestSimulatedDisk_DurableCommitSurvivesPowerFailure(t *testing.T) {
	for _, sync := range []Sync{SyncFull, SyncNormal} {
		for seed := range uint64(50) {
			fsys := disk.New(seed, disk.Config{})
			db, err := open(simulatedPath, 0o600, &Options{Synchronous: sync}, fsys)
			if err != nil {
				t.Fatal(err)
			}
			simulatedPut(t, db, "key", "value")
			if sync == SyncFull {
				simulatedCrash(fsys, db)
			} else if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			fsys.Restart(true)
			if value, err := simulatedGet(t, fsys, sync, "key"); err != nil || value != "value" {
				t.Fatalf("sync=%d seed %d: after a power failure: got %q, %v, want %q", sync, seed, value, err, "value")
			}
		}
	}
}

// A process crash keeps the operating-system cache, so it keeps a SyncNormal commit that no sync made durable.
func TestSimulatedDisk_ProcessCrashKeepsUnsyncedCommit(t *testing.T) {
	fsys := disk.New(1, disk.Config{})
	db, err := open(simulatedPath, 0o600, &Options{Synchronous: SyncNormal, CheckpointThresholdBytes: 1 << 40}, fsys)
	if err != nil {
		t.Fatal(err)
	}
	simulatedPut(t, db, "key", "value")
	simulatedCrash(fsys, db)
	fsys.Restart(false)
	if value, err := simulatedGet(t, fsys, SyncNormal, "key"); err != nil || value != "value" {
		t.Fatalf("after a process crash: got %q, %v, want %q", value, err, "value")
	}
}

// When file syncs do nothing, a power failure can lose a SyncFull commit.
// The durability test above must be able to fail, so some seed must lose the commit here.
func TestSimulatedDisk_SyncLieLosesCommit(t *testing.T) {
	for seed := range uint64(50) {
		fsys := disk.New(seed, disk.Config{SyncLie: disk.SyncLieAllFiles})
		db, err := open(simulatedPath, 0o600, &Options{Synchronous: SyncFull}, fsys)
		if err != nil {
			t.Fatal(err)
		}
		simulatedPut(t, db, "key", "value")
		simulatedCrash(fsys, db)
		fsys.Restart(true)
		recovered, err := open(simulatedPath, 0o600, &Options{Synchronous: SyncFull}, fsys)
		if err != nil {
			// Losing the database file is also a loss.
			return
		}
		value, err := recovered.Get(testBucketName, []byte("key"))
		recovered.Close()
		if err != nil || string(value) != "value" {
			return
		}
	}
	t.Fatal("50 seeds kept the commit although file syncs did nothing")
}
