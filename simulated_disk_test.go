package kvlite

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/Issaminu/kvlite/internal/blackout/disk"
	"github.com/Issaminu/kvlite/internal/fileio"
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

// A commit whose pages equal their committed images writes only a commit marker to the WAL.
// Recovery must replay the commits after it.
// Blackout found this: a SyncFull Put of an unchanged value with continuation pages, and then recovery dropped two synced commits.
func TestSimulatedDisk_EmptyWALTransactionKeepsLaterCommits(t *testing.T) {
	fsys := disk.New(1, disk.Config{})
	db, err := open(simulatedPath, 0o600, &Options{Synchronous: SyncNormal, CheckpointThresholdBytes: 1 << 40}, fsys)
	if err != nil {
		t.Fatal(err)
	}
	// The value needs continuation pages, so the second Put writes no page record.
	value := strings.Repeat("v", 3*os.Getpagesize())
	simulatedPut(t, db, "key", value)
	simulatedPut(t, db, "key", value)
	simulatedPut(t, db, "later", "commit")
	simulatedCrash(fsys, db)
	fsys.Restart(false)
	if value, err := simulatedGet(t, fsys, SyncNormal, "later"); err != nil || value != "commit" {
		t.Fatalf("the commit after the empty WAL transaction: got %q, %v, want %q", value, err, "commit")
	}
}

// statFailFS fails one later Stat of the main file.
type statFailFS struct {
	fileio.FS
	armed bool
	skip  int // successful Stat calls before the failure
}

func (fsys *statFailFS) OpenFile(name string, flag int, perm os.FileMode) (fileio.File, error) {
	file, err := fsys.FS.OpenFile(name, flag, perm)
	if err != nil || strings.HasSuffix(name, "-wal") {
		return file, err
	}
	return &statFailFile{File: file, Mapper: file.(fileio.Mapper), fsys: fsys}, nil
}

// statFailFile is a main file of statFailFS. It keeps the mapping methods of the file it wraps.
type statFailFile struct {
	fileio.File
	fileio.Mapper
	fsys *statFailFS
}

func (file *statFailFile) Stat() (os.FileInfo, error) {
	if file.fsys.armed {
		if file.fsys.skip == 0 {
			file.fsys.armed = false
			return nil, errors.New("injected Stat failure")
		}
		file.fsys.skip--
	}
	return file.File.Stat()
}

// On Windows a mapping does not show later writes, so a checkpoint must map the main file again.
// If that fails after the checkpoint cleared the WAL, reads must not use the old mapping.
// Blackout found this: a Stat error in refreshMainFileMapping kept the old mapping, and the page walk read old pages.
func TestSimulatedDisk_FailedRemapDropsStaleMapping(t *testing.T) {
	fsys := &statFailFS{FS: disk.New(1, disk.Config{StaleMappings: true})}
	db, err := open(simulatedPath, 0o600, &Options{Synchronous: SyncNormal, CheckpointThresholdBytes: 1 << 40}, fsys)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	simulatedPut(t, db, "key", "old")
	if err := BlackoutCheckpoint(db); err != nil {
		t.Fatal(err)
	}
	simulatedPut(t, db, "key", "new")
	// The checkpoint's own Stat succeeds, so the checkpoint completes, and the Stat of the remap fails.
	fsys.armed, fsys.skip = true, 1
	if err := BlackoutCheckpoint(db); err == nil {
		t.Fatal("the checkpoint did not report the failed remap")
	}
	if db.wal.Stats().CommittedRecordCount != 0 {
		t.Fatal("the checkpoint kept WAL pages, so this test does not reach the stale mapping")
	}
	if value, err := db.Get(testBucketName, []byte("key")); err != nil || string(value) != "new" {
		t.Fatalf("after a failed remap: got %q, %v, want %q", value, err, "new")
	}
}

// corruptOneMainReadFS flips one bit in one read of the first tree page of the main file at name, once, after armed is set.
type corruptOneMainReadFS struct {
	fileio.FS
	name  string
	armed bool
}

func (fsys *corruptOneMainReadFS) OpenFile(name string, flag int, perm os.FileMode) (fileio.File, error) {
	file, err := fsys.FS.OpenFile(name, flag, perm)
	if err != nil || name != fsys.name {
		return file, err
	}
	return &corruptOneMainReadFile{File: file, fsys: fsys}, nil
}

type corruptOneMainReadFile struct {
	fileio.File
	fsys *corruptOneMainReadFS
}

func (file *corruptOneMainReadFile) ReadAt(p []byte, offset int64) (int, error) {
	n, err := file.File.ReadAt(p, offset)
	pageStart := int64(firstTreePageID) * int64(os.Getpagesize())
	if file.fsys.armed && offset <= pageStart && pageStart < offset+int64(n) {
		file.fsys.armed = false
		p[pageStart-offset+20] ^= 1
	}
	return n, err
}

// Backup must save one committed state.
// With data-page checksums, a read that returns a flipped bit must fail the backup, not make a damaged backup that Backup reports as complete.
// Blackout found this: a flipped bit in the copy of the main file went into the backup, and Backup returned nil.
func TestSimulatedDisk_CorruptedReadDoesNotMakeADamagedBackup(t *testing.T) {
	fsys := &corruptOneMainReadFS{FS: disk.New(1, disk.Config{DisableMap: true}), name: simulatedPath}
	db, err := open(simulatedPath, 0o600, &Options{Synchronous: SyncFull, DataPageChecksums: true, CheckpointThresholdBytes: 1}, fsys)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	simulatedPut(t, db, "key", "value")
	fsys.armed = true
	if err := db.Backup("/sim/backup"); err != nil {
		// An error is a correct answer.
		return
	}
	if fsys.armed {
		t.Fatal("Backup did not read the first tree page from the main file, so this test does not reach the damage")
	}
	backup, err := open("/sim/backup", 0o600, &Options{ReadOnly: true}, fsys)
	if err != nil {
		t.Fatalf("Backup returned nil, but the backup does not open: %v", err)
	}
	defer backup.Close()
	if value, err := backup.Get(testBucketName, []byte("key")); err != nil || string(value) != "value" {
		t.Fatalf("Backup returned nil, but the backup is damaged: got %q, %v", value, err)
	}
}
