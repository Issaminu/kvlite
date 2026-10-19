package kvlite

import (
	"bytes"
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

// A checkpoint makes T1 and T2 durable in the main file, and its WAL truncate is not synced.
// The next commit starts a new WAL generation over the old records.
// A power failure can then keep some sectors of the new generation and lose its header and the truncate.
// The old header then validates a prefix of the old records, which is older than the main file, and recovery must not apply it.
// Blackout found this: recovery rolled back checkpointed data, or Open failed with "allocation high page does not match metadata page".
func TestSimulatedDisk_NewWALGenerationCannotReviveOldRecords(t *testing.T) {
	for seed := range uint64(300) {
		fsys := disk.New(seed, disk.Config{})
		db, err := open(simulatedPath, 0o600, &Options{Synchronous: SyncNormal, CheckpointThresholdBytes: 1 << 40}, fsys)
		if err != nil {
			t.Fatal(err)
		}
		checkpoint := func() {
			t.Helper()
			if err := BlackoutCheckpoint(db); err != nil {
				t.Fatal(err)
			}
		}
		simulatedPut(t, db, "filler", "")
		checkpoint()
		// The old generation: a small T1 in the first WAL sector, then a T2 that covers many sectors.
		simulatedPut(t, db, "key", "T1")
		if err := db.Update(func(tx *Tx) error {
			bucket, err := tx.Bucket(testBucketName)
			if err != nil {
				return err
			}
			if err := bucket.Put([]byte("filler"), bytes.Repeat([]byte("2"), 8000)); err != nil {
				return err
			}
			return bucket.Put([]byte("key"), []byte("T2"))
		}); err != nil {
			t.Fatal(err)
		}
		checkpoint()
		// SyncNormal does not sync this commit.
		simulatedPut(t, db, "filler", strings.Repeat("3", 12000))
		simulatedCrash(fsys, db)
		fsys.Restart(true)
		if value, err := simulatedGet(t, fsys, SyncNormal, "key"); err != nil || value != "T2" {
			t.Fatalf("seed %d: the checkpointed key after a power failure: got %q, %v, want %q", seed, value, err, "T2")
		}
	}
}

// A sector need not be atomic, so a power failure can keep only part of the WAL header.
// A generation syncs its header before a commit can depend on it, so a torn header holds no durable commit.
// Open must then succeed without that WAL, as SQLite ignores a WAL with a bad header.
// Blackout found this with torn sectors: the header kept 17 of its 20 bytes, and Open failed for good.
func TestSimulatedDisk_TornWALHeaderOpensWithoutTheWAL(t *testing.T) {
	fsys := disk.New(1, disk.Config{})
	db, err := open(simulatedPath, 0o600, &Options{Synchronous: SyncNormal, CheckpointThresholdBytes: 1 << 40}, fsys)
	if err != nil {
		t.Fatal(err)
	}
	simulatedPut(t, db, "key", "unsynced")
	simulatedCrash(fsys, db)
	fsys.Restart(false)
	// The torn first sector kept 17 header bytes; the last 3 bytes are zero.
	wal, err := fsys.OpenFile(simulatedPath+"-wal", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.WriteAt([]byte{0, 0, 0}, 17); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := simulatedGet(t, fsys, SyncNormal, "key"); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("the commit of the torn generation: got %v, want ErrBucketNotFound", err)
	}
}

// A process can truncate the WAL without a sync and then crash.
// The next handle sees an empty WAL, but storage can still hold the old records.
// So every new WAL generation must sync its header first, not only one that this handle truncated.
// Blackout found this: a damage probe truncated the WAL in its recovery, the next handle wrote a new generation, and a power failure revived part of the old one.
func TestSimulatedDisk_NewHandleCannotReviveOldRecords(t *testing.T) {
	for seed := range uint64(300) {
		fsys := disk.New(seed, disk.Config{})
		options := &Options{Synchronous: SyncNormal, CheckpointThresholdBytes: 1 << 40}
		db, err := open(simulatedPath, 0o600, options, fsys)
		if err != nil {
			t.Fatal(err)
		}
		simulatedPut(t, db, "filler", "")
		if err := BlackoutCheckpoint(db); err != nil {
			t.Fatal(err)
		}
		simulatedPut(t, db, "key", "T1")
		if err := db.Update(func(tx *Tx) error {
			bucket, err := tx.Bucket(testBucketName)
			if err != nil {
				return err
			}
			if err := bucket.Put([]byte("filler"), bytes.Repeat([]byte("2"), 8000)); err != nil {
				return err
			}
			return bucket.Put([]byte("key"), []byte("T2"))
		}); err != nil {
			t.Fatal(err)
		}
		if err := BlackoutCheckpoint(db); err != nil {
			t.Fatal(err)
		}
		// The process crashes after the unsynced truncate, so the next handle finds an empty WAL.
		simulatedCrash(fsys, db)
		fsys.Restart(false)
		db, err = open(simulatedPath, 0o600, options, fsys)
		if err != nil {
			t.Fatal(err)
		}
		simulatedPut(t, db, "filler", strings.Repeat("3", 12000))
		simulatedCrash(fsys, db)
		fsys.Restart(true)
		if value, err := simulatedGet(t, fsys, SyncNormal, "key"); err != nil || value != "T2" {
			t.Fatalf("seed %d: the checkpointed key after a power failure: got %q, %v, want %q", seed, value, err, "T2")
		}
	}
}

// A power failure can lose the name of a new main file and keep its WAL.
// Open then creates a new main file next to the old WAL.
// The empty main file holds no database, so Open ignores the WAL, and every read of the handle, and of a later handle, shows the empty database.
// Blackout found this: View showed no bucket, but DB.Get found it, and a clean reopen showed the bucket.
func TestSimulatedDisk_NewMainFileWithOldWALShowsOneState(t *testing.T) {
	fsys := disk.New(1, disk.Config{})
	db, err := open(simulatedPath, 0o600, &Options{Synchronous: SyncNormal, CheckpointThresholdBytes: 1 << 40}, fsys)
	if err != nil {
		t.Fatal(err)
	}
	simulatedPut(t, db, "key", "value")
	simulatedCrash(fsys, db)
	fsys.Restart(false)
	// The main file name is lost. The WAL stays.
	if err := fsys.Remove(simulatedPath); err != nil {
		t.Fatal(err)
	}

	read := func() (viewErr, getErr error) {
		db, err := open(simulatedPath, 0o600, &Options{Synchronous: SyncNormal}, fsys)
		if err != nil {
			t.Fatalf("Open with the old WAL: %v", err)
		}
		defer db.Close()
		viewErr = db.View(func(tx *Tx) error {
			bucket, err := tx.Bucket(testBucketName)
			if err != nil {
				return err
			}
			_, err = bucket.Get([]byte("key"))
			return err
		})
		_, getErr = db.Get(testBucketName, []byte("key"))
		return viewErr, getErr
	}
	viewErr, getErr := read()
	if !errors.Is(viewErr, ErrBucketNotFound) || !errors.Is(getErr, ErrBucketNotFound) {
		t.Errorf("the handle does not show the empty database: View returned %v, DB.Get returned %v", viewErr, getErr)
	}
	if againView, _ := read(); !errors.Is(againView, viewErr) && !errors.Is(viewErr, againView) {
		t.Errorf("a clean reopen changed the state: View returned %v, then %v", viewErr, againView)
	}
}

// A crash at any file change of the first Open happens before any commit.
// So the next Open must give a usable empty database, after a process crash and after a power failure.
// Blackout found this: the next Open failed with "metadata last page N exceeds main-file and WAL page IDs".
func TestSimulatedDisk_CrashWhileOpenCreatesTheFile(t *testing.T) {
	for _, powerFailure := range []bool{false, true} {
		for crashAt := 1; crashAt <= 12; crashAt++ {
			fsys := disk.New(uint64(crashAt), disk.Config{})
			fsys.ScheduleCrash(crashAt)
			db, err := open(simulatedPath, 0o600, nil, fsys)
			if crashed, _ := fsys.Crashed(); !crashed {
				// Open made fewer changes than crashAt.
				if err != nil {
					t.Fatalf("crash point %d: Open without a crash: %v", crashAt, err)
				}
				fsys.CancelCrash()
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				continue
			}
			if err == nil {
				simulatedCrash(fsys, db)
			}
			_, event := fsys.Crashed()
			fsys.Restart(powerFailure)
			// A read-only handle cannot create the database, so an empty file is an error.
			if readOnly, err := open(simulatedPath, 0o600, &Options{ReadOnly: true}, fsys); err == nil {
				if err := readOnly.Close(); err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Errorf("power failure %t, crash point %d (%s): read-only Open after the crash: %v", powerFailure, crashAt, event, err)
			}
			db, err = open(simulatedPath, 0o600, nil, fsys)
			if err != nil {
				t.Errorf("power failure %t, crash point %d (%s): Open after the crash: %v", powerFailure, crashAt, event, err)
				continue
			}
			simulatedPut(t, db, "key", "value")
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// A failed sync can drop a WAL write from storage for good while the cache keeps it, and a later sync does not write it.
// After a process crash, recovery reads that write from the cache, and its WAL sync does not store it.
// If a power failure then tears the main-file writes of recovery, the durable WAL must still repair them.
// Blackout found this: the recovered tree had keys out of order.
func TestSimulatedDisk_RecoveryStoresTheWALItReads(t *testing.T) {
	dropped := 0
	for seed := range uint64(40) {
		for crashAt := 1; crashAt <= 12; crashAt++ {
			fsys := disk.New(seed, disk.Config{SyncFault: disk.Always})
			db, err := open(simulatedPath, 0o600, &Options{Synchronous: SyncNormal, CheckpointThresholdBytes: 1 << 40}, fsys)
			if err != nil {
				t.Fatal(err)
			}
			simulatedPut(t, db, "base", "v0")
			if err := BlackoutCheckpoint(db); err != nil {
				t.Fatal(err)
			}
			simulatedPut(t, db, "key", strings.Repeat("v", 3*os.Getpagesize()))
			fsys.SetFaults(true)
			_ = BlackoutCheckpoint(db)
			fsys.SetFaults(false)
			if cache, durable, _ := fsys.FileData(simulatedPath + "-wal"); bytes.Equal(cache, durable) {
				// The failed sync stored every WAL write.
				simulatedCrash(fsys, db)
				break
			}
			dropped++
			simulatedCrash(fsys, db)
			fsys.Restart(false)
			fsys.ScheduleCrash(crashAt)
			if db, err := open(simulatedPath, 0o600, &Options{Synchronous: SyncNormal}, fsys); err == nil {
				fsys.CancelCrash()
				if crashed, _ := fsys.Crashed(); !crashed {
					// Recovery made fewer changes than crashAt.
					if err := db.Close(); err != nil {
						t.Fatal(err)
					}
					break
				}
				BlackoutAbandon(db)
			}
			fsys.Restart(true)
			db, err = open(simulatedPath, 0o600, &Options{Synchronous: SyncNormal}, fsys)
			if err != nil {
				t.Fatalf("seed %d, crash point %d: Open after a power failure in recovery: %v", seed, crashAt, err)
			}
			if err := BlackoutCheckPages(db); err != nil {
				t.Fatalf("seed %d, crash point %d: pages after a power failure in recovery: %v", seed, crashAt, err)
			}
			if value, err := db.Get(testBucketName, []byte("base")); err != nil || value == nil {
				t.Fatalf("seed %d, crash point %d: checkpointed key: %v", seed, crashAt, err)
			}
			if _, err := db.Get(testBucketName, []byte("key")); err != nil && !errors.Is(err, ErrKeyNotFound) {
				t.Fatalf("seed %d, crash point %d: unsynced key: %v", seed, crashAt, err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if dropped == 0 {
		t.Fatal("no seed dropped a WAL write, so this test does not reach the case")
	}
}

// After ErrWALWriteFailed, the documented step is to close, open again, and check the data.
// The answer of that open must be final: a commit that it does not show must not come back after a later power failure.
// Blackout found this: the WAL sync of a commit succeeded and the directory sync failed, so the commit failed and its rollback truncated the WAL without a sync.
// A power failure then lost the truncate, and the commit came back.
func TestSimulatedDisk_FailedCommitStaysLostAfterPowerFailure(t *testing.T) {
	// Keep almost no unsynced change at the power failure.
	fsys := disk.New(1, disk.Config{SyncDirFault: disk.Always, KeepChange: disk.Ratio{Num: 1, Den: 1 << 30}})
	db, err := open(simulatedPath, 0o600, &Options{Synchronous: SyncFull}, fsys)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = open(simulatedPath, 0o600, &Options{Synchronous: SyncFull}, fsys)
	if err != nil {
		t.Fatal(err)
	}
	// The first commit of a new WAL syncs the directory, and that sync fails.
	fsys.SetFaults(true)
	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket(testBucketName)
		if err != nil {
			return err
		}
		return bucket.Put([]byte("key"), []byte("failed"))
	})
	fsys.SetFaults(false)
	if !errors.Is(err, ErrWALWriteFailed) {
		t.Fatalf("commit with a failed directory sync: %v, want ErrWALWriteFailed", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := simulatedGet(t, fsys, SyncFull, "key"); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("the failed commit after the reopen: %v, want ErrBucketNotFound", err)
	}
	fsys.Crash()
	fsys.Restart(true)
	if _, err := simulatedGet(t, fsys, SyncFull, "key"); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("the failed commit after a power failure: %v, want ErrBucketNotFound; the reopen said it was lost", err)
	}
}

// After a failed sync, the WAL file content is unknown: Linux can drop the unsynced pages after a writeback error.
// A checkpoint that then writes the main file has no WAL that can repair a torn write.
// So a failed WAL sync in a checkpoint must stop the handle like a failed commit, and Close must keep the WAL for the next Open.
// Blackout found this: a checkpoint retry after a failed WAL sync wrote main pages, a power failure tore them, and the recovered database was invalid.
func TestSimulatedDisk_FailedWALSyncInCheckpointStopsTheHandle(t *testing.T) {
	fsys := disk.New(1, disk.Config{SyncFault: disk.Always, DropCache: disk.Always})
	db, err := open(simulatedPath, 0o600, &Options{Synchronous: SyncNormal, CheckpointThresholdBytes: 1 << 40}, fsys)
	if err != nil {
		t.Fatal(err)
	}
	simulatedPut(t, db, "key", "value")
	fsys.SetFaults(true)
	err = BlackoutCheckpoint(db)
	fsys.SetFaults(false)
	if err == nil {
		t.Fatal("the checkpoint with a failed WAL sync succeeded")
	}
	if err := db.Put(testBucketName, []byte("later"), []byte("value")); !errors.Is(err, ErrWALWriteFailed) {
		t.Fatalf("Put after a failed WAL sync in a checkpoint: %v, want ErrWALWriteFailed", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close after a failed WAL sync in a checkpoint: %v", err)
	}
	// The dropped cache can keep or lose the commit, but the state must be one of the two.
	if value, err := simulatedGet(t, fsys, SyncNormal, "key"); !errors.Is(err, ErrBucketNotFound) && (err != nil || value != "value") {
		t.Fatalf("after the reopen: got %q, %v", value, err)
	}
}
