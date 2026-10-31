package kvlite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
)

func TestBackupLiveSnapshot(t *testing.T) {
	for _, mode := range []Sync{SyncFull, SyncNormal, SyncNone} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(filepath.Join(dir, "source.db"), 0600, &Options{Synchronous: mode, CheckpointThresholdBytes: 1 << 30})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			createBucket(t, db, testBucketName)
			write := func(value string) error {
				return db.Update(func(tx *Tx) error {
					bucket, err := tx.Bucket(testBucketName)
					if err != nil {
						return err
					}
					for i := range 400 {
						if err := bucket.Put(fmt.Appendf(nil, "%04d", i), bytes.Repeat([]byte(value), 50)); err != nil {
							return err
						}
					}
					return nil
				})
			}
			if err := write("base"); err != nil {
				t.Fatal(err)
			}
			db.operationMu.Lock()
			err = db.checkpointWAL()
			db.operationMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Put(testBucketName, []byte("0000"), []byte("snapshot")); err != nil {
				t.Fatal(err)
			}
			mainBefore, err := os.ReadFile(db.path)
			if err != nil {
				t.Fatal(err)
			}
			before, _ := db.Stats()
			walBefore, err := os.Stat(db.path + "-wal")
			if err != nil {
				t.Fatal(err)
			}
			db.operationMu.Lock()
			db.wal.SetCheckpointThresholdBytes(1)
			db.operationMu.Unlock()
			path := filepath.Join(dir, "backup.db")
			err = db.backup(path, func(dst io.Writer, src io.Reader) (int64, error) {
				if err := write("later"); err != nil {
					return 0, err
				}
				if err := db.Backup(filepath.Join(dir, "second.db")); err != nil {
					return 0, err
				}
				if err := db.DeleteBucket(testBucketName); err != nil {
					return 0, err
				}
				createBucket(t, db, testBucketName)
				if err := write("reused"); err != nil {
					return 0, err
				}
				mainDuring, err := os.ReadFile(db.path)
				if err != nil {
					return 0, err
				}
				if !bytes.Equal(mainBefore, mainDuring) {
					t.Fatal("main file changed during backup")
				}
				stats, _ := db.Stats()
				if stats.CheckpointCount != before.CheckpointCount {
					t.Fatal("checkpoint ran during backup")
				}
				walDuring, err := os.Stat(db.path + "-wal")
				if err != nil {
					return 0, err
				}
				if stats.WALBytesSinceCheckpoint <= before.WALBytesSinceCheckpoint {
					t.Fatal("WAL record prefix did not grow")
				}
				if walDuring.Size() < walBefore.Size() {
					t.Fatal("WAL file shrank during backup")
				}
				return io.Copy(dst, src)
			}, fileio.Sync)
			if err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if runtime.GOOS != "windows" && info.Mode().Perm() != 0600 {
				t.Fatalf("backup permissions: got %04o, want 0600", info.Mode().Perm())
			}
			if _, err := os.Stat(path + "-wal"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("backup WAL: %v", err)
			}
			backup, err := Open(path, 0600, &Options{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			for i := range 400 {
				want := bytes.Repeat([]byte("base"), 50)
				if i == 0 {
					want = []byte("snapshot")
				}
				got, err := backup.Get(testBucketName, fmt.Appendf(nil, "%04d", i))
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("snapshot key %d: got %q, error %v", i, got, err)
				}
			}
			if err := backup.Close(); err != nil {
				t.Fatal(err)
			}
			if err := db.Put(testBucketName, []byte("0000"), []byte("after")); err != nil {
				t.Fatal(err)
			}
			stats, _ := db.Stats()
			if stats.CheckpointCount <= before.CheckpointCount {
				t.Fatal("checkpoint did not resume")
			}
		})
	}
}

func TestBackupReadOnlyWAL(t *testing.T) {
	dir := t.TempDir()
	db, err := openDB(filepath.Join(dir, "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	key := bytes.Repeat([]byte("k"), MaxKeySize)
	value := bytes.Repeat([]byte("v"), 3*int(db.meta.PageSize()))
	if err := db.Put(testBucketName, key, value); err != nil {
		t.Fatal(err)
	}
	// Keep the main file and WAL as they are after a process exit.
	if err := db.unmapMainFile(); err != nil {
		t.Fatal(err)
	}
	if err := db.closeFiles(); err != nil {
		t.Fatal(err)
	}
	db.stopWriteBatcherAndWait()
	readOnly, err := Open(db.path, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	path := filepath.Join(dir, "backup.db")
	if err := readOnly.Backup(path); err != nil {
		t.Fatal(err)
	}
	backup, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
	got, err := backup.Get(testBucketName, key)
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("backup lost WAL value: length %d, %v", len(got), err)
	}
	if err := backup.Put(testBucketName, key, []byte("restored")); err != nil {
		t.Fatal(err)
	}
}

func TestBackupContinuedTree(t *testing.T) {
	for _, mode := range []Sync{SyncNormal, SyncFull} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(filepath.Join(dir, "source.db"), 0600, &Options{Synchronous: mode, CheckpointThresholdBytes: 1 << 30})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			createBucket(t, db, testBucketName)
			keys := make([][]byte, 8)
			values := make([][]byte, len(keys))
			for i := range keys {
				keys[i] = bytes.Repeat([]byte{byte('a' + i)}, MaxKeySize)
				values[i] = bytes.Repeat([]byte{byte(i)}, 3*int(db.meta.PageSize())+i)
				if err := db.Put(testBucketName, keys[i], values[i]); err != nil {
					t.Fatal(err)
				}
			}
			db.operationMu.Lock()
			err = db.checkpointWAL()
			db.operationMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			values[0] = bytes.Repeat([]byte("snapshot"), int(db.meta.PageSize()))
			if err := db.Put(testBucketName, keys[0], values[0]); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "backup.db")
			err = db.backup(path, func(dst io.Writer, src io.Reader) (int64, error) {
				if err := db.DeleteBucket(testBucketName); err != nil {
					return 0, err
				}
				createBucket(t, db, testBucketName)
				if err := db.Put(testBucketName, keys[0], []byte("later")); err != nil {
					return 0, err
				}
				return io.Copy(dst, src)
			}, fileio.Sync)
			if err != nil {
				t.Fatal(err)
			}
			backup, err := Open(path, 0600, &Options{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer backup.Close()
			for i := range keys {
				got, err := backup.Get(testBucketName, keys[i])
				if err != nil || !bytes.Equal(got, values[i]) {
					t.Fatalf("backup key %d: length %d, error %v", i, len(got), err)
				}
			}
		})
	}
}

func TestBackupFailureAndPaths(t *testing.T) {
	dir := t.TempDir()
	db, err := openDB(filepath.Join(dir, "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Backup(""); !errors.Is(err, ErrPathRequired) {
		t.Fatalf("empty path: %v", err)
	}
	for _, path := range []string{db.path, db.path + "-wal"} {
		if err := db.Backup(path); !errors.Is(err, os.ErrExist) {
			t.Fatalf("existing path: %v", err)
		}
	}
	path := filepath.Join(dir, "backup.db")
	if err := os.WriteFile(path+"-wal", []byte("stale"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := db.Backup(path); !errors.Is(err, os.ErrExist) {
		t.Fatalf("stale WAL: %v", err)
	}
	if err := os.Remove(path + "-wal"); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("copy failed")
	if err := db.backup(path, func(io.Writer, io.Reader) (int64, error) { return 0, wantErr }, fileio.Sync); !errors.Is(err, wantErr) {
		t.Fatalf("copy failure: %v", err)
	}
	if db.activeBackups != 0 {
		t.Fatal("failed backup kept checkpoints paused")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed backup published a file: %v", err)
	}
	files, err := filepath.Glob(filepath.Join(dir, ".kvlite-backup-*"))
	if err != nil || len(files) != 0 {
		t.Fatalf("temporary files remain: %v, %v", files, err)
	}
	if err := db.Backup(path); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Backup(filepath.Join(dir, "closed.db")); !errors.Is(err, ErrDatabaseNotOpen) {
		t.Fatalf("closed database: %v", err)
	}
}

func TestBackupCloseWaits(t *testing.T) {
	dir := t.TempDir()
	db, err := openDB(filepath.Join(dir, "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	backupDone := make(chan error, 1)
	go func() {
		backupDone <- db.backup(filepath.Join(dir, "backup.db"), func(dst io.Writer, src io.Reader) (int64, error) {
			close(started)
			<-release
			return io.Copy(dst, src)
		}, fileio.Sync)
	}()
	<-started
	closeDone := make(chan error, 1)
	go func() { closeDone <- db.Close() }()
	select {
	case err := <-closeDone:
		close(release)
		t.Fatalf("Close returned before backup: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-backupDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
}

func TestBackupProcessExit(t *testing.T) {
	if phase := os.Getenv("KVLITE_BACKUP_EXIT_PHASE"); phase != "" {
		dir := os.Getenv("KVLITE_BACKUP_EXIT_DIR")
		db, err := Open(filepath.Join(dir, "source.db"), 0600, &Options{Synchronous: SyncFull, CheckpointThresholdBytes: 1 << 30})
		if err != nil {
			t.Fatal(err)
		}
		createBucket(t, db, testBucketName)
		if err := db.Put(testBucketName, []byte("key"), []byte("snapshot")); err != nil {
			t.Fatal(err)
		}
		mainBefore, err := os.ReadFile(db.path)
		if err != nil {
			t.Fatal(err)
		}
		db.operationMu.Lock()
		db.wal.SetCheckpointThresholdBytes(1)
		db.operationMu.Unlock()
		err = db.backup(filepath.Join(dir, "backup.db"), func(dst io.Writer, src io.Reader) (int64, error) {
			if err := db.Put(testBucketName, []byte("key"), []byte("during copy")); err != nil {
				return 0, err
			}
			if phase == "during-copy" {
				if _, err := io.CopyN(dst, src, 16); err != nil {
					return 0, err
				}
				// Exit without running deferred cleanup or Close.
				os.Exit(86)
			}
			return io.Copy(dst, src)
		}, fileio.Sync)
		if err != nil {
			t.Fatal(err)
		}
		mainAfter, err := os.ReadFile(db.path)
		if err != nil || !bytes.Equal(mainBefore, mainAfter) {
			t.Fatalf("backup changed source main file: %v", err)
		}
		if db.activeBackups != 0 {
			t.Fatal("backup returned with checkpoints paused")
		}
		os.Exit(86)
	}

	for _, phase := range []string{"during-copy", "after-return"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBackupProcessExit$")
			command.Env = append(os.Environ(), "KVLITE_BACKUP_EXIT_PHASE="+phase, "KVLITE_BACKUP_EXIT_DIR="+dir)
			output, err := command.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 86 {
				t.Fatalf("child exit: %v\n%s", err, output)
			}
			files, err := filepath.Glob(filepath.Join(dir, ".kvlite-backup-*"))
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "backup.db")
			if phase == "during-copy" {
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("partial backup became visible: %v", err)
				}
				if len(files) != 1 {
					t.Fatalf("temporary files: %v", files)
				}
			} else {
				if len(files) != 0 {
					t.Fatalf("backup returned before cleanup: %v", files)
				}
				backup, err := Open(path, 0600, &Options{ReadOnly: true})
				if err != nil {
					t.Fatal(err)
				}
				got, readErr := backup.Get(testBucketName, []byte("key"))
				closeErr := backup.Close()
				if readErr != nil || closeErr != nil || string(got) != "snapshot" {
					t.Fatalf("backup state: %q, %v, %v", got, readErr, closeErr)
				}
			}
			source, err := Open(filepath.Join(dir, "source.db"), 0600, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, readErr := source.Get(testBucketName, []byte("key"))
			closeErr := source.Close()
			if readErr != nil || closeErr != nil || string(got) != "during copy" {
				t.Fatalf("source recovery: %q, %v, %v", got, readErr, closeErr)
			}
		})
	}
}

func TestBackupSyncMode(t *testing.T) {
	for _, readOnly := range []bool{false, true} {
		for _, mode := range []Sync{SyncDefault, SyncFull, SyncNormal, SyncNone} {
			t.Run(fmt.Sprintf("readonly=%t/mode=%d", readOnly, mode), func(t *testing.T) {
				dir := t.TempDir()
				sourcePath := filepath.Join(dir, "source.db")
				db, err := openDB(sourcePath)
				if err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db, err = Open(sourcePath, 0600, &Options{ReadOnly: readOnly, Synchronous: mode})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				var calls []string
				err = db.backup(filepath.Join(dir, "backup.db"), io.Copy, func(file *os.File) error {
					info, err := file.Stat()
					if err != nil {
						return err
					}
					if info.IsDir() {
						calls = append(calls, "directory")
					} else {
						calls = append(calls, "file")
					}
					return fileio.Sync(file)
				})
				if err != nil {
					t.Fatal(err)
				}
				var want []string
				if mode != SyncNone {
					want = []string{"file", "directory"}
				}
				if !slices.Equal(calls, want) {
					t.Fatalf("sync calls: got %v, want %v", calls, want)
				}
			})
		}
	}
}

func TestBackupSyncFailure(t *testing.T) {
	for _, failDirectory := range []bool{false, true} {
		t.Run(fmt.Sprintf("directory=%t", failDirectory), func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(filepath.Join(dir, "source.db"), 0600, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			path := filepath.Join(dir, "backup.db")
			wantErr := errors.New("sync failed")
			err = db.backup(path, io.Copy, func(file *os.File) error {
				info, err := file.Stat()
				if err != nil {
					return err
				}
				if info.IsDir() == failDirectory {
					return wantErr
				}
				return fileio.Sync(file)
			})
			if !errors.Is(err, wantErr) {
				t.Fatalf("sync error: got %v, want %v", err, wantErr)
			}
			if db.activeBackups != 0 {
				t.Fatal("sync failure kept checkpoints paused")
			}
			if failDirectory {
				backup, err := Open(path, 0600, &Options{ReadOnly: true})
				if err != nil {
					t.Fatal(err)
				}
				if err := backup.Close(); err != nil {
					t.Fatal(err)
				}
			} else if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("backup became visible before file sync: %v", err)
			}
		})
	}
}

func TestBackupRawSpanAcrossAllocationFrames(t *testing.T) {
	path, bucket, entries, _ := writeContinuedReadDatabase(t, true)
	source, err := Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	backupPath := filepath.Join(t.TempDir(), "backup.db")
	if err := source.Backup(backupPath); err != nil {
		t.Fatal(err)
	}
	backup, err := Open(backupPath, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
	for _, entry := range entries {
		got, err := backup.Get(bucket, entry.Key())
		if err != nil || !bytes.Equal(got, entry.Value()) {
			t.Fatalf("backup value after allocation-frame boundary: length %d, error %v", len(got), err)
		}
	}
}

func TestBackupNestedRawSpanChecksums(t *testing.T) {
	for _, pendingWAL := range []bool{false, true} {
		t.Run(fmt.Sprintf("WAL=%t", pendingWAL), func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(filepath.Join(dir, "source.db"), 0600, &Options{
				Synchronous: SyncNormal, DataPageChecksums: true, CheckpointThresholdBytes: 1 << 30,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			key := bytes.Repeat([]byte("k"), int(db.meta.PageSize())+23)
			marker := []byte("backup-span-nested-value")
			value := bytes.Repeat(marker, (3*int(db.meta.PageSize())+65536)/len(marker)+1)
			grandValue := bytes.Repeat([]byte("grandchild-value"), int(db.meta.PageSize())/4+1)
			if err := db.Update(func(tx *Tx) error {
				parent, err := tx.CreateBucket([]byte("parent"))
				if err != nil {
					return err
				}
				child, err := parent.CreateBucket([]byte("child"))
				if err != nil {
					return err
				}
				if err := child.Put(key, value); err != nil {
					return err
				}
				grandchild, err := child.CreateBucket([]byte("grandchild"))
				if err != nil {
					return err
				}
				return grandchild.Put([]byte("key"), grandValue)
			}); err != nil {
				t.Fatal(err)
			}
			db.operationMu.Lock()
			err = db.checkpointWAL()
			db.operationMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if pendingWAL {
				value[0] ^= 1
				if err := db.Update(func(tx *Tx) error {
					parent, err := tx.Bucket([]byte("parent"))
					if err != nil {
						return err
					}
					child, err := parent.Bucket([]byte("child"))
					if err != nil {
						return err
					}
					return child.Put(key, value)
				}); err != nil {
					t.Fatal(err)
				}
			}
			path := filepath.Join(dir, "backup.db")
			if err := db.Backup(path); err != nil {
				t.Fatal(err)
			}
			copy, err := Open(path, 0600, &Options{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			err = copy.View(func(tx *Tx) error {
				parent, err := tx.Bucket([]byte("parent"))
				if err != nil {
					return err
				}
				child, err := parent.Bucket([]byte("child"))
				if err != nil {
					return err
				}
				got, err := child.Get(key)
				if err != nil || !bytes.Equal(got, value) {
					return fmt.Errorf("nested value: %v", err)
				}
				grandchild, err := child.Bucket([]byte("grandchild"))
				if err != nil {
					return err
				}
				got, err = grandchild.Get([]byte("key"))
				if err != nil || !bytes.Equal(got, grandValue) {
					return fmt.Errorf("grandchild value: %v", err)
				}
				return nil
			})
			if err := errors.Join(err, copy.Close()); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			at := bytes.Index(data, value)
			if at < 0 {
				t.Fatal("backup does not contain a contiguous nested value")
			}
			data[at+2*int(db.meta.PageSize())] ^= 1
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			damaged, err := Open(path, 0600, &Options{ReadOnly: true})
			if err != nil {
				if !errors.Is(err, page.ErrChecksum) {
					t.Fatal(err)
				}
				return
			}
			defer damaged.Close()
			if err := damaged.Backup(filepath.Join(dir, "damaged-copy.db")); !errors.Is(err, page.ErrChecksum) {
				t.Fatalf("backup accepted a damaged raw tail: %v", err)
			}
		})
	}
}
