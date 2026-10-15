package kvlite

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Issaminu/kvlite/internal/btree"
)

func checkpointChecksumTest(t *testing.T, db *DB) {
	t.Helper()
	db.operationMu.Lock()
	err := db.checkpointWAL()
	db.operationMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

func checkDataPageFields(t *testing.T, db *DB, enabled bool) {
	t.Helper()
	for id := firstTreePageID; id <= db.meta.LastPage(); id++ {
		if !db.allocation.allocated(id) || db.allocation.isSegmentPage(id) {
			continue
		}
		data, err := db.readMainPage(id)
		if err != nil {
			t.Fatal(err)
		}
		if enabled {
			if err := btree.CheckNodeChecksum(data, id); err != nil {
				t.Fatalf("page %d: %v", id, err)
			}
		} else {
			offset := 14
			if btree.NodeType(binary.LittleEndian.Uint16(data[:2])) == btree.NodeTypeContinuation {
				offset = 10
			}
			if sum := binary.LittleEndian.Uint32(data[offset : offset+4]); sum != 0 {
				t.Fatalf("page %d: checksum %x, want zero", id, sum)
			}
		}
	}
}

func TestSetDataPageChecksums_RoundTrip(t *testing.T) {
	for _, syncMode := range []Sync{SyncNone, SyncNormal, SyncFull} {
		t.Run(fmt.Sprint(syncMode), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			db, err := Open(path, 0600, &Options{Synchronous: syncMode, CheckpointThresholdBytes: 1 << 30})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			large := bytes.Repeat([]byte("v"), 64<<10)
			if err := db.Update(func(tx *Tx) error {
				b, err := tx.CreateBucket([]byte("bucket"))
				if err != nil {
					return err
				}
				for i := range 100 {
					if err := b.Put([]byte(fmt.Sprintf("key/%03d", i)), bytes.Repeat([]byte("a"), 128)); err != nil {
						return err
					}
				}
				return b.Put(bytes.Repeat([]byte("k"), MaxKeySize), large)
			}); err != nil {
				t.Fatal(err)
			}
			// Pending WAL data must be included in the conversion.
			if err := db.SetDataPageChecksums(true); err != nil {
				t.Fatal(err)
			}
			if !db.meta.DataPageChecksums() {
				t.Fatal("mode is still disabled")
			}
			checkDataPageFields(t, db, true)
			before := db.wal.Stats()
			if err := db.SetDataPageChecksums(true); err != nil {
				t.Fatal(err)
			}
			if after := db.wal.Stats(); after != before {
				t.Fatal("same mode changed WAL stats")
			}
			got, err := db.Get([]byte("bucket"), bytes.Repeat([]byte("k"), MaxKeySize))
			if err != nil || !bytes.Equal(got, large) {
				t.Fatalf("large read: %d bytes, %v", len(got), err)
			}
			beforeMain, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.SetDataPageChecksums(false); err != nil {
				t.Fatal(err)
			}
			afterMain, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(beforeMain, afterMain) {
				t.Fatal("disable changed the main file without a checkpoint")
			}
			// A changed leaf becomes unchecked. Untouched pages retain sums.
			if err := db.Put([]byte("bucket"), []byte("key/000"), bytes.Repeat([]byte("b"), 128)); err != nil {
				t.Fatal(err)
			}
			checkpointChecksumTest(t, db)
			zero, retained := 0, 0
			for id := firstTreePageID; id <= db.meta.LastPage(); id++ {
				if !db.allocation.allocated(id) || db.allocation.isSegmentPage(id) {
					continue
				}
				data, err := db.readMainPage(id)
				if err != nil {
					t.Fatal(err)
				}
				if btree.CheckNodeChecksumMode(data, true) == nil {
					retained++
				} else {
					zero++
				}
			}
			if zero == 0 || retained == 0 {
				t.Fatalf("want mixed fields: zero=%d, retained=%d", zero, retained)
			}
			backup := path + ".backup"
			if err := db.Backup(backup); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{path, backup} {
				for _, readOnly := range []bool{true, false} {
					db, err = Open(name, 0600, &Options{ReadOnly: readOnly, Synchronous: syncMode, DataPageChecksums: true})
					if err != nil {
						t.Fatal(err)
					}
					if db.meta.DataPageChecksums() {
						t.Fatal("mixed file reopened with enabled mode")
					}
					got, err := db.Get([]byte("bucket"), []byte("key/000"))
					if err != nil || !bytes.Equal(got, bytes.Repeat([]byte("b"), 128)) {
						t.Fatalf("mixed read: %q, %v", got, err)
					}
					if !readOnly {
						if err := db.SetDataPageChecksums(true); err != nil {
							t.Fatal(err)
						}
						checkDataPageFields(t, db, true)
					}
					if err := db.Close(); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

func TestSetDataPageChecksums_RetainedMismatch(t *testing.T) {
	for _, continued := range []bool{false, true} {
		t.Run(fmt.Sprint(continued), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			db, err := Open(path, 0600, &Options{Synchronous: SyncNone, DataPageChecksums: true})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			createBucket(t, db, []byte("bucket"))
			value := []byte("retained-checksum-value-marker")
			if continued {
				value = bytes.Repeat(value, 1000)
			}
			if err := db.Put([]byte("bucket"), []byte("key"), value); err != nil {
				t.Fatal(err)
			}
			checkpointChecksumTest(t, db)
			if err := db.SetDataPageChecksums(false); err != nil {
				t.Fatal(err)
			}
			checkpointChecksumTest(t, db)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			at := bytes.Index(data, []byte("retained-checksum-value-marker"))
			if continued {
				at = bytes.LastIndex(data, []byte("retained-checksum-value-marker"))
			}
			if at < 0 {
				t.Fatal("value marker is absent")
			}
			if _, err := db.file.WriteAt([]byte{'X'}, int64(at)); err != nil {
				t.Fatal(err)
			}
			// Reopen to remove previously decoded cache entries.
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = Open(path, 0600, &Options{Synchronous: SyncNone})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			got, err := db.Get([]byte("bucket"), []byte("key"))
			if err != nil || bytes.Equal(got, value) {
				t.Fatalf("disabled read: %d bytes, %v", len(got), err)
			}
			if err := db.View(func(tx *Tx) error {
				b, err := tx.Bucket([]byte("bucket"))
				if err != nil {
					return err
				}
				if _, err := b.Get([]byte("key")); err != nil {
					return err
				}
				cursor, err := b.Cursor()
				if err != nil {
					return err
				}
				if _, _, err := cursor.First(); err != nil {
					return err
				}
				for _, scan := range []func() error{
					func() error { return b.ScanPrefix(nil, func(_, _ []byte) error { return nil }) },
					func() error { return b.ScanReverse(func(_, _ []byte) error { return nil }) },
				} {
					if err := scan(); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := db.SetDataPageChecksums(true); !errors.Is(err, ErrChecksum) {
				t.Fatalf("enable: %v, want checksum error", err)
			}
			if db.meta.DataPageChecksums() {
				t.Fatal("failed conversion enabled the mode")
			}
		})
	}
}

func TestSetDataPageChecksums_Errors(t *testing.T) {
	db := OpenMemory()
	for _, mode := range []bool{false, true} {
		if err := db.SetDataPageChecksums(mode); !errors.Is(err, errors.ErrUnsupported) {
			t.Fatalf("memory: %v", err)
		}
	}
	db.Close()
	if err := db.SetDataPageChecksums(true); !errors.Is(err, ErrDatabaseNotOpen) {
		t.Fatalf("closed: %v", err)
	}
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	db, err = Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.SetDataPageChecksums(false); !errors.Is(err, ErrDatabaseReadOnly) {
		t.Fatalf("read-only: %v", err)
	}
}

func TestSetDataPageChecksums_BackupWait(t *testing.T) {
	for _, initial := range []bool{false, true} {
		t.Run(fmt.Sprint(initial), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			db, err := Open(path, 0600, &Options{Synchronous: SyncNone, DataPageChecksums: initial})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			createBucket(t, db, []byte("bucket"))
			if err := db.Put([]byte("bucket"), []byte("key"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			started, release := make(chan struct{}), make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			backupDone := make(chan error, 1)
			backup := path + ".backup"
			go func() {
				backupDone <- db.backup(backup, func(w io.Writer, r io.Reader) (int64, error) { close(started); <-release; return io.Copy(w, r) }, (*os.File).Sync)
			}()
			<-started
			modeDone := make(chan error, 1)
			go func() { modeDone <- db.SetDataPageChecksums(!initial) }()
			if initial {
				select {
				case err := <-modeDone:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("disable waited for the backup")
				}
			} else {
				select {
				case err := <-modeDone:
					t.Fatalf("enable did not wait: %v", err)
				case <-time.After(25 * time.Millisecond):
				}
			}
			close(release)
			select {
			case err := <-backupDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("backup did not finish")
			}
			if !initial {
				select {
				case err := <-modeDone:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("enable did not finish")
				}
			}
			copy, err := Open(backup, 0600, &Options{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer copy.Close()
			if copy.meta.DataPageChecksums() != initial {
				t.Fatal("backup lost its captured mode")
			}
			got, err := copy.Get([]byte("bucket"), []byte("key"))
			if err != nil || string(got) != "value" {
				t.Fatalf("backup read: %q, %v", got, err)
			}
		})
	}
}

func TestSetDataPageChecksums_InterruptedBatchRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNormal, CheckpointThresholdBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("bucket"))
		if err != nil {
			return err
		}
		for i := range 400 {
			if err := b.Put([]byte(fmt.Sprintf("key/%03d", i)), bytes.Repeat([]byte{byte(i)}, 3000)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	checkpointChecksumTest(t, db)
	injected := errors.New("conversion WAL sync failure")
	syncs := 0
	db.wal.SetSyncFileForTesting(func() error {
		syncs++
		if syncs == 2 {
			return injected
		}
		return db.wal.FileForTesting().Sync()
	})
	if err := db.SetDataPageChecksums(true); !errors.Is(err, injected) {
		t.Fatalf("enable: %v", err)
	}
	if db.meta.DataPageChecksums() {
		t.Fatal("failed batch published enabled mode")
	}
	// The first batch reached the main file. The second has full redo images.
	protected := 0
	for id := firstTreePageID; id <= db.meta.LastPage(); id++ {
		if !db.allocation.allocated(id) || db.allocation.isSegmentPage(id) {
			continue
		}
		data, err := db.readMainPage(id)
		if err != nil {
			t.Fatal(err)
		}
		if btree.CheckNodeChecksumMode(data, true) == nil {
			protected++
		}
	}
	if protected == 0 {
		t.Fatal("no completed conversion batch")
	}
	pages := db.wal.CommittedRecords()
	if len(pages) == 0 {
		t.Fatal("failed batch has no redo images")
	}
	// Simulate an interrupted main-file page write covered by that redo batch.
	for id, record := range pages {
		if record.Node != nil {
			if _, err := db.file.WriteAt(bytes.Repeat([]byte{0xff}, int(db.meta.PageSize())), int64(id)*db.meta.PageSize()); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	main, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	log, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	db.wal.SetSyncFileForTesting(nil)
	for _, readOnly := range []bool{true, false} {
		copyPath := fmt.Sprintf("%s.recovered.%t", path, readOnly)
		if err := os.WriteFile(copyPath, main, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(copyPath+"-wal", log, 0600); err != nil {
			t.Fatal(err)
		}
		copy, err := Open(copyPath, 0600, &Options{ReadOnly: readOnly, Synchronous: SyncNormal})
		if err != nil {
			t.Fatal(err)
		}
		if copy.meta.DataPageChecksums() {
			t.Fatal("recovery published enabled mode")
		}
		for i := range 400 {
			got, err := copy.Get([]byte("bucket"), []byte(fmt.Sprintf("key/%03d", i)))
			if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{byte(i)}, 3000)) {
				t.Fatalf("recovered key %d: %d bytes, %v", i, len(got), err)
			}
		}
		if !readOnly {
			if err := copy.SetDataPageChecksums(true); err != nil {
				t.Fatal(err)
			}
			checkDataPageFields(t, copy, true)
		}
		if err := copy.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.SetDataPageChecksums(true); err != nil {
		t.Fatal(err)
	}
	checkDataPageFields(t, db, true)
}

func TestSetDataPageChecksums_DisableCommitFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncFull, DataPageChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	injected := errors.New("mode commit failed")
	db.wal.SetSyncFileForTesting(func() error { return injected })
	if err := db.SetDataPageChecksums(false); !errors.Is(err, injected) {
		t.Fatalf("disable: %v", err)
	}
	if !db.meta.DataPageChecksums() {
		t.Fatal("failed commit changed mode")
	}
	checkDataPageFields(t, db, true)
	if err := db.SetDataPageChecksums(false); !errors.Is(err, ErrWALWriteFailed) {
		t.Fatalf("disable retry on the stopped handle: got %v, want ErrWALWriteFailed", err)
	}
	db = reopenAfterWALWriteFailure(t, db, path, &Options{Synchronous: SyncFull})
	if err := db.SetDataPageChecksums(false); err != nil {
		t.Fatal(err)
	}
}

func TestSetDataPageChecksums_FinalCommitFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncFull, CheckpointThresholdBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	injected := errors.New("final mode commit failed")
	syncs := 0
	db.wal.SetSyncFileForTesting(func() error {
		syncs++
		if syncs == 2 {
			return injected
		}
		return db.wal.FileForTesting().Sync()
	})
	if err := db.SetDataPageChecksums(true); !errors.Is(err, injected) {
		t.Fatalf("enable: %v (syncs=%d)", err, syncs)
	}
	if db.meta.DataPageChecksums() {
		t.Fatal("failed final commit changed mode")
	}
	checkDataPageFields(t, db, true)
	db = reopenAfterWALWriteFailure(t, db, path, &Options{Synchronous: SyncFull, CheckpointThresholdBytes: 1 << 30})
	if db.meta.DataPageChecksums() {
		t.Fatal("reopen after failed final commit changed mode")
	}
	if err := db.SetDataPageChecksums(true); err != nil {
		t.Fatal(err)
	}
	if !db.meta.DataPageChecksums() {
		t.Fatal("retry did not change mode")
	}
}

func TestSetDataPageChecksums_RecoveryUsesModeCommit(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, readOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("enabled=%t/readOnly=%t", enabled, readOnly), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "database")
				db, err := Open(path, 0600, &Options{Synchronous: SyncFull, DataPageChecksums: !enabled, CheckpointThresholdBytes: 1 << 30})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				createBucket(t, db, []byte("bucket"))
				if err := db.Put([]byte("bucket"), []byte("key"), []byte("before")); err != nil {
					t.Fatal(err)
				}
				checkpointChecksumTest(t, db)
				if err := db.SetDataPageChecksums(enabled); err != nil {
					t.Fatal(err)
				}
				if err := db.Put([]byte("bucket"), []byte("key"), []byte("after!")); err != nil {
					t.Fatal(err)
				}
				// Metadata is still in the WAL. The following value uses a patch.
				main, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				log, err := os.ReadFile(path + "-wal")
				if err != nil {
					t.Fatal(err)
				}
				copyPath := path + ".crash"
				main[24] ^= 1
				main[int(db.meta.PageSize())+24] ^= 1
				if err := os.WriteFile(copyPath, main, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(copyPath+"-wal", log, 0600); err != nil {
					t.Fatal(err)
				}
				copy, err := Open(copyPath, 0600, &Options{ReadOnly: readOnly, Synchronous: SyncFull})
				if err != nil {
					t.Fatal(err)
				}
				defer copy.Close()
				if copy.meta.DataPageChecksums() != enabled {
					t.Fatal("recovery chose the wrong mode")
				}
				got, err := copy.Get([]byte("bucket"), []byte("key"))
				if err != nil || string(got) != "after!" {
					t.Fatalf("recovered read: %q, %v", got, err)
				}
			})
		}
	}
}

func TestSetDataPageChecksums_ConversionMainWriteFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNormal, CheckpointThresholdBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	createBucket(t, db, []byte("bucket"))
	if err := db.Put([]byte("bucket"), []byte("key"), bytes.Repeat([]byte("v"), 64<<10)); err != nil {
		t.Fatal(err)
	}
	checkpointChecksumTest(t, db)
	original := db.file
	readOnly, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	db.file = readOnly
	err = db.SetDataPageChecksums(true)
	db.file = original
	readOnly.Close()
	if err == nil {
		t.Fatal("conversion accepted a failed main-file write")
	}
	if db.meta.DataPageChecksums() {
		t.Fatal("write failure published enabled mode")
	}
	if db.wal.Stats().CommittedRecordCount == 0 {
		t.Fatal("write failure lost redo images")
	}
	if err := db.SetDataPageChecksums(true); err != nil {
		t.Fatal(err)
	}
	checkDataPageFields(t, db, true)
	got, err := db.Get([]byte("bucket"), []byte("key"))
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte("v"), 64<<10)) {
		t.Fatalf("retry read: %d bytes, %v", len(got), err)
	}
}

func TestSetDataPageChecksums_WaitsForView(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	started, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	viewDone := make(chan error, 1)
	go func() {
		viewDone <- db.View(func(tx *Tx) error {
			close(started)
			<-release
			if tx.meta.DataPageChecksums() {
				return errors.New("mode changed inside an active view")
			}
			return nil
		})
	}()
	<-started
	modeDone := make(chan error, 1)
	go func() { modeDone <- db.SetDataPageChecksums(true) }()
	select {
	case err := <-modeDone:
		t.Fatalf("mode changed during view: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if err := <-viewDone; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-modeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("mode change did not finish")
	}
}

func TestSetDataPageChecksums_RetainedPagesSyncBeforeCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncFull, DataPageChecksums: true, CheckpointThresholdBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.SetDataPageChecksums(false); err != nil {
		t.Fatal(err)
	}
	checkpointChecksumTest(t, db)
	checkDataPageFields(t, db, true)
	if len(db.mappedFile) == 0 {
		t.Fatal("test needs the main-file mapping")
	}
	closed, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	closed.Close()
	original := db.file
	db.file = closed
	err = db.SetDataPageChecksums(true)
	db.file = original
	if err == nil {
		t.Fatal("enabled mode without syncing retained pages")
	}
	if db.meta.DataPageChecksums() {
		t.Fatal("sync failure published enabled mode")
	}
	if db.wal.Stats().CommittedRecordCount != 0 {
		t.Fatal("mode commit preceded the failed main sync")
	}
	if err := db.SetDataPageChecksums(true); err != nil {
		t.Fatal(err)
	}
}

func TestSetDataPageChecksums_DisabledWritesAndReuse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone, DataPageChecksums: true, CheckpointThresholdBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	createBucket(t, db, []byte("bucket"))
	large := bytes.Repeat([]byte("v"), 32<<10)
	if err := db.Put([]byte("bucket"), []byte("key"), large); err != nil {
		t.Fatal(err)
	}
	createBucket(t, db, []byte("anchor"))
	if err := db.Put([]byte("anchor"), []byte("key"), []byte("a")); err != nil {
		t.Fatal(err)
	}
	checkpointChecksumTest(t, db)
	if err := db.SetDataPageChecksums(false); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteBucket([]byte("bucket")); err != nil {
		t.Fatal(err)
	}
	createBucket(t, db, []byte("replacement"))
	if err := db.Put([]byte("replacement"), []byte("key"), large); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("anchor"), []byte("key"), []byte("b")); err != nil {
		t.Fatal(err)
	}
	checkpointChecksumTest(t, db)
	if db.stats.PagesReused == 0 {
		t.Fatal("fixture did not reuse pages")
	}
	checkDataPageFields(t, db, false)
	backup := path + ".backup"
	if err := db.Backup(backup); err != nil {
		t.Fatal(err)
	}
	copy, err := Open(backup, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer copy.Close()
	checkDataPageFields(t, copy, false)
	if err := db.SetDataPageChecksums(true); err != nil {
		t.Fatal(err)
	}
	checkDataPageFields(t, db, true)
}
