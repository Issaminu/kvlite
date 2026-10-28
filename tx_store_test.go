package kvlite

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func largeDeleteFixture(t *testing.T, mode Sync, checksums bool) (*DB, string, []KeyValue) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "large-delete.db")
	options := &Options{Synchronous: mode, DataPageChecksums: checksums}
	db, err := Open(path, 0600, options)
	if err != nil {
		t.Fatal(err)
	}
	createBucket(t, db, testBucketName)
	entries := make([]KeyValue, 192)
	value := bytes.Repeat([]byte("v"), 64<<10)
	for i := range entries {
		entries[i] = KeyValue{Key: fmt.Appendf(nil, "key%04d", i), Value: value}
	}
	if err := db.PutBatch(testBucketName, entries); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := mustBucket(t, tx, testBucketName).CreateBucket([]byte("zzz"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, 0600, options)
	if err != nil {
		t.Fatal(err)
	}
	return db, path, entries
}

func TestLargeLeafDeleteRollbackRetirementAndRecovery(t *testing.T) {
	for _, mode := range []Sync{SyncNone, SyncNormal, SyncFull} {
		for _, checksums := range []bool{false, true} {
			t.Run(fmt.Sprintf("mode=%d/checksums=%t", mode, checksums), func(t *testing.T) {
				db, path, entries := largeDeleteFixture(t, mode, checksums)
				defer func() { db.Close() }()
				rollback := errors.New("rollback large deletes")
				keys := [][]byte{entries[191].Key, entries[0].Key, []byte("key0048x"), entries[48].Key, entries[48].Key, entries[97].Key}
				if err := db.Update(func(tx *Tx) error {
					bucket := mustBucket(t, tx, testBucketName)
					if err := bucket.DeleteBatch(keys); err != nil {
						return err
					}
					if _, err := bucket.Get(entries[0].Key); !errors.Is(err, ErrKeyNotFound) {
						return fmt.Errorf("deleted key visible: %v", err)
					}
					if err := bucket.Put(entries[0].Key, []byte("new")); err != nil {
						return err
					}
					return rollback
				}); !errors.Is(err, rollback) {
					t.Fatalf("rollback: %v", err)
				}
				for _, i := range []int{0, 48, 97, 191} {
					if got, err := db.Get(testBucketName, entries[i].Key); err != nil || !bytes.Equal(got, entries[i].Value) {
						t.Fatalf("rollback key%d: %v", i, err)
					}
				}
				if err := db.DeleteBatch(testBucketName, [][]byte{entries[0].Key, []byte("zzz")}); !errors.Is(err, ErrIncompatibleValue) {
					t.Fatalf("bucket conflict after deletion: %v", err)
				}
				if got, err := db.Get(testBucketName, entries[0].Key); err != nil || !bytes.Equal(got, entries[0].Value) {
					t.Fatalf("failed batch changed earlier leaf: %v", err)
				}
				for _, i := range []int{0, 96, 191} {
					if err := db.Delete(testBucketName, entries[i].Key); err != nil {
						t.Fatal(err)
					}
				}
				all := make([][]byte, 0, len(entries)+2)
				for i := len(entries) - 1; i >= 0; i-- {
					all = append(all, entries[i].Key)
				}
				all = append(all, entries[48].Key, []byte("missing"))
				if err := db.Update(func(tx *Tx) error {
					bucket := mustBucket(t, tx, testBucketName)
					if err := bucket.DeleteBatch(all); err != nil {
						return err
					}
					if err := bucket.DeleteBucket([]byte("zzz")); err != nil {
						return err
					}
					return bucket.Put([]byte("fresh"), entries[0].Value)
				}); err != nil {
					t.Fatal(err)
				}
				if err := db.validatePageOwnership(); err != nil {
					t.Fatal(err)
				}
				// Copy the acknowledged files before Close checkpoints the WAL.
				recovery := filepath.Join(t.TempDir(), "recovered.db")
				for _, suffix := range []string{"", "-wal"} {
					data, err := os.ReadFile(path + suffix)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(recovery+suffix, data, 0600); err != nil {
						t.Fatal(err)
					}
				}
				recovered, err := Open(recovery, 0600, &Options{Synchronous: SyncFull})
				if err != nil {
					t.Fatal(err)
				}
				defer recovered.Close()
				if got, err := recovered.Get(testBucketName, []byte("fresh")); err != nil || !bytes.Equal(got, entries[0].Value) {
					t.Fatalf("recovered replacement: %v", err)
				}
				for _, entry := range entries {
					if _, err := recovered.Get(testBucketName, entry.Key); !errors.Is(err, ErrKeyNotFound) {
						t.Fatalf("recovered deleted key%s: %v", entry.Key, err)
					}
				}
				if err := recovered.validatePageOwnership(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestLargeLeafDeleteWALFailure(t *testing.T) {
	for _, mode := range []Sync{SyncNormal, SyncFull} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			db, path, entries := largeDeleteFixture(t, mode, true)
			injected := errors.New("large-delete WAL failure")
			var restore func()
			if mode == SyncFull {
				db.wal.SetSyncFileForTesting(func() error { return injected })
				restore = func() { db.wal.SetSyncFileForTesting(nil) }
			} else {
				writable := db.wal.FileForTesting()
				readOnly, err := os.Open(path + "-wal")
				if err != nil {
					t.Fatal(err)
				}
				db.wal.ReplaceFileForTesting(readOnly)
				restore = func() { db.wal.ReplaceFileForTesting(writable); readOnly.Close() }
			}
			err := db.DeleteBatch(testBucketName, [][]byte{entries[0].Key, entries[48].Key, entries[191].Key})
			restore()
			if !errors.Is(err, ErrWALWriteFailed) {
				t.Fatalf("failed delete commit: %v", err)
			}
			if _, err := db.Get(testBucketName, entries[96].Key); !errors.Is(err, ErrWALWriteFailed) {
				t.Fatalf("failed handle remained usable: %v", err)
			}
			_ = db.Close()
			db, err = Open(path, 0600, &Options{Synchronous: mode})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			// A failed commit can be recovered; an unrelated value must survive either outcome.
			if got, err := db.Get(testBucketName, entries[96].Key); err != nil || !bytes.Equal(got, entries[96].Value) {
				t.Fatalf("unrelated value after failed commit: %v", err)
			}
			if err := db.validatePageOwnership(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
