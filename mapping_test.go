package kvlite

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpoint_ReturnsFreeTailPagesToOS(t *testing.T) {
	for _, test := range []struct {
		name string
		mode Sync
	}{{"SyncNone", SyncNone}, {"SyncFull", SyncFull}} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			db, err := Open(path, 0600, &Options{Synchronous: test.mode})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if db != nil && !db.closed {
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
				for index := range 256 {
					if err := bucket.Put([]byte(fmt.Sprintf("key-%04d", index)), make([]byte, 512)); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := db.checkpointWAL(); err != nil {
				t.Fatal(err)
			}
			before, err := db.file.Stat()
			if err != nil {
				t.Fatal(err)
			}
			lastPageBefore := db.meta.LastPage()

			if err := db.Update(func(tx *Tx) error {
				bucket, err := tx.Bucket([]byte("data"))
				if err != nil {
					return err
				}
				for index := range 256 {
					if err := bucket.Delete([]byte(fmt.Sprintf("key-%04d", index))); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if db.meta.LastPage() >= lastPageBefore {
				t.Fatalf("last page did not decrease: before=%d after=%d", lastPageBefore, db.meta.LastPage())
			}
			var retainedWAL []byte
			if test.mode == SyncFull {
				retainedWAL, err = os.ReadFile(path + "-wal")
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := db.checkpointWAL(); err != nil {
				t.Fatal(err)
			}
			after, err := db.file.Stat()
			if err != nil {
				t.Fatal(err)
			}
			wantSize := int64(db.meta.LastPage()+1) * db.meta.PageSize()
			if after.Size() >= before.Size() || after.Size() != wantSize {
				t.Fatalf("file size: before=%d after=%d want=%d", before.Size(), after.Size(), wantSize)
			}
			if got := int64(len(db.mappedFile)); got != wantSize {
				t.Fatalf("mapped size: got %d, want %d", got, wantSize)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if retainedWAL != nil {
				// Model a stop after the main file sync but before WAL cleanup.
				if err := os.WriteFile(path+"-wal", retainedWAL, 0600); err != nil {
					t.Fatal(err)
				}
			}
			db, err = Open(path, 0600, &Options{Synchronous: test.mode})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Get([]byte("data"), []byte("key-0000")); !errors.Is(err, ErrKeyNotFound) {
				t.Fatalf("deleted key after reopen: got %v, want %v", err, ErrKeyNotFound)
			}
		})
	}
}

func TestOpen_MapsTheCompleteMainFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("bucket"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte("key"), []byte("value"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(db.mappedFile), int(info.Size()); got != want {
		t.Fatalf("mapped length: got %d, want %d", got, want)
	}
}

func TestCheckpoint_ReplacesTheMappingAfterTheMainFileGrows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{
		Synchronous:              SyncNone,
		CheckpointThresholdBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	before := len(db.mappedFile)
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("bucket"))
		if err != nil {
			return err
		}
		for index := 0; index < 128; index++ {
			if err := bucket.Put([]byte{byte(index + 1)}, make([]byte, 256)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	info, err := db.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if len(db.mappedFile) <= before {
		t.Fatalf("mapped length did not grow: before=%d after=%d", before, len(db.mappedFile))
	}
	if got, want := len(db.mappedFile), int(info.Size()); got != want {
		t.Fatalf("mapped length after checkpoint: got %d, want %d", got, want)
	}
	if value, err := db.Get([]byte("bucket"), []byte{128}); err != nil || len(value) != 256 {
		t.Fatalf("Get after remap: value length=%d err=%v", len(value), err)
	}
}

func TestClose_UnmapsTheMainFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{
		Synchronous:              SyncNone,
		CheckpointThresholdBytes: math.MaxUint64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(db.mappedFile) == 0 {
		t.Fatal("Open did not map the main file")
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if db.mappedFile != nil {
		t.Fatalf("mapped bytes remain after Close: %d", len(db.mappedFile))
	}
}

func TestCheckpoint_PreservesChangedAndUnchangedMappedEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{
		Synchronous:              SyncNone,
		CheckpointThresholdBytes: math.MaxUint64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("bucket"))
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte("changed"), []byte("old")); err != nil {
			return err
		}
		return bucket.Put([]byte("unchanged"), []byte("stable"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, 0600, &Options{
		Synchronous:              SyncNone,
		CheckpointThresholdBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put([]byte("bucket"), []byte("changed"), []byte("new")); err != nil {
		t.Fatal(err)
	}

	for key, want := range map[string]string{"changed": "new", "unchanged": "stable"} {
		value, err := db.Get([]byte("bucket"), []byte(key))
		if err != nil {
			t.Fatalf("Get %q: %v", key, err)
		}
		if string(value) != want {
			t.Fatalf("Get %q: got %q, want %q", key, value, want)
		}
	}
}
