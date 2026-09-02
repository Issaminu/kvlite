package kvlite

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestOpen_MapsTheCompleteMainFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
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
		Synchronous:              SyncNormal,
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
		Synchronous:              SyncNormal,
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
		Synchronous:              SyncNormal,
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
		Synchronous:              SyncNormal,
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
