package kvlite

// The tests in this file are adapted from etcd-io/bbolt
// (https://github.com/etcd-io/bbolt), MIT License, Copyright (c) 2013 Ben Johnson.
// kvlite targets a bbolt-compatible API so bbolt's own suite can serve as a
// conformance oracle.
//
// This file is the Phase 1 spec: "DB open + meta/page foundation".
//   - Live tests below define behavior you can drive now. A couple are RED until the
//     meta page exists (that's the point — they pull the meta/page work into being).
//   - Skipped placeholders at the bottom capture the rest of the open contract; un-skip
//     and flesh each out as its prerequisite component lands (see each TODO).

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// tempfile returns a temporary file path for a database.
func tempfile() string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("kvlite-%d.db", time.Now().UnixNano()))
}

// -----------------------------------------------------------------------------
// Open: creation & basic lifecycle  (should be GREEN)
// -----------------------------------------------------------------------------

// TestOpen ensures that a database can be created and opened without error.
func TestOpen(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	} else if db == nil {
		t.Fatal("expected db")
	}

	if s := db.Path(); s != path {
		t.Fatalf("unexpected path: %s", s)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestOpen_ErrPathRequired ensures that opening with a blank path returns an error.
func TestOpen_ErrPathRequired(t *testing.T) {
	_, err := Open("", 0600, nil)
	if err == nil {
		t.Fatalf("expected error")
	}
}

// TestOpen_ErrNotExists ensures that opening a database in a directory that does not
// exist returns an error.
func TestOpen_ErrNotExists(t *testing.T) {
	_, err := Open(filepath.Join(tempfile(), "bad-path"), 0600, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

// TestOpen_Reopen ensures a created database can be closed and reopened as a valid DB.
// (Trivially green today; becomes a real guard once Open initializes meta on create and
// validates it on reopen — see TestOpen_ErrInvalid.)
func TestOpen_Reopen(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, 0600, nil)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	if s := db.Path(); s != path {
		t.Fatalf("unexpected path after reopen: %s", s)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestDB_Path ensures Path returns the database's file path.
func TestDB_Path(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if s := db.Path(); s != path {
		t.Fatalf("unexpected path: %s", s)
	}
}

// -----------------------------------------------------------------------------
// Open: rejecting non-kvlite / corrupt files  (RED until meta validation lands —
// these are the tests that force you to build & validate the meta page)
// -----------------------------------------------------------------------------

// TestOpen_ErrInvalid ensures that opening a file that is not a kvlite database returns
// ErrInvalid. (Forces Open to validate a meta page — and therefore to initialize a valid
// one when creating a fresh file.)
func TestOpen_ErrInvalid(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintln(f, "this is not a kvlite database"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path, 0600, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected ErrInvalid, got: %v", err)
	}
}

// TestOpen_FileTooSmall ensures that opening a file too small to hold the meta pages
// returns an error.
func TestOpen_FileTooSmall(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	// Fewer bytes than a single page/meta could ever occupy.
	if err := os.WriteFile(path, make([]byte, 16), 0600); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path, 0600, nil); err == nil {
		t.Fatal("expected error opening a too-small file")
	}
}

// -----------------------------------------------------------------------------
// Phase 1 spec, deferred: un-skip & implement as each prerequisite lands.
// Ports of the same-named tests in bbolt's db_test.go / db_whitebox_test.go.
// -----------------------------------------------------------------------------

// TestOpen_ErrVersionMismatch: a DB whose meta pages carry a different format version
// must fail to open with ErrVersionMismatch.
// TODO(meta.go): create a valid DB, close it, flip the `version` field in BOTH meta
// pages on disk, reopen, and assert errors.Is(err, ErrVersionMismatch).
func TestOpen_ErrVersionMismatch(t *testing.T) {
	t.Skip("pending meta-page layout (meta.go)")
}

// TestOpen_ErrChecksum: a DB whose meta pages have a corrupted checksum must fail to open
// with ErrChecksum.
// TODO(meta.go): create a valid DB, corrupt a meta field (e.g. root pgid) in BOTH meta
// pages so the stored checksum no longer matches, reopen, assert errors.Is(err, ErrChecksum).
func TestOpen_ErrChecksum(t *testing.T) {
	t.Skip("pending meta-page checksum (meta.go)")
}

// TestOpen_ReadPageSize_FromMeta1: if meta page 0 is corrupt, the page size and the DB
// must still be recoverable from meta page 1.
// TODO(meta.go): needs dual meta pages + page-size-from-meta detection.
func TestOpen_ReadPageSize_FromMeta1(t *testing.T) {
	t.Skip("pending dual meta pages + page-size detection")
}

// TestOpen_Size: a freshly created DB lays out a fixed initial set of pages (bbolt: 2
// meta + 1 freelist + 1 leaf root = 4 pages), and a reopen + small write must not balloon
// the file.
// TODO(page/meta/tx): needs fixed page size, initial layout, and Update/Put (Phase 3).
func TestOpen_Size(t *testing.T) {
	t.Skip("pending page size + initial layout + writes")
}

// TestOpen_Check: a freshly created and a reopened DB both pass an integrity check.
// TODO(tx_check): needs tx.Check(); belongs to the hardening surface.
func TestOpen_Check(t *testing.T) {
	t.Skip("pending tx.Check() integrity checker")
}

// TestDB_Open_ReadOnly: Options{ReadOnly:true} allows reads, rejects writes
// (ErrDatabaseReadOnly), and permits concurrent read-only openers (shared lock).
// TODO(lock/tx): needs O_RDONLY (no create), a shared file lock, and transactions.
func TestDB_Open_ReadOnly(t *testing.T) {
	t.Skip("pending file locking + read-only tx semantics")
}

// TestDB_Open_ReadOnly_NoCreate: opening a non-existent path read-only must error and
// must never create the file.
// TODO(open): read-only open must not pass O_CREATE.
func TestDB_Open_ReadOnly_NoCreate(t *testing.T) {
	t.Skip("pending read-only open semantics")
}

// TestOpen_MultipleGoroutines: many goroutines opening/closing the same DB concurrently
// must be safe (exclusive file lock).
// TODO(lock): needs an exclusive flock held across Open/Close.
func TestOpen_MultipleGoroutines(t *testing.T) {
	t.Skip("pending file locking")
}

// TestOpen_MetaInitWriteError: write errors while initializing the meta pages must
// surface from Open. (bbolt itself leaves this pending.)
func TestOpen_MetaInitWriteError(t *testing.T) {
	t.Skip("pending (fault injection)")
}
