package kvlite

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

var cursorAllocationSink *Cursor

func prepareCursorBucket(t *testing.T, db *DB) {
	t.Helper()
	err := db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		for index := 0; index < 40; index++ {
			key := fmt.Appendf(nil, "key-%02d", index)
			value := bytes.Repeat([]byte{byte(index + 1)}, 1024)
			if err := bucket.Put(key, value); err != nil {
				return err
			}
		}
		if err := bucket.Put([]byte("empty"), nil); err != nil {
			return err
		}
		_, err = bucket.CreateBucket([]byte("nested"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCursor_Traversal(t *testing.T) {
	db, err := openDB(t.TempDir() + "/database")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	prepareCursorBucket(t, db)

	err = db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		cursor, err := bucket.Cursor()
		if err != nil {
			return err
		}
		key, _, err := cursor.First()
		if err != nil || !bytes.Equal(key, []byte("empty")) {
			t.Fatalf("first key: got %q err=%v, want empty", key, err)
		}
		for index := 0; index < 40; index++ {
			key, _, err = cursor.Next()
			want := fmt.Appendf(nil, "key-%02d", index)
			if err != nil || !bytes.Equal(key, want) {
				t.Fatalf("forward key %d: got %q err=%v, want %q", index, key, err, want)
			}
		}
		key, _, err = cursor.Next()
		if err != nil || !bytes.Equal(key, []byte("nested")) {
			t.Fatalf("final key: got %q err=%v, want nested", key, err)
		}

		key, _, err = cursor.Last()
		if err != nil || !bytes.Equal(key, []byte("nested")) {
			t.Fatalf("last key: got %q err=%v, want nested", key, err)
		}
		for index := 39; index >= 0; index-- {
			key, _, err = cursor.Prev()
			want := fmt.Appendf(nil, "key-%02d", index)
			if err != nil || !bytes.Equal(key, want) {
				t.Fatalf("reverse key %d: got %q err=%v, want %q", index, key, err, want)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCursor_Seek(t *testing.T) {
	db, err := openDB(t.TempDir() + "/database")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	prepareCursorBucket(t, db)

	err = db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		cursor, err := bucket.Cursor()
		if err != nil {
			return err
		}
		key, _, err := cursor.Seek([]byte("key-20"))
		if err != nil || !bytes.Equal(key, []byte("key-20")) {
			t.Fatalf("exact seek: got %q err=%v", key, err)
		}
		key, _, err = cursor.Seek([]byte("key-20x"))
		if err != nil || !bytes.Equal(key, []byte("key-21")) {
			t.Fatalf("lower-bound seek: got %q err=%v", key, err)
		}
		key, value, err := cursor.Seek([]byte("zzz"))
		if err != nil || key != nil || value != nil {
			t.Fatalf("seek after end: key=%q value=%q err=%v", key, value, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCursor_ValueKinds(t *testing.T) {
	db, err := openDB(t.TempDir() + "/database")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	prepareCursorBucket(t, db)

	err = db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		cursor, err := bucket.Cursor()
		if err != nil {
			return err
		}
		key, value, err := cursor.Seek([]byte("empty"))
		if err != nil || !bytes.Equal(key, []byte("empty")) || value == nil || len(value) != 0 {
			t.Fatalf("empty plain value: key=%q value=%v err=%v", key, value, err)
		}
		key, value, err = cursor.Seek([]byte("nested"))
		if err != nil || !bytes.Equal(key, []byte("nested")) || value != nil {
			t.Fatalf("nested bucket value: key=%q value=%v err=%v", key, value, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBucketCursor_AllocatesOneCursor(t *testing.T) {
	db, err := openDB(t.TempDir() + "/database")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		allocations := testing.AllocsPerRun(100, func() {
			cursor, cursorErr := bucket.Cursor()
			if cursorErr != nil {
				panic(cursorErr)
			}
			cursorAllocationSink = cursor
		})
		if allocations > 1 {
			t.Fatalf("cursor allocations: got %.0f, want at most 1", allocations)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCursor_ClosedTransaction(t *testing.T) {
	db, err := openDB(t.TempDir() + "/database")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var cursor *Cursor
	err = db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		cursor, err = bucket.Cursor()
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	movements := []struct {
		name string
		move func() ([]byte, []byte, error)
	}{
		{name: "first", move: cursor.First},
		{name: "last", move: cursor.Last},
		{name: "seek", move: func() ([]byte, []byte, error) { return cursor.Seek([]byte("key")) }},
		{name: "next", move: cursor.Next},
		{name: "prev", move: cursor.Prev},
	}
	for _, movement := range movements {
		t.Run(movement.name, func(t *testing.T) {
			key, value, err := movement.move()
			if key != nil || value != nil || !errors.Is(err, ErrTxClosed) {
				t.Fatalf("closed cursor: key=%q value=%q err=%v", key, value, err)
			}
		})
	}
}

func TestCursor_BucketChange(t *testing.T) {
	db, err := openDB(t.TempDir() + "/database")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		cursor, err := bucket.Cursor()
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte("new-key"), []byte("value")); err != nil {
			return err
		}
		key, value, err := cursor.First()
		if key != nil || value != nil || !errors.Is(err, ErrCursorInvalidated) {
			t.Fatalf("cursor after bucket change: key=%q value=%q err=%v", key, value, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCursor_RejectedWriteDoesNotInvalidate(t *testing.T) {
	db, err := openDB(t.TempDir() + "/database")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte("key"), []byte("value")); err != nil {
			return err
		}
		cursor, err := bucket.Cursor()
		if err != nil {
			return err
		}
		if err := bucket.Put(nil, []byte("rejected")); !errors.Is(err, ErrKeyRequired) {
			t.Fatalf("rejected write: got %v, want ErrKeyRequired", err)
		}
		key, value, err := cursor.First()
		if err != nil || !bytes.Equal(key, []byte("key")) || !bytes.Equal(value, []byte("value")) {
			t.Fatalf("cursor after rejected write: key=%q value=%q err=%v", key, value, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

var scanTestKeys = [][]byte{
	[]byte("acct/001"),
	[]byte("acct/002"),
	[]byte("acct/010"),
	[]byte("event/001"),
	[]byte("event/002"),
	[]byte("user/001"),
}

func prepareScanBucket(t *testing.T, db *DB) {
	t.Helper()
	err := db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		for _, key := range scanTestKeys {
			if err := bucket.Put(key, append([]byte("value:"), key...)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func collectScanKey(key, _ []byte, keys *[][]byte) error {
	*keys = append(*keys, bytes.Clone(key))
	return nil
}

func requireScanKeys(t *testing.T, got [][]byte, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("scan key count: got %d (%q), want %d (%q)", len(got), got, len(want), want)
	}
	for index := range want {
		if !bytes.Equal(got[index], []byte(want[index])) {
			t.Fatalf("scan key %d: got %q, want %q", index, got[index], want[index])
		}
	}
}

func TestBucketScan_Prefix(t *testing.T) {
	db, err := openDB(t.TempDir() + "/database")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	prepareScanBucket(t, db)

	err = db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		var keys [][]byte
		if err := bucket.ScanPrefix([]byte("acct/"), func(key, value []byte) error {
			return collectScanKey(key, value, &keys)
		}); err != nil {
			return err
		}
		requireScanKeys(t, keys, "acct/001", "acct/002", "acct/010")

		keys = nil
		if err := bucket.ScanPrefix(nil, func(key, value []byte) error {
			return collectScanKey(key, value, &keys)
		}); err != nil {
			return err
		}
		requireScanKeys(t, keys, "acct/001", "acct/002", "acct/010", "event/001", "event/002", "user/001")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBucketScan_Range(t *testing.T) {
	db, err := openDB(t.TempDir() + "/database")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	prepareScanBucket(t, db)

	err = db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		var keys [][]byte
		if err := bucket.ScanRange([]byte("acct/002"), []byte("event/002"), func(key, value []byte) error {
			return collectScanKey(key, value, &keys)
		}); err != nil {
			return err
		}
		requireScanKeys(t, keys, "acct/002", "acct/010", "event/001")

		keys = nil
		if err := bucket.ScanRange(nil, []byte("event/001"), func(key, value []byte) error {
			return collectScanKey(key, value, &keys)
		}); err != nil {
			return err
		}
		requireScanKeys(t, keys, "acct/001", "acct/002", "acct/010")

		keys = nil
		if err := bucket.ScanRange([]byte("event/001"), nil, func(key, value []byte) error {
			return collectScanKey(key, value, &keys)
		}); err != nil {
			return err
		}
		requireScanKeys(t, keys, "event/001", "event/002", "user/001")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBucketScan_Callback(t *testing.T) {
	db, err := openDB(t.TempDir() + "/database")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	prepareScanBucket(t, db)

	errStop := errors.New("stop scan")
	err = db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		if err := bucket.ScanPrefix([]byte("acct/"), nil); !errors.Is(err, ErrScanCallbackRequired) {
			t.Fatalf("nil callback: got %v, want ErrScanCallbackRequired", err)
		}
		calls := 0
		err = bucket.ScanPrefix([]byte("acct/"), func(_, _ []byte) error {
			calls++
			if calls == 2 {
				return errStop
			}
			return nil
		})
		if !errors.Is(err, errStop) || calls != 2 {
			t.Fatalf("callback stop: calls=%d err=%v", calls, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBucketScan_ClosedTransactionPrecedesBounds(t *testing.T) {
	db, err := openDB(t.TempDir() + "/database")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var bucket *Bucket
	err = db.View(func(tx *Tx) error {
		bucket, err = tx.Bucket(testBucketName)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = bucket.ScanRange([]byte("z"), []byte("a"), func(_, _ []byte) error { return nil })
	if !errors.Is(err, ErrTxClosed) {
		t.Fatalf("closed invalid range: got %v, want ErrTxClosed", err)
	}
}

func TestCursor_TraversalPersistsAfterReopen(t *testing.T) {
	path := t.TempDir() + "/database"
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	prepareScanBucket(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		cursor, err := bucket.Cursor()
		if err != nil {
			return err
		}
		var keys [][]byte
		key, _, err := cursor.First()
		for key != nil && err == nil {
			keys = append(keys, bytes.Clone(key))
			key, _, err = cursor.Next()
		}
		if err != nil {
			return err
		}
		requireScanKeys(t, keys, "acct/001", "acct/002", "acct/010", "event/001", "event/002", "user/001")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
