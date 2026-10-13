package kvlite

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

var cursorAllocationSink *Cursor

// BenchmarkCursorReadSetup includes one View, bucket lookup, and cursor per read.
// Setup and a complete traversal warm the database before measurement.
func BenchmarkCursorReadSetup(b *testing.B) {
	for _, storage := range []string{"file", "memory"} {
		b.Run(storage, func(b *testing.B) {
			path := b.TempDir() + "/database"
			var db *DB
			if storage == "memory" {
				db = OpenMemory()
			} else {
				var err error
				db, err = Open(path, 0600, &Options{Synchronous: SyncNone})
				if err != nil {
					b.Fatal(err)
				}
			}
			keys := make([][]byte, 1_000)
			value := bytes.Repeat([]byte("v"), 128)
			if err := db.Update(func(tx *Tx) error {
				bucket, err := tx.CreateBucket(testBucketName)
				if err != nil {
					return err
				}
				for i := range keys {
					keys[i] = fmt.Appendf(nil, "key-%08d", i)
					if err := bucket.Put(keys[i], value); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			if storage == "file" {
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
				var err error
				db, err = Open(path, 0600, &Options{Synchronous: SyncNone})
				if err != nil {
					b.Fatal(err)
				}
			}
			b.Cleanup(func() { db.Close() })
			read := func(tx *Tx, start, count int) error {
				bucket, err := tx.Bucket(testBucketName)
				if err != nil {
					return err
				}
				cursor, err := bucket.Cursor()
				if err != nil {
					return err
				}
				key, got, err := cursor.Seek(keys[start])
				for i := range count {
					if err != nil {
						return err
					}
					if !bytes.Equal(key, keys[start+i]) || !bytes.Equal(got, value) {
						return fmt.Errorf("cursor returned the wrong entry at %d", start+i)
					}
					if i+1 < count {
						key, got, err = cursor.Next()
					}
				}
				return nil
			}
			if err := db.View(func(tx *Tx) error { return read(tx, 0, len(keys)) }); err != nil {
				b.Fatal(err)
			}
			b.Run("empty-view", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if err := db.View(func(*Tx) error { return nil }); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("point-get", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					got, err := db.Get(testBucketName, keys[0])
					if err != nil || !bytes.Equal(got, value) {
						b.Fatalf("point read: %q, %v", got, err)
					}
				}
			})
			b.Run("empty-update", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if err := db.Update(func(*Tx) error { return nil }); err != nil {
						b.Fatal(err)
					}
				}
			})
			for _, count := range []int{1, 10, 100} {
				b.Run(fmt.Sprintf("entries=%d", count), func(b *testing.B) {
					iteration := 0
					b.ReportAllocs()
					for b.Loop() {
						start := iteration % (len(keys) - count + 1)
						if err := db.View(func(tx *Tx) error { return read(tx, start, count) }); err != nil {
							b.Fatal(err)
						}
						iteration++
					}
				})
			}
		})
	}
}

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

		keys = nil
		if err := bucket.ScanPrefix([]byte("acct/"), func(key, value []byte) error {
			return collectScanKey(key, value, &keys)
		}); err != nil {
			return err
		}
		requireScanKeys(t, keys, "acct/001", "acct/002", "acct/010")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBucketScan_FullPrefixPersistsAfterReopen(t *testing.T) {
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
		if _, err := bucket.Cursor(); err != nil {
			return err
		}
		var keys [][]byte
		if err := bucket.ScanPrefix(nil, func(key, value []byte) error {
			return collectScanKey(key, value, &keys)
		}); err != nil {
			return err
		}
		requireScanKeys(t, keys, "acct/001", "acct/002", "acct/010", "event/001", "event/002", "user/001")

		keys = nil
		if err := bucket.ScanRange([]byte("acct/002"), []byte("event/002"), func(key, value []byte) error {
			return collectScanKey(key, value, &keys)
		}); err != nil {
			return err
		}
		requireScanKeys(t, keys, "acct/002", "acct/010", "event/001")

		errStop := errors.New("stop mapped scan")
		calls := 0
		err = bucket.ScanPrefix(nil, func(_, _ []byte) error {
			calls++
			if calls == 2 {
				return errStop
			}
			return nil
		})
		if !errors.Is(err, errStop) || calls != 2 {
			t.Fatalf("mapped callback stop: calls=%d err=%v", calls, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBucketScanReverse(t *testing.T) {
	for _, mapped := range []bool{false, true} {
		name := "wal"
		if mapped {
			name = "mapped"
		}
		t.Run(name, func(t *testing.T) {
			path := t.TempDir() + "/database"
			db, err := openDB(path)
			if err != nil {
				t.Fatal(err)
			}
			prepareCursorBucket(t, db)
			if mapped {
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db, err = openDB(path)
				if err != nil {
					t.Fatal(err)
				}
			}
			defer db.Close()

			errStop := errors.New("stop scan")
			err = db.View(func(tx *Tx) error {
				bucket, err := tx.Bucket(testBucketName)
				if err != nil {
					return err
				}
				var keys [][]byte
				if err := bucket.ScanReverse(func(key, value []byte) error {
					if string(key) == "nested" && value != nil {
						t.Fatal("nested bucket has a value")
					}
					if string(key) == "empty" && value == nil {
						t.Fatal("empty value is nil")
					}
					return collectScanKey(key, value, &keys)
				}); err != nil {
					return err
				}
				want := []string{"nested"}
				for index := 39; index >= 0; index-- {
					want = append(want, fmt.Sprintf("key-%02d", index))
				}
				want = append(want, "empty")
				requireScanKeys(t, keys, want...)
				if err := bucket.ScanReverse(nil); !errors.Is(err, ErrScanCallbackRequired) {
					t.Fatalf("nil callback: got %v, want ErrScanCallbackRequired", err)
				}
				calls := 0
				err = bucket.ScanReverse(func(_, _ []byte) error {
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
		})
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

func TestPrefixRangeEnd(t *testing.T) {
	tests := []struct {
		name   string
		prefix []byte
		want   []byte
	}{
		{name: "empty"},
		{name: "text", prefix: []byte("acct/"), want: []byte("acct0")},
		{name: "carry", prefix: []byte{0x01, 0xff}, want: []byte{0x02}},
		{name: "no upper bound", prefix: []byte{0xff}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := prefixRangeEnd(test.prefix); !bytes.Equal(got, test.want) {
				t.Fatalf("prefixRangeEnd(%x) = %x, want %x", test.prefix, got, test.want)
			}
		})
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

// TestCursor_EmptyLeaves checks complete reads through a stored tree with gaps.
// It keeps empty leaves in the fixture so a merge cannot remove the condition.
func TestCursor_EmptyLeaves(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty=%t", empty), func(t *testing.T) {
			path := t.TempDir() + "/database"
			db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { db.Close() }()
			name := []byte("gaps")
			if err := db.Update(func(tx *Tx) error {
				bucket, err := tx.CreateBucket(name)
				if err != nil {
					return err
				}
				// Eight leaves form a complete tree. Only leaves b and f have data.
				// The other leaves include empty subtrees and empty boundary leaves.
				var build func(int, int, page.ID) (*btree.Node, error)
				build = func(first, end int, id page.ID) (*btree.Node, error) {
					var node *btree.Node
					if end-first == 1 {
						node = btree.NewLeafNode(id)
						if !empty && (first == 1 || first == 5) {
							key := []byte{byte('a' + first)}
							if err := node.InsertEntry(btree.NewEntry(0, key, append([]byte("value-"), key...))); err != nil {
								return nil, err
							}
						}
					} else {
						middle := (first + end) / 2
						children := make([]*btree.Node, 2)
						for i, bounds := range [][2]int{{first, middle}, {middle, end}} {
							childID, err := tx.store.AllocatePage()
							if err != nil {
								return nil, err
							}
							children[i], err = build(bounds[0], bounds[1], childID)
							if err != nil {
								return nil, err
							}
						}
						node = btree.NewRootNode(id, children[0], children[1], []byte{byte('a' + middle)})
					}
					tx.store.StageNode(node)
					return node, nil
				}
				root, err := build(0, 8, bucket.rootNode.PageID())
				if err != nil {
					return err
				}
				bucket.setRoot(root)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			check := func(tx *Tx) error {
				bucket, err := tx.Bucket(name)
				if err != nil {
					return err
				}
				cursor, err := bucket.Cursor()
				if err != nil {
					return err
				}
				for _, reverse := range []bool{false, true} {
					want := []string{"b", "f"}
					first, next := cursor.First, cursor.Next
					if reverse {
						want = []string{"f", "b"}
						first, next = cursor.Last, cursor.Prev
					}
					if empty {
						want = nil
					}
					key, value, err := first()
					for _, expected := range want {
						if err != nil || string(key) != expected || string(value) != "value-"+expected {
							t.Fatalf("reverse=%t: got %q=%q err=%v, want %s", reverse, key, value, err, expected)
						}
						key, value, err = next()
					}
					if err != nil || key != nil || value != nil {
						t.Fatalf("reverse=%t: end key=%q value=%q err=%v", reverse, key, value, err)
					}
					count := 0
					visit := func(key, value []byte) error {
						if count >= len(want) || string(key) != want[count] || string(value) != "value-"+want[count] {
							t.Fatalf("reverse=%t: unexpected scan key=%q value=%q", reverse, key, value)
						}
						count++
						return nil
					}
					if reverse {
						err = bucket.ScanReverse(visit)
					} else {
						err = bucket.ScanRange(nil, nil, visit)
					}
					if err != nil || count != len(want) {
						t.Fatalf("reverse=%t: scan count=%d err=%v, want %d", reverse, count, err, len(want))
					}
				}
				for _, test := range []struct{ target, want string }{{"a", "b"}, {"c", "f"}, {"g", ""}} {
					want := test.want
					if empty {
						want = ""
					}
					key, _, err := cursor.Seek([]byte(test.target))
					if err != nil || string(key) != want || (want == "" && key != nil) {
						t.Fatalf("seek %s: key=%q err=%v, want %s", test.target, key, err, want)
					}
				}
				return nil
			}
			for phase := range 2 {
				if err := db.View(check); err != nil {
					t.Fatal(err)
				}
				if err := db.Update(check); err != nil {
					t.Fatal(err)
				}
				if phase == 0 {
					if err := db.Close(); err != nil {
						t.Fatal(err)
					}
					db, err = Open(path, 0600, &Options{Synchronous: SyncNone})
					if err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

func TestBucketScans_ContinuedAfterReopen(t *testing.T) {
	for _, checksums := range []bool{false, true} {
		for _, largeKeys := range []bool{false, true} {
			t.Run(fmt.Sprintf("checksums=%t/largeKeys=%t", checksums, largeKeys), func(t *testing.T) {
				path := t.TempDir() + "/database"
				db, err := Open(path, 0600, &Options{Synchronous: SyncNone, DataPageChecksums: checksums, CheckpointThresholdBytes: 1 << 30})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { db.Close() }()
				keys := [][]byte{[]byte("a"), []byte("m"), []byte("z")}
				values := [][]byte{[]byte("first"), bytes.Repeat([]byte("v"), 64<<10), []byte("last")}
				if largeKeys {
					for i := range keys {
						keys[i] = bytes.Repeat(keys[i], MaxKeySize)
						values[i] = []byte("value")
					}
				}
				if err := db.Update(func(tx *Tx) error {
					bucket, err := tx.CreateBucket(testBucketName)
					if err != nil {
						return err
					}
					for i := range keys {
						if err := bucket.Put(keys[i], values[i]); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					db.Close()
					t.Fatal(err)
				}
				check := func(tx *Tx) error {
					bucket, err := tx.Bucket(testBucketName)
					if err != nil {
						return err
					}
					index := 0
					err = bucket.ScanRange(nil, nil, func(key, value []byte) error {
						if index >= len(keys) || !bytes.Equal(key, keys[index]) || !bytes.Equal(value, values[index]) {
							return fmt.Errorf("wrong scan entry %d", index)
						}
						index++
						return nil
					})
					if err != nil {
						return err
					}
					if index != len(keys) {
						return fmt.Errorf("scan returned %d entries, want %d", index, len(keys))
					}
					index = len(keys) - 1
					err = bucket.ScanReverse(func(key, value []byte) error {
						if index < 0 || !bytes.Equal(key, keys[index]) || !bytes.Equal(value, values[index]) {
							return fmt.Errorf("wrong reverse scan entry %d", index)
						}
						index--
						return nil
					})
					if err != nil {
						return err
					}
					if index != -1 {
						return fmt.Errorf("reverse scan stopped at entry %d", index)
					}
					index = 1
					if err := bucket.ScanRange(keys[1], keys[2], func(key, value []byte) error {
						if index != 1 || !bytes.Equal(key, keys[1]) || !bytes.Equal(value, values[1]) {
							return errors.New("wrong bounded scan entry")
						}
						index++
						return nil
					}); err != nil {
						return err
					}
					if index != 2 {
						return errors.New("bounded scan omitted its entry")
					}
					count := 0
					if err := bucket.ScanPrefix([]byte("m"), func(key, value []byte) error {
						if !bytes.Equal(key, keys[1]) || !bytes.Equal(value, values[1]) {
							return errors.New("wrong prefix scan entry")
						}
						count++
						return nil
					}); err != nil {
						return err
					}
					if count != 1 {
						return fmt.Errorf("prefix scan returned %d entries", count)
					}
					stop := errors.New("stop continued scan")
					for _, reverse := range []bool{false, true} {
						count = 0
						visit := func(_, _ []byte) error { count++; return stop }
						if reverse {
							err = bucket.ScanReverse(visit)
						} else {
							err = bucket.ScanRange(keys[1], nil, visit)
						}
						if !errors.Is(err, stop) || count != 1 {
							return fmt.Errorf("stop scan: count=%d, err=%v", count, err)
						}
					}
					return nil
				}
				for phase := range 2 {
					if err := db.View(check); err != nil {
						t.Fatalf("phase %d: %v", phase, err)
					}
					if phase == 0 {
						if err := db.Close(); err != nil {
							t.Fatal(err)
						}
						db, err = Open(path, 0600, &Options{ReadOnly: true})
						if err != nil {
							t.Fatal(err)
						}
					}
				}
			})
		}
	}
}
