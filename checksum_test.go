package kvlite

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

func TestDataPageChecksums_DefaultOff(t *testing.T) {
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
		db.Close()
		t.Fatal(err)
	}
	pageSize := int(db.meta.PageSize())
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for at := 3 * pageSize; at < len(data); at += pageSize {
		if sum := binary.LittleEndian.Uint32(data[at+14 : at+18]); sum != 0 {
			t.Fatalf("data page %d has checksum %x, want zero", at/pageSize, sum)
		}
	}
}

func TestDataPageChecksums_ReopenAndBackup(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%t", enabled), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			db, err := Open(path, 0600, &Options{Synchronous: SyncNone, DataPageChecksums: enabled, CheckpointThresholdBytes: 1 << 30})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			createBucket(t, db, []byte("bucket"))
			key := bytes.Repeat([]byte("k"), MaxKeySize)
			value := bytes.Repeat([]byte("v"), 64<<10)
			if err := db.Put([]byte("bucket"), key, value); err != nil {
				t.Fatal(err)
			}
			backup := path + ".backup"
			if err := db.Backup(backup); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{path, backup} {
				for _, readOnly := range []bool{false, true} {
					reopened, err := Open(name, 0600, &Options{ReadOnly: readOnly, Synchronous: SyncNone, DataPageChecksums: !enabled})
					if err != nil {
						t.Fatal(err)
					}
					if reopened.meta.DataPageChecksums() != enabled {
						t.Fatal("reopen changed the stored checksum mode")
					}
					got, err := reopened.Get([]byte("bucket"), key)
					if err != nil || !bytes.Equal(got, value) {
						t.Fatalf("reopened read: got %d bytes, %v", len(got), err)
					}
					if !readOnly {
						if err := reopened.Put([]byte("bucket"), []byte("small"), []byte("value")); err != nil {
							t.Fatal(err)
						}
					}
					if err := reopened.Close(); err != nil {
						t.Fatal(err)
					}
				}
				data, err := os.ReadFile(name)
				if err != nil {
					t.Fatal(err)
				}
				pageSize := int(binary.LittleEndian.Uint64(data[8:16]))
				for at := 3 * pageSize; at < len(data); at += pageSize {
					if kind := binary.LittleEndian.Uint16(data[at : at+2]); kind == 0 {
						continue
					}
					if err := btree.CheckNodeChecksumMode(data[at:at+pageSize], enabled); err != nil {
						t.Fatalf("page %d mode: %v", at/pageSize, err)
					}
				}
			}
		})
	}
}

func TestDataPageChecksums_ReadCorruption(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, continued := range []bool{false, true} {
			t.Run(fmt.Sprintf("enabled=%t/continued=%t", enabled, continued), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "database")
				db, err := Open(path, 0600, &Options{Synchronous: SyncNone, DataPageChecksums: enabled})
				if err != nil {
					t.Fatal(err)
				}
				createBucket(t, db, []byte("bucket"))
				value := []byte("value-with-checksum-test-marker")
				if continued {
					value = bytes.Repeat(value, 600)
				}
				if err := db.Put([]byte("bucket"), []byte("key"), value); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				at := bytes.Index(data, []byte("value-with-checksum-test-marker"))
				if continued {
					pageSize := int(binary.LittleEndian.Uint64(data[8:16]))
					// Change a value byte on a later continuation page.
					for offset := 3 * pageSize; offset < len(data); offset += pageSize {
						if binary.LittleEndian.Uint16(data[offset:offset+2]) == uint16(btree.NodeTypeContinuation) {
							at = offset + btree.ContinuationHeaderSize
							break
						}
					}
				}
				if at < 0 {
					t.Fatal("value bytes are missing")
				}
				data[at] ^= 1
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
				db, err = Open(path, 0600, &Options{ReadOnly: true})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				checks := []struct {
					name string
					read func() ([]byte, error)
				}{
					{"Get", func() ([]byte, error) { return db.Get([]byte("bucket"), []byte("key")) }},
					{"Bucket.Get", func() (value []byte, err error) {
						err = db.View(func(tx *Tx) error {
							value, err = mustBucket(t, tx, []byte("bucket")).Get([]byte("key"))
							return err
						})
						return value, err
					}},
					{"Cursor", func() (value []byte, err error) {
						err = db.View(func(tx *Tx) error {
							cursor, cursorErr := mustBucket(t, tx, []byte("bucket")).Cursor()
							if cursorErr != nil {
								return cursorErr
							}
							_, value, err = cursor.First()
							return err
						})
						return value, err
					}},
				}
				if !continued {
					for _, reverse := range []bool{false, true} {
						checks = append(checks, struct {
							name string
							read func() ([]byte, error)
						}{fmt.Sprintf("Scan/reverse=%t", reverse), func() (value []byte, err error) {
							err = db.View(func(tx *Tx) error {
								bucket := mustBucket(t, tx, []byte("bucket"))
								visit := func(_, got []byte) error { value = bytes.Clone(got); return nil }
								if reverse {
									return bucket.ScanReverse(visit)
								}
								return bucket.ScanRange(nil, nil, visit)
							})
							return value, err
						}})
					}
				}
				for _, check := range checks {
					got, err := check.read()
					if enabled {
						if !errors.Is(err, ErrChecksum) || got != nil {
							t.Fatalf("%s: got %d bytes, %v; want checksum error", check.name, len(got), err)
						}
					} else if err != nil || len(got) != len(value) || bytes.Equal(got, value) {
						t.Fatalf("%s: got %d bytes, %v; want unchecked changed value", check.name, len(got), err)
					}
				}
			})
		}
	}
}

func TestDataPageChecksums_ZeroCannotDisableProtectedPage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone, DataPageChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	root, pageSize := db.meta.Root(), db.meta.PageSize()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	clear(data[int64(root)*pageSize+14 : int64(root)*pageSize+18])
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if db, err := Open(path, 0600, nil); !errors.Is(err, ErrChecksum) {
		if db != nil {
			db.Close()
		}
		t.Fatalf("zero protected checksum: got %v, want checksum error", err)
	}
}

func TestDataPageChecksums_ControlPagesStayProtected(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, target := range []string{"metadata", "allocation", "WAL"} {
			t.Run(fmt.Sprintf("enabled=%t/%s", enabled, target), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "database")
				db, err := Open(path, 0600, &Options{Synchronous: SyncNone, DataPageChecksums: enabled, CheckpointThresholdBytes: 1 << 30})
				if err != nil {
					t.Fatal(err)
				}
				createBucket(t, db, []byte("bucket"))
				pageSize := int(db.meta.PageSize())
				log, err := os.ReadFile(path + "-wal")
				if err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				switch target {
				case "metadata":
					data[page.MetaSize-1] ^= 1
					data[pageSize+page.MetaSize-1] ^= 1
				case "allocation":
					data[2*pageSize+allocationHeaderSize] ^= 1
				case "WAL":
					log[len(log)-1] ^= 1
					if _, err := wal.DecodeWALRecord(bytes.NewReader(log[len(log)-wal.HeaderSize-wal.ChecksumSize:]), int64(pageSize)); !errors.Is(err, ErrChecksum) {
						t.Fatalf("damaged commit marker: got %v, want checksum error", err)
					}
					if err := os.WriteFile(path+"-wal", log, 0600); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
				if target == "WAL" {
					recovered, err := Open(path, 0600, nil)
					if err != nil {
						t.Fatal(err)
					}
					if err := recovered.Close(); err != nil {
						t.Fatal(err)
					}
					return
				}
				if db, err := Open(path, 0600, nil); !errors.Is(err, ErrChecksum) {
					if db != nil {
						db.Close()
					}
					t.Fatalf("damaged %s: got %v, want checksum error", target, err)
				}
			})
		}
	}
}

func TestDataPageChecksums_RecoveryUsesWALMetadata(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, readOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("enabled=%t/readOnly=%t", enabled, readOnly), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "database")
				db, err := Open(path, 0600, &Options{Synchronous: SyncNone, DataPageChecksums: enabled, CheckpointThresholdBytes: 1 << 30})
				if err != nil {
					t.Fatal(err)
				}
				createBucket(t, db, []byte("bucket"))
				value := bytes.Repeat([]byte("a"), 128)
				if err := db.Put([]byte("bucket"), []byte("key"), value); err != nil {
					t.Fatal(err)
				}
				db.operationMu.Lock()
				err = db.checkpointWAL()
				db.operationMu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
				// Write an unchanged base image before the compact record.
				if err := db.Put([]byte("bucket"), []byte("key"), value); err != nil {
					t.Fatal(err)
				}
				value[0] = 'b'
				if err := db.Put([]byte("bucket"), []byte("key"), value); err != nil {
					t.Fatal(err)
				}
				// An allocated page puts the stored mode into committed WAL metadata.
				createBucket(t, db, []byte("other"))
				records, err := db.wal.ReadRecords()
				if err != nil {
					t.Fatal(err)
				}
				// Remove the base transaction to model a WAL from the older writer.
				log, err := os.ReadFile(path + "-wal")
				if err != nil {
					t.Fatal(err)
				}
				headerSize := len(log)
				for _, record := range records {
					headerSize -= wal.HeaderSize + len(record.Payload) + wal.ChecksumSize
				}
				log = log[:headerSize]
				patched := false
				for _, record := range records {
					if record.Header.TxID == records[0].Header.TxID {
						continue
					}
					log = wal.AppendEncodedWALRecord(log, &record)
					patched = patched || record.Header.Type == wal.RecordTypePatch
				}
				if !patched {
					t.Fatal("fixture did not write a patch that needs a main-page base")
				}
				main, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				pageSize := int(db.meta.PageSize())
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				// Restore a recoverable pre-checkpoint state with damaged main metadata.
				main[24] ^= 1
				main[pageSize+24] ^= 1
				if err := os.WriteFile(path, main, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path+"-wal", log, 0600); err != nil {
					t.Fatal(err)
				}
				db, err = Open(path, 0600, &Options{ReadOnly: readOnly, Synchronous: SyncNone, DataPageChecksums: !enabled})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				got, err := db.Get([]byte("bucket"), []byte("key"))
				if err != nil || !bytes.Equal(got, value) || db.meta.DataPageChecksums() != enabled {
					t.Fatalf("recovered read: got %q, %v; checksum mode=%t", got, err, db.meta.DataPageChecksums())
				}
			})
		}
	}
}
