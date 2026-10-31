package kvlite

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

func TestRecovery_ZeroFilledWALHeaderOpens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	createBucket(t, db, testBucketName)
	if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// A crash during the first commit after a clean open can keep the WAL size but not its bytes.
	if err := os.WriteFile(path+"-wal", make([]byte, 4096), 0600); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatalf("open with zero-filled WAL: %v", err)
	}
	defer reopened.Close()
	if got, err := reopened.Get(testBucketName, []byte("key")); err != nil || !bytes.Equal(got, []byte("value")) {
		t.Fatalf("checkpointed value after zero-filled WAL: got %q, error %v", got, err)
	}
}

// An empty main file holds no database, so Open ignores a WAL next to it, as SQLite does.
// A person can delete a database file and keep its WAL.
// Open then copied the pages of that WAL into the new file, and every later Open failed with "read allocation page 2: EOF".
func TestRecovery_EmptyMainFileIgnoresTheWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncFull, CheckpointThresholdBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	createBucket(t, db, testBucketName)
	for index := range 200 {
		if err := db.Put(testBucketName, []byte{byte(index), 'k'}, make([]byte, 300)); err != nil {
			t.Fatal(err)
		}
	}
	// The checkpoint moves most pages into the main file, so the WAL alone is not a database.
	if err := db.checkpointWAL(); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("old"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	staleWAL, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+"-wal", staleWAL, 0600); err != nil {
		t.Fatal(err)
	}

	// A read-only handle cannot create the database.
	if _, err := Open(path, 0600, &Options{ReadOnly: true}); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read-only Open of an empty file next to a WAL: got %v, want os.ErrNotExist", err)
	}
	for attempt := range 2 {
		db, err := Open(path, 0600, &Options{Synchronous: SyncFull})
		if err != nil {
			t.Fatalf("Open %d of an empty file next to a WAL: %v", attempt, err)
		}
		if _, err := db.Get(testBucketName, []byte("old")); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("Open %d shows data of the WAL: got %v, want ErrBucketNotFound", attempt, err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// walOpenCountFS counts the calls that open the WAL, the failed calls among them, and the syncs of the WAL.
// Its WAL files are not *os.File values, so KVLite syncs them with their Sync method.
type walOpenCountFS struct {
	fileio.OSFS
	opens       int
	failedOpens int
	syncs       int
}

func (fsys *walOpenCountFS) OpenFile(name string, flag int, perm os.FileMode) (fileio.File, error) {
	file, err := fsys.OSFS.OpenFile(name, flag, perm)
	if !strings.HasSuffix(name, "-wal") {
		return file, err
	}
	fsys.opens++
	if err != nil {
		fsys.failedOpens++
		return file, err
	}
	return &walSyncCountFile{File: file, fsys: fsys}, nil
}

type walSyncCountFile struct {
	fileio.File
	fsys *walOpenCountFS
}

func (file *walSyncCountFile) Sync() error {
	file.fsys.syncs++
	return file.File.Sync()
}

// Close removes the WAL, so the Open of a cleanly closed database creates the WAL again.
// That Open must create it with one call and must not sync it.
func TestRecovery_CleanOpenCreatesWALWithOneCall(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	createBucket(t, db, testBucketName)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	fsys := &walOpenCountFS{}
	db, err = open(path, 0600, &Options{Synchronous: SyncFull}, fsys)
	if err != nil {
		t.Fatal(err)
	}
	if fsys.opens != 1 || fsys.failedOpens != 0 || fsys.syncs != 0 {
		t.Errorf("Open of a cleanly closed database: %d WAL opens, %d failed, %d WAL syncs; want 1, 0, 0", fsys.opens, fsys.failedOpens, fsys.syncs)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// A WAL that Open finds comes from a process that did not close the database.
// A writable Open must sync it before it returns, also when it holds no record (see open).
func TestRecovery_FoundEmptyWALIsSyncedBeforeOpenReturns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	createBucket(t, db, testBucketName)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+"-wal", nil, 0600); err != nil {
		t.Fatal(err)
	}

	fsys := &walOpenCountFS{}
	db, err = open(path, 0600, &Options{Synchronous: SyncFull}, fsys)
	if err != nil {
		t.Fatal(err)
	}
	if fsys.syncs != 1 {
		t.Errorf("Open that found an empty WAL: %d WAL syncs, want 1", fsys.syncs)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRecovery_PartialCheckpoint(t *testing.T) {
	for _, test := range []struct {
		name      string
		checksums bool
		deleting  bool
	}{
		{name: "overwrite", checksums: true},
		{name: "delete", deleting: true},
		{name: "delete-checksums", checksums: true, deleting: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			options := &Options{DataPageChecksums: test.checksums, CheckpointThresholdBytes: 1 << 30}
			db, err := Open(path, 0600, options)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Update(func(tx *Tx) error {
				bucket, err := tx.CreateBucket(testBucketName)
				if err != nil {
					return err
				}
				for index := range 35 {
					if err := bucket.Put(fmt.Appendf(nil, "key-%03d", index), bytes.Repeat([]byte{byte(index + 1)}, 80)); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			main, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			db, err = Open(path, 0600, options)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var rootID page.ID
			if err := db.View(func(tx *Tx) error {
				bucket, err := tx.Bucket(testBucketName)
				if err == nil {
					rootID = bucket.rootPageID
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if test.deleting {
				for _, key := range []string{"key-015", "key-020"} {
					if err := db.Delete(testBucketName, []byte(key)); err != nil {
						t.Fatal(err)
					}
				}
			} else {
				for index := 25; index <= 26; index++ {
					if err := db.Put(testBucketName, fmt.Appendf(nil, "key-%03d", index), bytes.Repeat([]byte{byte(index + 100)}, 80)); err != nil {
						t.Fatal(err)
					}
				}
			}
			log, err := os.ReadFile(path + "-wal")
			if err != nil {
				t.Fatal(err)
			}
			offset := int(rootID) * int(db.meta.PageSize())
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			complete, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			// Model a checkpoint that wrote the first sector but left later sectors old.
			torn := bytes.Clone(main)
			copy(torn[offset:offset+512], complete[offset:offset+512])
			if bytes.Equal(torn, main) || bytes.Equal(torn, complete) {
				t.Fatal("test needs a mix of old and new page sectors")
			}
			for _, readOnly := range []bool{false, true} {
				t.Run(fmt.Sprintf("read-only=%t", readOnly), func(t *testing.T) {
					crashPath := filepath.Join(t.TempDir(), "database")
					if err := os.WriteFile(crashPath, torn, 0600); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(crashPath+"-wal", log, 0600); err != nil {
						t.Fatal(err)
					}
					recovered, err := Open(crashPath, 0600, &Options{ReadOnly: readOnly})
					if err != nil {
						t.Fatalf("recover committed log over a partial checkpoint: %v", err)
					}
					defer recovered.Close()
					for index := range 35 {
						key := fmt.Appendf(nil, "key-%03d", index)
						got, err := recovered.Get(testBucketName, key)
						if test.deleting && (index == 15 || index == 20) {
							if !errors.Is(err, ErrKeyNotFound) {
								t.Fatalf("deleted key %q: %v", key, err)
							}
							continue
						}
						value := byte(index + 1)
						if !test.deleting && (index == 25 || index == 26) {
							value = byte(index + 100)
						}
						if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{value}, 80)) {
							t.Fatalf("committed value for %q was not recovered: %v", key, err)
						}
					}
				})
			}
		})
	}
}

func TestRecovery_IncompleteWALTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{CheckpointThresholdBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	createBucket(t, db, testBucketName)
	if err := db.Put(testBucketName, []byte("stable"), []byte("committed")); err != nil {
		t.Fatal(err)
	}
	main, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := readWALPrefix(db)
	if err != nil {
		t.Fatal(err)
	}
	pendingValue := bytes.Repeat([]byte("pending"), 200)
	if err := db.Put(testBucketName, []byte("pending"), pendingValue); err != nil {
		t.Fatal(err)
	}
	complete, err := readWALPrefix(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(testBucketName, []byte("later"), []byte("must be discarded")); err != nil {
		t.Fatal(err)
	}
	following, err := readWALPrefix(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	tail := complete[len(committed):]
	checksumSeed, err := wal.RecordChecksumSeed(committed)
	if err != nil {
		t.Fatal(err)
	}
	markerSize := wal.HeaderSize + wal.ChecksumSize
	kept := 1024 - len(committed)%512
	if len(tail)-markerSize <= kept {
		t.Fatal("test needs a record that spans several sectors")
	}
	for _, damage := range []string{"short", "no-marker", "partial-sector", "partial-header", "bad-length", "marker-like-data", "embedded-marker", "committed-data", "committed-length", "committed-short-read", "commit-checksum", "record-type", "record-id", "marker-page", "marker-payload", "marker-id", "none"} {
		for _, mode := range []struct {
			readOnly bool
			sync     Sync
		}{{sync: SyncFull}, {sync: SyncNone}, {readOnly: true, sync: SyncNone}} {
			t.Run(fmt.Sprintf("%s/read-only=%t/sync=%d", damage, mode.readOnly, mode.sync), func(t *testing.T) {
				candidate := bytes.Clone(tail)
				switch damage {
				case "short":
					candidate = candidate[:40]
				case "no-marker":
					candidate = candidate[:len(candidate)-markerSize]
				case "partial-sector":
					clear(candidate[kept:])
				case "partial-header":
					clear(candidate[10:])
				case "bad-length":
					clear(candidate[kept:])
					binary.LittleEndian.PutUint32(candidate[wal.HeaderSize-4:wal.HeaderSize], ^uint32(0))
				case "marker-like-data", "embedded-marker":
					marker := bytes.Clone(candidate[len(candidate)-markerSize:])
					clear(candidate[kept:])
					if damage == "marker-like-data" {
						marker[len(marker)-1] ^= 1
					}
					copy(candidate[100:], marker)
				case "committed-data":
					candidate[wal.HeaderSize+100] ^= 1
				case "committed-length":
					binary.LittleEndian.PutUint32(candidate[wal.HeaderSize-4:wal.HeaderSize], ^uint32(0))
				case "committed-short-read":
					binary.LittleEndian.PutUint32(candidate[wal.HeaderSize-4:wal.HeaderSize], uint32(os.Getpagesize()))
				case "commit-checksum":
					candidate[len(candidate)-1] ^= 1
				case "record-type", "record-id", "marker-page", "marker-payload", "marker-id":
					marker := wal.WALRecord{Header: wal.RecordHeader{
						Type: wal.RecordTypeCommit,
						TxID: wal.TxID(binary.LittleEndian.Uint64(tail[len(tail)-markerSize+9 : len(tail)-markerSize+17])),
					}}
					switch damage {
					case "record-type":
						marker.Header.Type = 255
					case "record-id":
						marker.Header.TxID = 0
					case "marker-page":
						marker.Header.PageID = 3
					case "marker-payload":
						marker.Payload = []byte("invalid")
					case "marker-id":
						marker.Header.TxID++
					}
					candidate = wal.AppendEncodedWALRecord(candidate[:len(candidate)-markerSize], &marker, checksumSeed)
				}
				if damage != "none" {
					candidate = append(candidate, following[len(complete):]...)
				}
				log := append(bytes.Clone(committed), candidate...)
				crashPath := filepath.Join(t.TempDir(), "database")
				if err := os.WriteFile(crashPath, main, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(crashPath+"-wal", log, 0600); err != nil {
					t.Fatal(err)
				}
				recovered, err := Open(crashPath, 0600, &Options{ReadOnly: mode.readOnly, Synchronous: mode.sync})
				if err != nil {
					t.Fatalf("recover earlier committed data: %v", err)
				}
				defer recovered.Close()
				got, err := recovered.Get(testBucketName, []byte("stable"))
				if err != nil || string(got) != "committed" {
					t.Fatalf("earlier committed value: got %q, %v", got, err)
				}
				got, err = recovered.Get(testBucketName, []byte("pending"))
				if damage == "none" {
					if err != nil || !bytes.Equal(got, pendingValue) {
						t.Fatalf("complete transaction: got %q, %v", got, err)
					}
				} else if !errors.Is(err, ErrKeyNotFound) {
					t.Fatalf("incomplete transaction became visible: %v", err)
				}
				if _, err := recovered.Get(testBucketName, []byte("later")); !errors.Is(err, ErrKeyNotFound) {
					t.Fatalf("transaction after damaged data became visible: %v", err)
				}
				stored, err := os.ReadFile(crashPath + "-wal")
				if err != nil {
					t.Fatal(err)
				}
				if mode.readOnly {
					if !bytes.Equal(stored, log) {
						t.Fatal("read-only recovery changed the WAL")
					}
				} else if len(stored) != 0 {
					t.Fatal("writable recovery did not clear the WAL")
				}
				if !mode.readOnly {
					if err := recovered.Put(testBucketName, []byte("after"), []byte("recovery")); err != nil {
						t.Fatal(err)
					}
					if err := recovered.Close(); err != nil {
						t.Fatal(err)
					}
					reopened, err := Open(crashPath, 0600, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer reopened.Close()
					got, err := reopened.Get(testBucketName, []byte("after"))
					if err != nil || string(got) != "recovery" {
						t.Fatalf("write after recovery: got %q, %v", got, err)
					}
				}
			})
		}
	}
}

func TestRecovery_UsesSyncMode(t *testing.T) {
	for _, mode := range []Sync{SyncFull, SyncNormal, SyncNone} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			db, err := Open(path, 0600, &Options{Synchronous: mode})
			if err != nil {
				t.Fatal(err)
			}
			defer db.closeFiles()
			createBucket(t, db, testBucketName)
			if err := db.Put(testBucketName, []byte("key"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			records, err := db.wal.ReadRecords()
			if err != nil {
				t.Fatal(err)
			}
			committed := committedWALRecords(records)
			main, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			log, err := os.ReadFile(path + "-wal")
			if err != nil {
				t.Fatal(err)
			}
			db.wal.ClearCommittedRecordsForTesting()
			syncErr := errors.New("recovery WAL sync failed")
			db.wal.SetSyncFileForTesting(func() error { return syncErr })
			err = db.ingestWalRecords(committed)
			if mode == SyncNone {
				if err != nil {
					t.Fatalf("SyncNone recovery called sync: %v", err)
				}
			} else {
				if !errors.Is(err, syncErr) {
					t.Fatalf("recovery: got %v, want WAL sync failure", err)
				}
				for name, want := range map[string][]byte{path: main, path + "-wal": log} {
					got, err := os.ReadFile(name)
					if err != nil || !bytes.Equal(got, want) {
						t.Fatalf("failed WAL sync changed %s: %v", name, err)
					}
				}
				// A failed WAL sync stops this WAL, so a new Open recovers again.
				db.wal.SetSyncFileForTesting(nil)
				// On Linux, the mapping keeps the main file and its lock after the descriptor closes.
				if err := db.unmapMainFile(); err != nil {
					t.Fatal(err)
				}
				if err := db.closeFiles(); err != nil {
					t.Fatal(err)
				}
				reopened, err := Open(path, 0600, &Options{Synchronous: mode})
				if err != nil {
					t.Fatalf("retry recovery in a new Open: %v", err)
				}
				defer reopened.Close()
			}
			if info, err := os.Stat(path + "-wal"); err != nil || info.Size() != 0 {
				t.Fatalf("successful recovery did not clear the WAL: %v", err)
			}
		})
	}
}

// Open must not place WAL pages with the page size of a metadata copy that fails its checksum.
// Blackout found this (issue 4): one changed bit in the page-size field of metadata page 0 made 16384 into 49152.
// Metadata page 1 was not at the operating-system page size, so Open read the WAL with 49152 and took the 16384 metadata from the WAL.
// The recovery checkpoint then wrote each page at 3 times its offset and cleared the WAL, and every later Open failed.
func TestRecovery_DamagedMetadataPageSizeKeepsCommittedData(t *testing.T) {
	const databasePageSize, hostPageSize = 16384, 4096
	path := filepath.Join(t.TempDir(), "database")
	db, err := open(path, 0o600, &Options{CheckpointThresholdBytes: 1 << 40, pageSize: databasePageSize}, fileio.OSFS{})
	if err != nil {
		t.Fatal(err)
	}
	createBucket(t, db, testBucketName)
	value := bytes.Repeat([]byte{'v'}, 3000)
	// The checkpoint writes only when the damaged page size divides the file size, so the file gets a multiple of 3 pages.
	keys := 0
	for keys == 0 || (db.meta.LastPage()+1)%3 != 0 {
		if err := db.Put(testBucketName, fmt.Appendf(nil, "key-%03d", keys), value); err != nil {
			t.Fatal(err)
		}
		keys++
	}
	// Keep the files of a process that stopped after its last commit, so the commits are only in the WAL.
	main, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	log, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	pageSizeByte := main[9]
	// Bit 79 is in the page-size field of metadata page 0.
	main[9] ^= 0x80
	if err := os.WriteFile(path, main, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+"-wal", log, 0o600); err != nil {
		t.Fatal(err)
	}
	checkKeys := func(db *DB, when string) {
		t.Helper()
		for index := range keys {
			got, err := db.Get(testBucketName, fmt.Appendf(nil, "key-%03d", index))
			if err != nil || !bytes.Equal(got, value) {
				t.Fatalf("%s: key %d: got %d bytes, error %v", when, index, len(got), err)
			}
		}
	}

	// On a host with 4 KiB pages, Open finds no valid metadata copy in the main file.
	// It can fail, but it must not damage the database.
	if damaged, err := open(path, 0o600, &Options{pageSize: hostPageSize}, fileio.OSFS{}); err == nil {
		checkKeys(damaged, "Open with damaged metadata")
		if err := damaged.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// The next read of page 0 is correct, as after a read error that does not stay.
	main, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	main[9] = pageSizeByte
	if err := os.WriteFile(path, main, 0o600); err != nil {
		t.Fatal(err)
	}
	reopened, err := open(path, 0o600, &Options{pageSize: hostPageSize}, fileio.OSFS{})
	if err != nil {
		t.Fatalf("Open after the damage is gone: %v", err)
	}
	defer reopened.Close()
	checkKeys(reopened, "Open after the damage is gone")
}

// Open must find metadata page 1 when damage stays in the page-size field of metadata page 0.
// Page 0 then gives no offset for page 1, and a host with 4 KiB pages gives no offset for 16 KiB pages either.
// So Open tries every supported page size, as bbolt does.
// Before, every Open of such a file failed, although page 1 and the WAL were intact.
func TestRecovery_FindsMetadataPage1AfterPage0Damage(t *testing.T) {
	const databasePageSize, hostPageSize = 16384, 4096
	for _, test := range []struct {
		name         string
		page0Damage  byte // the changed bits in byte 9, in the page-size field of page 0
		committedWAL bool
		damageMeta1  bool // a changed bit in the high-water field of page 1 breaks its checksum but keeps its page-size field
	}{
		{name: "checkpointed", page0Damage: 0x80},
		{name: "committed WAL", page0Damage: 0x80, committedWAL: true},
		{name: "both copies damaged, committed WAL", page0Damage: 0x80, committedWAL: true, damageMeta1: true},
		// Two changed bits make 16384 into 32768, which is a power of two, so only page 1 can give the WAL page size.
		{name: "both copies damaged, page 0 gives another power of two", page0Damage: 0xc0, committedWAL: true, damageMeta1: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			db, err := open(path, 0o600, &Options{CheckpointThresholdBytes: 1 << 40, pageSize: databasePageSize}, fileio.OSFS{})
			if err != nil {
				t.Fatal(err)
			}
			createBucket(t, db, testBucketName)
			value := bytes.Repeat([]byte{'v'}, 3000)
			const keys = 8
			for index := range keys {
				if err := db.Put(testBucketName, fmt.Appendf(nil, "key-%03d", index), value); err != nil {
					t.Fatal(err)
				}
			}
			// Without a checkpoint, the commits are only in the WAL, as after a process crash.
			main, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			log, err := os.ReadFile(path + "-wal")
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if !test.committedWAL {
				// Close checkpointed every commit.
				if main, err = os.ReadFile(path); err != nil {
					t.Fatal(err)
				}
				log = nil
			}
			// Byte 9 is in the page-size field of metadata page 0, and bit 79 makes 16384 into 49152.
			main[9] ^= test.page0Damage
			if test.damageMeta1 {
				main[databasePageSize+20] ^= 0x01
			}
			if err := os.WriteFile(path, main, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path+"-wal", log, 0o600); err != nil {
				t.Fatal(err)
			}

			reopened, err := open(path, 0o600, &Options{pageSize: hostPageSize}, fileio.OSFS{})
			if err != nil {
				t.Fatalf("Open with damaged metadata page 0: %v", err)
			}
			defer reopened.Close()
			if got := reopened.meta.PageSize(); got != databasePageSize {
				t.Fatalf("page size: got %d, want %d", got, databasePageSize)
			}
			for index := range keys {
				got, err := reopened.Get(testBucketName, fmt.Appendf(nil, "key-%03d", index))
				if err != nil || !bytes.Equal(got, value) {
					t.Fatalf("key %d: got %d bytes, error %v", index, len(got), err)
				}
			}
		})
	}
}

func TestCommittedWALRecords_StopsAtInvalidTransaction(t *testing.T) {
	stable := wal.WALRecord{Header: wal.RecordHeader{Type: wal.RecordTypeNode, PageID: 3, TxID: 1}}
	marker := wal.WALRecord{Header: wal.RecordHeader{Type: wal.RecordTypeCommit, TxID: 2}}
	for _, fault := range []string{"empty", "mismatch", "mixed-records", "page-id", "payload"} {
		t.Run(fault, func(t *testing.T) {
			records := []wal.WALRecord{stable, {Header: wal.RecordHeader{Type: wal.RecordTypeCommit, TxID: 1}}}
			if fault != "empty" {
				records = append(records, wal.WALRecord{Header: wal.RecordHeader{Type: wal.RecordTypeNode, PageID: 3, TxID: 2}})
			}
			invalid := marker
			switch fault {
			case "mismatch":
				invalid.Header.TxID = 9
			case "mixed-records":
				records = append(records, wal.WALRecord{Header: wal.RecordHeader{Type: wal.RecordTypeNode, PageID: 4, TxID: 9}})
			case "page-id":
				invalid.Header.PageID = 3
			case "payload":
				invalid.Payload = []byte("invalid")
			}
			records = append(records, invalid,
				wal.WALRecord{Header: wal.RecordHeader{Type: wal.RecordTypeNode, PageID: 3, TxID: 3}},
				wal.WALRecord{Header: wal.RecordHeader{Type: wal.RecordTypeCommit, TxID: 3}})
			got := committedWALRecords(records)
			if len(got) != 1 || got[0].Header != stable.Header {
				t.Fatalf("valid transaction before invalid marker: got %+v", got)
			}
		})
	}
}

func TestOpenIgnoresDroppedAllocationSegmentInWAL(t *testing.T) {
	for _, test := range []struct {
		name     string
		readOnly bool
		badPage  bool
	}{
		{"read-only", true, false},
		{"writable", false, false},
		{"invalid allocation page", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			initial, err := Open(path, 0600, &Options{Synchronous: SyncNone})
			if err != nil {
				t.Fatal(err)
			}
			meta := *initial.meta
			if err := initial.Close(); err != nil {
				t.Fatal(err)
			}

			bitmap := newAllocationBitmap(meta.PageSize())
			bitmap.addSegment()
			firstDropped := bitmap.segmentsPerGroup()
			segmentPageID := bitmap.segmentPageID(firstDropped)
			initialLastPage := meta.LastPage()
			recordPageID := segmentPageID
			if test.badPage {
				recordPageID = meta.Root() + 1
			}
			walFile, err := os.OpenFile(path+"-wal", os.O_RDWR|os.O_CREATE, 0600)
			if err != nil {
				t.Fatal(err)
			}
			log := wal.New(wal.Config{File: walFile, PageSize: meta.PageSize()})
			meta.SetLastPage(bitmap.segmentPageID(len(bitmap.segments) - 1))
			meta.AdvanceGeneration()
			meta.RefreshChecksum()
			if _, err := log.Commit(nil, []wal.AllocationRecord{{PageID: recordPageID, Payload: bitmap.encodeSegment(firstDropped)}}, page.EncodeMeta(&meta), nil); err != nil {
				t.Fatal(err)
			}
			meta.SetLastPage(initialLastPage)
			meta.AdvanceGeneration()
			meta.RefreshChecksum()
			if _, err := log.Commit(nil, nil, page.EncodeMeta(&meta), nil); err != nil {
				t.Fatal(err)
			}
			if err := log.Close(); err != nil {
				t.Fatal(err)
			}

			reopened, err := Open(path, 0600, &Options{ReadOnly: test.readOnly, Synchronous: SyncNone})
			if test.badPage {
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("open with invalid allocation page: got %v, want %v", err, ErrInvalid)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if _, found := reopened.wal.CommittedRecord(segmentPageID); found {
				t.Fatalf("recovery kept dropped allocation page %d", segmentPageID)
			}
		})
	}
}

func TestRecoverySkipsOldPatchesAfterFreeOrFullNode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rootID := db.meta.Root()
	root := btree.NewLeafNode(rootID)
	fullRoot := btree.EncodeWALNode(root)
	records := []wal.WALRecord{
		{Header: wal.RecordHeader{Type: wal.RecordTypeNode, PageID: rootID + 1, TxID: 1}, Payload: btree.EncodeWALNode(btree.NewLeafNode(rootID + 1))},
		{Header: wal.RecordHeader{Type: wal.RecordTypeBytePatch, PageID: rootID + 1, TxID: 1}, Payload: []byte{0xff}},
		{Header: wal.RecordHeader{Type: wal.RecordTypeEntryPatch, PageID: rootID + 1, TxID: 1}, Payload: []byte{0xff}},
		{Header: wal.RecordHeader{Type: wal.RecordTypeBytePatch, PageID: rootID, TxID: 1}, Payload: []byte{0xff}},
		{Header: wal.RecordHeader{Type: wal.RecordTypeEntryPatch, PageID: rootID, TxID: 1}, Payload: []byte{0xff}},
		{Header: wal.RecordHeader{Type: wal.RecordTypeNode, PageID: rootID, TxID: 2}, Payload: fullRoot},
	}
	if err := db.loadCommittedIntoOverlay(records); err != nil {
		t.Fatal(err)
	}
	if _, ok := db.wal.CommittedRecord(rootID + 1); ok {
		t.Fatal("recovery kept a freed page")
	}
	got, ok := db.wal.CommittedRecord(rootID)
	if !ok || got.Header.Type != wal.RecordTypeNode || !bytes.Equal(got.Payload, fullRoot) {
		t.Fatalf("recovered root: found=%t type=%d", ok, got.Header.Type)
	}
}

// TestRecoveryRejectsPatchWithoutCompleteImage checks that recovery never uses a main-file page as the base of a patch.
// The first change to a page after a checkpoint is a complete image, so a WAL that has a patch without one is invalid.
func TestRecoveryRejectsPatchWithoutCompleteImage(t *testing.T) {
	for _, recordType := range []wal.RecordType{wal.RecordTypeBytePatch, wal.RecordTypeEntryPatch} {
		t.Run(fmt.Sprintf("type=%d", recordType), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			records := []wal.WALRecord{
				{Header: wal.RecordHeader{Type: recordType, PageID: firstTreePageID, TxID: 1}},
			}
			if err := db.loadCommittedIntoOverlay(records); !errors.Is(err, page.ErrInvalid) {
				t.Fatalf("patch without a complete image: got %v, want ErrInvalid", err)
			}
		})
	}
}

// TestMixedPatchChainRecoversInReadOnlyAndWritableModes replays one leaf from a chain of byte patches and entry patches.
func TestMixedPatchChainRecoversInReadOnlyAndWritableModes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	name := []byte("bucket")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket(name)
		if err != nil {
			return err
		}
		for index := range 50 {
			if err := bucket.Put(fmt.Appendf(nil, "key-%03d", index), []byte("value")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, 0600, &Options{Synchronous: SyncNormal, CheckpointThresholdBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	// The first change after the checkpoint writes a complete image.
	// Then a same-size value change writes a byte patch, and a change of the entry count writes an entry patch.
	for _, step := range []struct {
		key, value string
	}{
		{"key-001", "VALU1"},
		{"key-001", "VALU2"},
		{"key-100", "new"},
		{"key-002", "VALU3"},
		{"key-003", ""},
	} {
		if step.value == "" {
			err = db.Delete(name, []byte(step.key))
		} else {
			err = db.Put(name, []byte(step.key), []byte(step.value))
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	var leafPageID page.ID
	if err := db.View(func(tx *Tx) error {
		leafPageID = mustBucket(t, tx, name).rootPageID
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	records, err := db.wal.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	var types []wal.RecordType
	for _, record := range records {
		if record.Header.PageID == leafPageID && record.Header.Type != wal.RecordTypeCommit {
			types = append(types, record.Header.Type)
		}
	}
	want := []wal.RecordType{wal.RecordTypeNode, wal.RecordTypeBytePatch, wal.RecordTypeEntryPatch, wal.RecordTypeBytePatch, wal.RecordTypeEntryPatch}
	if !slices.Equal(types, want) {
		t.Fatalf("leaf %d record types: got %v, want %v", leafPageID, types, want)
	}
	// Close the file handles without a checkpoint to test both recovery modes.
	if err := db.wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.unmapMainFile(); err != nil {
		t.Fatal(err)
	}
	if err := db.file.Close(); err != nil {
		t.Fatal(err)
	}
	check := func(db *DB) {
		t.Helper()
		for key, want := range map[string]string{"key-001": "VALU2", "key-002": "VALU3", "key-004": "value", "key-100": "new"} {
			if value, err := db.Get(name, []byte(key)); err != nil || string(value) != want {
				t.Fatalf("key %s: got %q, error %v; want %q", key, value, err, want)
			}
		}
		if value, err := db.Get(name, []byte("key-003")); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("deleted key: got %q, error %v", value, err)
		}
	}
	for _, readOnly := range []bool{true, false} {
		recovered, err := Open(path, 0600, &Options{ReadOnly: readOnly, Synchronous: SyncNormal})
		if err != nil {
			t.Fatal(err)
		}
		check(recovered)
		if err := recovered.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEntryPatchWALRecoversInReadOnlyAndWritableModes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	name := []byte("bucket")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket(name)
		if err != nil {
			return err
		}
		for index := range 50 {
			if err := bucket.Put(fmt.Appendf(nil, "key-%03d", index), []byte("value")); err != nil {
				return err
			}
		}
		child, err := bucket.CreateBucket([]byte("child"))
		if err != nil {
			return err
		}
		return child.Put([]byte("inside"), []byte("value"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, 0600, &Options{Synchronous: SyncNormal, CheckpointThresholdBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(name)
		if err != nil {
			return err
		}
		return bucket.DeleteBatch([][]byte{[]byte("key-010"), []byte("key-020")})
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(name)
		if err != nil {
			return err
		}
		return bucket.DeleteBucket([]byte("child"))
	}); err != nil {
		t.Fatal(err)
	}
	records, err := db.wal.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	foundPatch := false
	for _, record := range records {
		foundPatch = foundPatch || record.Header.Type == wal.RecordTypeEntryPatch
	}
	if !foundPatch {
		t.Fatal("WAL did not use an entry patch")
	}
	// Close the file handles without a checkpoint to test both recovery modes.
	if err := db.wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.unmapMainFile(); err != nil {
		t.Fatal(err)
	}
	if err := db.file.Close(); err != nil {
		t.Fatal(err)
	}
	check := func(db *DB) {
		t.Helper()
		for _, test := range []struct {
			key     string
			missing bool
		}{
			{"key-010", true},
			{"key-020", true},
			{"key-030", false},
		} {
			value, err := db.Get(name, []byte(test.key))
			if test.missing {
				if !errors.Is(err, ErrKeyNotFound) {
					t.Fatalf("deleted key %s: got %q, error %v", test.key, value, err)
				}
			} else if err != nil || string(value) != "value" {
				t.Fatalf("retained key %s: got %q, error %v", test.key, value, err)
			}
		}
		if err := db.View(func(tx *Tx) error {
			bucket, err := tx.Bucket(name)
			if err != nil {
				return err
			}
			_, err = bucket.Bucket([]byte("child"))
			return err
		}); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("deleted child bucket: got %v, want ErrBucketNotFound", err)
		}
	}
	readOnly, err := Open(path, 0600, &Options{ReadOnly: true, Synchronous: SyncNormal})
	if err != nil {
		t.Fatal(err)
	}
	check(readOnly)
	if err := readOnly.Close(); err != nil {
		t.Fatal(err)
	}
	writable, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
	if err != nil {
		t.Fatal(err)
	}
	check(writable)
	if err := writable.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEntryPatchWALReplayAfterCheckpointWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	name := []byte("bucket")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket(name)
		if err != nil {
			return err
		}
		for index := range 50 {
			if err := bucket.Put(fmt.Appendf(nil, "key-%03d", index), []byte("value")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, 0600, &Options{Synchronous: SyncNormal, CheckpointThresholdBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(name, []byte("key-011"), []byte("VALUE")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(name, []byte("key-010")); err != nil {
		t.Fatal(err)
	}
	if err := db.wal.Close(); err != nil {
		t.Fatal(err)
	}
	readOnlyLog, err := os.Open(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	db.wal.ReplaceFileForTesting(readOnlyLog)
	if err := db.checkpointWAL(); err == nil {
		t.Fatal("checkpoint did not report the WAL cleanup failure")
	}
	if err := readOnlyLog.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.unmapMainFile(); err != nil {
		t.Fatal(err)
	}
	if err := db.file.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if _, err := recovered.Get(name, []byte("key-010")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("deleted key after repeated replay: got %v, want ErrKeyNotFound", err)
	}
	if value, err := recovered.Get(name, []byte("key-011")); err != nil || string(value) != "VALUE" {
		t.Fatalf("next key after repeated replay: got %q, error %v", value, err)
	}
}

func TestEntryPatchWALReplayAfterFailedCleanupAndMoreWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	name := []byte("bucket")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket(name)
		if err != nil {
			return err
		}
		for index := range 50 {
			if err := bucket.Put(fmt.Appendf(nil, "key-%03d", index), []byte("value")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, 0600, &Options{Synchronous: SyncNormal, CheckpointThresholdBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(name, []byte("key-010")); err != nil {
		t.Fatal(err)
	}
	if err := db.wal.Close(); err != nil {
		t.Fatal(err)
	}
	readOnlyLog, err := os.Open(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	db.wal.ReplaceFileForTesting(readOnlyLog)
	if err := db.checkpointWAL(); err == nil {
		t.Fatal("checkpoint did not report the WAL cleanup failure")
	}
	if err := readOnlyLog.Close(); err != nil {
		t.Fatal(err)
	}
	writableLog, err := os.OpenFile(path+"-wal", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	db.wal.ReplaceFileForTesting(writableLog)
	if err := db.Put(name, []byte("key-011"), []byte("VALUE")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(name, []byte("key-012")); err != nil {
		t.Fatal(err)
	}
	if err := db.wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.unmapMainFile(); err != nil {
		t.Fatal(err)
	}
	if err := db.file.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	for _, key := range []string{"key-010", "key-012"} {
		if _, err := recovered.Get(name, []byte(key)); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("deleted key %s: got %v, want ErrKeyNotFound", key, err)
		}
	}
	if value, err := recovered.Get(name, []byte("key-011")); err != nil || string(value) != "VALUE" {
		t.Fatalf("updated key: got %q, error %v", value, err)
	}
	if value, err := recovered.Get(name, []byte("key-013")); err != nil || string(value) != "value" {
		t.Fatalf("retained key: got %q, error %v", value, err)
	}
}

func TestMetaFromCommittedRecords_RejectsInvalidPageSize(t *testing.T) {
	encoded := page.EncodeMeta(page.NewMeta(4096))
	binary.LittleEndian.PutUint64(encoded[8:16], uint64(MaxValueSize+1))
	meta, err := page.DecodeMeta(encoded)
	if err != nil {
		t.Fatal(err)
	}
	meta.RefreshChecksum()

	const txID wal.TxID = 1
	records := []wal.WALRecord{
		{
			Header:  wal.RecordHeader{Type: wal.RecordTypeMeta, PageID: page.Meta0ID, TxID: txID},
			Payload: page.EncodeMeta(meta),
		},
		{Header: wal.RecordHeader{Type: wal.RecordTypeCommit, TxID: txID}},
	}

	committed := committedWALRecords(records)
	if _, err := metaFromCommittedRecords(committed); !errors.Is(err, ErrInvalid) {
		t.Fatalf("metadata error: got %v, want ErrInvalid", err)
	}
}

// Every page size of KVLite is a power of two, as in SQLite.
// One changed bit turns a power of two into a value that is not a power of two, so validation finds that damage although the checksum matches.
func TestValidateMeta_RejectsPageSizeThatIsNotAPowerOfTwo(t *testing.T) {
	for _, test := range []struct {
		pageSize int64
		valid    bool
	}{
		{pageSize: 4096, valid: true},
		{pageSize: 16384, valid: true},
		{pageSize: 65536, valid: true},
		{pageSize: 16384 | 1<<15}, // the changed bit of issue 4
		{pageSize: 16384 | 1<<6},
		{pageSize: 4096 | 1<<13},
	} {
		meta := page.NewMeta(test.pageSize)
		meta.RefreshChecksum()
		if err := validateMeta(meta); (err == nil) != test.valid {
			t.Errorf("page size %d: got error %v, want valid %t", test.pageSize, err, test.valid)
		} else if err != nil && !errors.Is(err, ErrInvalid) {
			t.Errorf("page size %d: got error %v, want ErrInvalid", test.pageSize, err)
		}
	}
}

func TestMetaFromCommittedRecords_NoMetadata(t *testing.T) {
	meta, err := metaFromCommittedRecords([]wal.WALRecord{{Header: wal.RecordHeader{Type: wal.RecordTypeNode, PageID: 2, TxID: 1}}})
	if err != nil || meta != nil {
		t.Fatalf("metadata without metadata record: meta=%v err=%v", meta, err)
	}
}

func TestLoadCommittedIntoOverlay_RejectsInvalidLeafDescriptor(t *testing.T) {
	meta := page.NewMeta(4096)
	node := btree.NewLeafNode(meta.Root())
	if err := node.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("value"))); err != nil {
		t.Fatal(err)
	}
	image := btree.EncodeWALNode(node)
	const firstLeafEntryOffsetField = btree.NodeHeaderSize + 1
	binary.LittleEndian.PutUint64(image[firstLeafEntryOffsetField:firstLeafEntryOffsetField+8], ^uint64(0))
	const txID wal.TxID = 1
	records := []wal.WALRecord{
		{
			Header:  wal.RecordHeader{Type: wal.RecordTypeNode, PageID: node.PageID(), TxID: txID},
			Payload: image,
		},
		{Header: wal.RecordHeader{Type: wal.RecordTypeCommit, TxID: txID}},
	}
	db := &DB{storage: storage{meta: meta, allocation: newAllocationBitmap(4096), wal: wal.New(wal.Config{PageSize: 4096})}}
	committed := committedWALRecords(records)

	if err := db.loadCommittedIntoOverlay(committed); !errors.Is(err, ErrInvalid) {
		t.Fatalf("read-only recovery error: got %v, want ErrInvalid", err)
	}
	if db.wal.Stats().CommittedRecordCount != 0 {
		t.Fatal("read-only recovery published an invalid node")
	}
}

// closeWithoutCheckpoint closes the file handles of db without a checkpoint, so the next Open must recover the WAL.
func closeWithoutCheckpoint(t *testing.T, db *DB) {
	t.Helper()
	if err := db.wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.unmapMainFile(); err != nil {
		t.Fatal(err)
	}
	if err := db.file.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRecovery_SyncNoneWAL(t *testing.T) {
	for _, readOnly := range []bool{false, true} {
		for _, disableWAL := range []bool{false, true} {
			t.Run(fmt.Sprintf("readonly=%t/disable-wal=%t", readOnly, disableWAL), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "db")
				db, err := Open(path, 0600, &Options{Synchronous: SyncNone, CheckpointThresholdBytes: 1 << 30, DataPageChecksums: true})
				if err != nil {
					t.Fatal(err)
				}
				createBucket(t, db, testBucketName)
				for _, value := range []string{"first", "later"} {
					if err := db.Put(testBucketName, []byte("key"), []byte(value)); err != nil {
						t.Fatal(err)
					}
				}
				if db.wal.Stats().CommittedRecordCount == 0 {
					t.Fatal("fixture has no committed WAL data")
				}
				closeWithoutCheckpoint(t, db)
				db, err = Open(path, 0600, &Options{Synchronous: SyncNone, ReadOnly: readOnly, DisableWAL: disableWAL})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if got, err := db.Get(testBucketName, []byte("key")); err != nil || string(got) != "later" {
					t.Fatalf("recovered value: %q, %v", got, err)
				}
			})
		}
	}
}

// TestAllocationBytePatchRecoversInReadOnlyAndWritableModes rebuilds an allocation page from a complete WAL image and later byte patches.
func TestAllocationBytePatchRecoversInReadOnlyAndWritableModes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	name := []byte("bucket")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
	if err != nil {
		t.Fatal(err)
	}
	createBucket(t, db, name)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, 0600, &Options{Synchronous: SyncNormal, CheckpointThresholdBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	// Each leaf split allocates a page. The first allocation change writes a complete image, and later ones write byte patches.
	value := bytes.Repeat([]byte("v"), 200)
	keys := 0
	for patched := false; !patched; keys++ {
		if keys == 10000 {
			t.Fatal("inserts did not write an allocation byte patch")
		}
		if err := db.Put(name, fmt.Appendf(nil, "key-%05d", keys), value); err != nil {
			t.Fatal(err)
		}
		records, err := db.wal.ReadRecords()
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range records {
			patched = patched || record.Header.Type == wal.RecordTypeBytePatch && db.allocation.isSegmentPage(record.Header.PageID)
		}
	}
	allocated := db.allocation.allocatedPageCount()
	closeWithoutCheckpoint(t, db)
	for _, readOnly := range []bool{true, false} {
		recovered, err := Open(path, 0600, &Options{ReadOnly: readOnly, Synchronous: SyncNormal})
		if err != nil {
			t.Fatal(err)
		}
		if got := recovered.allocation.allocatedPageCount(); got != allocated {
			t.Fatalf("readOnly=%t: allocated pages after recovery: got %d, want %d", readOnly, got, allocated)
		}
		for index := range keys {
			if got, err := recovered.Get(name, fmt.Appendf(nil, "key-%05d", index)); err != nil || !bytes.Equal(got, value) {
				t.Fatalf("readOnly=%t: key %d after recovery: error %v", readOnly, index, err)
			}
		}
		if err := recovered.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestContinuedNodeBytePatchRecoversInReadOnlyAndWritableModes rebuilds a continued leaf from complete WAL page images and later byte patches.
func TestContinuedNodeBytePatchRecoversInReadOnlyAndWritableModes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	name := []byte("bucket")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
	if err != nil {
		t.Fatal(err)
	}
	createBucket(t, db, name)
	value := bytes.Repeat([]byte("v"), 40<<10)
	if err := db.Put(name, []byte("large"), value); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, 0600, &Options{Synchronous: SyncNormal, CheckpointThresholdBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	// The first edit after the checkpoint writes complete page images. The second edit writes a byte patch.
	for _, offset := range []int{100, 30 << 10} {
		value = bytes.Clone(value)
		copy(value[offset:], "edited")
		if err := db.Put(name, []byte("large"), value); err != nil {
			t.Fatal(err)
		}
	}
	records, err := db.wal.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	patches := 0
	for _, record := range records {
		if record.Header.Type == wal.RecordTypeBytePatch {
			patches++
		}
	}
	if patches != 1 {
		t.Fatalf("WAL byte patches: got %d, want one for the page that the second edit changed", patches)
	}
	closeWithoutCheckpoint(t, db)
	for _, readOnly := range []bool{true, false} {
		recovered, err := Open(path, 0600, &Options{ReadOnly: readOnly, Synchronous: SyncNormal})
		if err != nil {
			t.Fatal(err)
		}
		if got, err := recovered.Get(name, []byte("large")); err != nil || !bytes.Equal(got, value) {
			t.Fatalf("readOnly=%t: continued value after recovery: error %v", readOnly, err)
		}
		if err := recovered.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
