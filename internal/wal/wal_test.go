package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"runtime/debug"
	"testing"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/checksum"
	"github.com/Issaminu/kvlite/internal/page"
)

func TestWALValidationErrors(t *testing.T) {
	if _, err := EncodedWALRecordSize(&WALRecord{Payload: make([]byte, 65)}, 64); !errors.Is(err, ErrRecordPayloadTooLarge) {
		t.Fatalf("oversized record: got %v, want ErrRecordPayloadTooLarge", err)
	}

	log := New(Config{PageSize: 64})
	if _, err := log.encodeTransaction([]NodeRecord{{}}, nil, nil, 1); !errors.Is(err, ErrNodeRecordMissingFinal) {
		t.Fatalf("node without final state: got %v, want ErrNodeRecordMissingFinal", err)
	}
	if _, err := log.appendNodeRecord(nil, &NodeRecord{Original: btree.NewLeafNode(1), Final: btree.NewLeafNode(2)}, 1, nil); !errors.Is(err, ErrNodePageMismatch) {
		t.Fatalf("changed node page ID: got %v, want ErrNodePageMismatch", err)
	}
	if _, err := log.CommittedPageToNode(&CommittedPage{}); !errors.Is(err, ErrRecordPayloadRequired) {
		t.Fatalf("page without payload: got %v, want ErrRecordPayloadRequired", err)
	}
}

func TestWALCommit_WritesDatabaseFormatHeader(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	log := New(Config{File: walFile, PageSize: 64})
	if _, err := log.Commit(nil, nil, []byte("metadata"), nil); err != nil {
		t.Fatal(err)
	}

	var header [walFileHeaderSize]byte
	if _, err := walFile.ReadAt(header[:], 0); err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint32(header[0:4]); got != walFileMagic {
		t.Fatalf("WAL magic: got %x, want %x", got, walFileMagic)
	}
	if got := binary.LittleEndian.Uint32(header[4:8]); got != page.FormatVersion {
		t.Fatalf("WAL format version: got %d, want %d", got, page.FormatVersion)
	}
}

// A header with a valid checksum but another magic number or format version is an error.
// A header that fails its checksum is torn or zero, and a crash tears a header only before its generation has a durable commit, so the WAL holds nothing.
func TestWALReadRecords_ChecksFileHeader(t *testing.T) {
	testCases := []struct {
		name    string
		change  func([]byte)
		wantErr error
	}{
		{
			name: "magic",
			change: func(header []byte) {
				header[0] ^= 0xff
				binary.LittleEndian.PutUint32(header[16:20], checksum.Sum32(header[:16]))
			},
			wantErr: page.ErrInvalid,
		},
		{
			name: "version",
			change: func(header []byte) {
				binary.LittleEndian.PutUint32(header[4:8], page.FormatVersion+1)
				binary.LittleEndian.PutUint32(header[16:20], checksum.Sum32(header[:16]))
			},
			wantErr: page.ErrVersionMismatch,
		},
		{
			name: "torn",
			change: func(header []byte) {
				clear(header[17:])
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			walFile, err := os.CreateTemp(t.TempDir(), "wal")
			if err != nil {
				t.Fatal(err)
			}
			defer walFile.Close()

			header := encodeWALFileHeader(1)
			testCase.change(header[:])
			if _, err := walFile.Write(header[:]); err != nil {
				t.Fatal(err)
			}

			log := New(Config{File: walFile, PageSize: 64})
			if records, err := log.ReadRecords(); !errors.Is(err, testCase.wantErr) || len(records) != 0 {
				t.Fatalf("ReadRecords: %d records, error %v, want error %v", len(records), err, testCase.wantErr)
			}
		})
	}
}

func TestWALReadRecords_IgnoresRecordsFromEarlierGeneration(t *testing.T) {
	path := t.TempDir() + "/wal"
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	log := New(Config{File: file, PageSize: 64, SyncOnCommit: true, CheckpointThresholdBytes: 1 << 20})
	// The first generation has three transactions of equal size.
	for _, value := range []string{"old-1", "old-2", "old-3"} {
		if _, err := log.Commit(nil, nil, []byte(value), nil); err != nil {
			t.Fatal(err)
		}
	}
	earlier, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// A checkpoint clears the WAL. The second generation has one transaction of the same size.
	if err := log.Truncate(); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Commit(nil, nil, []byte("new-1"), nil); err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(current) >= len(earlier) {
		t.Fatalf("second generation size %d, want less than %d", len(current), len(earlier))
	}

	// A crash kept the new bytes but lost the truncate, so the earlier tail follows the new transaction.
	crashed := append(bytes.Clone(current), earlier[len(current):]...)
	crashPath := t.TempDir() + "/crashed-wal"
	if err := os.WriteFile(crashPath, crashed, 0600); err != nil {
		t.Fatal(err)
	}
	crashFile, err := os.Open(crashPath)
	if err != nil {
		t.Fatal(err)
	}
	defer crashFile.Close()
	records, err := New(Config{File: crashFile, PageSize: 64}).ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Fatalf("recovered %d records, want the 3 records of the second generation", len(records))
	}
	newTxID := records[0].Header.TxID
	for _, record := range records {
		if record.Header.TxID != newTxID {
			t.Fatalf("recovered transaction %d from an earlier generation after transaction %d", record.Header.TxID, newTxID)
		}
	}
	if !bytes.Equal(records[0].Payload, []byte("new-1")) {
		t.Fatalf("recovered payload %q, want %q", records[0].Payload, "new-1")
	}
}

func TestWALReadRecords_ZeroFilledHeaderIsEmpty(t *testing.T) {
	header := encodeWALFileHeader(1)
	for _, test := range []struct {
		name string
		data []byte
	}{
		// A crash during the first commit of a generation can keep the file size without the written bytes.
		{name: "zero-filled", data: make([]byte, 256)},
		// A crash can also keep only part of the header.
		{name: "short", data: header[:12]},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := t.TempDir() + "/wal"
			if err := os.WriteFile(path, test.data, 0600); err != nil {
				t.Fatal(err)
			}
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			records, err := New(Config{File: file, PageSize: 64}).ReadRecords()
			if err != nil || len(records) != 0 {
				t.Fatalf("incomplete WAL header: got %d records and error %v, want an empty WAL", len(records), err)
			}
		})
	}
}

func TestWALCommit_NewGenerationChangesSalt(t *testing.T) {
	path := t.TempDir() + "/wal"
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	log := New(Config{File: file, PageSize: 64, CheckpointThresholdBytes: 1 << 20})
	if _, err := log.Commit(nil, nil, []byte("first"), nil); err != nil {
		t.Fatal(err)
	}
	first := log.checksumSeed
	// A commit after a checkpoint starts a new generation in the same WAL.
	if err := log.Truncate(); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Commit(nil, nil, []byte("second"), nil); err != nil {
		t.Fatal(err)
	}
	second := log.checksumSeed
	if second == first {
		t.Fatalf("generation after truncate: seed %d, previous %d", second, first)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if seed, err := RecordChecksumSeed(data); err != nil || seed != second {
		t.Fatalf("file header seed: got %d and error %v, want %d", seed, err, second)
	}

	// A reopened database reads the old salt before it clears the WAL, so its next generation differs too.
	reopened := New(Config{File: file, PageSize: 64, CheckpointThresholdBytes: 1 << 20})
	if _, err := reopened.ReadRecords(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Truncate(); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Commit(nil, nil, []byte("third"), nil); err != nil {
		t.Fatal(err)
	}
	if third := reopened.checksumSeed; third == second {
		t.Fatalf("generation after reopen: seed %d, previous %d", third, second)
	}
}

func TestWALCommit_DoesNotUseCurrentFilePosition(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	log := New(Config{
		File:                     walFile,
		PageSize:                 64,
		CheckpointThresholdBytes: 1 << 20,
	})
	first := []byte("first")
	if _, err := log.Commit(nil, nil, first, nil); err != nil {
		t.Fatal(err)
	}
	firstSize, err := walFile.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}

	const unrelatedPosition = int64(3)
	if _, err := walFile.Seek(unrelatedPosition, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	second := []byte("second")
	if _, err := log.Commit(nil, nil, second, nil); err != nil {
		t.Fatal(err)
	}

	position, err := walFile.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if position != unrelatedPosition {
		t.Fatalf("file position after commit: got %d, want %d", position, unrelatedPosition)
	}
	info, err := walFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() <= firstSize {
		t.Fatalf("WAL size after second commit: got %d, want more than %d", info.Size(), firstSize)
	}
}

func TestWALCommit_SyncFailureRestoresAppendOffset(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	log := New(Config{
		File:                     walFile,
		PageSize:                 64,
		SyncOnCommit:             true,
		CheckpointThresholdBytes: 1 << 20,
	})
	syncErr := errors.New("injected sync failure")
	log.syncFile = func() error { return syncErr }
	encodedMeta := []byte("value")
	if _, err := log.Commit(nil, nil, encodedMeta, nil); !errors.Is(err, syncErr) {
		t.Fatalf("first commit error: got %v, want %v", err, syncErr)
	}
	if log.appendOffset != 0 {
		t.Fatalf("append offset after failed commit: got %d, want 0", log.appendOffset)
	}
	if got := log.Stats().TotalBytesWritten; got != 0 {
		t.Fatalf("total WAL bytes after failed commit: got %d, want 0", got)
	}

	log.syncFile = nil
	if _, err := log.Commit(nil, nil, encodedMeta, nil); !errors.Is(err, ErrWriteFailed) {
		t.Fatalf("commit after failed append: got %v, want ErrWriteFailed", err)
	}

	// A reopened database gets a new WAL. It appends at the restored offset.
	reopened := New(Config{
		File:                     walFile,
		PageSize:                 64,
		SyncOnCommit:             true,
		CheckpointThresholdBytes: 1 << 20,
	})
	if _, err := reopened.ReadRecords(); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Commit(nil, nil, encodedMeta, nil); err != nil {
		t.Fatal(err)
	}
	info, err := walFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != reopened.appendOffset {
		t.Fatalf("WAL size after retry: got %d, append offset %d", info.Size(), reopened.appendOffset)
	}
	if got := reopened.Stats().TotalBytesWritten; got != uint64(info.Size()) {
		t.Fatalf("total WAL bytes after retry: got %d, want %d", got, info.Size())
	}
}

func TestWALCommit_SyncFailureDoesNotUseFilePosition(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	log := New(Config{
		File:                     walFile,
		PageSize:                 64,
		SyncOnCommit:             true,
		CheckpointThresholdBytes: 1 << 20,
	})
	syncErr := errors.New("injected sync failure")
	log.syncFile = func() error { return syncErr }
	const unrelatedPosition = int64(3)
	if _, err := walFile.Seek(unrelatedPosition, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	encodedMeta := []byte("value")
	if _, err := log.Commit(nil, nil, encodedMeta, nil); !errors.Is(err, syncErr) {
		t.Fatalf("commit error: got %v, want %v", err, syncErr)
	}
	position, err := walFile.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if position != unrelatedPosition {
		t.Fatalf("file position after rollback: got %d, want %d", position, unrelatedPosition)
	}
}

func TestWALTruncate_ResetsAppendOffset(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	log := New(Config{
		File:                     walFile,
		PageSize:                 64,
		CheckpointThresholdBytes: 1 << 20,
	})
	encodedMeta := []byte("value")
	if _, err := log.Commit(nil, nil, encodedMeta, nil); err != nil {
		t.Fatal(err)
	}
	if log.appendOffset == 0 {
		t.Fatal("commit did not advance append offset")
	}

	if err := log.Truncate(); err != nil {
		t.Fatal(err)
	}
	if log.appendOffset != 0 {
		t.Fatalf("append offset after truncate: got %d, want 0", log.appendOffset)
	}
	if _, err := log.Commit(nil, nil, encodedMeta, nil); err != nil {
		t.Fatal(err)
	}
	info, err := walFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != log.appendOffset {
		t.Fatalf("WAL size after truncate and commit: got %d, append offset %d", info.Size(), log.appendOffset)
	}
}

// A failed truncate can still change the size, so the next commit must not append at the old offset.
// It starts a new generation at offset zero and syncs the header first.
func TestWALTruncate_FailureStartsANewGeneration(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	log := &WAL{file: walFile, appendOffset: 73}
	if err := walFile.Close(); err != nil {
		t.Fatal(err)
	}

	if err := log.Truncate(); err == nil {
		t.Fatal("truncate on closed WAL succeeded")
	}
	if log.appendOffset != 0 || log.fileKnownEmpty || !log.headerSyncNeeded {
		t.Fatalf("after a failed truncate: append offset %d, file known empty %t, header sync needed %t", log.appendOffset, log.fileKnownEmpty, log.headerSyncNeeded)
	}
}

// A new generation syncs its header only when storage can still hold records of an earlier generation.
// That is true for the first generation of a handle and after a truncate without a sync.
func TestWALCommit_SyncsHeaderOnlyAfterAnUnsyncedTruncate(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()
	log := New(Config{File: walFile, PageSize: 64, SyncOnCommit: true, SyncOnCheckpoint: true, CheckpointThresholdBytes: 1 << 20})
	syncs := 0
	log.SetSyncFileForTesting(func() error { syncs++; return nil })
	expectSyncs := func(step string, want int, run func() error) {
		t.Helper()
		syncs = 0
		if err := run(); err != nil {
			t.Fatalf("%s: %v", step, err)
		}
		if syncs != want {
			t.Fatalf("%s: got %d syncs, want %d", step, syncs, want)
		}
	}
	commit := func() error {
		_, err := log.Commit(nil, nil, []byte("value"), nil)
		return err
	}

	expectSyncs("first commit of the handle", 2, commit)
	expectSyncs("next commit", 1, commit)
	expectSyncs("sync without a truncate", 0, log.SyncTruncate)
	if err := log.Truncate(); err != nil {
		t.Fatal(err)
	}
	expectSyncs("first commit after an unsynced truncate", 2, commit)
	if err := log.Truncate(); err != nil {
		t.Fatal(err)
	}
	expectSyncs("sync of the truncate", 1, log.SyncTruncate)
	expectSyncs("first commit after a synced truncate", 1, commit)
}

// Open truncates the WAL of every writable handle.
// A WAL that was empty at open needs no system call, but a WAL with any bytes, even a torn header, does.
func TestWALTruncate_SkipsOnlyAFileKnownToBeEmpty(t *testing.T) {
	header := encodeWALFileHeader(1)
	for _, test := range []struct {
		name     string
		content  []byte
		wantCall bool
	}{
		{name: "empty"},
		{name: "torn header", content: header[:3], wantCall: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			walFile, err := os.CreateTemp(t.TempDir(), "wal")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := walFile.Write(test.content); err != nil {
				t.Fatal(err)
			}
			log := New(Config{Path: walFile.Name(), File: walFile, PageSize: 4096})
			records, err := log.ReadRecords()
			if err != nil || len(records) != 0 {
				t.Fatalf("read records: got %d records and error %v, want none", len(records), err)
			}
			// A closed file fails every system call, so Truncate returns nil only when it makes none.
			if err := walFile.Close(); err != nil {
				t.Fatal(err)
			}
			err = log.Truncate()
			if called := err != nil; called != test.wantCall {
				t.Fatalf("Truncate made a system call: %t (error %v), want %t", called, err, test.wantCall)
			}
		})
	}
}

func TestWALFailedAppendBlocksLaterCommits(t *testing.T) {
	for _, test := range []struct {
		name       string
		closedFile bool
	}{
		{name: "sync failure"},
		// A closed file fails the write and the rollback truncate.
		{name: "write and rollback failure", closedFile: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			walFile, err := os.CreateTemp(t.TempDir(), "wal")
			if err != nil {
				t.Fatal(err)
			}
			defer walFile.Close()
			log := New(Config{File: walFile, PageSize: 64, SyncOnCommit: true, CheckpointThresholdBytes: 1 << 20})
			syncErr := errors.New("injected sync failure")
			log.syncFile = func() error { return syncErr }
			if test.closedFile {
				if err := walFile.Close(); err != nil {
					t.Fatal(err)
				}
			}

			_, failure := log.Commit(nil, nil, []byte("value"), nil)
			if !errors.Is(failure, ErrWriteFailed) || log.AppendFailure() != failure {
				t.Fatalf("failed append: got %v, stored %v, want ErrWriteFailed", failure, log.AppendFailure())
			}
			if !test.closedFile && !errors.Is(failure, syncErr) {
				t.Fatalf("failed append: got %v, want the sync error %v", failure, syncErr)
			}
			log.syncFile = nil
			if !test.closedFile {
				if err := log.Truncate(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := log.Commit(nil, nil, []byte("value"), nil); err != failure {
				t.Fatalf("commit after failed append: got %v, want the first failure %v", err, failure)
			}
		})
	}
}

func TestWALCommitErrorBeforeAppendDoesNotBlockLaterCommits(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()
	log := New(Config{File: walFile, PageSize: 64, SyncOnCommit: true, CheckpointThresholdBytes: 1 << 20})

	if _, err := log.Commit(nil, nil, make([]byte, 65), nil); !errors.Is(err, ErrRecordPayloadTooLarge) || errors.Is(err, ErrWriteFailed) {
		t.Fatalf("oversized metadata: got %v, want only ErrRecordPayloadTooLarge", err)
	}
	if log.AppendFailure() != nil {
		t.Fatalf("encoding error stopped the WAL: %v", log.AppendFailure())
	}
	if _, err := log.Commit(nil, nil, []byte("value"), nil); err != nil {
		t.Fatalf("commit after encoding error: %v", err)
	}
}

func TestWALReadRecords_InitializesAppendOffsetForRecovery(t *testing.T) {
	path := t.TempDir() + "/wal"
	walFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}
	firstLog := New(Config{
		File:                     walFile,
		PageSize:                 64,
		CheckpointThresholdBytes: 1 << 20,
	})
	first := []byte("first")
	if _, err := firstLog.Commit(nil, nil, first, nil); err != nil {
		t.Fatal(err)
	}
	firstInfo, err := walFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err := walFile.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedFile, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedFile.Close()
	recoveredLog := New(Config{
		File:                     reopenedFile,
		PageSize:                 64,
		CheckpointThresholdBytes: 1 << 20,
	})
	records, err := recoveredLog.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Fatalf("recovered record count: got %d, want 3", len(records))
	}
	if recoveredLog.appendOffset != firstInfo.Size() {
		t.Fatalf("recovered append offset: got %d, want %d", recoveredLog.appendOffset, firstInfo.Size())
	}

	second := []byte("second")
	if _, err := recoveredLog.Commit(nil, nil, second, nil); err != nil {
		t.Fatal(err)
	}
	secondInfo, err := reopenedFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if secondInfo.Size() <= firstInfo.Size() {
		t.Fatalf("WAL size after recovered append: got %d, want more than %d", secondInfo.Size(), firstInfo.Size())
	}
}

func TestWALReadRecords_StopsAtDamagedRecord(t *testing.T) {
	for _, name := range []string{"incomplete", "committed"} {
		t.Run(name, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "wal")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			log := New(Config{File: file, PageSize: 4096})
			if _, err := log.Commit(nil, nil, []byte("stable"), nil); err != nil {
				t.Fatal(err)
			}
			start := log.appendOffset
			// Later records must stay discarded even when a complete marker follows.
			if _, err := log.Commit(nil, nil, bytes.Repeat([]byte("p"), 2020), nil); err != nil {
				t.Fatal(err)
			}
			if _, err := file.WriteAt([]byte("x"), start+HeaderSize); err != nil {
				t.Fatal(err)
			}
			if name == "incomplete" {
				if err := file.Truncate(log.appendOffset - HeaderSize - ChecksumSize); err != nil {
					t.Fatal(err)
				}
			}
			records, err := log.ReadRecords()
			if err != nil || len(records) != 3 || !bytes.Equal(records[0].Payload, []byte("stable")) {
				t.Fatalf("earlier committed transaction: got %+v, %v", records, err)
			}
		})
	}
}

func TestWALReadRecords_ReturnsStatError(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	log := New(Config{File: walFile, PageSize: 64})
	if err := walFile.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := log.ReadRecords(); err == nil {
		t.Fatal("read records on closed WAL returned no error")
	}
}

func TestWALReadRecords_AllowsMissingReadOnlyWAL(t *testing.T) {
	log := New(Config{PageSize: 4096})
	records, err := log.ReadRecords()
	if err != nil {
		t.Fatalf("read records without WAL file: %v", err)
	}
	if records != nil {
		t.Fatalf("records without WAL file: got %v, want nil", records)
	}
}

func TestWALEncodeTransaction_ReusesBuffer(t *testing.T) {
	log := &WAL{
		pageSize: 64,
		nextTxid: 1,
	}
	encodedMeta := []byte("value")
	if _, err := log.encodeTransaction(nil, nil, encodedMeta, log.nextTxid); err != nil {
		t.Fatal(err)
	}

	allocations := testing.AllocsPerRun(100, func() {
		if _, err := log.encodeTransaction(nil, nil, encodedMeta, log.nextTxid); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("allocations per warmed encoding: got %.0f, want 0", allocations)
	}
}

func TestWALEncodeTransaction_ReusesBytePatchBuffers(t *testing.T) {
	base := btree.NewLeafNode(2)
	if err := base.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("old-value"))); err != nil {
		t.Fatal(err)
	}
	target := base.Clone()
	if err := target.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("new-value"))); err != nil {
		t.Fatal(err)
	}

	log := New(Config{PageSize: 128})
	log.LoadCommittedRecord(WALRecord{Header: RecordHeader{Type: RecordTypeNode, PageID: base.PageID()}, Payload: btree.EncodeWALNode(base)})
	nodes := []NodeRecord{{Original: base, Final: target}}
	if _, err := log.encodeTransaction(nodes, nil, nil, log.nextTxid); err != nil {
		t.Fatal(err)
	}

	allocations := testing.AllocsPerRun(100, func() {
		if _, err := log.encodeTransaction(nodes, nil, nil, log.nextTxid); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("allocations per warmed entry-patch encoding: got %.0f, want 0", allocations)
	}
}

func TestWALCommit_EncodesSmallerBytePatchAndPublishesFinalNode(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	base := btree.NewLeafNode(2)
	if err := base.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("old-value"))); err != nil {
		t.Fatal(err)
	}
	target := btree.NewLeafNode(2)
	if err := target.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("new-value"))); err != nil {
		t.Fatal(err)
	}
	log := New(Config{File: walFile, PageSize: 128, CheckpointThresholdBytes: 1 << 20})
	if _, err := log.Commit([]NodeRecord{{Final: base}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	nodes := []NodeRecord{{
		Original: base,
		Final:    target,
	}}
	if _, err := log.Commit(nodes, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	decoded, err := log.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 4 || decoded[0].Header.Type != RecordTypeNode || decoded[2].Header.Type != RecordTypeBytePatch {
		t.Fatalf("decoded WAL records: got %+v, want a complete image followed by a byte patch", decoded)
	}
	patched, err := ApplyBytePatch(btree.EncodeWALNode(base), decoded[2].Payload, 128)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(patched, btree.EncodeWALNode(target)) {
		t.Fatal("byte patch did not rebuild the final node")
	}
	published, ok := log.Lookup(target.PageID())
	if !ok || published.Node != target || published.Payload != nil {
		t.Fatal("committed overlay did not keep only the final node")
	}
}

func TestWALCommit_EncodesEntryPatchWhenEntryCountChanges(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	base := btree.NewLeafNode(2)
	if err := base.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("old"))); err != nil {
		t.Fatal(err)
	}
	target := base.Clone()
	if err := target.InsertEntry(btree.NewEntry(0, []byte("next"), []byte("value"))); err != nil {
		t.Fatal(err)
	}
	log := New(Config{File: walFile, PageSize: 128, CheckpointThresholdBytes: 1 << 20})
	if _, err := log.Commit([]NodeRecord{{Final: base}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Commit([]NodeRecord{{
		Original: base,
		Final:    target,
	}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	decoded, err := log.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 4 || decoded[2].Header.Type != RecordTypeEntryPatch {
		t.Fatalf("decoded WAL records: got %+v, want an entry patch after the complete image", decoded)
	}
	if got, want := decoded[2].Payload, appendEntryPatchSet(nil, 0, []byte("next"), []byte("value")); !bytes.Equal(got, want) {
		t.Fatalf("entry patch: got %v, want %v", got, want)
	}
	rebuilt := base.Clone()
	if err := ApplyEntryPatch(rebuilt, decoded[2].Payload); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(btree.EncodeWALNode(rebuilt), btree.EncodeWALNode(target)) {
		t.Fatal("entry patch did not rebuild the final node")
	}
	published, ok := log.Lookup(target.PageID())
	if !ok || published.Node != target {
		t.Fatalf("committed overlay: got %+v, want the final node", published)
	}
}

func TestWALCommit_EncodesPatchForBranchNode(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	base := btree.NewRootNode(2, btree.NewLeafNode(3), btree.NewLeafNode(4), []byte("middle"))
	target := base.Clone()
	target.Children[1] = 5
	log := New(Config{File: walFile, PageSize: 128, CheckpointThresholdBytes: 1 << 20})
	if _, err := log.Commit([]NodeRecord{{Final: base}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Commit([]NodeRecord{{Original: base, Final: target}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	decoded, err := log.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 4 || decoded[0].Header.Type != RecordTypeNode || decoded[2].Header.Type != RecordTypeBytePatch {
		t.Fatalf("decoded WAL records: got %+v, want a complete image followed by a byte patch", decoded)
	}
	patched, err := ApplyBytePatch(btree.EncodeWALNode(base), decoded[2].Payload, 128)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(patched, btree.EncodeWALNode(target)) {
		t.Fatal("byte patch did not rebuild the final branch node")
	}
}

func TestWALCommit_DoesNotRetainLargeEncodingBuffer(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	const retentionLimit = maxRetainedEncodingBufferBytes
	log := New(Config{
		File:                     walFile,
		PageSize:                 4 << 20,
		CheckpointThresholdBytes: 4 << 20,
	})
	encodedMeta := make([]byte, retentionLimit+1)
	if _, err := log.Commit(nil, nil, encodedMeta, nil); err != nil {
		t.Fatal(err)
	}
	if capacity := cap(log.encodingBuffer); capacity > retentionLimit {
		t.Fatalf("retained encoding capacity: got %d, want at most %d", capacity, retentionLimit)
	}
}

func TestWALEncodeTransaction_ReusesLargeEncodingBuffer(t *testing.T) {
	// A garbage collection can free the pooled buffer. Stop collections so the reuse is certain.
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	const pageSize = 4096
	nodes := make([]NodeRecord, 2*maxRetainedEncodingBufferBytes/pageSize)
	for index := range nodes {
		node := btree.NewLeafNode(page.ID(3 + index))
		if err := node.InsertEntry(btree.NewEntry(0, []byte("key"), make([]byte, 3900))); err != nil {
			t.Fatal(err)
		}
		nodes[index] = NodeRecord{Final: node}
	}
	log := &WAL{pageSize: pageSize}
	first, err := log.encodeTransaction(nodes, nil, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) <= maxRetainedEncodingBufferBytes {
		t.Fatalf("transaction size: got %d, want more than %d", len(first), maxRetainedEncodingBufferBytes)
	}
	// The race detector makes sync.Pool drop some items, so try more than once.
	previous := first
	for attempt := range 20 {
		log.releaseEncodingBuffer()
		if capacity := cap(log.encodingBuffer); capacity > maxRetainedEncodingBufferBytes {
			t.Fatalf("retained encoding capacity: got %d, want at most %d", capacity, maxRetainedEncodingBufferBytes)
		}
		next, err := log.encodeTransaction(nodes, nil, nil, TxID(attempt+2))
		if err != nil {
			t.Fatal(err)
		}
		if &next[0] == &previous[0] {
			return
		}
		previous = next
	}
	t.Fatal("later large transactions did not reuse the pooled encoding buffer")
}

func TestWALCommit_RetainsBenchmarkSizedEncodingBuffer(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	const transactionBytes = 1400 << 10
	log := New(Config{
		File:                     walFile,
		PageSize:                 transactionBytes,
		CheckpointThresholdBytes: 4 << 20,
	})
	const recordOverhead = HeaderSize + ChecksumSize
	encodedMeta := make([]byte, (transactionBytes-3*recordOverhead+1)/2)
	if _, err := log.Commit(nil, nil, encodedMeta, nil); err != nil {
		t.Fatal(err)
	}
	if capacity := cap(log.encodingBuffer); capacity < transactionBytes || capacity > maxRetainedEncodingBufferBytes {
		t.Fatalf("retained encoding capacity: got %d, want %d..%d", capacity, transactionBytes, maxRetainedEncodingBufferBytes)
	}
}

func TestWALCommit_ReusesAllTransactionStorage(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	log := New(Config{
		File:                     walFile,
		PageSize:                 64,
		CheckpointThresholdBytes: ^uint64(0),
	})
	encodedMeta := []byte("value")
	if _, err := log.Commit(nil, nil, encodedMeta, nil); err != nil {
		t.Fatal(err)
	}

	allocations := testing.AllocsPerRun(100, func() {
		if _, err := log.Commit(nil, nil, encodedMeta, nil); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("allocations per warmed commit: got %.0f, want 0", allocations)
	}
}

func TestWALCommit_PublishesBothMetadataPages(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	log := New(Config{
		File:                     walFile,
		PageSize:                 64,
		CheckpointThresholdBytes: 1 << 20,
	})
	encodedMeta := []byte("metadata")

	needsCheckpoint, err := log.Commit(nil, nil, encodedMeta, nil)
	if err != nil {
		t.Fatal(err)
	}
	if needsCheckpoint {
		t.Fatal("small transaction reached the checkpoint threshold")
	}
	stats := log.Stats()
	info, err := walFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if stats.TotalBytesWritten != uint64(info.Size()) {
		t.Fatalf("total WAL bytes written: got %d, want %d", stats.TotalBytesWritten, info.Size())
	}
	if got := stats.CommittedRecordCount; got != 2 {
		t.Fatalf("committed record count: got %d, want 2", got)
	}
	for _, pageID := range [...]page.ID{page.Meta0ID, page.Meta1ID} {
		record, ok := log.CommittedRecord(pageID)
		if !ok || record.Header.Type != RecordTypeMeta || !bytes.Equal(record.Payload, encodedMeta) {
			t.Fatalf("metadata page %d: found=%t record=%+v", pageID, ok, record)
		}
	}
}

func TestWALCommit_WritesTreeAllocationAndMetadataTogether(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()
	mainFile, err := os.CreateTemp(t.TempDir(), "main")
	if err != nil {
		t.Fatal(err)
	}
	defer mainFile.Close()

	log := New(Config{File: walFile, PageSize: 64})
	image := bytes.Repeat([]byte{0x5a}, 64)
	node := btree.NewLeafNode(3)
	if _, err := log.Commit([]NodeRecord{{Final: node}}, []AllocationRecord{{PageID: 2, Payload: image}}, []byte("metadata"), nil); err != nil {
		t.Fatal(err)
	}
	records, err := log.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 5 || records[0].Header.Type != RecordTypeNode || records[1].Header.Type != RecordTypeAllocation || records[1].Header.PageID != 2 || !bytes.Equal(records[1].Payload, image) || !IsCommitMarker(&records[4]) {
		t.Fatalf("WAL transaction: got %+v", records)
	}
	if committed, ok := log.CommittedRecord(2); !ok || committed.Header.Type != RecordTypeAllocation || !bytes.Equal(committed.Payload, image) {
		t.Fatalf("committed allocation page: got %+v, found %t", committed, ok)
	}
	if err := log.Checkpoint(mainFile, 4*64, true); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(image))
	if _, err := mainFile.ReadAt(got, 2*64); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, image) {
		t.Fatal("checkpoint did not write the allocation page")
	}
}

func TestWALCommit_InvalidPageDoesNotPublishOrAppend(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	log := New(Config{File: walFile, PageSize: 64})
	if _, err := log.Commit(nil, []AllocationRecord{{PageID: 2, Payload: make([]byte, 63)}}, nil, nil); !errors.Is(err, page.ErrInvalid) {
		t.Fatalf("invalid allocation page error: got %v", err)
	}
	if _, ok := log.CommittedRecord(2); ok {
		t.Fatal("invalid allocation page was published")
	}
	info, err := walFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("WAL file after invalid page: got %d bytes, want 0", info.Size())
	}
}

func TestWALCheckpoint_WritesCommittedPagesToMainFile(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()
	mainFile, err := os.CreateTemp(t.TempDir(), "main")
	if err != nil {
		t.Fatal(err)
	}
	defer mainFile.Close()

	log := New(Config{File: walFile, PageSize: 64})
	nodes := make([]NodeRecord, 0, 32)
	for index := range 32 {
		pageID := page.ID(33 - index)
		node := btree.NewLeafNode(pageID)
		if err := node.InsertEntry(btree.NewEntry(0, []byte("key"), []byte{byte(pageID)})); err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, NodeRecord{Final: node})
	}
	if _, err := log.Commit(nodes, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	writtenBytes := log.Stats().TotalBytesWritten

	if err := log.Checkpoint(mainFile, 34*64, true); err != nil {
		t.Fatal(err)
	}
	stats := log.Stats()
	if stats.TotalBytesWritten != writtenBytes {
		t.Fatalf("total WAL bytes after checkpoint: got %d, want %d", stats.TotalBytesWritten, writtenBytes)
	}
	if got := stats.CheckpointCount; got != 1 {
		t.Fatalf("checkpoint count: got %d, want 1", got)
	}
	if err := log.Checkpoint(mainFile, 34*64, true); err != nil {
		t.Fatal(err)
	}
	if got := log.Stats().CheckpointCount; got != 1 {
		t.Fatalf("checkpoint count after empty checkpoint: got %d, want 1", got)
	}
	for pageID := page.ID(2); pageID <= 33; pageID++ {
		pageContent := make([]byte, 64)
		if _, err := mainFile.ReadAt(pageContent, int64(pageID)*64); err != nil {
			t.Fatal(err)
		}
		node, err := btree.DecodeNode(pageContent, pageID, 64, nil, true)
		if err != nil {
			t.Fatalf("decode checkpoint page %d: %v", pageID, err)
		}
		entry, found, err := node.FindEntryRef([]byte("key"))
		if err != nil || !found || !bytes.Equal(entry.Value(), []byte{byte(pageID)}) {
			t.Fatalf("checkpoint page %d entry: found=%t value=%v err=%v", pageID, found, entry.Value(), err)
		}
	}
}

func TestWALCheckpoint_MissingLastPageKeepsWAL(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()
	mainFile, err := os.CreateTemp(t.TempDir(), "main")
	if err != nil {
		t.Fatal(err)
	}
	defer mainFile.Close()

	log := New(Config{File: walFile, PageSize: 64})
	if _, err := log.Commit([]NodeRecord{{Final: btree.NewLeafNode(3)}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := log.Checkpoint(mainFile, 5*64, true); !errors.Is(err, page.ErrInvalid) {
		t.Fatalf("checkpoint with missing last page: got %v, want %v", err, page.ErrInvalid)
	}
	if _, ok := log.CommittedRecord(3); !ok {
		t.Fatal("failed checkpoint removed the committed page")
	}
	if err := log.Checkpoint(mainFile, 4*64, true); err != nil {
		t.Fatal(err)
	}
}

func TestWALRecordCodec_UsesFixedLittleEndianLayout(t *testing.T) {
	record := &WALRecord{
		Header: RecordHeader{
			Type:   RecordTypeNode,
			PageID: page.ID(1),
			TxID:   TxID(2),
		},
		Payload: []byte("xy"),
	}
	// Layout: record type, page ID, transaction ID, content length, content,
	// and the CRC32C checksum of the little-endian generation salt and all preceding bytes.
	want := []byte{
		3,
		1, 0, 0, 0, 0, 0, 0, 0,
		2, 0, 0, 0, 0, 0, 0, 0,
		2, 0, 0, 0,
		'x', 'y',
	}
	const salt uint64 = 0x0807060504030201
	salted := append([]byte{1, 2, 3, 4, 5, 6, 7, 8}, want...)
	wantChecksum := crc32.Checksum(salted, crc32.MakeTable(crc32.Castagnoli))
	want = binary.LittleEndian.AppendUint32(want, wantChecksum)

	seed := saltChecksumSeed(salt)
	encoded, err := EncodeWALRecord(record, int64(len(record.Payload)), seed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, want) {
		t.Fatalf("encode WAL record: got %x, want %x", encoded, want)
	}

	decoded, err := DecodeWALRecord(bytes.NewReader(encoded), int64(len(record.Payload)), seed)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Header != record.Header || !bytes.Equal(decoded.Payload, record.Payload) {
		t.Fatalf("decode WAL record: got %+v, want %+v", decoded, record)
	}
	// The seed of another generation must reject the record.
	if _, err := DecodeWALRecord(bytes.NewReader(encoded), int64(len(record.Payload)), seed+1); !errors.Is(err, page.ErrChecksum) {
		t.Fatalf("decode with another generation: got %v, want checksum error", err)
	}
}

func TestWAL_ByteCounterDoesNotWrapAtMaxInt64(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	const largestInt64 = 1<<63 - 1
	log := &WAL{
		file:                     walFile,
		bytesSinceCheckpoint:     largestInt64 - 1,
		checkpointThresholdBytes: largestInt64,
	}

	// Two bytes move the counter from one byte below the signed boundary to one
	// byte above it.
	needsCheckpoint, err := log.appendTransaction([]byte{0, 0})
	if err != nil {
		t.Fatal(err)
	}
	if !needsCheckpoint {
		t.Fatal("WAL counter crossed MaxInt64 without starting a checkpoint")
	}
	if log.bytesSinceCheckpoint <= largestInt64 {
		t.Fatalf("WAL byte counter wrapped to %d", log.bytesSinceCheckpoint)
	}
}

func TestWAL_EncodesTransactionWithoutDatabase(t *testing.T) {
	const (
		pageSize      int64 = 64 // The record content must fit inside this page size.
		transactionID TxID  = 1  // A new WAL starts with transaction ID one.
	)

	log := &WAL{
		pageSize: pageSize,
		nextTxid: transactionID,
	}
	transaction, err := log.encodeTransaction(nil, nil, []byte("meta"), transactionID)
	if err != nil {
		t.Fatal(err)
	}
	reader := bytes.NewReader(transaction)
	firstMeta, err := DecodeWALRecord(reader, pageSize, log.checksumSeed)
	if err != nil {
		t.Fatal(err)
	}
	secondMeta, err := DecodeWALRecord(reader, pageSize, log.checksumSeed)
	if err != nil {
		t.Fatal(err)
	}
	commitMarker, err := DecodeWALRecord(reader, pageSize, log.checksumSeed)
	if err != nil {
		t.Fatal(err)
	}
	if firstMeta.Header.TxID != transactionID || secondMeta.Header.TxID != transactionID || commitMarker.Header.TxID != transactionID {
		t.Fatalf("transaction IDs: first metadata=%d second metadata=%d commit=%d, want %d", firstMeta.Header.TxID, secondMeta.Header.TxID, commitMarker.Header.TxID, transactionID)
	}
	if firstMeta.Header.Type != RecordTypeMeta || firstMeta.Header.PageID != page.Meta0ID || secondMeta.Header.Type != RecordTypeMeta || secondMeta.Header.PageID != page.Meta1ID {
		t.Fatalf("metadata records: first=%+v second=%+v", firstMeta.Header, secondMeta.Header)
	}
	if !IsCommitMarker(commitMarker) {
		t.Fatal("encoded transaction does not end with a commit marker")
	}
}

func TestWALCommit_ContinuedNodeSyncFailureAndRetry(t *testing.T) {
	const pageSize int64 = 64
	original := btree.NewLeafNode(3)
	if err := original.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("old"))); err != nil {
		t.Fatal(err)
	}
	final := original.Clone()
	if err := final.InsertEntry(btree.NewEntry(0, []byte("key"), make([]byte, 200))); err != nil {
		t.Fatal(err)
	}
	ids := make([]page.ID, btree.NodePageCount(final, pageSize))
	ids[0] = final.PageID()
	for index := 1; index < len(ids); index++ {
		ids[index] = page.ID(400 - index*7)
	}
	final.SetOverflowPages(ids[1:])
	file, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	log := New(Config{File: file, PageSize: pageSize, SyncOnCommit: true})
	if _, err := log.Commit([]NodeRecord{{Final: original}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	before, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	syncErr := errors.New("injected continuation sync failure")
	log.SetSyncFileForTesting(func() error { return syncErr })
	changes := []NodeRecord{{Original: original, Final: final}}
	if _, err := log.Commit(changes, nil, nil, nil); !errors.Is(err, syncErr) {
		t.Fatalf("failed commit: %v", err)
	}
	after, err := file.Stat()
	if err != nil || after.Size() != before.Size() || log.nextTxid != 2 {
		t.Fatalf("failed append was not rolled back: %v", err)
	}
	if record, ok := log.Lookup(ids[0]); !ok || record.Node != original || len(log.overlay) != 1 {
		t.Fatal("failed commit changed the overlay")
	}
	log.SetSyncFileForTesting(nil)
	if _, err := log.Commit(changes, nil, nil, nil); !errors.Is(err, ErrWriteFailed) {
		t.Fatalf("commit after failed append: got %v, want ErrWriteFailed", err)
	}

	// A reopened database gets a new WAL. Its retry reuses the transaction ID that the failed commit did not use.
	reopened := New(Config{File: file, PageSize: pageSize, SyncOnCommit: true})
	if _, err := reopened.ReadRecords(); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Commit(changes, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	records, err := reopened.ReadRecords()
	if err != nil || len(records) != len(ids)+3 || reopened.nextTxid != 3 || len(reopened.overlay) != len(ids) {
		t.Fatalf("retry: %d records, %v", len(records), err)
	}
	for _, record := range records[2 : len(records)-1] {
		if record.Header.TxID != 2 || record.Header.Type != RecordTypeNode {
			t.Fatal("retry changed the page record type or transaction ID")
		}
	}
	if !IsCommitMarker(&records[len(records)-1]) || records[len(records)-1].Header.TxID != 2 {
		t.Fatal("retry lacks its commit marker")
	}
}

func TestCommittedPageToNodeRejectsInvalidContinuationPayloads(t *testing.T) {
	const pageSize int64 = 64
	node := btree.NewLeafNode(3)
	if err := node.InsertEntry(btree.NewEntry(0, []byte("key"), make([]byte, 200))); err != nil {
		t.Fatal(err)
	}
	ids := make([]page.ID, btree.NodePageCount(node, pageSize))
	for i := range ids {
		ids[i] = page.ID(3 + i*7)
	}
	images, err := btree.EncodeWALNodePages(node, pageSize, ids)
	if err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"missing", "first-short", "middle-short", "last-short", "last-extra", "checksum", "page-id", "page-type"} {
		t.Run(fault, func(t *testing.T) {
			log := New(Config{PageSize: pageSize})
			for i, id := range ids {
				log.LoadCommittedRecord(WALRecord{Header: RecordHeader{Type: RecordTypeNode, PageID: id}, Payload: bytes.Clone(images[i])})
			}
			lastID := ids[len(ids)-1]
			record, _ := log.Lookup(lastID)
			want := btree.ErrInvalid
			switch fault {
			case "missing":
				delete(log.overlay, lastID)
				want = ErrRecordPayloadRequired
			case "first-short", "middle-short", "last-short":
				id := lastID
				if fault == "first-short" {
					id = ids[0]
				}
				if fault == "middle-short" {
					id = ids[1]
				}
				short, _ := log.Lookup(id)
				short.Payload = short.Payload[:len(short.Payload)-1]
				log.overlay[id] = short
			case "last-extra":
				record.Payload = append(record.Payload, 0)
				log.overlay[lastID] = record
			case "checksum":
				binary.LittleEndian.PutUint32(record.Payload[10:14], 1)
			case "page-id":
				binary.LittleEndian.PutUint64(record.Payload[2:10], uint64(lastID+1))
			case "page-type":
				binary.LittleEndian.PutUint16(record.Payload[:2], uint16(btree.NodeTypeLeaf))
			}
			first, _ := log.Lookup(ids[0])
			if _, err := log.CommittedPageToNode(&first); !errors.Is(err, want) {
				t.Fatalf("%s: got %v, want %v", fault, err, want)
			}
			if _, _, _, err := log.LookupCommittedPage(&first, []byte("key"), true); !errors.Is(err, want) {
				t.Fatalf("direct lookup %s: got %v, want %v", fault, err, want)
			}
		})
	}
	if _, err := btree.DecodeWALNode(images[0], ids[0], pageSize, nil); !errors.Is(err, btree.ErrInvalid) {
		t.Fatalf("missing continuation reader: %v", err)
	}
}
