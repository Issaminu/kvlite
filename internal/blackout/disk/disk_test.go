package disk

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/Issaminu/kvlite/internal/fileio"
)

const testDir = "/sim"

var testName = filepath.Join(testDir, "file")

func openRW(t *testing.T, fsys *FS, name string) fileio.File {
	t.Helper()
	file, err := fsys.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func mustWrite(t *testing.T, file fileio.File, data []byte, offset int64) {
	t.Helper()
	if _, err := file.WriteAt(data, offset); err != nil {
		t.Fatal(err)
	}
}

func mustSync(t *testing.T, file fileio.File) {
	t.Helper()
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
}

// content returns the cache view of name after a restart, or nil when the name does not exist.
func content(fsys *FS, name string) []byte {
	data, _, ok := fsys.FileData(name)
	if !ok {
		return nil
	}
	return data
}

func TestSyncedDataSurvivesPowerFailure(t *testing.T) {
	synced := bytes.Repeat([]byte{'a'}, 3000)
	for seed := range uint64(100) {
		fsys := New(seed, Config{})
		file := openRW(t, fsys, testName)
		mustWrite(t, file, synced, 0)
		mustSync(t, file)
		if err := fsys.SyncDir(testDir); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, file, bytes.Repeat([]byte{'b'}, 5000), 1000)
		fsys.Restart(true)

		got := content(fsys, testName)
		if len(got) < 1000 || !bytes.Equal(got[:1000], synced[:1000]) {
			t.Fatalf("seed %d: lost synced bytes before the unsynced write", seed)
		}
		for index := 1000; index < 3000; index++ {
			if got[index] != 'a' && got[index] != 'b' {
				t.Fatalf("seed %d: byte %d is %q, want old or new data", seed, index, got[index])
			}
		}
	}
}

func TestPowerFailureKeepsLosesOrTearsUnsyncedWrites(t *testing.T) {
	const length = 8 * 512
	var kept, lost, torn int
	for seed := range uint64(200) {
		fsys := New(seed, Config{})
		file := openRW(t, fsys, testName)
		mustWrite(t, file, make([]byte, length), 0)
		mustSync(t, file)
		if err := fsys.SyncDir(testDir); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, file, bytes.Repeat([]byte{1}, length), 0)
		fsys.Restart(true)

		got := content(fsys, testName)
		switch ones := bytes.Count(got, []byte{1}); ones {
		case length:
			kept++
		case 0:
			lost++
		default:
			torn++
			if ones%512 != 0 {
				t.Fatalf("seed %d: kept %d bytes, want whole sectors without TornSector", seed, ones)
			}
		}
	}
	if kept == 0 || lost == 0 || torn == 0 {
		t.Fatalf("kept=%d lost=%d torn=%d, want each outcome", kept, lost, torn)
	}
}

func TestPowerFailureCanKeepLaterWriteAndLoseEarlierWrite(t *testing.T) {
	for seed := range uint64(200) {
		fsys := New(seed, Config{})
		file := openRW(t, fsys, testName)
		mustWrite(t, file, make([]byte, 1024), 0)
		mustSync(t, file)
		if err := fsys.SyncDir(testDir); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, file, bytes.Repeat([]byte{1}, 512), 0)
		mustWrite(t, file, bytes.Repeat([]byte{2}, 512), 512)
		fsys.Restart(true)
		got := content(fsys, testName)
		if got[0] == 0 && got[512] == 2 {
			return
		}
	}
	t.Fatal("no seed kept the later write without the earlier write")
}

func TestPowerFailureCanKeepLengthWithoutData(t *testing.T) {
	for seed := range uint64(300) {
		fsys := New(seed, Config{})
		file := openRW(t, fsys, testName)
		mustSync(t, file)
		if err := fsys.SyncDir(testDir); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, file, bytes.Repeat([]byte{7}, 512), 0)
		fsys.Restart(true)
		got := content(fsys, testName)
		if len(got) == 512 && bytes.Equal(got, make([]byte, 512)) {
			return
		}
	}
	t.Fatal("no power failure kept the new length without its data")
}

// TestPowerFailureKeepsSectorHistoryPrefix checks that an atomic sector never keeps a later write without an earlier write to the same sector.
func TestPowerFailureKeepsSectorHistoryPrefix(t *testing.T) {
	for seed := range uint64(300) {
		fsys := New(seed, Config{})
		file := openRW(t, fsys, testName)
		mustWrite(t, file, make([]byte, 512), 0)
		mustSync(t, file)
		if err := fsys.SyncDir(testDir); err != nil {
			t.Fatal(err)
		}
		// Like a WAL header and the first record: two writes to one sector.
		mustWrite(t, file, bytes.Repeat([]byte{1}, 20), 0)
		mustWrite(t, file, bytes.Repeat([]byte{2}, 492), 20)
		fsys.Restart(true)
		got := content(fsys, testName)
		if got[0] == 0 && got[20] == 2 {
			t.Fatalf("seed %d: sector kept the second write without the first", seed)
		}
	}
}

func TestProcessCrashKeepsCacheView(t *testing.T) {
	fsys := New(1, Config{})
	file := openRW(t, fsys, testName)
	mustWrite(t, file, []byte("unsynced"), 0)
	fsys.Crash()
	if _, err := file.WriteAt([]byte("x"), 0); !errors.Is(err, ErrCrashed) {
		t.Fatalf("write after crash: %v, want ErrCrashed", err)
	}
	fsys.Restart(false)
	if got := content(fsys, testName); string(got) != "unsynced" {
		t.Fatalf("after process crash: %q", got)
	}
	if _, err := file.ReadAt(make([]byte, 1), 0); !errors.Is(err, ErrCrashed) {
		t.Fatalf("old handle after restart: %v, want ErrCrashed", err)
	}
	// The data is still unsynced, so a later power failure can lose it.
	lostOnce := false
	for seed := range uint64(50) {
		fsys := New(seed, Config{})
		file := openRW(t, fsys, testName)
		mustWrite(t, file, []byte("unsynced"), 0)
		if err := fsys.SyncDir(testDir); err != nil {
			t.Fatal(err)
		}
		fsys.Restart(false)
		fsys.Restart(true)
		if string(content(fsys, testName)) != "unsynced" {
			lostOnce = true
		}
	}
	if !lostOnce {
		t.Fatal("a power failure after a process crash never lost unsynced data")
	}
}

func TestNewNameNeedsDirectorySync(t *testing.T) {
	var vanished bool
	for seed := range uint64(50) {
		for _, syncDir := range []bool{false, true} {
			fsys := New(seed, Config{})
			file := openRW(t, fsys, testName)
			mustWrite(t, file, []byte("data"), 0)
			mustSync(t, file)
			if syncDir {
				if err := fsys.SyncDir(testDir); err != nil {
					t.Fatal(err)
				}
			}
			fsys.Restart(true)
			_, _, exists := fsys.FileData(testName)
			if syncDir && !exists {
				t.Fatalf("seed %d: synced name vanished", seed)
			}
			if !syncDir && !exists {
				vanished = true
			}
		}
	}
	if !vanished {
		t.Fatal("an unsynced name never vanished")
	}
}

func TestRemovedNameCanReturnWithDurableData(t *testing.T) {
	for seed := range uint64(50) {
		fsys := New(seed, Config{})
		file := openRW(t, fsys, testName)
		mustWrite(t, file, []byte("durable"), 0)
		mustSync(t, file)
		if err := fsys.SyncDir(testDir); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, file, []byte("DURABLE"), 0)
		if err := fsys.Remove(testName); err != nil {
			t.Fatal(err)
		}
		if _, err := fsys.OpenFile(testName, os.O_RDONLY, 0); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("open removed name: %v", err)
		}
		fsys.Restart(true)
		if got := content(fsys, testName); got != nil {
			if string(got) != "durable" && string(got) != "DURABLE" && string(got) != "DURABle" {
				// Sector decisions can only keep the whole 7-byte write or none of it.
				t.Fatalf("seed %d: returned name holds %q", seed, got)
			}
			return
		}
	}
	t.Fatal("a removed name never returned after a power failure")
}

func TestScheduledCrashStopsTheCall(t *testing.T) {
	fsys := New(3, Config{})
	file := openRW(t, fsys, testName)
	fsys.ScheduleCrash(2)
	mustWrite(t, file, []byte("first"), 0)
	if _, err := file.WriteAt([]byte("second"), 5); !errors.Is(err, ErrCrashed) {
		t.Fatalf("scheduled crash: %v, want ErrCrashed", err)
	}
	crashed, event := fsys.Crashed()
	if !crashed || event.Op != "write" {
		t.Fatalf("Crashed() = %t, %v", crashed, event)
	}
	if _, err := fsys.OpenFile(testName, os.O_RDONLY, 0); !errors.Is(err, ErrCrashed) {
		t.Fatalf("open after crash: %v, want ErrCrashed", err)
	}
	fsys.Restart(false)
	reopened, err := fsys.OpenFile(testName, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 5)
	if _, err := reopened.ReadAt(data, 0); err != nil || string(data) != "first" {
		t.Fatalf("read after restart: %q, %v", data, err)
	}
}

// TestReadAtMatchesOSFile checks the end-of-file results that KVLite depends on.
func TestReadAtMatchesOSFile(t *testing.T) {
	osFile, err := os.Create(filepath.Join(t.TempDir(), "file"))
	if err != nil {
		t.Fatal(err)
	}
	defer osFile.Close()
	simFile := openRW(t, New(5, Config{}), testName)
	for _, file := range []fileio.File{osFile, simFile} {
		mustWrite(t, file, []byte("0123456789"), 0)
	}
	for _, test := range []struct {
		length int
		offset int64
	}{{4, 0}, {4, 8}, {4, 10}, {4, 12}, {0, 10}, {0, 12}, {10, 0}} {
		wantData := make([]byte, test.length)
		wantN, wantErr := osFile.ReadAt(wantData, test.offset)
		gotData := make([]byte, test.length)
		gotN, gotErr := simFile.ReadAt(gotData, test.offset)
		if gotN != wantN || !errors.Is(gotErr, io.EOF) != !errors.Is(wantErr, io.EOF) || (gotErr == nil) != (wantErr == nil) || !bytes.Equal(gotData, wantData) {
			t.Errorf("ReadAt(len=%d, off=%d) = %d, %v, want %d, %v", test.length, test.offset, gotN, gotErr, wantN, wantErr)
		}
	}
	for _, file := range []fileio.File{osFile, simFile} {
		if err := file.Truncate(20); err != nil {
			t.Fatal(err)
		}
		info, err := file.Stat()
		if err != nil || info.Size() != 20 {
			t.Fatalf("size after extending truncate: %v, %v", info, err)
		}
	}
}

func TestSameSeedMakesSameDecisions(t *testing.T) {
	run := func() ([]byte, uint64) {
		fsys := New(42, Config{InOrder: OneIn(3)})
		file := openRW(t, fsys, testName)
		for index := range 20 {
			mustWrite(t, file, bytes.Repeat([]byte{byte(index)}, 700), int64(index*300))
			if index%7 == 0 {
				mustSync(t, file)
			}
		}
		fsys.Restart(true)
		return content(fsys, testName), fsys.TraceHash()
	}
	firstData, firstHash := run()
	secondData, secondHash := run()
	if !bytes.Equal(firstData, secondData) || firstHash != secondHash {
		t.Fatal("the same seed made different decisions")
	}
}

func TestSyncLieLosesSyncedData(t *testing.T) {
	for seed := range uint64(50) {
		fsys := New(seed, Config{SyncLie: SyncLieAllFiles})
		file := openRW(t, fsys, testName)
		mustWrite(t, file, []byte("data"), 0)
		mustSync(t, file)
		if err := fsys.SyncDir(testDir); err != nil {
			t.Fatal(err)
		}
		fsys.Restart(true)
		if !bytes.Equal(content(fsys, testName), []byte("data")) {
			return
		}
	}
	t.Fatal("SyncLieAllFiles never lost synced data")
}
