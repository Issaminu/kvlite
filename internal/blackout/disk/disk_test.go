package disk

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"syscall"
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

// TestDiscardRemovesANameForGood requires that a discarded name never returns, and that Discard changes nothing that the run depends on.
func TestDiscardRemovesANameForGood(t *testing.T) {
	for seed := range uint64(50) {
		fsys := New(seed, Config{})
		file := openRW(t, fsys, testName)
		mustWrite(t, file, []byte("durable"), 0)
		mustSync(t, file)
		if err := fsys.SyncDir(testDir); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, file, []byte("DURABLE"), 0)
		mutations, hash := fsys.Mutations(), fsys.TraceHash()
		fsys.Discard(testName)
		if fsys.Mutations() != mutations || fsys.TraceHash() != hash {
			t.Fatalf("seed %d: Discard counted a mutation or recorded an event", seed)
		}
		fsys.Restart(true)
		if got := content(fsys, testName); got != nil {
			t.Fatalf("seed %d: a discarded name returned after a power failure with %q", seed, got)
		}
	}
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

func TestFailedSyncCanDropChangesForGood(t *testing.T) {
	for seed := range uint64(100) {
		fsys := New(seed, Config{SyncFault: Always})
		file := openRW(t, fsys, testName)
		mustSync(t, file)
		if err := fsys.SyncDir(testDir); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, file, []byte("dropped"), 0)
		fsys.SetFaults(true)
		if err := file.Sync(); !errors.Is(err, ErrInjected) || !errors.Is(err, syscall.EIO) {
			t.Fatalf("failed sync: %v, want ErrInjected and EIO", err)
		}
		fsys.SetFaults(false)
		// The kernel marked the dropped pages clean, so this sync writes nothing.
		mustSync(t, file)
		fsys.Restart(true)
		if got := content(fsys, testName); !bytes.Equal(got, []byte("dropped")) {
			return
		}
	}
	t.Fatal("a failed sync never dropped a change for good")
}

func TestFailedSyncCanDropTheCacheView(t *testing.T) {
	for seed := range uint64(100) {
		fsys := New(seed, Config{SyncFault: Always, DropCache: Always})
		file := openRW(t, fsys, testName)
		mustSync(t, file)
		mustWrite(t, file, []byte("dropped"), 0)
		fsys.SetFaults(true)
		if err := file.Sync(); !errors.Is(err, ErrInjected) {
			t.Fatalf("failed sync: %v, want ErrInjected", err)
		}
		fsys.SetFaults(false)
		if fsys.Stats().DroppedCaches == 0 {
			continue
		}
		// The running process now reads the durable bytes: the write is gone without a crash.
		if got := content(fsys, testName); bytes.Equal(got, []byte("dropped")) {
			t.Fatalf("seed %d: a dropped cache view still holds the write", seed)
		}
		return
	}
	t.Fatal("a failed sync never dropped the cache view")
}

func TestInjectedErrorsNeedEnabledFaults(t *testing.T) {
	fsys := New(4, Config{ReadFault: Always, WriteFault: Always})
	file := openRW(t, fsys, testName)
	mustWrite(t, file, []byte("data"), 0)
	fsys.SetFaults(true)
	if _, err := file.ReadAt(make([]byte, 4), 0); !errors.Is(err, ErrInjected) {
		t.Fatalf("read with faults: %v, want ErrInjected", err)
	}
	if _, err := file.WriteAt([]byte("data"), 0); !errors.Is(err, ErrInjected) {
		t.Fatalf("write with faults: %v, want ErrInjected", err)
	}
	if got := fsys.Stats().InjectedFaults; got != 2 {
		t.Fatalf("InjectedFaults = %d, want 2", got)
	}
}

func TestCloneCacheHoldsOneCopyOfTheCacheView(t *testing.T) {
	fsys := New(1, Config{})
	file := openRW(t, fsys, testName)
	mustWrite(t, file, []byte("synced"), 0)
	mustSync(t, file)
	// Unsynced, and the name is not durable either.
	mustWrite(t, file, []byte("cached"), 6)

	clone := fsys.CloneCache(2)
	clone.Crash()
	clone.Restart(false)
	if got := content(clone, testName); string(got) != "syncedcached" {
		t.Fatalf("clone after a process crash: got %q, want %q", got, "syncedcached")
	}
	// The clone is independent: a write to it does not change fsys.
	cloneFile := openRW(t, clone, testName)
	mustWrite(t, cloneFile, []byte("X"), 0)
	if got := content(fsys, testName); string(got) != "syncedcached" {
		t.Fatalf("original after a write to the clone: got %q", got)
	}
	// The clone keeps no durable view and no unsynced change, so each file costs one copy.
	if _, durable, _ := clone.FileData(testName); durable != nil || len(clone.names[testName].pending) != 0 {
		t.Fatalf("clone: durable view %q and %d unsynced changes", durable, len(clone.names[testName].pending))
	}
	defer func() {
		if recover() == nil {
			t.Fatal("a power failure on the clone did not panic")
		}
	}()
	clone.Restart(true)
}

// allocatedBytes returns the number of bytes that fn allocates.
func allocatedBytes(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// TestDamageDoesNotCopyTheFile requires that a power failure with a misdirected write, and bit rot in a small range, reuse the arrays of the file.
// Long runs grow files to hundreds of MiB, so each full copy costs much memory.
func TestDamageDoesNotCopyTheFile(t *testing.T) {
	const size = 4 << 20
	fsys := New(1, Config{Misdirect: Always})
	file := openRW(t, fsys, testName)
	mustWrite(t, file, bytes.Repeat([]byte{'.'}, size), 0)
	mustSync(t, file)
	if err := fsys.SyncDir(testDir); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, file, bytes.Repeat([]byte{'w'}, 4096), size/2)
	fsys.Crash()
	if allocated := allocatedBytes(func() { fsys.Restart(true) }); allocated > size/4 {
		t.Errorf("a power failure of a %d-byte file allocated %d bytes", size, allocated)
	}
	if got, _ := fsys.FileSize(testName); got != size {
		t.Fatalf("the file has %d bytes after the power failure, want %d", got, size)
	}
	if allocated := allocatedBytes(func() { fsys.Corrupt(testName, 0, 4096, 3) }); allocated > size/4 {
		t.Errorf("bit rot in 4096 bytes of a %d-byte file allocated %d bytes", size, allocated)
	}
}

func TestMappingFollowsWritesUnlessStale(t *testing.T) {
	for _, stale := range []bool{false, true} {
		fsys := New(1, Config{StaleMappings: stale})
		file := openRW(t, fsys, testName)
		mustWrite(t, file, []byte("before"), 0)
		mapper := file.(fileio.Mapper)
		data, err := mapper.Map(6)
		if err != nil {
			t.Fatal(err)
		}
		mustWrite(t, file, []byte("AFTER!"), 0)
		want := "AFTER!"
		if stale {
			want = "before"
		}
		if string(data) != want || mapper.MappingFollowsWrites() == stale {
			t.Errorf("stale=%t: mapping holds %q, want %q", stale, data, want)
		}
		if err := mapper.Unmap(data); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, bytes.Repeat([]byte{0xEE}, 6)) {
			t.Errorf("stale=%t: an unmapped mapping holds %q, want poison bytes", stale, data)
		}
		if err := mapper.Unmap(data); err == nil {
			t.Errorf("stale=%t: a second Unmap succeeded", stale)
		}
	}
}

func TestTruncateUnderMappingIsAViolation(t *testing.T) {
	fsys := New(1, Config{})
	file := openRW(t, fsys, testName)
	mustWrite(t, file, []byte("0123456789"), 0)
	data, err := file.(fileio.Mapper).Map(10)
	if err != nil {
		t.Fatal(err)
	}
	// Growth does not cut the mapping.
	if err := file.Truncate(20); err != nil {
		t.Fatal(err)
	}
	if got := fsys.Stats().MappingViolations; got != 0 {
		t.Fatalf("violations after growth: %d", got)
	}
	if err := file.Truncate(4); err != nil {
		t.Fatal(err)
	}
	if got := fsys.Stats().MappingViolations; got != 1 {
		t.Fatalf("violations after a truncate under the mapping: %d, want 1", got)
	}
	if string(data[:4]) != "0123" || data[4] != 0xEE {
		t.Fatalf("mapping after the truncate: %q, want the first 4 bytes and then poison", data)
	}
}

func TestRestartEndsMappings(t *testing.T) {
	fsys := New(1, Config{})
	file := openRW(t, fsys, testName)
	mustWrite(t, file, []byte("data"), 0)
	data, err := file.(fileio.Mapper).Map(4)
	if err != nil {
		t.Fatal(err)
	}
	fsys.Crash()
	fsys.Restart(false)
	if data[0] != 0xEE {
		t.Fatalf("a mapping of the crashed process holds %q, want poison bytes", data)
	}
	// The new process can truncate: the old mapping is gone.
	reopened := openRW(t, fsys, testName)
	if err := reopened.Truncate(0); err != nil {
		t.Fatal(err)
	}
	if got := fsys.Stats().MappingViolations; got != 0 {
		t.Fatalf("violations after a restart: %d", got)
	}
}

func TestDisableMapFails(t *testing.T) {
	fsys := New(1, Config{DisableMap: true})
	file := openRW(t, fsys, testName)
	mustWrite(t, file, []byte("data"), 0)
	if _, err := file.(fileio.Mapper).Map(4); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("Map with DisableMap: %v, want ErrUnsupported", err)
	}
}

func TestTornSectorsKeepPartOfASector(t *testing.T) {
	torn := false
	for seed := range uint64(200) {
		fsys := New(seed, Config{TornSectors: true, SectorSize: 512})
		file := openRW(t, fsys, testName)
		mustWrite(t, file, bytes.Repeat([]byte{'a'}, 512), 0)
		mustSync(t, file)
		mustWrite(t, file, bytes.Repeat([]byte{'b'}, 512), 0)
		fsys.Crash()
		fsys.Restart(true)
		got := content(fsys, testName)
		count := bytes.Count(got, []byte{'b'})
		if count > 0 && count < 512 {
			torn = true
			if !bytes.Equal(got[:count], bytes.Repeat([]byte{'b'}, count)) {
				t.Fatalf("seed %d: a torn sector must keep a prefix of the write: %q", seed, got)
			}
		}
	}
	if !torn || New(1, Config{}).Stats().TornSectors != 0 {
		t.Fatal("200 seeds did not tear a sector")
	}
}

func TestGarbageFillsLengthWithoutData(t *testing.T) {
	garbage := false
	for seed := range uint64(200) {
		fsys := New(seed, Config{Garbage: true})
		file := openRW(t, fsys, testName)
		mustSync(t, file)
		if err := fsys.SyncDir(testDir); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, file, bytes.Repeat([]byte{'x'}, 4096), 0)
		fsys.Crash()
		fsys.Restart(true)
		// A lost sector of the kept length holds random bytes: neither the written byte nor zero.
		for _, value := range content(fsys, testName) {
			if value != 'x' && value != 0 {
				garbage = true
			}
		}
	}
	if !garbage {
		t.Fatal("200 seeds did not fill kept length with random bytes")
	}
}

func TestDiskFullEpisodeFailsGrowth(t *testing.T) {
	fsys := New(1, Config{FullFault: Always})
	file := openRW(t, fsys, testName)
	mustWrite(t, file, []byte("data"), 0)
	fsys.SetFaults(true)
	defer fsys.SetFaults(false)
	// An overwrite inside the file needs no space.
	if _, err := file.WriteAt([]byte("DA"), 0); err != nil {
		t.Fatalf("overwrite during a disk-full episode: %v", err)
	}
	n, err := file.WriteAt([]byte("TAIL+more"), 2)
	if !errors.Is(err, syscall.ENOSPC) || !errors.Is(err, ErrInjected) || n != 2 {
		t.Fatalf("growth during a disk-full episode: n=%d err=%v, want n=2 and ENOSPC", n, err)
	}
	if got := content(fsys, testName); string(got) != "DATA" {
		t.Fatalf("file after the failed growth: %q, want %q", got, "DATA")
	}
	if err := file.Truncate(100); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("growing truncate during a disk-full episode: %v, want ENOSPC", err)
	}
}

func TestReadCorruptionLeavesStoredBytes(t *testing.T) {
	fsys := New(1, Config{ReadCorruption: Always})
	wal := openRW(t, fsys, testName+"-wal")
	mustWrite(t, wal, []byte("record"), 0)
	main := openRW(t, fsys, testName)
	mustWrite(t, main, []byte("record"), 0)
	fsys.SetFaults(true)
	buffer := make([]byte, 6)
	if _, err := wal.ReadAt(buffer, 0); err != nil || string(buffer) != "record" {
		t.Fatalf("WAL read: %q, %v, want no flipped bit", buffer, err)
	}
	if _, err := main.ReadAt(buffer, 0); err != nil || string(buffer) == "record" {
		t.Fatalf("main-file read: %q, %v, want a flipped bit", buffer, err)
	}
	fsys.SetFaults(false)
	if got := content(fsys, testName); string(got) != "record" {
		t.Fatalf("stored main-file bytes after a corrupted read: %q", got)
	}
}

func TestHealUndoesBitRotAndKeepsLaterWrites(t *testing.T) {
	fsys := New(1, Config{})
	file := openRW(t, fsys, testName)
	original := bytes.Repeat([]byte{'a'}, 64)
	mustWrite(t, file, original, 0)
	mustSync(t, file)
	if !fsys.Corrupt(testName, 0, 64, 4) || !fsys.Damaged() {
		t.Fatal("Corrupt did not change the file")
	}
	if got := content(fsys, testName); bytes.Equal(got, original) {
		t.Fatal("bit rot left the stored bytes unchanged")
	}
	// A later write replaces the first damaged bytes.
	mustWrite(t, file, bytes.Repeat([]byte{'b'}, 8), 0)
	fsys.Heal()
	want := append(bytes.Repeat([]byte{'b'}, 8), original[8:]...)
	if got := content(fsys, testName); !bytes.Equal(got, want) || fsys.Damaged() {
		t.Fatalf("after Heal: %q, want %q", got, want)
	}
}

// FileSize gives the size of the cache view without a copy, for the checks after every step.
func TestFileSizeReportsTheCacheView(t *testing.T) {
	fsys := New(1, Config{})
	file := openRW(t, fsys, testName)
	mustWrite(t, file, bytes.Repeat([]byte{'a'}, 64), 0)
	if size, exists := fsys.FileSize(testName); !exists || size != 64 {
		t.Fatalf("FileSize: %d, %t, want 64, true", size, exists)
	}
	if _, exists := fsys.FileSize(testDir + "/missing"); exists {
		t.Fatal("FileSize found a missing file")
	}
}

// Two damages can change the same bytes, for example bit rot in a block that a misdirected write changed.
// Heal must undo the newer damage first, or it brings back bytes of the older damage.
func TestHealUndoesOverlappingDamageNewestFirst(t *testing.T) {
	fsys := New(1, Config{})
	file := openRW(t, fsys, testName)
	original := bytes.Repeat([]byte{'a'}, 8)
	mustWrite(t, file, original, 0)
	mustSync(t, file)
	for range 2 {
		if !fsys.Corrupt(testName, 0, 8, 32) {
			t.Fatal("Corrupt did not change the file")
		}
	}
	fsys.Heal()
	if got := content(fsys, testName); !bytes.Equal(got, original) {
		t.Fatalf("after Heal: %q, want %q", got, original)
	}
}

// A writeback error marks data pages clean, but the journal keeps a size change.
// Blackout found that a dropped truncate left main-file pages past the logical end, which no real file system does.
func TestFailedSyncKeepsTruncates(t *testing.T) {
	for seed := range uint64(20) {
		fsys := New(seed, Config{SyncFault: Always})
		file := openRW(t, fsys, testName)
		mustWrite(t, file, bytes.Repeat([]byte{'a'}, 64), 0)
		mustSync(t, file)
		if err := file.Truncate(16); err != nil {
			t.Fatal(err)
		}
		fsys.SetFaults(true)
		if err := file.Sync(); err == nil {
			t.Fatal("the sync did not fail")
		}
		fsys.SetFaults(false)
		if _, durable, _ := fsys.FileData(testName); len(durable) != 16 {
			t.Fatalf("seed %d: durable size after a failed sync: %d, want 16", seed, len(durable))
		}
	}
}

// On Btrfs, a failed sync reverts the data and the metadata of the file, and the cache shows the old bytes again.
func TestFailedSyncRevertsLikeBtrfs(t *testing.T) {
	for seed := range uint64(20) {
		fsys := New(seed, Config{SyncFault: Always, RevertFailedSync: true})
		file := openRW(t, fsys, testName)
		original := bytes.Repeat([]byte{'a'}, 64)
		mustWrite(t, file, original, 0)
		mustSync(t, file)
		if err := file.Truncate(16); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, file, bytes.Repeat([]byte{'b'}, 32), 8)
		fsys.SetFaults(true)
		if err := file.Sync(); err == nil {
			t.Fatal("the sync did not fail")
		}
		fsys.SetFaults(false)
		cache, durable, _ := fsys.FileData(testName)
		if !bytes.Equal(durable, original) || !bytes.Equal(cache, original) {
			t.Fatalf("seed %d: after a reverted sync: cache %q, durable %q, want both %q", seed, cache, durable, original)
		}
		if stats := fsys.Stats(); stats.RevertedSyncs != 1 || stats.DroppedCaches != 1 {
			t.Fatalf("seed %d: reverted syncs %d, dropped caches %d, want 1 and 1", seed, stats.RevertedSyncs, stats.DroppedCaches)
		}
		// A later sync has nothing to write.
		mustSync(t, file)
		if _, durable, _ := fsys.FileData(testName); !bytes.Equal(durable, original) {
			t.Fatalf("seed %d: durable bytes after a later sync: %q", seed, durable)
		}
	}
}

func TestMisdirectedWriteMovesDataAndHealRestoresIt(t *testing.T) {
	moved := false
	for seed := range uint64(200) {
		fsys := New(seed, Config{Misdirect: Always, KeepChange: Always})
		file := openRW(t, fsys, testName)
		mustWrite(t, file, bytes.Repeat([]byte{'.'}, 8*4096), 0)
		mustSync(t, file)
		if err := fsys.SyncDir(testDir); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, file, bytes.Repeat([]byte{'w'}, 4096), 4*4096)
		fsys.Crash()
		fsys.Restart(true)
		if !fsys.Damaged() {
			continue
		}
		moved = true
		got := content(fsys, testName)
		if bytes.Count(got[4*4096:5*4096], []byte{'w'}) != 0 || bytes.Count(got, []byte{'w'}) != 4096 {
			t.Fatalf("seed %d: a misdirected write must leave the intended block old and land elsewhere", seed)
		}
		fsys.Heal()
		got = content(fsys, testName)
		if !bytes.Equal(got[4*4096:5*4096], bytes.Repeat([]byte{'w'}, 4096)) || bytes.Count(got, []byte{'w'}) != 4096 {
			t.Fatalf("seed %d: Heal did not restore the correct result", seed)
		}
	}
	if !moved {
		t.Fatal("200 seeds did not misdirect a write")
	}
}

func TestHealKeepsTheDamagedPartsOutsideALaterWrite(t *testing.T) {
	fsys := New(1, Config{})
	file := openRW(t, fsys, testName)
	mustWrite(t, file, bytes.Repeat([]byte{'a'}, 64), 0)
	mustSync(t, file)
	// Damage every byte, so one run covers the whole file.
	fsys.mu.Lock()
	node := fsys.names[testName]
	correct := slices.Clone(node.durable)
	for index := range node.durable {
		node.durable[index] = 'x'
	}
	node.data = slices.Clone(node.durable)
	fsys.damage = append(fsys.damage, damage{node: node, runs: diffRuns(correct, node.durable, 0)})
	fsys.mu.Unlock()
	// A write replaces the middle of the run.
	mustWrite(t, file, bytes.Repeat([]byte{'b'}, 8), 28)
	fsys.Heal()
	want := append(append(bytes.Repeat([]byte{'a'}, 28), bytes.Repeat([]byte{'b'}, 8)...), bytes.Repeat([]byte{'a'}, 28)...)
	if got := content(fsys, testName); !bytes.Equal(got, want) {
		t.Fatalf("after Heal: %q, want %q", got, want)
	}
}

func TestRelativeNamesAndSymbolicLinks(t *testing.T) {
	fsys := New(1, Config{WorkingDir: testDir})
	file := openRW(t, fsys, "data")
	mustWrite(t, file, []byte("x"), 0)
	if got := content(fsys, testDir+"/data"); string(got) != "x" {
		t.Fatalf("a relative name: got %q at the absolute name", got)
	}
	fsys.Symlink("data", testDir+"/link")
	if resolved, err := fsys.EvalSymlinks("link"); err != nil || resolved != testDir+"/data" {
		t.Fatalf("EvalSymlinks: %q, %v", resolved, err)
	}
	if info, err := fsys.Lstat("link"); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("Lstat of a link: %v, %v", info, err)
	}
	through, err := fsys.OpenFile("link", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, through, []byte("y"), 1)
	if got := content(fsys, testDir+"/data"); string(got) != "xy" {
		t.Fatalf("a write through a link: %q", got)
	}
}

func TestHardLinkSharesTheFileUntilADirectorySync(t *testing.T) {
	fsys := New(1, Config{KeepChange: Ratio{Num: 1, Den: 1 << 30}})
	temp, tempName, err := fsys.CreateTemp(testDir, ".tmp-*")
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, temp, []byte("backup"), 0)
	mustSync(t, temp)
	if err := fsys.Link(tempName, testName); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Link(tempName, testName); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("a second link to an existing name: %v, want ErrExist", err)
	}
	if err := fsys.Remove(tempName); err != nil {
		t.Fatal(err)
	}
	if got := content(fsys, testName); string(got) != "backup" {
		t.Fatalf("the linked name after the remove: %q", got)
	}
	// Without a directory sync, a power failure loses both name changes.
	fsys.Crash()
	fsys.Restart(true)
	if _, _, ok := fsys.FileData(testName); ok {
		t.Fatal("the new name survived a power failure without a directory sync")
	}
}

func TestDurableNeedsSyncedDataAndNames(t *testing.T) {
	fsys := New(1, Config{})
	if !fsys.Durable() {
		t.Fatal("an empty FS is not durable")
	}
	file := openRW(t, fsys, testName)
	mustWrite(t, file, []byte("data"), 0)
	if fsys.Durable() {
		t.Fatal("an unsynced write is durable")
	}
	mustSync(t, file)
	if fsys.Durable() {
		t.Fatal("a name without a directory sync is durable")
	}
	if err := fsys.SyncDir(testDir); err != nil {
		t.Fatal(err)
	}
	if !fsys.Durable() {
		t.Fatal("synced data and a synced name are not durable")
	}
}
