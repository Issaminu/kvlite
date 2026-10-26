package kvlite

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/Issaminu/kvlite/internal/fileio"
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

type directStatsMappingFile struct {
	fileio.File
	follows             bool
	stats, maps, unmaps int
	mapping             []byte
	statErr             error
}

func (file *directStatsMappingFile) Stat() (os.FileInfo, error) {
	file.stats++
	if file.statErr != nil {
		return nil, file.statErr
	}
	return file.File.Stat()
}

func (file *directStatsMappingFile) WriteAt(data []byte, offset int64) (int, error) {
	n, err := file.File.WriteAt(data, offset)
	if file.follows && offset >= 0 && offset <= int64(len(file.mapping)) && int64(n) <= int64(len(file.mapping))-offset {
		copy(file.mapping[offset:offset+int64(n)], data[:n])
	}
	return n, err
}

func (file *directStatsMappingFile) Map(length int) ([]byte, error) {
	file.maps++
	file.mapping = make([]byte, length)
	if _, err := io.ReadFull(io.NewSectionReader(file.File, 0, int64(length)), file.mapping); err != nil {
		return nil, err
	}
	return file.mapping, nil
}

func (file *directStatsMappingFile) Unmap([]byte) error {
	file.unmaps++
	file.mapping = nil
	return nil
}

func (file *directStatsMappingFile) MappingFollowsWrites() bool { return file.follows }

func TestDirectMappingReserveFallbackAndBounds(t *testing.T) {
	failure := errors.New("reserve failed")
	var calls []int
	view, backing, err := mapDirectFileRegion(nil, 97, func(_ *os.File, length int, writable bool) ([]byte, error) {
		calls = append(calls, length)
		if !writable {
			t.Fatal("direct mapping did not request writes")
		}
		if length != 97 {
			return nil, failure
		}
		return make([]byte, length), nil
	})
	if err != nil || len(calls) != 2 || calls[0] != mappingReservationChunkSize || calls[1] != 97 || len(view) != 97 || cap(view) != 97 || len(backing) != 97 {
		t.Fatalf("reserve fallback: lengths=%v view=%d backing=%d error=%v", calls, len(view), len(backing), err)
	}
	for _, length := range []int{0, math.MaxInt} {
		calls = nil
		_, _, err := mapDirectFileRegion(nil, length, func(_ *os.File, size int, _ bool) ([]byte, error) { calls = append(calls, size); return nil, failure })
		if length == 0 {
			if !errors.Is(err, os.ErrInvalid) || len(calls) != 0 {
				t.Fatal("zero mapping reached the OS")
			}
		} else if !errors.Is(err, failure) || len(calls) != 1 || calls[0] != length {
			t.Fatal("large map length wrapped during rounding")
		}
	}
	file, err := os.CreateTemp(t.TempDir(), "mapper")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := file.Truncate(4096); err != nil {
		t.Fatal(err)
	}
	fake := &directStatsMappingFile{File: file, follows: true}
	db := &DB{file: fake, options: &Options{Synchronous: SyncNone}}
	if err := db.mapMainFile(); err != nil {
		t.Fatal(err)
	}
	defer db.unmapMainFile()
	if db.mappedBacking != nil || db.mappedWritable || len(db.mappedFile) != 4096 || db.resizeDirectMappedView(4097) {
		t.Fatal("test mapper used reserved OS mapping rules")
	}
}

func TestDirectMappingReserveUnmapsFullBacking(t *testing.T) {
	if !systemMappingAllowsWrites {
		t.Skip("writable mappings are unavailable")
	}
	db, err := Open(filepath.Join(t.TempDir(), "db"), 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	full := db.mappedBacking
	if len(full) < 2 {
		t.Skip("no direct OS mapping")
	}
	view := db.mappedFile
	// The Unix map registry rejects a slice that omits part of the original map.
	db.mappedBacking = full[:len(full)-1]
	if err := db.unmapMainFile(); err == nil || len(db.mappedFile) != len(view) || &db.mappedFile[0] != &view[0] {
		t.Fatal("failed unmap discarded a live view")
	}
	db.mappedBacking = full
	if err := db.unmapMainFile(); err != nil {
		t.Fatal(err)
	}
	if db.mappedFile != nil || db.mappedBacking != nil || db.mappedWritable {
		t.Fatal("full unmap kept mapping state")
	}
}

func TestDirectMappedFileWrites(t *testing.T) {
	if !systemMappingAllowsWrites {
		t.Skip("writable mapping is unavailable")
	}
	file, err := os.CreateTemp(t.TempDir(), "mapped")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	const length = 4096
	if err := file.Truncate(length); err != nil {
		t.Fatal(err)
	}
	db := &DB{file: file, options: &Options{Synchronous: SyncNone}}
	if err := db.mapMainFile(); err != nil {
		t.Fatal(err)
	}
	defer db.unmapMainFile()
	if !db.mappedWritable {
		t.Fatal("mapping is not writable")
	}
	writer := db.directWriteFile().(*directMappedFile)
	failure := errors.New("unexpected file write")
	guard := &directWriteGuard{File: file, err: failure}
	writer.File = guard
	if n, err := writer.WriteAt([]byte("mapped"), 64); err != nil || n != 6 || guard.writes != 0 {
		t.Fatalf("mapped write: n=%d error=%v file writes=%d", n, err, guard.writes)
	}
	data := make([]byte, 6)
	if _, err := file.ReadAt(data, 64); err != nil || string(data) != "mapped" {
		t.Fatalf("file after mapped write: %q, %v", data, err)
	}
	// Bytes past the visible range must use the file, even inside the reservation.
	writer.File = file
	if n, err := writer.WriteAt([]byte("grown"), length); err != nil || n != 5 {
		t.Fatalf("growth write: n=%d error=%v", n, err)
	}
	data = make([]byte, 5)
	if _, err := file.ReadAt(data, length); err != nil || string(data) != "grown" {
		t.Fatalf("file after growth: %q, %v", data, err)
	}
	if len(db.mappedFile) != length {
		t.Fatal("writer exposed bytes before the caller updated the view")
	}
	if _, err := writer.WriteAt([]byte("invalid"), -1); err == nil {
		t.Fatal("negative offset succeeded")
	}
	// File wrappers must retain their fault-injection behavior.
	db.file = guard
	if db.directWriteFile() != guard {
		t.Fatal("file wrapper was bypassed by the mapped writer")
	}
}

func TestMappedWALWriteLimit(t *testing.T) {
	if !systemMappingAllowsWrites {
		t.Skip("writable shared mappings are unavailable")
	}
	for _, size := range []int{1, mappedWALWriteLimit - 1, mappedWALWriteLimit, mappedWALWriteLimit + 1, 64 << 10} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			raw, err := os.Create(filepath.Join(t.TempDir(), "wal"))
			if err != nil {
				t.Fatal(err)
			}
			file := mapWALWrites(raw).(*mappedWALFile)
			defer file.Close()
			header := bytes.Repeat([]byte{0x31}, 20)
			if _, err = file.WriteAt(header, 0); err != nil {
				t.Fatal(err)
			}
			if len(file.backing) != 0 {
				t.Fatal("header write created a mapping")
			}
			data := bytes.Repeat([]byte{0xce}, size)
			if n, err := file.WriteAt(data, 20); err != nil || n != len(data) {
				t.Fatalf("write: %d, %v", n, err)
			}
			mapped := size <= mappedWALWriteLimit
			if (len(file.backing) != 0) != mapped {
				t.Fatalf("mapping selected=%t, want %t", len(file.backing) != 0, mapped)
			}
			info, err := file.Stat()
			if err != nil {
				t.Fatal(err)
			}
			wantSize := int64(20 + size)
			if mapped {
				wantSize = mappedWALGrowthSize
			}
			if info.Size() != wantSize {
				t.Fatalf("file length %d, want %d", info.Size(), wantSize)
			}
			got := make([]byte, 20+size)
			if _, err = file.ReadAt(got, 0); err != nil || !bytes.Equal(got, append(header, data...)) {
				t.Fatalf("readback: %v", err)
			}
			if err = file.Sync(); err != nil {
				t.Fatal(err)
			}
			if err = file.Truncate(0); err != nil {
				t.Fatal(err)
			}
			if len(file.backing) != 0 || file.fileSize != 0 {
				t.Fatal("truncate retained mapped bytes or an old file length")
			}
			if _, err = file.WriteAt(header, 0); err != nil {
				t.Fatal(err)
			}
			if _, err = file.WriteAt(data, 20); err != nil {
				t.Fatal(err)
			}
			if _, err = file.ReadAt(got, 0); err != nil || !bytes.Equal(got, append(header, data...)) {
				t.Fatalf("new generation: %v", err)
			}
			if err = file.Close(); err != nil {
				t.Fatal(err)
			}
			if len(file.backing) != 0 {
				t.Fatal("close retained the mapping")
			}
			if _, err = raw.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("file after close: %v", err)
			}
		})
	}
}

func TestMappedWALAlternatingWrites(t *testing.T) {
	if !systemMappingAllowsWrites {
		t.Skip("writable shared mappings are unavailable")
	}
	raw, err := os.Create(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	file := mapWALWrites(raw).(*mappedWALFile)
	defer file.Close()
	offset := int64(0)
	for _, size := range []int{20, 1024, 64 << 10, 4096, int(mappedWALGrowthSize) + 31, 2048} {
		data := bytes.Repeat([]byte{byte(offset)}, size)
		if n, err := file.WriteAt(data, offset); err != nil || n != size {
			t.Fatalf("write size %d: %d, %v", size, n, err)
		}
		got := make([]byte, size)
		if _, err = file.ReadAt(got, offset); err != nil || !bytes.Equal(got, data) {
			t.Fatalf("size %d: %v", size, err)
		}
		offset += int64(size)
	}
	if file.fileSize != 2*mappedWALGrowthSize {
		t.Fatalf("allocated length %d", file.fileSize)
	}
	before, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if n, err := file.WriteAt(nil, offset); err != nil || n != 0 {
		t.Fatalf("empty write: %d %v", n, err)
	}
	after, err := file.Stat()
	if err != nil || before.Size() != after.Size() {
		t.Fatalf("empty write changed file length: %v", err)
	}
	for _, off := range []int64{-1, math.MaxInt64} {
		if _, err = file.WriteAt([]byte("xx"), off); !errors.Is(err, os.ErrInvalid) {
			t.Fatalf("offset %d: %v", off, err)
		}
	}
}

func TestMappedWALMappingFailureUsesFileWrites(t *testing.T) {
	if !systemMappingAllowsWrites {
		t.Skip("writable shared mappings are unavailable")
	}
	path := filepath.Join(t.TempDir(), "wal")
	// A write-only descriptor supports WriteAt and file growth, but not a read/write mapping.
	raw, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	file := mapWALWrites(raw).(*mappedWALFile)
	defer file.Close()
	header := bytes.Repeat([]byte{0x31}, 20)
	if _, err = file.WriteAt(header, 0); err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte{0xce}, 256)
	if _, err = file.WriteAt(data, 20); err != nil {
		t.Fatal(err)
	}
	if !file.mappingFailed || len(file.backing) != 0 {
		t.Fatal("failed mapping did not select ordinary writes")
	}
	if _, err = file.WriteAt(data, 276); err != nil {
		t.Fatal(err)
	}
	reader, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got := make([]byte, 512)
	if _, err = reader.ReadAt(got, 20); err != nil || !bytes.Equal(got, bytes.Repeat([]byte{0xce}, 512)) {
		t.Fatalf("fallback data: %v", err)
	}
}

func TestMappedWALTruncateFailureDropsCachedSize(t *testing.T) {
	if !systemMappingAllowsWrites {
		t.Skip("writable shared mappings are unavailable")
	}
	path := filepath.Join(t.TempDir(), "wal")
	if err := os.WriteFile(path, make([]byte, 4096), 0600); err != nil {
		t.Fatal(err)
	}
	raw, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	file := &mappedWALFile{File: raw, fileSize: 4096}
	if err = file.Truncate(0); err == nil {
		t.Fatal("truncate on a read-only descriptor succeeded")
	}
	if file.fileSize >= 0 {
		t.Fatal("failed truncate retained a cached length")
	}
	if err = raw.Close(); err != nil {
		t.Fatal(err)
	}
	// A failed truncate can have changed the file. Model that change before a retry.
	file.File, err = os.OpenFile(path, os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err = file.WriteAt(make([]byte, 20), 0); err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteAt([]byte("new generation"), 20); err != nil {
		t.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil || info.Size() != mappedWALGrowthSize {
		t.Fatalf("file length after retry: %v", err)
	}
}

func TestMappedWALUnmapFailureKeepsFileOpen(t *testing.T) {
	if !systemMappingAllowsWrites {
		t.Skip("writable shared mappings are unavailable")
	}
	raw, err := os.Create(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	file := mapWALWrites(raw).(*mappedWALFile)
	if _, err = file.WriteAt([]byte("mapped"), 20); err != nil {
		t.Fatal(err)
	}
	backing := file.backing
	defer func() { file.backing = backing; _ = file.Close() }()
	// Munmap requires the complete slice returned by Mmap.
	file.backing = backing[:len(backing)-1]
	if err = file.Close(); err == nil {
		t.Fatal("close accepted an incomplete mapping")
	}
	if _, err = raw.Stat(); err != nil {
		t.Fatalf("failed unmap closed the file: %v", err)
	}
	if err = file.Truncate(0); err == nil {
		t.Fatal("truncate accepted an incomplete mapping")
	}
	info, err := raw.Stat()
	if err != nil || info.Size() != mappedWALGrowthSize {
		t.Fatalf("failed unmap changed the file: %v", err)
	}
	file.backing = backing
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	backing = nil
}

func TestMapWALWritesKeepsCustomFiles(t *testing.T) {
	raw, err := os.Create(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	wrapped := struct{ fileio.File }{raw}
	if got := mapWALWrites(wrapped); got != wrapped {
		t.Fatal("custom file wrapper was bypassed")
	}
	if mapWALWrites(nil) != nil {
		t.Fatal("nil file changed")
	}
	if !systemMappingAllowsWrites && mapWALWrites(raw) != raw {
		t.Fatal("unsupported platform selected mapped writes")
	}
}
