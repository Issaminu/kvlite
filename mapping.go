package kvlite

import (
	"errors"
	"math"
	"os"

	"github.com/Issaminu/kvlite/internal/fileio"
)

// mapMainFile maps the main file for reads.
// Direct writes use a writable mapping when the operating system supports it.
// File reads remain available if mapping fails.
// A file of a test FS is mapped only when it implements [fileio.Mapper]. Otherwise its reads use ReadAt.
func (db *DB) mapMainFile() error {
	mapper, isMapper := db.file.(fileio.Mapper)
	osFile, isOSFile := db.file.(*os.File)
	if !isMapper && !isOSFile {
		return nil
	}
	info, err := db.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 || info.Size() > int64(math.MaxInt) {
		return nil
	}

	var data []byte
	if isMapper {
		data, err = mapper.Map(int(info.Size()))
	} else {
		db.mappedWritable = db.options.Synchronous == SyncNone && !db.options.ReadOnly && systemMappingAllowsWrites
		if db.mappedWritable {
			data, db.mappedBacking, err = mapDirectFileRegion(osFile, int(info.Size()), systemMapMainFile)
		} else {
			data, err = systemMapMainFile(osFile, int(info.Size()), false)
		}
	}
	if err != nil {
		db.mappedWritable = false
		return nil
	}
	db.mappedFile = data
	return nil
}

// unmapMainFile releases the mapped bytes after all readers stop.
func (db *DB) unmapMainFile() error {
	if len(db.mappedFile) == 0 && len(db.mappedBacking) == 0 {
		db.mappedFile = nil
		return nil
	}
	var err error
	if len(db.mappedBacking) != 0 {
		err = systemUnmapMainFile(db.mappedBacking)
	} else if mapper, ok := db.file.(fileio.Mapper); ok {
		err = mapper.Unmap(db.mappedFile)
	} else {
		err = systemUnmapMainFile(db.mappedFile)
	}
	if err != nil {
		return err
	}
	db.mappedFile = nil
	db.mappedBacking = nil
	db.mappedWritable = false
	return nil
}

// refreshMainFileMapping replaces a mapping that cannot represent the current main file.
func (db *DB) refreshMainFileMapping() error {
	followsWrites := db.mainFileMappingFollowsWrites()
	info, err := db.file.Stat()
	if err != nil {
		// A mapping that does not show later writes can hold old file bytes after a write.
		// Remove it, so reads use the file until the next refresh.
		if !followsWrites {
			err = errors.Join(err, db.unmapMainFile())
		}
		return err
	}
	if followsWrites && int64(len(db.mappedFile)) == info.Size() {
		return nil
	}
	if err := db.unmapMainFile(); err != nil {
		return err
	}
	return db.mapMainFile()
}

// mappingReservationChunkSize is the address-space reservation step. It does not extend the file.
const mappingReservationChunkSize = 1 << 30

// mapDirectFileRegion reserves address space without extending the file.
// Later file growth can reuse this mapping without changing its base address.
// Only its first length bytes can be read or written.
func mapDirectFileRegion(file *os.File, length int, mapFile func(*os.File, int, bool) ([]byte, error)) ([]byte, []byte, error) {
	if length <= 0 {
		return nil, nil, os.ErrInvalid
	}
	reserved := length
	if length <= math.MaxInt-(mappingReservationChunkSize-1) {
		reserved = (length + mappingReservationChunkSize - 1) / mappingReservationChunkSize * mappingReservationChunkSize
	}
	data, err := mapFile(file, reserved, true)
	if err != nil && reserved != length {
		data, err = mapFile(file, length, true)
	}
	if err != nil {
		return nil, nil, err
	}
	return data[:length:length], data, nil
}

// resizeDirectMappedView changes the visible byte range within a reserved mapping.
// The caller must grow the file before it accesses added bytes.
func (db *DB) resizeDirectMappedView(size int64) bool {
	if _, realFile := db.file.(*os.File); !realFile || !db.mappedWritable || size <= 0 || size > int64(len(db.mappedBacking)) {
		return false
	}
	db.mappedFile = db.mappedBacking[:size:size]
	return true
}

func (db *DB) mainFileMappingFollowsWrites() bool {
	if mapper, ok := db.file.(fileio.Mapper); ok {
		return mapper.MappingFollowsWrites()
	}
	return systemMappingFollowsWrites
}

// directWriteFile uses a shared mapping for writes that fit its current range.
// The operation lock excludes readers. File growth still uses ordinary writes.
func (db *DB) directWriteFile() fileio.File {
	if _, ok := db.file.(*os.File); ok && db.mappedWritable {
		db.directWriter.File, db.directWriter.db = db.file, db
		return &db.directWriter
	}
	return db.file
}

type directMappedFile struct {
	fileio.File
	db *DB
}

func (file directMappedFile) WriteAt(data []byte, offset int64) (int, error) {
	if offset >= 0 && offset <= int64(len(file.db.mappedFile)) && int64(len(data)) <= int64(len(file.db.mappedFile))-offset {
		return copy(file.db.mappedFile[offset:offset+int64(len(data))], data), nil
	}
	return file.File.WriteAt(data, offset)
}

const (
	// mappedWALWriteLimit applies to a complete encoded transaction buffer.
	// Larger buffers use ordinary writes, which avoid the cost of large mapped copies.
	mappedWALWriteLimit = 16 << 10
	// mappedWALGrowthSize limits file-growth calls during a WAL generation.
	// It covers the default checkpoint threshold on a host with 16 KiB OS pages.
	mappedWALGrowthSize int64 = 16 << 20
)

// mapWALWrites selects mapped byte writes for a writable native file.
// Custom files and platforms without writable mappings keep ordinary file I/O.
func mapWALWrites(file fileio.File) fileio.File {
	if native, ok := file.(*os.File); ok && systemMappingAllowsWrites {
		return &mappedWALFile{File: native, fileSize: -1}
	}
	return file
}

// mappedWALFile copies small appends into shared file bytes.
// The WAL owns record boundaries, append offsets, and storage synchronization.
// Stat and ReadAt still expose the whole file, including unused allocated space.
// Calls must be serialized. Truncate and Close release the complete mapping first.
// All file-length changes must go through this value.
type mappedWALFile struct {
	*os.File
	backing       []byte
	fileSize      int64 // Last known file length. A negative value requires a new Stat call.
	mappingFailed bool  // A failed mapping keeps later writes on ordinary file I/O.
}

func (file *mappedWALFile) WriteAt(data []byte, offset int64) (int, error) {
	if offset < 0 || offset > math.MaxInt64-int64(len(data)) {
		return 0, os.ErrInvalid
	}
	if len(data) == 0 {
		return 0, nil
	}
	// Offset zero writes the generation header or rewrites recovered records.
	// Keep this path ordinary, so a log of large transactions needs no mapping.
	end := offset + int64(len(data))
	if offset == 0 || len(data) > mappedWALWriteLimit || file.mappingFailed || end > int64(math.MaxInt) {
		return file.writeAt(data, offset)
	}
	if file.fileSize < 0 {
		info, err := file.File.Stat()
		if err != nil {
			return 0, err
		}
		file.fileSize = info.Size()
	}
	if file.fileSize > int64(math.MaxInt) {
		return file.writeAt(data, offset)
	}
	if end > file.fileSize {
		if end > math.MaxInt64-(mappedWALGrowthSize-1) {
			return file.writeAt(data, offset)
		}
		size := (end + mappedWALGrowthSize - 1) / mappedWALGrowthSize * mappedWALGrowthSize
		if size > int64(math.MaxInt) {
			return file.writeAt(data, offset)
		}
		// File growth must finish before a copy can touch the added bytes.
		// An error can still change the file length, so discard the cached size first.
		file.fileSize = -1
		if err := file.File.Truncate(size); err != nil {
			return 0, err
		}
		file.fileSize = size
	}
	if end > int64(len(file.backing)) {
		if err := file.unmap(); err != nil {
			return 0, err
		}
		_, backing, err := mapDirectFileRegion(file.File, int(file.fileSize), systemMapMainFile)
		if err != nil {
			file.mappingFailed = true
			return file.writeAt(data, offset)
		}
		file.backing = backing
	}
	return copy(file.backing[offset:end], data), nil
}

func (file *mappedWALFile) writeAt(data []byte, offset int64) (int, error) {
	n, err := file.File.WriteAt(data, offset)
	if err != nil {
		file.fileSize = -1
	} else if file.fileSize >= 0 {
		file.fileSize = max(file.fileSize, offset+int64(n))
	}
	return n, err
}

func (file *mappedWALFile) unmap() error {
	if len(file.backing) == 0 {
		return nil
	}
	if err := systemUnmapMainFile(file.backing); err != nil {
		return err
	}
	file.backing = nil
	return nil
}

func (file *mappedWALFile) Truncate(size int64) error {
	if err := file.unmap(); err != nil {
		return err
	}
	file.fileSize = -1
	if err := file.File.Truncate(size); err != nil {
		return err
	}
	file.fileSize = size
	return nil
}

func (file *mappedWALFile) Sync() error {
	return fileio.SyncData(file.File)
}

func (file *mappedWALFile) Close() error {
	if err := file.unmap(); err != nil {
		return err
	}
	return file.File.Close()
}
