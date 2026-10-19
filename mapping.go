package kvlite

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
)

// mapMainFile maps the current main-file length for read-only access.
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
		data, err = systemMapMainFile(osFile, int(info.Size()))
	}
	if err != nil {
		return nil
	}
	db.mappedFile = data
	return nil
}

// unmapMainFile releases the mapped bytes after all readers stop.
func (db *DB) unmapMainFile() error {
	if len(db.mappedFile) == 0 {
		db.mappedFile = nil
		return nil
	}
	var err error
	if mapper, ok := db.file.(fileio.Mapper); ok {
		err = mapper.Unmap(db.mappedFile)
	} else {
		err = systemUnmapMainFile(db.mappedFile)
	}
	if err != nil {
		return err
	}
	db.mappedFile = nil
	return nil
}

// refreshMainFileMapping replaces a mapping that cannot represent the current main file.
func (db *DB) refreshMainFileMapping() error {
	followsWrites := systemMappingFollowsWrites
	if mapper, ok := db.file.(fileio.Mapper); ok {
		followsWrites = mapper.MappingFollowsWrites()
	}
	info, err := db.file.Stat()
	if err != nil {
		// A mapping that does not show later writes can hold pages that a checkpoint replaced.
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

// readMainPage returns one complete main-file page.
// Mapped bytes stay valid while the caller holds the database operation lock.
func (db *DB) readMainPage(pageID page.ID) ([]byte, error) {
	pageSize := db.meta.PageSize()
	if pageSize <= 0 || uint64(pageID) > uint64(math.MaxInt64)/uint64(pageSize) {
		return nil, fmt.Errorf("read page %d offset: %w", pageID, page.ErrInvalid)
	}
	offset := int64(pageID) * pageSize
	mappedSize := int64(len(db.mappedFile))
	if offset <= mappedSize && pageSize <= mappedSize-offset {
		start := int(offset)
		end := start + int(pageSize)
		return db.mappedFile[start:end:end], nil
	}

	data := make([]byte, int(pageSize))
	n, err := db.file.ReadAt(data, offset)
	if err != nil {
		if errors.Is(err, io.EOF) && n > 0 {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if n != len(data) {
		return nil, io.ErrUnexpectedEOF
	}
	return data, nil
}

// readMainNodePage reads one data page and checks its stored checksum mode.
// The codec checks nonzero fields only when the stored mode is enabled.
func (db *DB) readMainNodePage(pageID page.ID) ([]byte, error) {
	data, err := db.readMainPage(pageID)
	if err != nil {
		return nil, err
	}
	if err := btree.CheckNodeChecksumMode(data, db.meta.DataPageChecksums()); err != nil {
		return nil, fmt.Errorf("read data page %d: %w", pageID, err)
	}
	return data, nil
}
