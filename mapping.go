package kvlite

import (
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/Issaminu/kvlite/internal/page"
)

// mapMainFile maps the current main-file length for read-only access.
// File reads remain available if mapping fails.
func (db *DB) mapMainFile() error {
	info, err := db.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 || info.Size() > int64(math.MaxInt) {
		return nil
	}

	data, err := systemMapMainFile(db.file, int(info.Size()))
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
	if err := systemUnmapMainFile(db.mappedFile); err != nil {
		return err
	}
	db.mappedFile = nil
	return nil
}

// refreshMainFileMapping replaces a mapping that cannot represent the current main file.
func (db *DB) refreshMainFileMapping() error {
	info, err := db.file.Stat()
	if err != nil {
		return err
	}
	if systemMainFileMappingIsCurrent(int64(len(db.mappedFile)), info.Size()) {
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
		return nil, fmt.Errorf("read page %d offset: %w", pageID, ErrInvalid)
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
