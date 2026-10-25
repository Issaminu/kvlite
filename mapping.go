package kvlite

import (
	"errors"
	"math"
	"os"

	"github.com/Issaminu/kvlite/internal/fileio"
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
