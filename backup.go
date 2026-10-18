package kvlite

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/wal"
)

// Backup saves one committed database state to a new file at path.
// Open can use the backup directly. No WAL file is required.
// Backup works with writable and read-only handles.
// It uses the source file permissions.
// A memory database returns an error that matches [errors.ErrUnsupported] without file operations.
// An empty path returns [ErrPathRequired]. An existing path or path+"-wal" returns an error that matches [os.ErrExist].
//
// Reads and writes can continue during the file copy.
// Checkpointing waits until all backups finish, so the WAL can grow during a backup.
// Backup retains the committed WAL page images from its start until it returns.
// Close waits for Backup to finish. Do not call Backup inside a transaction callback.
// Backup completes the copy, file cleanup, and checkpoint release before it returns success.
// It does not start a checkpoint on the source database.
//
// With [SyncFull] or [SyncNormal], Backup syncs the new file before it makes path visible.
// It then removes the temporary name and syncs the parent directory.
// Windows skips directory sync.
// With [SyncNone], Backup skips both sync calls. A system failure can lose or damage the backup.
// An error after path becomes visible leaves a complete backup at path.
// The file system must support hard links. Except on Windows, SyncFull and SyncNormal also require directory sync.
// The caller must keep path and path+"-wal" unused by other processes during Backup.
func (db *DB) Backup(path string) error {
	return db.backup(path, io.Copy, fileio.Sync)
}

func (db *DB) backup(path string, copyFile func(io.Writer, io.Reader) (int64, error), syncFile func(*os.File) error) error {
	fsys := db.fs
	if osFS, ok := fsys.(fileio.OSFS); ok {
		// An operating-system directory is synced with syncFile too, so tests can count and fail that call.
		osFS.SyncDirectory = syncFile
		fsys = osFS
	}
	// syncBackupFile syncs the new file: an operating-system file with syncFile, and a file of another FS with its own Sync.
	syncBackupFile := func(file fileio.File) error {
		if osFile, ok := file.(*os.File); ok {
			return syncFile(osFile)
		}
		return file.Sync()
	}
	if err := db.beginOperation(); err != nil {
		return err
	}
	defer db.endOperation()
	if path == "" {
		return ErrPathRequired
	}
	if db.memoryNodes != nil {
		return errors.ErrUnsupported
	}
	if err := backupPathAvailable(fsys, path); err != nil {
		return err
	}

	db.operationMu.Lock()
	if err := db.walWriteFailure(); err != nil {
		db.operationMu.Unlock()
		return err
	}
	size, err := db.logicalFileSize()
	if err != nil {
		db.operationMu.Unlock()
		return err
	}
	info, err := db.file.Stat()
	if err != nil {
		db.operationMu.Unlock()
		return err
	}
	pageSize := db.meta.PageSize()
	dataPageChecksums := db.meta.DataPageChecksums()
	pages := db.wal.CommittedRecords()
	if db.activeBackups == 0 {
		db.backupsDone = make(chan struct{})
	}
	db.activeBackups++
	db.operationMu.Unlock()
	defer func() {
		db.operationMu.Lock()
		db.activeBackups--
		if db.activeBackups == 0 {
			close(db.backupsDone)
		}
		db.operationMu.Unlock()
	}()

	dir := filepath.Dir(path)
	temp, tempName, err := fsys.CreateTemp(dir, ".kvlite-backup-*")
	if err != nil {
		return err
	}
	defer fsys.Remove(tempName)
	defer temp.Close()
	if err := fsys.Chmod(tempName, info.Mode().Perm()); err != nil {
		return err
	}
	// The main file stays unchanged until this backup releases activeBackups.
	// New pages can exist only in the saved WAL images.
	length := min(info.Size(), size)
	n, err := copyFile(io.NewOffsetWriter(temp, 0), io.NewSectionReader(db.file, 0, length))
	if err != nil {
		return err
	}
	if n != length {
		return io.ErrUnexpectedEOF
	}
	if err := wal.WriteCommittedPages(temp, pages, pageSize, dataPageChecksums); err != nil {
		return err
	}
	backupInfo, err := temp.Stat()
	if err != nil {
		return err
	}
	if backupInfo.Size() < size {
		return ErrInvalid
	}
	if err := temp.Truncate(size); err != nil {
		return err
	}
	if db.options.Synchronous != SyncNone {
		if err := syncBackupFile(temp); err != nil {
			return err
		}
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := backupPathAvailable(fsys, path); err != nil {
		return err
	}
	// A hard link publishes the complete file without replacing an existing path.
	if err := fsys.Link(tempName, path); err != nil {
		return err
	}
	if err := fsys.Remove(tempName); err != nil {
		return err
	}
	if db.options.Synchronous == SyncNone {
		return nil
	}
	return fsys.SyncDir(dir)
}

func backupPathAvailable(fsys fileio.FS, path string) error {
	for _, name := range []string{path, path + "-wal"} {
		if _, err := fsys.Lstat(name); err == nil {
			return fmt.Errorf("backup path %s: %w", name, os.ErrExist)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}
