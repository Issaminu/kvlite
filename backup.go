package kvlite

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

// Backup saves one committed database state to a new file at path.
// Open can use the backup directly. No WAL file is required.
// Backup works with writable and read-only handles.
// It uses the source file permissions.
// A memory database returns an error that matches [errors.ErrUnsupported] without file operations.
// An empty path returns [ErrPathRequired]. An existing path or path+"-wal" returns an error that matches [os.ErrExist].
//
// SyncFull and SyncNormal can accept writes while the copy runs. SyncNone writes wait for the backup.
// Checkpointing waits until all backups finish, so the WAL can grow during a backup.
// Backup retains the committed WAL page images from its start until it returns.
// Close waits for Backup to finish. Do not call Backup inside a transaction callback.
// Backup completes the copy, file cleanup, and checkpoint release before it returns success.
// It reads the copied allocation pages again and checks them, and the copied tree pages too with data-page checksums, so a damaged read fails the backup.
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
	if err := db.writeFailure(); err != nil {
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
	// A commit replaces changed segments with private copies, so a copy of the list keeps this state.
	allocation := *db.allocation
	allocation.segments = slices.Clone(db.allocation.segments)
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
	if err := checkBackupPages(temp, &allocation, pages, size, dataPageChecksums); err != nil {
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

// checkBackupPages reads again the backup pages that came from the main file, and checks each page that has a checksum.
// A read can return wrong bytes, and the backup must hold one committed state.
// Allocation pages always have a checksum. Tree pages have one only with data-page checksums.
// WAL node images come from memory. Allocation spans are checked in all cases.
func checkBackupPages(file io.ReaderAt, allocation *allocationBitmap, pages map[page.ID]wal.CommittedPage, size int64, dataPageChecksums bool) error {
	for id := page.Meta1ID + 1; int64(id)*allocation.pageSize < size; id++ {
		if !allocation.allocated(id) {
			continue
		}
		segment := allocation.isSegmentPage(id)
		if segment {
			if !allocation.isSegmentHead(id) {
				continue
			}
			data := make([]byte, allocation.pageSize)
			if _, err := file.ReadAt(data, int64(id)*allocation.pageSize); err != nil {
				return err
			}
			if err := verifyAllocationPage(data, allocation.pageSize, id); err != nil {
				return fmt.Errorf("check backup allocation %d: %w", id, err)
			}
			continue
		}
		if committed, inWAL := pages[id]; inWAL {
			if committed.Header.Type == wal.RecordTypeNode {
				count := 0
				if committed.Node != nil {
					count = btree.NodePageCount(committed.Node, allocation.pageSize)
				} else {
					var err error
					count, err = btree.NodeSpan(committed.Payload, id, allocation.pageSize)
					if err != nil {
						return err
					}
				}
				id += page.ID(count - 1)
			}
			continue
		}
		if !dataPageChecksums {
			continue
		}
		first := make([]byte, allocation.pageSize)
		if _, err := file.ReadAt(first, int64(id)*allocation.pageSize); err != nil {
			return err
		}
		count, err := btree.NodeSpan(first, id, allocation.pageSize)
		if err != nil {
			return err
		}
		if int64(count)*allocation.pageSize > size-int64(id)*allocation.pageSize {
			return ErrInvalid
		}
		data := first
		if count > 1 {
			data = make([]byte, int64(count)*allocation.pageSize)
			if _, err := file.ReadAt(data, int64(id)*allocation.pageSize); err != nil {
				return err
			}
		}
		if err := btree.CheckNodeChecksum(data, id); err != nil {
			return fmt.Errorf("check backup node %d: %w", id, err)
		}
		id += page.ID(count - 1)
	}
	return nil
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
