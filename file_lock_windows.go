//go:build windows

package kvlite

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

func tryFileLock(file *os.File, exclusive bool) (bool, error) {
	flags := uint32(windows.LOCKFILE_FAIL_IMMEDIATELY)
	if exclusive {
		flags |= windows.LOCKFILE_EXCLUSIVE_LOCK
	}
	var minusOne = ^uint32(0)
	overlapped := windows.Overlapped{Offset: minusOne, OffsetHigh: minusOne}
	if err := windows.LockFileEx(windows.Handle(file.Fd()), flags, 0, 1, 0, &overlapped); err != nil {
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) || errors.Is(err, windows.ERROR_IO_PENDING) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
