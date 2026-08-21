//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package kvlite

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func tryFileLock(file *os.File, exclusive bool) (bool, error) {
	flag := unix.LOCK_NB | unix.LOCK_SH
	if exclusive {
		flag = unix.LOCK_NB | unix.LOCK_EX
	}
	if err := unix.Flock(int(file.Fd()), flag); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
