//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package kvbench

import (
	"errors"
	"os"
	"syscall"
)

func fileAllocatedBytes(path string) (int64, bool, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, true, nil
	}
	if err != nil {
		return 0, false, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false, nil
	}
	return stat.Blocks * 512, true, nil
}
