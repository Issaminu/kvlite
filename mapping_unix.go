//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || solaris

package kvlite

import (
	"os"

	"golang.org/x/sys/unix"
)

func systemMapMainFile(file *os.File, length int, writable bool) ([]byte, error) {
	protection := unix.PROT_READ
	if writable {
		protection |= unix.PROT_WRITE
	}
	return unix.Mmap(int(file.Fd()), 0, length, protection, unix.MAP_SHARED)
}

func systemUnmapMainFile(data []byte) error {
	return unix.Munmap(data)
}

// systemSyncMainFile flushes writes through a shared mapping before the file is synced.
func systemSyncMainFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return unix.Msync(data, unix.MS_SYNC)
}

// These systems have a unified buffer cache, so a shared mapping of a file shows later writes to that file.
const systemMappingFollowsWrites = true

// Shared mappings on these systems can receive direct writes.
const systemMappingAllowsWrites = true
