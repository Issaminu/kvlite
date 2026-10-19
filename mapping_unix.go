//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || solaris

package kvlite

import (
	"os"

	"golang.org/x/sys/unix"
)

func systemMapMainFile(file *os.File, length int) ([]byte, error) {
	return unix.Mmap(int(file.Fd()), 0, length, unix.PROT_READ, unix.MAP_SHARED)
}

func systemUnmapMainFile(data []byte) error {
	return unix.Munmap(data)
}

// These systems have a unified buffer cache, so a shared mapping of a file shows later writes to that file.
const systemMappingFollowsWrites = true
