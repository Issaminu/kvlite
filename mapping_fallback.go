//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !solaris && !windows

package kvlite

import (
	"errors"
	"fmt"
	"os"
)

// Without a mapping, reads use the file.
// OpenBSD builds this file: it has no unified buffer cache, so a mapping can show old pages after a write to the file.
// SQLite also disables memory-mapped I/O on OpenBSD, because it is known to not work there (sqliteInt.h).
func systemMapMainFile(*os.File, int) ([]byte, error) {
	return nil, fmt.Errorf("main-file mapping: %w", errors.ErrUnsupported)
}

func systemUnmapMainFile([]byte) error {
	return nil
}

const systemMappingFollowsWrites = false
