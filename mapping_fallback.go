//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris && !windows

package kvlite

import (
	"errors"
	"fmt"
	"os"
)

func systemMapMainFile(*os.File, int) ([]byte, error) {
	return nil, fmt.Errorf("main-file mapping: %w", errors.ErrUnsupported)
}

func systemUnmapMainFile([]byte) error {
	return nil
}

func systemMainFileMappingIsCurrent(int64, int64) bool {
	return false
}
