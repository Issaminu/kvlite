//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris && !windows

package kvlite

import (
	"errors"
	"os"
)

func systemMapMainFile(*os.File, int) ([]byte, error) {
	return nil, errors.New("main-file mapping is not supported")
}

func systemUnmapMainFile([]byte) error {
	return nil
}

func systemMainFileMappingIsCurrent(int64, int64) bool {
	return false
}
