//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris && !windows

package kvlite

import (
	"errors"
	"os"
)

func tryFileLock(*os.File, bool) (bool, error) {
	return false, errors.ErrUnsupported
}
