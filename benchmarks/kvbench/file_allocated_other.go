//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package kvbench

func fileAllocatedBytes(string) (int64, bool, error) {
	return 0, false, nil
}
