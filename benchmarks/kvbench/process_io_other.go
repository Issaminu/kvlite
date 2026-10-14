//go:build !linux

package kvbench

// readProcessIO reports false because only Linux has /proc/self/io.
func readProcessIO() (processIO, bool) {
	return processIO{}, false
}
