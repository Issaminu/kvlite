//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package kvbench

import "time"

// waitForReleasedFileLocks does nothing on platforms without flock.
func waitForReleasedFileLocks(string, time.Duration) error {
	return nil
}
