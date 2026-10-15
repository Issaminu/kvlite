//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package kvbench

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// waitForReleasedFileLocks waits until no process holds a file lock on a file in dataDir.
// KVLite and bbolt lock their database file with flock. If the lock is held, each engine sleeps 50 ms before it tries again.
func waitForReleasedFileLocks(dataDir string, timeout time.Duration) error {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		if err := waitForReleasedFileLock(filepath.Join(dataDir, entry.Name()), deadline); err != nil {
			return err
		}
	}
	return nil
}

func waitForReleasedFileLock(path string, deadline time.Time) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	// Close releases the test lock.
	defer file.Close()
	for {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			return fmt.Errorf("test lock on %s: %w", path, err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("a killed process still locks %s", path)
		}
		time.Sleep(100 * time.Microsecond)
	}
}
