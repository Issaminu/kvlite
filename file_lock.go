package kvlite

import (
	"fmt"
	"os"
	"time"
)

// fileLockRetryInterval limits how often a waiting Open asks the operating system for the same lock.
const fileLockRetryInterval = 50 * time.Millisecond

// lockDatabaseFile waits for a shared read-only lock or an exclusive writable lock on file. A zero timeout waits without a limit, while a positive timeout returns an error that matches [ErrDatabaseLocked] when the lock remains unavailable.
func lockDatabaseFile(file *os.File, readOnly bool, timeout time.Duration) error {
	// The zero time value represents the configured unlimited wait without a separate flag.
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	for {
		// tryFileLock makes one non-blocking operating-system call. A read-only handle requests a shared lock; a writable handle requests an exclusive lock.
		locked, err := tryFileLock(file, !readOnly)
		if err != nil {
			// Lock contention produces locked=false. Any returned error is an operating-system failure that another attempt cannot be expected to fix.
			return fmt.Errorf("lock database file: %w", err)
		}
		if locked {
			// The file descriptor owns the acquired lock until Open cleanup or DB.Close closes it.
			return nil
		}

		// Sleep between attempts so a waiting Open does not continuously use CPU time.
		delay := fileLockRetryInterval
		// If deadline == 0, this means that we should keep trying to aquire the lock indefinetly.
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return fmt.Errorf("wait %s for database lock: %w", timeout, ErrDatabaseLocked)
			}
			if remaining < delay {
				// Shorten the final sleep so the retry interval does not add avoidable time beyond the configured timeout.
				delay = remaining
			}
		}
		time.Sleep(delay)
	}
}
