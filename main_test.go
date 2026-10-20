package kvlite

import (
	"os"
	"testing"

	"github.com/Issaminu/kvlite/internal/fileio"
)

// TestMain makes the real-file syncs of this test binary skip the drive-cache flush on macOS (see [fileio.SkipDriveCacheFlushForTesting]).
// No test here can check what a power failure keeps on a real drive, so the flush only makes the tests slow: about 4 ms for each of more than 1000 syncs.
// The tests still make every sync call, so tests that count or fail syncs do not change.
func TestMain(m *testing.M) {
	fileio.SkipDriveCacheFlushForTesting = true
	os.Exit(m.Run())
}
