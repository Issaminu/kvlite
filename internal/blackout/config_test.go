package blackout

import (
	"testing"

	"github.com/Issaminu/kvlite/internal/blackout/disk"
)

// TestReadCorruptionNeedsFixedChecksums requires read corruption only in a run whose data pages keep their checksums for the whole run.
// Without those checksums, KVLite cannot find a flipped bit, so the run would fail without a bug.
func TestReadCorruptionNeedsFixedChecksums(t *testing.T) {
	corrupted := false
	for seed := uint64(1); seed <= 200; seed++ {
		config := newConfig(seed)
		if config.fixedChecksums() {
			corrupted = corrupted || config.Disk.ReadCorruption != disk.Never
		} else if config.Disk.ReadCorruption != disk.Never {
			t.Fatalf("seed %d: read corruption %v without fixed data-page checksums", seed, config.Disk.ReadCorruption)
		}
	}
	if !corrupted {
		t.Fatal("no seed of 200 has read corruption")
	}
}
