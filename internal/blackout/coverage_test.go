package blackout

import (
	"slices"
	"testing"
)

// With no runs, every case is missing, so the coverage test cannot pass by accident.
func TestMissingCoverageListsEveryCaseWithoutRuns(t *testing.T) {
	missing := MissingCoverage(nil)
	for _, want := range []string{"operation update", "change put", "scan ScanRange", "contract panic in Update", "torn write", "crash inside sync wal", "sync=normal with power failures", "values of several MiB"} {
		if !slices.Contains(missing, want) {
			t.Errorf("MissingCoverage(nil) lacks %q", want)
		}
	}
	if len(missing) < 60 {
		t.Errorf("MissingCoverage(nil) lists %d cases, want at least 60: %q", len(missing), missing)
	}
}
