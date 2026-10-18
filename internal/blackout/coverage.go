package blackout

import (
	"fmt"

	"github.com/Issaminu/kvlite"
)

// MissingCoverage returns each important case that no run of results reached.
// The cases are every operation, change, scan, and contract, every outcome that the checks handle, every kind of fault, and the important option combinations.
// A change to Blackout that makes a case unreachable shows up here, even when every seed passes.
func MissingCoverage(results []Result) []string {
	var total Stats
	reached := map[string]bool{}
	for _, result := range results {
		total.Add(result.Stats)
		config := result.Config
		reached[fmt.Sprintf("sync=%s checksums=%t", syncNames[config.Sync], config.Checksums)] = true
		if config.PowerFailures {
			reached[fmt.Sprintf("sync=%s with power failures", syncNames[config.Sync])] = true
		}
		reached[fmt.Sprintf("injected I/O errors=%t", config.IOFaults)] = true
		reached[fmt.Sprintf("nested buckets=%t", config.NestedBuckets)] = true
		reached[fmt.Sprintf("key shape %d", config.KeyShape)] = true
		reached[fmt.Sprintf("access pattern %d", config.Access)] = true
		if config.ValueWeights[len(config.ValueWeights)-1] > 0 {
			reached["values of several MiB"] = true
		}
		if config.MaxMutations >= 1000 {
			reached["thousands of changes in one commit"] = true
		}
	}

	var missing []string
	require := func(name string, ok bool) {
		if !ok {
			missing = append(missing, name)
		}
	}
	for _, sync := range []kvlite.Sync{kvlite.SyncFull, kvlite.SyncNormal, kvlite.SyncNone} {
		for _, checksums := range []bool{false, true} {
			name := fmt.Sprintf("sync=%s checksums=%t", syncNames[sync], checksums)
			require(name, reached[name])
		}
	}
	for _, sync := range []kvlite.Sync{kvlite.SyncFull, kvlite.SyncNormal} {
		name := fmt.Sprintf("sync=%s with power failures", syncNames[sync])
		require(name, reached[name])
	}
	for _, enabled := range []bool{false, true} {
		for _, format := range []string{"injected I/O errors=%t", "nested buckets=%t"} {
			name := fmt.Sprintf(format, enabled)
			require(name, reached[name])
		}
	}
	for shape := range keyShapeCount {
		name := fmt.Sprintf("key shape %d", shape)
		require(name, reached[name])
	}
	for access := range accessCount {
		name := fmt.Sprintf("access pattern %d", access)
		require(name, reached[name])
	}
	require("values of several MiB", reached["values of several MiB"])
	require("thousands of changes in one commit", reached["thousands of changes in one commit"])

	for op, count := range total.Steps {
		require("operation "+operationNames[op], count > 0)
	}
	for kind, count := range total.Mutations {
		// SetDataPageChecksums is an operation, not a change inside a callback.
		require("change "+mutationNames[kind], count > 0 || mutationKind(kind) == mutationChecksums)
	}
	for kind, count := range total.Scans {
		require("scan "+scanNames[kind], count > 0)
	}
	for kind, count := range total.Contracts {
		require("contract "+contractNames[kind], count > 0)
	}
	for _, outcome := range []struct {
		name  string
		count int
	}{
		{"acknowledged commit", total.Commits},
		{"aborted transaction", total.Aborts},
		{"commit with an unknown result", total.Uncertain},
		{"unknown result that recovery kept", total.UncertainKept},
		{"unknown result that recovery lost", total.UncertainLost},
		{"unsynced commit that a power failure lost", total.LostUnsyncedCommits},
		{"crash between operations", total.CrashesNow},
		{"crash inside Open", total.CrashesInOpen},
		{"failed Open", total.FailedOpens},
		{"failed Close", total.FailedCloses},
		{"handle stopped by ErrWALWriteFailed", total.StoppedHandles},
		{"process-crash image check", total.ImageChecks},
		{"memory database check", total.MemoryChecks},
		{"injected I/O error", total.Disk.InjectedFaults},
		{"failed sync that dropped changes", total.Disk.DroppedSyncs},
		{"failed sync that dropped the cache view", total.Disk.DroppedCaches},
		{"process crash", total.Disk.ProcessCrashes},
		{"power failure", total.Disk.PowerFailures},
		{"crash inside a call", total.Disk.ScheduledCrashes},
		{"torn write", total.Disk.TornWrites},
		{"lost write", total.Disk.LostWrites},
		{"lost truncate", total.Disk.LostTruncates},
		{"lost name change", total.Disk.LostNameChanges},
	} {
		require(outcome.name, outcome.count > 0)
	}
	for _, kind := range []string{"write wal", "sync wal", "truncate wal", "write main", "sync main"} {
		require("crash inside "+kind, total.CrashesInCalls[kind] > 0)
	}
	return missing
}
