package blackout

import (
	"os"
	"slices"
	"testing"

	"github.com/Issaminu/kvlite"
	"github.com/Issaminu/kvlite/internal/blackout/disk"
)

// TestKeepBackupDiscardsTheBackupThatItDoesNotKeep requires that only the kept backup stays on the simulated storage.
// A backup that stays for the rest of a run holds a full copy of the database, and a long seed grew past 11 GiB that way.
func TestKeepBackupDiscardsTheBackupThatItDoesNotKeep(t *testing.T) {
	for _, test := range []struct {
		name     string
		sync     kvlite.Sync
		complete bool
		kept     string
	}{
		{"complete", kvlite.SyncFull, true, "/backup-2"},
		{"failed", kvlite.SyncFull, false, "/backup-1"},
		{"SyncNone", kvlite.SyncNone, true, "/backup-1"},
	} {
		fsys := disk.New(1, disk.Config{})
		for _, name := range []string{"/backup-1", "/backup-2"} {
			file, err := fsys.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.WriteAt([]byte("database"), 0); err != nil {
				t.Fatal(err)
			}
			file.Close()
		}
		runner := &runner{fsys: fsys, session: session{sync: test.sync}}
		kept := runner.keepBackup(&backupRecord{path: "/backup-1"}, &backupRecord{path: "/backup-2"}, test.complete)
		if kept.path != test.kept || !slices.Equal(fsys.Names(), []string{test.kept}) {
			t.Errorf("%s: kept %s, and the storage holds %q", test.name, kept.path, fsys.Names())
		}
	}
}
