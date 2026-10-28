package blackout

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Issaminu/kvlite"
	"github.com/Issaminu/kvlite/internal/blackout/disk"
)

func TestSessionFaultPolicyKeepsDurableModes(t *testing.T) {
	for _, mode := range []kvlite.Sync{kvlite.SyncFull, kvlite.SyncNormal, kvlite.SyncNone} {
		t.Run(syncNames[mode], func(t *testing.T) {
			config := newConfig(2)
			config.OptionChanges = false
			config.Sync = mode
			config.IOFaults = true
			config.Disk = disk.Config{WriteFault: disk.Always}
			r := newRunner(Target{}, 2, config, Settings{})
			r.sessions = 1
			r.faults = true
			r.fsys.SetFaults(true)
			r.startSession()
			file, err := r.fsys.OpenFile("/probe", os.O_CREATE|os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			_, err = file.WriteAt([]byte("data"), 0)
			if mode == kvlite.SyncNone {
				if err != nil || r.faults {
					t.Fatalf("SyncNone healthy-storage lane: write=%v faults=%t", err, r.faults)
				}
				return
			}
			if !errors.Is(err, disk.ErrInjected) || !r.faults {
				t.Fatalf("durable mode lost fault coverage: write=%v faults=%t", err, r.faults)
			}
		})
	}
}

func TestSyncNoneSessionCancelsPendingCrash(t *testing.T) {
	config := newConfig(2)
	config.OptionChanges = false
	config.Sync = kvlite.SyncNone
	config.Disk = disk.Config{}
	r := newRunner(Target{}, 2, config, Settings{})
	r.sessions = 1
	file, err := r.fsys.OpenFile("/probe", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	r.fsys.ScheduleCrash(1)
	r.startSession()
	if _, err := file.WriteAt([]byte("data"), 0); err != nil {
		t.Fatalf("SyncNone inherited a scheduled durable-mode crash: %v", err)
	}
	if crashed, _ := r.fsys.Crashed(); crashed {
		t.Fatal("SyncNone inherited a scheduled durable-mode crash")
	}
}

func TestSessionFaultPolicyRestoresFaultsAfterSyncNone(t *testing.T) {
	for _, next := range []kvlite.Sync{kvlite.SyncFull, kvlite.SyncNormal} {
		t.Run(syncNames[next], func(t *testing.T) {
			config := newConfig(2)
			config.OptionChanges = false
			config.Sync = kvlite.SyncNone
			config.IOFaults = true
			config.Disk = disk.Config{WriteFault: disk.Always}
			r := newRunner(Target{}, 2, config, Settings{})
			r.sessions = 1
			r.startSession()
			file, err := r.fsys.OpenFile("/probe", os.O_CREATE|os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if _, err := file.WriteAt([]byte("data"), 0); err != nil {
				t.Fatalf("SyncNone write: %v", err)
			}
			r.config.Sync = next
			r.startSession()
			if _, err := file.WriteAt([]byte("data"), 0); !errors.Is(err, disk.ErrInjected) {
				t.Fatalf("fault coverage after SyncNone: %v", err)
			}
			if !r.tainted {
				t.Fatal("a mode change alone cleared the unsynced-data restriction")
			}
			r.crash.enabled = false
			r.startSession()
			if _, err := file.WriteAt([]byte("data"), 0); err != nil || r.faults {
				t.Fatalf("liveness must stay fault-free: write=%v faults=%t", err, r.faults)
			}
		})
	}
}

func TestSyncNoneUnexpectedDirectFailureIsNotAnOrdinaryAbort(t *testing.T) {
	config := newConfig(2)
	config.Sync = kvlite.SyncNone
	r := newRunner(Target{}, 2, config, Settings{})
	r.session.sync = kvlite.SyncNone
	mark := r.model.mark()
	r.model.apply(txMutation{kind: mutationCreateBucket, path: "data"})
	err := errors.Join(kvlite.ErrDatabaseWriteFailed, disk.ErrInjected)
	r.finishCommit("update", mark, err, nil)
	if r.failure == nil {
		t.Fatal("a partial main-file write was accepted as an unchanged transaction")
	}
	if _, exists := r.model.state["data"]; exists {
		t.Fatal("failed direct commit retained unacknowledged model changes")
	}
}

func TestSyncNoneHealthyWritesKeepModelAndRollbackChecks(t *testing.T) {
	config := newConfig(2)
	config.Sync = kvlite.SyncNone
	config.Checksums = false
	r := newRunner(Target{}, 2, config, Settings{})
	r.session.sync = kvlite.SyncNone
	path := filepath.Join(t.TempDir(), "db")
	db, err := kvlite.Open(path, 0o600, &kvlite.Options{Synchronous: kvlite.SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mark := r.model.mark()
	var applied []txMutation
	err = db.Update(func(tx *kvlite.Tx) error {
		return r.runCallback(tx, []txMutation{
			{kind: mutationCreateBucket, path: "data"},
			{kind: mutationPut, path: "data", key: "key", value: []byte("committed")},
		}, false, endReturn, &applied)
	})
	r.finishCommit("update", mark, err, applied)
	if r.failure != nil || r.model.latest != 1 || r.stats.Commits != 1 {
		t.Fatalf("healthy commit: failure=%v latest=%d commits=%d", r.failure, r.model.latest, r.stats.Commits)
	}
	err = db.Update(func(tx *kvlite.Tx) error {
		return r.runCallback(tx, []txMutation{
			{kind: mutationPut, path: "data", key: "key", value: []byte("aborted")},
		}, true, endReturn, &applied)
	})
	if !errors.Is(err, errAbort) || len(applied) != 0 || r.model.latest != 1 {
		t.Fatalf("callback rollback: error=%v applied=%d latest=%d", err, len(applied), r.model.latest)
	}
	value, err := db.Get([]byte("data"), []byte("key"))
	if err := r.checkValue("Get", "data", "key", value, err); err != nil {
		t.Fatalf("rollback changed the engine or model: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := kvlite.Open(path, 0o600, &kvlite.Options{Synchronous: kvlite.SyncNormal})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	value, err = reopened.Get([]byte("data"), []byte("key"))
	if err := r.checkValue("Get after mode change", "data", "key", value, err); err != nil {
		t.Fatal(err)
	}
	if err := r.checkValue("wrong value", "data", "key", []byte("corrupt"), nil); !errors.Is(err, errCheckFailed) || r.failure == nil {
		t.Fatal("SyncNone stopped checking returned values against the model")
	}
}

func TestDurableSessionKeepsPendingCrash(t *testing.T) {
	for _, mode := range []kvlite.Sync{kvlite.SyncFull, kvlite.SyncNormal} {
		t.Run(syncNames[mode], func(t *testing.T) {
			config := newConfig(2)
			config.OptionChanges = false
			config.Sync = mode
			config.Disk = disk.Config{}
			r := newRunner(Target{}, 2, config, Settings{})
			r.sessions = 1
			file, err := r.fsys.OpenFile("/probe", os.O_CREATE|os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			r.fsys.ScheduleCrash(1)
			r.startSession()
			if _, err := file.WriteAt([]byte("data"), 0); !errors.Is(err, disk.ErrCrashed) {
				t.Fatalf("durable mode lost its scheduled crash: %v", err)
			}
		})
	}
}
