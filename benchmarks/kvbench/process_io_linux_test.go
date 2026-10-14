//go:build linux

package kvbench

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadProcessIO_CountsWrites(t *testing.T) {
	before, ok := readProcessIO()
	if !ok {
		t.Skip("/proc/self/io is not readable")
	}
	file, err := os.Create(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	for range 4 {
		if _, err := file.Write(make([]byte, 4096)); err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	after, ok := readProcessIO()
	if !ok {
		t.Fatal("/proc/self/io became unreadable")
	}
	if after.writeCalls < before.writeCalls+4 {
		t.Fatalf("write calls: before %d, after %d, want at least 4 more", before.writeCalls, after.writeCalls)
	}
	if after.storageWrittenBytes < before.storageWrittenBytes {
		t.Fatalf("storage bytes decreased: before %d, after %d", before.storageWrittenBytes, after.storageWrittenBytes)
	}
}
