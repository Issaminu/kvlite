//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package kvlite

import "testing"

func TestMainFileMappingIsCurrentOnUnix(t *testing.T) {
	tests := []struct {
		name       string
		mappedSize int64
		fileSize   int64
		want       bool
	}{
		{name: "same size", mappedSize: 4096, fileSize: 4096, want: true},
		{name: "file grew", mappedSize: 4096, fileSize: 8192, want: false},
		{name: "file shrank", mappedSize: 8192, fileSize: 4096, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := systemMainFileMappingIsCurrent(test.mappedSize, test.fileSize); got != test.want {
				t.Fatalf("current mapping: got %t, want %t", got, test.want)
			}
		})
	}
}
