package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/Issaminu/kvlite/internal/page"
)

func makePagePatch(base, target []byte) []byte {
	if len(base) != len(target) {
		panic("page patch test images have different sizes")
	}
	var patch PagePatch
	patch.reset()
	patch.appendChangedRanges(0, base, target)
	return patch.appendEncoded(nil)
}

func TestPagePatch_RoundTripsChangedRanges(t *testing.T) {
	base := []byte("0123456789abcdefghijkl")
	target := bytes.Clone(base)
	copy(target[4:6], "AB")
	copy(target[14:17], "XYZ")
	patch := makePagePatch(base, target)

	got, err := ApplyPagePatch(base, patch, 64)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, target) {
		t.Fatalf("patched page: got %q, want %q", got, target)
	}
}

func TestPagePatch_ReplayChainRestoresLatestImage(t *testing.T) {
	base := []byte("0123456789")
	first := []byte("01AA456789")
	latest := []byte("01AA45ZZ89")
	firstPatch := makePagePatch(base, first)
	latestPatch := makePagePatch(first, latest)

	for _, start := range [][]byte{base, latest} {
		got, err := ApplyPagePatch(start, firstPatch, 64)
		if err != nil {
			t.Fatal(err)
		}
		got, err = ApplyPagePatch(got, latestPatch, 64)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, latest) {
			t.Fatalf("replayed page from %q: got %q, want %q", start, got, latest)
		}
	}
}

func TestApplyPagePatch_RejectsInvalidRange(t *testing.T) {
	patch := makePagePatch([]byte("base"), []byte("targ"))
	binary.LittleEndian.PutUint32(patch[4:8], ^uint32(0))

	if _, err := ApplyPagePatch([]byte("base"), patch, 64); !errors.Is(err, page.ErrInvalid) {
		t.Fatalf("invalid range error: got %v, want ErrInvalid", err)
	}
}

func TestApplyPagePatch_RejectsIncompleteRangeHeader(t *testing.T) {
	patch := append(makePagePatch([]byte("base"), []byte("targ")), 0)

	if _, err := ApplyPagePatch([]byte("base"), patch, 64); !errors.Is(err, page.ErrInvalid) {
		t.Fatalf("incomplete range header error: got %v, want ErrInvalid", err)
	}
}

func TestApplyPagePatch_RejectsBaseLargerThanPage(t *testing.T) {
	patch := makePagePatch([]byte("base"), []byte("targ"))

	if _, err := ApplyPagePatch([]byte("base"), patch, 3); !errors.Is(err, page.ErrInvalid) {
		t.Fatalf("large base error: got %v, want ErrInvalid", err)
	}
}
