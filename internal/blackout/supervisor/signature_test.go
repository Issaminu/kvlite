package main

import (
	"slices"
	"strings"
	"testing"
)

func TestNormalize(t *testing.T) {
	for _, test := range []struct{ in, want string }{
		{"page 22 of 1048576", "page N of N"},
		{"checksum 0x1a2b mismatch", "checksum H mismatch"},
		{"trace deadbeef1234 differs", "trace H differs"},
		{"hash 1a2b3c4d5e6f7a8b", "hash H"},
		{"short ab12 run", "short abN run"}, // too short for a hexadecimal run
		{"facade decade", "facade decade"},  // letters only: a word, not a number
		{"main-file and WAL page IDs", "main-file and WAL page IDs"},
		{"crc32c at offset -4096", "crcNc at offset -N"},
	} {
		if got := normalize(test.in); got != test.want {
			t.Errorf("normalize(%q) = %q, want %q", test.in, got, test.want)
		}
	}
}

func TestFailSignatureDropsSeedStepAndNumbers(t *testing.T) {
	first := "blackout seed 1364 failed at step 87: Open: unexpected error after 1 attempts: metadata last page 22 exceeds main-file and WAL page IDs: invalid database"
	second := "blackout seed 99 failed at step 3: Open: unexpected error after 12 attempts: metadata last page 7 exceeds main-file and WAL page IDs: invalid database"
	want := "fail: Open: unexpected error after N attempts: metadata last page N exceeds main-file and WAL page IDs: invalid database"
	if got := signatureText(kindFail, first, ""); got != want {
		t.Fatalf("signature text:\n got %q\nwant %q", got, want)
	}
	if signatureText(kindFail, second, "") != want {
		t.Fatal("two reports that differ only in numbers have different signatures")
	}
}

const panicOutput = `panic: runtime error: index out of range [5] with length 3

goroutine 7 [running]:
github.com/Issaminu/kvlite/internal/btree.(*Tree).insert(0x1400012e000, {0x14000180000, 0x10, 0x10})
	/Users/x/kvlite/internal/btree/tree.go:123 +0x1a4
github.com/Issaminu/kvlite/internal/btree.(*Tree).Put(...)
	/Users/x/kvlite/internal/btree/tree.go:80
github.com/Issaminu/kvlite.(*DB).applyBatch(0x14000100000, 0x14000190000)
	/Users/x/kvlite/write_batch.go:200 +0x88
github.com/Issaminu/kvlite.(*DB).writeWorker(0x14000100000)
	/Users/x/kvlite/write_batch.go:150 +0x50
created by github.com/Issaminu/kvlite.Open in goroutine 6
	/Users/x/kvlite/db.go:30 +0x100

goroutine 1 [chan receive]:
testing.(*T).Run(0x14000003a00, {0x1030b2f5e, 0xc}, 0x1031f2a48)
	/usr/local/go/src/testing/testing.go:1750 +0x37c
`

const quitOutput = `SIGQUIT: quit
PC=0x18a0 m=0 sigcode=0

goroutine 0 gp=0x1 m=0 mp=0x2 [idle]:
runtime.mach_semaphore_wait(...)

goroutine 1 [chan receive]:
testing.(*T).Run(0x14000003a00, {0x1030b2f5e, 0xc}, 0x1031f2a48)
	/usr/local/go/src/testing/testing.go:1750 +0x37c
main.main()
	_testmain.go:45 +0x9c

goroutine 18 [select]:
github.com/Issaminu/kvlite.(*DB).Update(0x1400010e000, 0x14000072f00)
	/x/db.go:300 +0x1f0
github.com/Issaminu/kvlite/internal/blackout.(*runner).update(0x1400011c000)
	/x/operations.go:50 +0x88
`

func TestStackFrames(t *testing.T) {
	got := stackFrames(panicOutput, 3)
	want := []string{
		"github.com/Issaminu/kvlite/internal/btree.(*Tree).insert",
		"github.com/Issaminu/kvlite/internal/btree.(*Tree).Put",
		"github.com/Issaminu/kvlite.(*DB).applyBatch",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("panic frames:\n got %q\nwant %q", got, want)
	}
	// The first goroutines of a SIGQUIT dump have no KVLite frames.
	got = stackFrames(quitOutput, 3)
	want = []string{"github.com/Issaminu/kvlite.(*DB).Update", "github.com/Issaminu/kvlite/internal/blackout.(*runner).update"}
	if !slices.Equal(got, want) {
		t.Fatalf("timeout frames:\n got %q\nwant %q", got, want)
	}
	if got := stackFrames("panic: boom\n\ngoroutine 1 [running]:\nmain.main()\n\t/x.go:1\n", 3); got != nil {
		t.Fatalf("frames without KVLite functions: got %q, want none", got)
	}
}

func TestPanicAndTimeoutSignatures(t *testing.T) {
	text := signatureText(kindPanic, crashMessage(panicOutput), panicOutput)
	want := "panic: github.com/Issaminu/kvlite/internal/btree.(*Tree).insert < github.com/Issaminu/kvlite/internal/btree.(*Tree).Put < github.com/Issaminu/kvlite.(*DB).applyBatch"
	if text != want {
		t.Fatalf("panic signature:\n got %q\nwant %q", text, want)
	}
	// Other addresses and line numbers give the same signature.
	moved := strings.NewReplacer("0x1400012e000", "0x1400099e000", "tree.go:123", "tree.go:456", "+0x1a4", "+0x2b0").Replace(panicOutput)
	if got := signatureText(kindPanic, crashMessage(moved), moved); got != text {
		t.Fatalf("moved panic signature: got %q, want %q", got, text)
	}
	noFrames := "fatal error: concurrent map writes\n\ngoroutine 3 [running]:\nruntime.throw()\n"
	if got := signatureText(kindPanic, crashMessage(noFrames), noFrames); got != "panic: fatal error: concurrent map writes" {
		t.Fatalf("panic signature without frames: got %q", got)
	}
	if got := signatureText(kindTimeout, "timeout", quitOutput); !strings.HasPrefix(got, "timeout: github.com/Issaminu/kvlite.(*DB).Update < ") {
		t.Fatalf("timeout signature: got %q", got)
	}
	if got := signatureText(kindError, "exit status 3: open 0x1234: error 17", ""); got != "error: exit status N: open H: error N" {
		t.Fatalf("error signature: got %q", got)
	}
}

func TestCrashMessageNeedsLineStart(t *testing.T) {
	if got := crashMessage("    blackout_test.go:107: step 3: panic: boom\n"); got != "" {
		t.Fatalf("an indented report line counts as a crash: %q", got)
	}
	if got := crashMessage("ok\npanic: boom [recovered]\n"); got != "panic: boom [recovered]" {
		t.Fatalf("crash message: got %q", got)
	}
}

func TestFailedStep(t *testing.T) {
	if got := failedStep("blackout seed 5 failed at step 87: Open: bad"); got != 87 {
		t.Fatalf("failed step: got %d, want 87", got)
	}
	if got := failedStep("panic: boom"); got != -1 {
		t.Fatalf("failed step of a panic: got %d, want -1", got)
	}
}

func TestTruncateKeepsUTF8(t *testing.T) {
	if got := truncate("aé", 2); got != "a" {
		t.Fatalf("truncate: got %q, want %q", got, "a")
	}
}
