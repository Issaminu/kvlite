package main

import (
	"errors"
	"testing"
)

func TestClassify(t *testing.T) {
	exitErr := errors.New("exit status 1")
	passed := &workerResult{Passed: true}
	failed := &workerResult{Passed: false, Failure: "blackout seed 1 failed at step 2: x"}
	for _, test := range []struct {
		name     string
		result   *workerResult
		waitErr  error
		output   string
		timedOut bool
		want     string
	}{
		{"passed", passed, nil, "PASS\n", false, kindPass},
		{"failed check", failed, exitErr, "--- FAIL: TestBlackout\n", false, kindFail},
		{"failed check with a panic line in the report", failed, exitErr, "    panic: boom\n", false, kindFail},
		{"panic in another goroutine", nil, exitErr, "panic: boom\n\ngoroutine 5 [running]:\n", false, kindPanic},
		{"fatal error", nil, exitErr, "fatal error: concurrent map writes\n", false, kindPanic},
		{"panic after a passed result", passed, exitErr, "panic: late\n", false, kindPanic},
		{"timeout", failed, exitErr, "SIGQUIT: quit\n", true, kindTimeout},
		{"exit without a result", nil, exitErr, "something odd\n", false, kindError},
		{"no test ran", nil, nil, "testing: warning: no tests to run\nPASS\n", false, kindError},
		{"non-zero exit after a passed result", passed, exitErr, "", false, kindError},
	} {
		if got := classify(test.result, test.waitErr, test.output, test.timedOut); got != test.want {
			t.Errorf("%s: got %s, want %s", test.name, got, test.want)
		}
	}
}

func TestShellQuote(t *testing.T) {
	if got := commandLine([]string{"GOMAXPROCS=1"}, "/bin/x.test", []string{"-test.run", "^TestBlackout$", "-blackout.seed=5"}); got != "GOMAXPROCS=1 /bin/x.test -test.run '^TestBlackout$' -blackout.seed=5" {
		t.Fatalf("command line: %s", got)
	}
}
