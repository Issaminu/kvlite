//go:build !unix

package main

import (
	"fmt"
	"os"
	"testing"
)

// TestMain runs the tests only when BLACKOUT=1, so a plain go test runs no Blackout tests.
// On Unix, worker_unix_test.go has the TestMain, because the test binary also plays a worker there.
func TestMain(m *testing.M) {
	if os.Getenv("BLACKOUT") != "1" {
		fmt.Println("skip: set BLACKOUT=1 to run the Blackout tests")
		os.Exit(0)
	}
	os.Exit(m.Run())
}
