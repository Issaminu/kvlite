package disk

import (
	"fmt"
	"os"
	"testing"
)

// TestMain runs the tests of this package only when BLACKOUT=1, so a plain go test runs no Blackout tests.
func TestMain(m *testing.M) {
	if os.Getenv("BLACKOUT") != "1" {
		fmt.Println("skip: set BLACKOUT=1 to run the Blackout tests")
		os.Exit(0)
	}
	os.Exit(m.Run())
}
