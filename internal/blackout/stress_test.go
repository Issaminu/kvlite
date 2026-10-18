package blackout

import "testing"

// The stress checker must find a damaged value and a value of another key.
func TestStressCommitFindsDamagedValues(t *testing.T) {
	value := stressValue(2, 7, 3)
	if commit, err := stressCommit(value, 2, 3); err != nil || commit != 7 {
		t.Fatalf("a correct value: commit %d, %v", commit, err)
	}
	damaged := append([]byte(nil), value...)
	damaged[len(damaged)-1] ^= 1
	if _, err := stressCommit(damaged, 2, 3); err == nil {
		t.Error("a damaged value passed")
	}
	if _, err := stressCommit(value, 2, 4); err == nil {
		t.Error("a value of another key passed")
	}
	if _, err := stressCommit([]byte("no tag"), 2, 3); err == nil {
		t.Error("a value without a tag passed")
	}
}

// The stress checker must find each history violation: a lost completed commit, an invented commit, and a read that goes back.
func TestCheckBoundsFindsHistoryViolations(t *testing.T) {
	for _, test := range []struct {
		name                       string
		commit, low, high, lastSee int64
		ok                         bool
	}{
		{"inside the bounds", 5, 3, 8, 4, true},
		{"older than a completed commit", 2, 3, 8, 0, false},
		{"newer than every started commit", 9, 3, 8, 0, false},
		{"older than an earlier read", 4, 3, 8, 5, false},
	} {
		run := &stressRun{name: "test", clients: 2}
		lastSeen := []int64{0, test.lastSee}
		run.checkBounds(0, 1, test.commit, test.low, test.high, lastSeen)
		if (run.err() == nil) != test.ok {
			t.Errorf("%s: failure %v, want ok=%t", test.name, run.err(), test.ok)
		}
	}
}
