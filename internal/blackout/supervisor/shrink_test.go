package main

import (
	"context"
	"errors"
	"testing"
)

// fakeTries returns a tryFunc that gives the signature of signatureAt and records each step count that it runs.
func fakeTries(signatureAt func(steps int) string, tried *[]int) tryFunc {
	return func(ctx context.Context, seed uint64, steps int) (string, error) {
		if seed != 7 {
			return "", errors.New("the shrink changed the seed")
		}
		*tried = append(*tried, steps)
		return signatureAt(steps), nil
	}
}

func TestShrinkFindsSmallestStepCount(t *testing.T) {
	var tried []int
	failsFrom37 := func(steps int) string {
		if steps >= 37 {
			return "sig"
		}
		return ""
	}
	got, err := shrinkSteps(context.Background(), fakeTries(failsFrom37, &tried), 7, 300, 0, "sig")
	if err != nil || got != 37 {
		t.Fatalf("shrink: got %d, %v; want 37", got, err)
	}
	if len(tried) > 9 {
		t.Fatalf("shrink used %d tries, want at most 9 for 300 steps: %v", len(tried), tried)
	}
}

func TestShrinkTriesHintFirst(t *testing.T) {
	var tried []int
	failsFrom37 := func(steps int) string {
		if steps >= 37 {
			return "sig"
		}
		return ""
	}
	got, err := shrinkSteps(context.Background(), fakeTries(failsFrom37, &tried), 7, 3000, 40, "sig")
	if err != nil || got != 37 {
		t.Fatalf("shrink: got %d, %v; want 37", got, err)
	}
	if tried[0] != 40 {
		t.Fatalf("first try: got %d steps, want the hint 40", tried[0])
	}
	if len(tried) > 7 {
		t.Fatalf("shrink with a hint used %d tries: %v", len(tried), tried)
	}
}

func TestShrinkAcceptsOnlyTheSameSignature(t *testing.T) {
	var tried []int
	// Short runs fail differently, because their liveness phase starts earlier.
	signatureAt := func(steps int) string {
		switch {
		case steps < 20:
			return "other"
		case steps < 37:
			return ""
		default:
			return "sig"
		}
	}
	got, err := shrinkSteps(context.Background(), fakeTries(signatureAt, &tried), 7, 300, 0, "sig")
	if err != nil || got != 37 {
		t.Fatalf("shrink: got %d, %v; want 37", got, err)
	}
}

func TestShrinkFindsLocalMinimumWhenFailuresAreNotMonotonic(t *testing.T) {
	var tried []int
	signatureAt := func(steps int) string {
		if (steps >= 10 && steps <= 12) || steps >= 50 {
			return "sig"
		}
		return ""
	}
	got, err := shrinkSteps(context.Background(), fakeTries(signatureAt, &tried), 7, 300, 0, "sig")
	if err != nil {
		t.Fatal(err)
	}
	if signatureAt(got) != "sig" || signatureAt(got-1) == "sig" {
		t.Fatalf("shrink: got %d steps, which is not a local minimum of the failing step counts", got)
	}
}

func TestShrinkStopsOnError(t *testing.T) {
	calls := 0
	try := func(ctx context.Context, seed uint64, steps int) (string, error) {
		calls++
		if calls == 3 {
			return "", errStopped
		}
		return "sig", nil
	}
	got, err := shrinkSteps(context.Background(), try, 7, 300, 0, "sig")
	if !errors.Is(err, errStopped) {
		t.Fatalf("error: got %v, want errStopped", err)
	}
	if got != 75 {
		t.Fatalf("best step count after two successful tries: got %d, want 75", got)
	}
}

func TestShrinkIgnoresHintOutOfRange(t *testing.T) {
	var tried []int
	always := func(int) string { return "sig" }
	if _, err := shrinkSteps(context.Background(), fakeTries(always, &tried), 7, 100, 100, "sig"); err != nil {
		t.Fatal(err)
	}
	if tried[0] != 50 {
		t.Fatalf("first try: got %d, want 50 (the hint is not smaller than the steps)", tried[0])
	}
}
