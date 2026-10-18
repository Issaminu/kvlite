package main

import "context"

// tryFunc runs seed with steps safety-phase steps.
// It returns the signature ID of the failure, or "" when the run passed.
// An error means that the run did not complete, for example because the supervisor stops.
type tryFunc func(ctx context.Context, seed uint64, steps int) (string, error)

// shrinkSteps returns the smallest step count that it finds for which seed fails with signature id.
// steps is a step count that fails with id.
// hint, when it is larger than zero and smaller than steps, is a step count to try first.
// A failure at step K gives the hint K+1, because the safety phase of a shorter run is a prefix of the longer run.
//
// The search is a binary search, and a failure need not occur for all step counts above the smallest.
// A shorter run starts its liveness phase earlier, so it can fail in a different way or pass.
// shrinkSteps accepts only a step count that gives the same signature, so its result is a local minimum.
// On an error, it returns the smallest step count that it found until then.
func shrinkSteps(ctx context.Context, try tryFunc, seed uint64, steps, hint int, id string) (int, error) {
	high := steps
	if hint > 0 && hint < high {
		got, err := try(ctx, seed, hint)
		if err != nil {
			return high, err
		}
		if got == id {
			high = hint
		}
	}
	low := 0 // the largest step count that is known not to give the signature
	for high-low > 1 {
		middle := low + (high-low)/2
		got, err := try(ctx, seed, middle)
		if err != nil {
			return high, err
		}
		if got == id {
			high = middle
		} else {
			low = middle
		}
	}
	return high, nil
}
