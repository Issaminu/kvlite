package main

// lane is a class of runs, for example short runs or long runs.
type lane struct {
	Name   string
	Weight int
	Steps  int
}

// defaultLanes are the classes of the new seeds: most seeds are short, and a fifth of them are long.
// Short runs test many configurations fast, and long runs make the large databases that some bugs need.
// The short step count is equal to blackout.DefaultSteps.
// The supervisor does not import the engine, so that it compiles and reports when the engine does not compile.
var defaultLanes = []lane{{"default", 80, 300}, {"long", 20, 3000}}

// totalWeight returns the sum of the lane weights.
func totalWeight(lanes []lane) int {
	total := 0
	for _, lane := range lanes {
		total += lane.Weight
	}
	return total
}

// pickLane returns the lane that n selects.
// n must be in [0, totalWeight(lanes)); a uniform n selects each lane in proportion to its weight.
func pickLane(lanes []lane, n int) lane {
	for _, lane := range lanes {
		if n < lane.Weight {
			return lane
		}
		n -= lane.Weight
	}
	return lanes[len(lanes)-1]
}
