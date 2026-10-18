package main

import (
	"fmt"
	"strconv"
	"strings"
)

// defaultLaneSteps is the step count of a lane without a steps key.
// It is equal to blackout.DefaultSteps.
// The supervisor does not import the engine, so that it compiles and reports when the engine does not compile.
const defaultLaneSteps = 300

// lane is a class of runs, for example short runs or long runs.
type lane struct {
	Name   string
	Weight int
	Steps  int
}

// parseLanes parses a spec such as "default=80:steps=300,long=20:steps=3000".
// Each lane is name=weight, followed by zero or more :key=value pairs.
func parseLanes(spec string) ([]lane, error) {
	var lanes []lane
	names := make(map[string]bool)
	for _, part := range strings.Split(spec, ",") {
		fields := strings.Split(strings.TrimSpace(part), ":")
		name, weightText, found := strings.Cut(fields[0], "=")
		if !found || name == "" {
			return nil, fmt.Errorf("lane %q: want name=weight", part)
		}
		if names[name] {
			return nil, fmt.Errorf("lane %q: the name is used two times", name)
		}
		names[name] = true
		weight, err := strconv.Atoi(weightText)
		if err != nil || weight <= 0 {
			return nil, fmt.Errorf("lane %q: the weight %q is not a positive integer", name, weightText)
		}
		current := lane{Name: name, Weight: weight, Steps: defaultLaneSteps}
		for _, field := range fields[1:] {
			key, value, _ := strings.Cut(field, "=")
			switch key {
			case "steps":
				steps, err := strconv.Atoi(value)
				if err != nil || steps <= 0 {
					return nil, fmt.Errorf("lane %q: steps %q is not a positive integer", name, value)
				}
				current.Steps = steps
			default:
				return nil, fmt.Errorf("lane %q: unknown key %q (the only key is steps)", name, key)
			}
		}
		lanes = append(lanes, current)
	}
	return lanes, nil
}

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
