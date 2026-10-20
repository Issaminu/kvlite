package main

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	// defaultMaxRSS is the default hard memory limit of one worker.
	// A 3000-step seed reached 3.44 GiB of RSS on macOS, so this limit leaves room for larger seeds.
	defaultMaxRSS = 4 << 30
	// rssInterval is the time between two RSS samples of a worker.
	rssInterval = time.Second
)

// sizePattern matches a size in bytes with the units of GOMEMLIMIT.
var sizePattern = regexp.MustCompile(`^([0-9]+)(B|KiB|MiB|GiB|TiB)?$`)

// parseBytes parses a size such as 4GiB, with the units of GOMEMLIMIT.
// "off" gives zero.
func parseBytes(text string) (int64, error) {
	if text == "off" {
		return 0, nil
	}
	match := sizePattern.FindStringSubmatch(text)
	if match == nil {
		return 0, fmt.Errorf("%q is not a size, such as 4GiB", text)
	}
	value, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a size: %w", text, err)
	}
	shift := map[string]int{"": 0, "B": 0, "KiB": 10, "MiB": 20, "GiB": 30, "TiB": 40}[match[2]]
	if value > math.MaxInt64>>shift {
		return 0, fmt.Errorf("%q is too large", text)
	}
	return value << shift, nil
}

// physicalMemory returns the size of the RAM in bytes, or zero when the supervisor cannot read it.
func physicalMemory() int64 {
	switch runtime.GOOS {
	case "darwin":
		output, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
		if err != nil {
			return 0
		}
		bytes, err := strconv.ParseInt(strings.TrimSpace(string(output)), 10, 64)
		if err != nil {
			return 0
		}
		return bytes
	case "linux":
		data, err := os.ReadFile("/proc/meminfo")
		if err != nil {
			return 0
		}
		// The line has the form "MemTotal:       16318412 kB".
		for line := range strings.Lines(string(data)) {
			if fields := strings.Fields(line); len(fields) == 3 && fields[0] == "MemTotal:" && fields[2] == "kB" {
				kib, err := strconv.ParseInt(fields[1], 10, 64)
				if err != nil {
					return 0
				}
				return kib << 10
			}
		}
	}
	return 0
}

// defaultWorkers returns the default number of workers.
// It uses at most half the cores, so the computer stays usable.
// It also keeps the memory limits of all workers together at or below half of the RAM.
// It returns at least 1, also when the RAM is smaller than two memory limits.
// A zero ram or maxRSS removes the RAM bound.
func defaultWorkers(cores int, ram, maxRSS int64) int {
	workers := cores / 2
	if ram > 0 && maxRSS > 0 {
		workers = min(workers, int(ram/2/maxRSS))
	}
	return max(1, workers)
}
