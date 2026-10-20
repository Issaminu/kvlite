package main

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	// targetMaxRSS is the memory limit of a worker when the memory budget allows it.
	// A 3000-step seed reached 3.44 GiB of RSS on an Apple M4 Max, so this limit leaves room for most large seeds.
	targetMaxRSS = 4 << 30
	// minMaxRSS is the smallest memory limit of a worker.
	// Most seeds use less than 400 MiB, but a smaller limit than this kills too many runs to be useful.
	minMaxRSS = 64 << 20
	// maxFailedSamples is the number of RSS samples in a row that can fail before the supervisor stops the worker.
	// A sample fails when the worker has just exited, so one failure is normal.
	maxFailedSamples = 3
	// memoryPollInterval is the time between two checks of the available memory while new runs wait for memory.
	memoryPollInterval = 5 * time.Second
)

// rssInterval returns the time between two RSS samples of a worker.
// On Linux, a sample reads /proc and costs little.
// On other systems, each sample starts ps, so the supervisor samples less often.
func rssInterval() time.Duration {
	if runtime.GOOS == "linux" {
		return 250 * time.Millisecond
	}
	return time.Second
}

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

// systemMemory returns the memory that the supervisor and its workers can use, and the part of it that is available now, in bytes.
// The memory is the RAM, or the cgroup memory limit of the supervisor when that is smaller, for example in a systemd unit with MemoryMax, or in a container.
// A zero value means that the supervisor cannot read it.
func systemMemory() (total, available int64) {
	switch runtime.GOOS {
	case "darwin":
		total = sysctlValue("hw.memsize")
		// kern.memorystatus_level is the available part of the memory, in percent.
		available = total * sysctlValue("kern.memorystatus_level") / 100
	case "linux":
		data, _ := os.ReadFile("/proc/meminfo")
		total, available = meminfoValue(data, "MemTotal:"), meminfoValue(data, "MemAvailable:")
		if limit, free := cgroupMemory(selfCgroupDir()); limit > 0 {
			if total == 0 || limit < total {
				total = limit
			}
			if available == 0 || free < available {
				available = free
			}
		}
	}
	return total, available
}

// sysctlValue returns the integer value of a sysctl variable of macOS, or zero.
func sysctlValue(name string) int64 {
	output, err := exec.Command("sysctl", "-n", name).Output()
	if err != nil {
		return 0
	}
	value, _ := strconv.ParseInt(strings.TrimSpace(string(output)), 10, 64)
	return value
}

// meminfoValue returns the value of the line of /proc/meminfo that starts with key, in bytes, or zero.
// The line has the form "MemTotal:       16318412 kB".
func meminfoValue(data []byte, key string) int64 {
	for line := range strings.Lines(string(data)) {
		if fields := strings.Fields(line); len(fields) == 3 && fields[0] == key && fields[2] == "kB" {
			kib, _ := strconv.ParseInt(fields[1], 10, 64)
			return kib << 10
		}
	}
	return 0
}

// cgroupMemory returns the memory limit (memory.max) of the cgroup v2 directory dir, and the memory that is available under it (memory.max minus memory.current).
// The limit is zero when dir is "", when dir has no limit, or when the supervisor cannot read it.
func cgroupMemory(dir string) (limit, available int64) {
	if dir == "" {
		return 0, 0
	}
	// The value "max" means no limit, and it parses as zero.
	if limit = cgroupValue(dir, "memory.max"); limit <= 0 {
		return 0, 0
	}
	return limit, max(0, limit-cgroupValue(dir, "memory.current"))
}

// cgroupValue returns the integer in the file name of the cgroup v2 directory dir, or zero.
func cgroupValue(dir, name string) int64 {
	data, _ := os.ReadFile(filepath.Join(dir, name))
	value, _ := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	return value
}

// cgroupRoot is the directory of the root of the cgroup v2 hierarchy.
const cgroupRoot = "/sys/fs/cgroup"

// selfCgroupDir returns the cgroup v2 directory of the supervisor, or "".
// The systemd unit sets MemoryMax on this cgroup, and a container shows its limit here too.
func selfCgroupDir() string {
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return ""
	}
	dir, _ := cgroupDir(data)
	return dir
}

// cgroupDir returns the directory of the cgroup v2 in the content of /proc/self/cgroup, which has the line "0::/PATH".
func cgroupDir(data []byte) (string, bool) {
	for line := range strings.Lines(string(data)) {
		if path, found := strings.CutPrefix(strings.TrimSpace(line), "0::"); found {
			return filepath.Join(cgroupRoot, path), true
		}
	}
	return "", false
}

// plan returns the number of workers and the memory limit of each worker for a memory budget and a number of CPUs.
// A workers value of zero and a negative maxRSS mean that plan chooses them; other values are choices of the user, and plan keeps them.
// A maxRSS of zero means no limit.
//
// Each worker gets targetMaxRSS when the memory budget allows it.
// With a smaller budget, one worker gets the whole budget: the computer then runs fewer seeds, but it never uses more memory.
// The number of workers is at most cpus, and at least 1.
// A zero memory budget means that the budget is not known, and then the memory does not limit the plan.
func plan(memoryBudget int64, cpus, workers int, maxRSS int64) (int, int64) {
	perWorker := maxRSS // the memory of one worker, for the number of workers
	if perWorker <= 0 {
		perWorker = targetMaxRSS
	}
	if workers == 0 {
		workers = max(1, cpus)
		if memoryBudget > 0 {
			workers = max(1, min(workers, int(memoryBudget/perWorker)))
		}
	}
	if maxRSS < 0 {
		maxRSS = targetMaxRSS
		if memoryBudget > 0 {
			maxRSS = min(targetMaxRSS, memoryBudget/int64(workers))
		}
	}
	return workers, maxRSS
}

// memoryAllowsWorker reports whether the system has the memory for one more worker: its memory limit, and a tenth of the total memory for the other processes.
// It also reports true when the supervisor cannot read the memory.
func memoryAllowsWorker(total, available, maxRSS int64) bool {
	if total <= 0 || available <= 0 {
		return true
	}
	return available >= maxRSS+total/10
}
