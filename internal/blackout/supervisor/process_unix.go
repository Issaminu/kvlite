//go:build unix

package main

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

// setProcessGroup puts the worker into a new process group.
// The supervisor can then kill the worker and each process that it starts, and a terminal SIGINT does not reach the worker.
func setProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// interruptProcess sends SIGQUIT to the process group of the worker.
// The Go runtime then prints the stack of each goroutine and exits.
func interruptProcess(cmd *exec.Cmd) {
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGQUIT)
}

// killProcess kills the process group of the worker.
func killProcess(cmd *exec.Cmd) {
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}

// killedBySystem reports whether a SIGKILL that the supervisor did not send stopped the process.
// The out-of-memory killers of Linux and macOS send SIGKILL.
func killedBySystem(state *os.ProcessState) bool {
	status, ok := state.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == syscall.SIGKILL
}

// lowPriority returns the command line that runs name with args at the lowest CPU priority, nice 19.
// The nice command sets the priority before the program starts, so every thread of the program has it.
func lowPriority(name string, args ...string) (string, []string) {
	return "nice", append([]string{"-n", "19", name}, args...)
}

// residentBytes returns the memory of the process pid, in bytes.
// On Linux, it reads the resident set size (RSS) and the swapped memory from /proc, so it starts no process.
// On other systems, it runs ps and returns the RSS.
// On macOS, the RSS also counts memory that the Go runtime gave back but the kernel did not take yet, so it can be larger than the memory footprint.
func residentBytes(pid int) (int64, error) {
	if runtime.GOOS == "linux" {
		data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
		if err != nil {
			return 0, err
		}
		// The lines have the form "VmRSS:     12345 kB".
		var kib int64
		found := false
		for line := range strings.Lines(string(data)) {
			if fields := strings.Fields(line); len(fields) == 3 && (fields[0] == "VmRSS:" || fields[0] == "VmSwap:") {
				value, err := strconv.ParseInt(fields[1], 10, 64)
				if err != nil {
					return 0, fmt.Errorf("/proc/%d/status: %w", pid, err)
				}
				kib += value
				found = found || fields[0] == "VmRSS:"
			}
		}
		if !found {
			// A zombie process has no VmRSS line.
			return 0, fmt.Errorf("/proc/%d/status has no VmRSS", pid)
		}
		return kib << 10, nil
	}
	output, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, fmt.Errorf("ps: %w", err)
	}
	kib, err := strconv.ParseInt(strings.TrimSpace(string(output)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("ps: %w", err)
	}
	return kib << 10, nil
}
