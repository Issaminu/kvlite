//go:build unix

package main

import (
	"fmt"
	"os/exec"
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

// residentBytes returns the resident set size (RSS) of the process pid, in bytes.
// It runs ps, which macOS and Linux both have.
// On macOS, the RSS also counts memory that the Go runtime gave back but the kernel did not take yet, so it can be larger than the memory footprint.
func residentBytes(pid int) (int64, error) {
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
