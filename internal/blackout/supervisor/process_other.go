//go:build !unix

package main

import (
	"errors"
	"os/exec"
)

// setProcessGroup does nothing on this system.
func setProcessGroup(cmd *exec.Cmd) {}

// interruptProcess kills the worker, because this system has no SIGQUIT.
func interruptProcess(cmd *exec.Cmd) {
	_ = cmd.Process.Kill()
}

// killProcess kills the worker.
func killProcess(cmd *exec.Cmd) {
	_ = cmd.Process.Kill()
}

// residentBytes fails, because the supervisor cannot read the RSS of a process on this system.
// Use -max-rss off.
func residentBytes(pid int) (int64, error) {
	return 0, errors.ErrUnsupported
}
