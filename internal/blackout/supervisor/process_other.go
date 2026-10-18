//go:build !unix

package main

import "os/exec"

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
