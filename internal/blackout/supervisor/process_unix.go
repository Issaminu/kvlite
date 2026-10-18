//go:build unix

package main

import (
	"os/exec"
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
