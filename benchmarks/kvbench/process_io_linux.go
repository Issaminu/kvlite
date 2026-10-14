//go:build linux

package kvbench

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// readProcessIO reads the write counters of this process from /proc/self/io.
// write_bytes counts the bytes that this process sent to storage. syscw counts
// write system calls, including writes to sockets.
func readProcessIO() (processIO, bool) {
	file, err := os.Open("/proc/self/io")
	if err != nil {
		return processIO{}, false
	}
	defer file.Close()
	var counters processIO
	found := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		name, value, ok := strings.Cut(scanner.Text(), ": ")
		if !ok {
			continue
		}
		number, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
		if err != nil {
			continue
		}
		switch name {
		case "syscw":
			counters.writeCalls = number
			found++
		case "write_bytes":
			counters.storageWrittenBytes = number
			found++
		}
	}
	return counters, scanner.Err() == nil && found == 2
}
