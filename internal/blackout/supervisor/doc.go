// Command supervisor is the autonomous runner of Blackout, KVLite's deterministic simulation test.
//
// The supervisor compiles the test binary of the root package once for each commit.
// Then it runs random seeds without end, each in its own process, and keeps a log of the failures.
// The choice of a seed is random, but the run of a seed is deterministic, so each failure in the log has a replay command.
//
// Usage:
//
//	go run ./internal/blackout/supervisor [flags]
//
// This command runs 2 workers for 30 seconds with short runs:
//
//	go run ./internal/blackout/supervisor -workers 2 -duration 30s -lanes default=1:steps=50 -out /tmp/blackout
//
// # Workers
//
// Each worker is a separate process of the test binary that runs one seed:
//
//	blackout-COMMIT.test -test.run '^TestBlackout$' -blackout.seed=N -blackout.steps=S -blackout.json=PATH
//
// A separate process isolates the runs: a panic, a deadlock, a leak, or a changed global stops only one run.
// Each worker gets GOMAXPROCS=1, so it uses at most one CPU, and the soft memory limit GOMEMLIMIT from -memlimit.
// A worker that runs longer than -timeout gets SIGQUIT, which makes the Go runtime print the stack of each goroutine.
// If the worker does not stop, the supervisor kills its process group.
//
// The supervisor puts the result of each run into one of these kinds:
//   - pass: the JSON result says that the seed passed.
//   - fail: the JSON result says that a check failed.
//   - panic: there is no JSON result, and the output has a "panic:" or "fatal error:" line.
//   - timeout: the run did not stop in time.
//   - memory: the RSS of the worker passed -max-rss, the supervisor could not read the RSS, or the system killed the worker with SIGKILL. This is not a failure of KVLite.
//   - error: all other results, for example a non-zero exit without a JSON result.
//
// # Resources
//
// The supervisor can run for good on a small computer that other programs share.
// By default, all workers together get half of the memory and half of the CPUs:
//   - The memory is the RAM, or the smallest cgroup memory limit of the supervisor and its ancestor cgroups when that is smaller, for example the MemoryMax of a systemd unit or of its slice.
//     The supervisor reads it with "sysctl" on macOS and from /proc and /sys/fs/cgroup on Linux.
//     -memory changes the budget.
//   - The CPUs are the CPUs of the computer, or the smallest cgroup CPU limit (cpu.max) of the supervisor and its ancestor cgroups when it is smaller, also a fraction such as 0.5.
//     -cpus changes the budget, also to less than one CPU.
//
// From the budgets, the supervisor chooses the number of workers and the hard memory limit of each worker, -max-rss:
//   - Each worker gets 4 GiB when the memory budget allows it.
//     A 3000-step seed reached 3.44 GiB of RSS on an Apple M4 Max.
//   - With a smaller budget, one worker gets the whole budget.
//     Most seeds need less than 400 MiB, but some long seeds need several GiB, and the limit kills them.
//   - There are at most as many workers as CPUs in the budget, and at least one.
//   - GOMEMLIMIT is half of -max-rss.
//
// For example, a computer with 14 cores and 36 GiB of RAM gets 4 workers with 4 GiB each.
// A computer with 1 CPU and 1 GiB of RAM gets 1 worker with 512 MiB, and the worker waits after each run as long as the run took, so it uses half a CPU on average.
//
// The supervisor protects the other programs in these ways:
//   - It reads the memory of each worker every 250 ms on Linux (from /proc, which costs little) and every second on other systems (with ps).
//     When the RSS and the swapped memory of a worker are larger than -max-rss, it kills the process group at once.
//     When it cannot read the memory 3 times in a row, it also stops the worker, because the limit then does not work.
//   - It starts a worker only when the system has the memory limit of the worker available, and a tenth of the memory more for the other programs.
//     Otherwise, the new runs wait and the log says so.
//   - On Linux, it asks the out-of-memory killer to kill the supervisor and its workers first.
//     The supervisor counts a SIGKILL that it did not send as a memory kill.
//   - The workers and the compile run at the lowest CPU priority, nice 19.
//     The compile runs no more programs at the same time than the CPU budget allows.
//   - The log of errors keeps at most 100 errors with 100 seeds each, so its memory stays small.
//
// A memory kill is not a failure of KVLite: status.json counts it in memory_kills, the log names the seed, and the supervisor makes no error and no shrink of it.
//
// The limit of each worker is a sample, not a limit of the kernel.
// A worker can grow by about 1 GiB in a second, so between two samples it can pass its limit by that much.
// On macOS, the RSS also counts memory that the Go runtime gave back but the kernel did not take yet, so the limit kills earlier than necessary.
// But under memory pressure, macOS compresses or swaps pages, which then leave the RSS, so the limit can kill later than necessary.
// For a hard limit of the kernel, run the supervisor in a systemd unit with MemoryMax, as blackout.service in this directory does.
// On Windows, the supervisor cannot read the RSS, so it refuses to start until -max-rss is off.
//
// # Lanes
//
// A lane is a class of runs with a weight and settings.
// The -lanes flag has the form name=weight:key=value, with a comma between two lanes.
// The only key is steps, the number of safety-phase steps.
// Each new seed picks a lane at random, in proportion to the weights.
//
// # Errors
//
// An error groups the failures that look the same.
// The error of a failed check is its kind and the first line of the report, without the seed and step prefix.
// Each decimal number in it becomes N, and each hexadecimal run of 6 or more characters becomes H.
// The error of a panic or a timeout is its kind and the first 3 stack frames in KVLite, without line numbers and addresses.
// Errors help triage only: one cause can show several messages, and two causes can show the same message.
//
// The supervisor shrinks the first seed of each new error.
// It runs the same seed with fewer steps and does a binary search for the smallest step count that fails with the same error.
// The safety phase of a shorter run is a prefix of the longer run, but the liveness phase starts earlier, so the failure can change.
// The search accepts only a step count that gives the same error, and it finds a local minimum only.
// The shorter run then becomes the first seed of the error.
// At most one shrink runs at a time, and its runs use the same worker slots as the new seeds.
//
// When a new error appears, the supervisor runs -alert-command with the seed and the first line of the report as arguments.
// The supervisor does not wait for the command.
//
// # Output
//
// status.json is the result of the runs of the current commit, rewritten every few seconds.
// It holds the commit, the platform, the run and failure counts, each error with every seed that failed with it, and the count of memory kills.
// For each seed, it holds the steps of the run, the step of the failure, and the message.
// Replay a seed at the commit of the status with:
//
//	go test -run 'TestBlackout$' -blackout.seed=SEED -blackout.steps=STEPS -blackout.trace=200 -v .
//
// A new supervisor, or a new commit in follow mode, renames the old status to status-COMMIT.json and starts a new status.
// The -out directory also has bin/ (the test binary of the current commit) and tmp/ (the working directories of the running workers).
// With -reports, the supervisor also writes the full report of each failed seed to reports/SEED.txt.
// A replay gives the same report, so the reports are off by default.
//
// COMMIT is the short hash of HEAD, with "-dirty" when the checkout has changes.
// A run of a dirty checkout does not replay exactly from its commit.
// Seeds are JSON strings, because many JSON readers lose the precision of integers larger than 2^53.
//
// # Follow mode
//
// With -follow, the supervisor fetches -remote every -follow-interval.
// When -remote/-branch points to a new commit, the supervisor checks out that commit with a detached HEAD.
// Then it lets the running workers finish, compiles the new test binary, and continues.
// Follow mode changes the checkout, so -repo must point to a checkout that only the runner uses.
// Do not use follow mode on a checkout that you work in.
//
// # Stop
//
// SIGINT or SIGTERM stops the start of new seeds.
// The supervisor then waits for the running workers, writes the final status, and exits.
// A second signal kills the running workers.
//
// # Operator notes
//
// Each seed selects the page size of its database, but the default checkpoint threshold is about 1,000 operating-system pages.
// Linux on x86-64 has 4 KiB pages, and macOS on Apple silicon has 16 KiB pages.
// So a seed replays exactly only on a platform with the same operating-system page size, and the log records that size.
//
// To run the supervisor on a server:
//  1. Make a checkout that only the runner uses, for example /var/lib/blackout/kvlite.
//  2. Give the server a deploy key with read-only access.
//     The runner fetches and never pushes.
//  3. Compile the supervisor with "go build -o /var/lib/blackout/bin/blackout-supervisor ./internal/blackout/supervisor".
//  4. Copy blackout.service from this directory to /etc/systemd/system and change its paths.
//  5. Start the service with "systemctl enable --now blackout".
//
// The unit restarts the supervisor when it stops.
// It uses KillMode=mixed, so that a stop sends SIGTERM to the supervisor only and the running workers can finish.
// It also sets limits that the kernel enforces at once, for the supervisor, its workers, and the compile together:
//   - MemoryMax=50% and MemorySwapMax=0: the service never uses more than half of the RAM, and no swap.
//     The supervisor plans its workers in half of MemoryMax, so the kernel limit is a second wall that a fast-growing worker can reach before a sample sees it.
//   - CPUSchedulingPolicy=idle, Nice=19, and IOSchedulingClass=idle: the service runs only when the other programs do not need the CPU or the disk.
//   - OOMScoreAdjust=1000: when the computer has no memory left, the kernel kills a worker first.
//
// The supervisor already uses half of the CPUs on average.
// For a hard CPU limit of the kernel, also set CPUQuota, where 100% is one CPU.
// For example, CPUQuota=100% on a server with 2 CPUs.
package main
