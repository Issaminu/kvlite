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
// Each worker gets GOMAXPROCS=1 and the soft memory limit GOMEMLIMIT from -memlimit.
// A worker that runs longer than -timeout gets SIGQUIT, which makes the Go runtime print the stack of each goroutine.
// If the worker does not stop, the supervisor kills its process group.
//
// The supervisor puts the result of each run into one of these kinds:
//   - pass: the JSON result says that the seed passed.
//   - fail: the JSON result says that a check failed.
//   - panic: there is no JSON result, and the output has a "panic:" or "fatal error:" line.
//   - timeout: the run did not stop in time.
//   - error: all other results, for example a non-zero exit without a JSON result.
//
// # Lanes
//
// A lane is a class of runs with a weight and settings.
// The -lanes flag has the form name=weight:key=value, with a comma between two lanes.
// The only key is steps, the number of safety-phase steps.
// Each new seed picks a lane at random, in proportion to the weights.
//
// # Signatures
//
// A signature groups the failures that look the same.
// The signature of a failed check is its kind and the first line of the report, without the seed and step prefix.
// Each decimal number in it becomes N, and each hexadecimal run of 6 or more characters becomes H.
// The signature of a panic or a timeout is its kind and the first 3 stack frames in KVLite, without line numbers and addresses.
// The signature ID is the first 12 hexadecimal characters of the SHA-256 hash of the signature text.
//
// Signatures help triage only.
// One cause can show several messages, and two causes can show the same message.
//
// The supervisor shrinks the first example of each new signature.
// It runs the same seed with fewer steps and does a binary search for the smallest step count that fails with the same signature.
// The safety phase of a shorter run is a prefix of the longer run, but the liveness phase starts earlier, so the failure can change.
// The search accepts only a step count that gives the same signature, and it finds a local minimum only.
// At most one shrink runs at a time, and its runs use the same worker slots as the new seeds.
//
// When a new signature appears, the supervisor runs -alert-command with the signature ID and the first line as arguments.
// The command also gets the environment variable BLACKOUT_SIGNATURE_DIR.
// The supervisor does not wait for the command.
//
// # Output
//
// The -out directory has this layout:
//
//	status.json                          the counters, rewritten every few seconds
//	failures.jsonl                       one JSON object for each failed seed, with a replay command
//	bin/blackout-COMMIT.test             the test binary of the current commit
//	tmp/                                 the working directories of the running workers
//	signatures/ID/signature.json         the signature text, the counts, and the shortest example
//	signatures/ID/seed-N/report.txt      the full report, or the output of the worker
//	signatures/ID/seed-N/result.json     the log entry and the JSON result of the worker
//
// COMMIT is the short hash of HEAD, with "-dirty" when the checkout has changes.
// A run of a dirty checkout does not replay exactly from its commit.
// The JSON files write seeds as strings, because many JSON readers lose the precision of integers larger than 2^53.
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
// The unit runs the supervisor at a low CPU priority (Nice=10) and restarts it when it stops.
// It uses KillMode=mixed, so that a stop sends SIGTERM to the supervisor only and the running workers can finish.
// GOMEMLIMIT is a soft limit; use MemoryMax in the unit for a hard limit on all workers together.
package main
