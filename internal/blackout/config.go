package blackout

import (
	"fmt"
	"strings"

	"github.com/Issaminu/kvlite"
	"github.com/Issaminu/kvlite/internal/blackout/disk"
)

// operation is one kind of workload step.
type operation uint8

const (
	opUpdate     operation = iota
	opBatch                // several callbacks in one WAL commit (SyncFull only)
	opGet                  // DB.Get
	opView                 // point reads and one scan
	opCheckpoint           // copy the WAL into the main file now
	opReopen               // a clean Close and Open
	opChecksums            // turn data-page checksums on or off
	opContracts            // one check of an API promise, such as an error for misuse
	operationCount
)

var operationNames = [operationCount]string{"update", "batch", "get", "view", "checkpoint", "reopen", "checksums", "contracts"}

const (
	keysOrdered  = iota // 8-character decimal IDs
	keysShort           // 1 to 24 random bytes
	keysLong            // up to two pages, so some keys need continuation pages
	keysPrefixed        // long shared prefixes, then a decimal ID
	keyShapeCount
)

const (
	accessHotCold    = iota // half of the picks use 4 hot keys
	accessAscending         // mostly the next key in order
	accessDescending        // mostly the previous key in order
	accessSkewed            // small key IDs are much more likely than large ones
	accessCount
)

// Config is the configuration that the config generator selects for one seed.
// Each seed selects different weights and settings, so some runs grow the tree, some delete, and some crash often (swarm testing).
type Config struct {
	// Database options.
	Sync            kvlite.Sync
	Checksums       bool
	CheckpointBytes uint64

	// Data shape.
	BucketCount  int
	KeyCount     int
	KeyShape     int
	Access       int
	ValueWeights []uint64 // small, medium, large, huge (several MiB)

	// Workload mix.
	NestedBuckets    bool     // create buckets inside buckets
	OperationWeights []uint64 // indexed by operation
	MutationWeights  []uint64 // indexed by mutationKind
	MaxMutations     int
	Abort            disk.Ratio

	// I/O errors.
	IOFaults bool // the disk injects errors in the safety phase

	// Crash settings.
	CrashNow      disk.Ratio // chance per step of a crash between operations
	ScheduleCrash disk.Ratio // chance per step of a crash inside one of the next file calls
	PowerFailures bool

	// Storage settings.
	Disk disk.Config
}

// syncNames are the short names of the sync modes in reports.
var syncNames = map[kvlite.Sync]string{kvlite.SyncFull: "full", kvlite.SyncNormal: "normal", kvlite.SyncNone: "none"}

func (config Config) String() string {
	syncName := syncNames[config.Sync]
	var operations []string
	for op, weight := range config.OperationWeights {
		if weight > 0 {
			operations = append(operations, fmt.Sprintf("%s=%d", operationNames[op], weight))
		}
	}
	return fmt.Sprintf("sync=%s checksums=%t checkpoint=%d buckets=%d nested=%t keys=%d shape=%d access=%d values=%v mutations=%v max=%d abort=%v crash-now=%v schedule-crash=%v power=%t io=%t sector=%d keep=%v in-order=%v ops=[%s]",
		syncName, config.Checksums, config.CheckpointBytes, config.BucketCount, config.NestedBuckets, config.KeyCount, config.KeyShape, config.Access,
		config.ValueWeights, config.MutationWeights, config.MaxMutations, config.Abort, config.CrashNow, config.ScheduleCrash,
		config.PowerFailures, config.IOFaults, config.Disk.SectorSize, config.Disk.KeepChange, config.Disk.InOrder, strings.Join(operations, " "))
}

// streamSeed derives the seed of one random generator from the run seed.
// Each generator gets its own stream, so a change to one part of the run does not move the decisions of the others.
func streamSeed(seed, stream uint64) uint64 {
	value := seed + stream*0x9e3779b97f4a7c15
	value ^= value >> 30
	value *= 0xbf58476d1ce4e5b9
	value ^= value >> 27
	value *= 0x94d049bb133111eb
	return value ^ value>>31
}

const (
	streamConfig uint64 = iota + 1
	streamWorkload
	streamCrash
	streamDisk
	streamCheck // decides which steps run the slow checks; the checks change nothing else
)

// swarmWeights enables a random non-empty subset of n choices with random weights.
func swarmWeights(rand *disk.Rand, n int) []uint64 {
	weights := make([]uint64, n)
	order := make([]int, n)
	for index := range order {
		order[index] = index
	}
	for index := n - 1; index > 0; index-- {
		other := rand.IntN(index + 1)
		order[index], order[other] = order[other], order[index]
	}
	for _, index := range order[:rand.Range(1, n)] {
		weights[index] = uint64(rand.Range(1, 100))
	}
	return weights
}

// sometimes returns a rate of one in Range(low, high), or never with probability one quarter.
func sometimes(rand *disk.Rand, low, high int) disk.Ratio {
	if rand.Chance(disk.OneIn(4)) {
		return disk.Never
	}
	return disk.OneIn(uint64(rand.Range(low, high)))
}

// newConfig runs the config generator.
func newConfig(seed uint64) Config {
	rand := disk.NewRand(streamSeed(seed, streamConfig))
	var config Config
	config.Sync = [...]kvlite.Sync{kvlite.SyncFull, kvlite.SyncNormal, kvlite.SyncNone}[rand.Weighted([]uint64{6, 3, 1})]
	config.Checksums = rand.Bool()
	// One byte checkpoints after every commit. Zero selects the default threshold.
	config.CheckpointBytes = [...]uint64{1, 16 << 10, 64 << 10, 256 << 10, 0}[rand.IntN(5)]

	config.BucketCount = rand.Range(1, 4)
	config.KeyCount = [...]int{8, 64, 512}[rand.IntN(3)]
	config.KeyShape = rand.IntN(keyShapeCount)
	config.Access = rand.IntN(accessCount)
	config.ValueWeights = append(swarmWeights(rand, 3), 0)
	if config.KeyCount <= 64 && rand.Chance(disk.OneIn(10)) {
		// Rare values of several MiB need long continuation chains. Few keys keep the memory small.
		config.ValueWeights[3] = 1
	}

	config.NestedBuckets = rand.Bool()
	config.OperationWeights = swarmWeights(rand, int(operationCount))
	config.OperationWeights[opUpdate] = max(config.OperationWeights[opUpdate], 50)
	config.OperationWeights[opContracts] = min(config.OperationWeights[opContracts], 10)
	if config.Sync != kvlite.SyncFull {
		// Group commit exists only in SyncFull.
		config.OperationWeights[opBatch] = 0
	}
	config.MutationWeights = []uint64{
		mutationPut:          uint64(rand.Range(20, 100)),
		mutationDelete:       uint64(rand.Range(0, 60)),
		mutationCreateBucket: uint64(rand.Range(1, 10)),
		mutationDeleteBucket: uint64(rand.Range(0, 4)),
		mutationGet:          uint64(rand.Range(0, 30)),
		mutationDeleteBatch:  uint64(rand.Range(0, 5)),
		mutationScan:         uint64(rand.Range(0, 5)),
	}
	config.MaxMutations = [...]int{1, 4, 16, 64}[rand.IntN(4)]
	if rand.Chance(disk.OneIn(20)) {
		// Thousands of changes in one commit.
		config.MaxMutations = 2000
	}
	config.Abort = disk.OneIn(uint64(rand.Range(5, 40)))

	config.CrashNow = sometimes(rand, 4, 60)
	config.ScheduleCrash = sometimes(rand, 4, 60)
	// SyncNone promises nothing after a power failure, so its runs crash only the process.
	config.PowerFailures = config.Sync != kvlite.SyncNone && !rand.Chance(disk.OneIn(5))

	config.Disk = disk.Config{
		SectorSize: [...]int{512, 4096}[rand.IntN(2)],
		KeepChange: disk.Ratio{Num: uint64(rand.Range(1, 3)), Den: 4},
		InOrder:    disk.OneIn(uint64(rand.Range(2, 8))),
	}

	config.IOFaults = rand.Bool()
	config.Disk.DropCache = disk.OneIn(2)
	if config.IOFaults {
		// Each kind of call fails with its own rate, or never.
		fault := func() disk.Ratio { return sometimes(rand, 20, 500) }
		config.Disk.ReadFault = fault()
		config.Disk.WriteFault = fault()
		config.Disk.SyncFault = fault()
		config.Disk.TruncateFault = fault()
		config.Disk.StatFault = fault()
		config.Disk.OpenFault = fault()
		config.Disk.RemoveFault = fault()
		config.Disk.SyncDirFault = fault()
	}
	return config
}
