module github.com/Issaminu/kvlite/benchmarks/kvbench

go 1.26.0

require (
	github.com/Issaminu/kvlite v0.0.0
	github.com/redis/go-redis/v9 v9.22.0
	go.etcd.io/bbolt v1.4.3
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)

replace github.com/Issaminu/kvlite => ../..
