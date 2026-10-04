#!/usr/bin/env bash

set -euo pipefail

readonly go_image="golang@sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2"
readonly redis_image="redis@sha256:027002f3d4161eb427643e0cc71023b582fb461c6199e699f46db843f0b39fb1"
readonly run_id="kvlite-kvbench-$$"
readonly network_name="${run_id}-network"
readonly volume_name="${run_id}-data"
readonly cache_volume_name="kvlite-kvbench-go-cache"
readonly redis_name="${run_id}-redis"
readonly go_name="${run_id}-go"
suite_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly suite_dir
repository_dir="$(cd "${suite_dir}/../.." && pwd)"
readonly repository_dir
results_dir="${KVBENCH_RESULTS_DIR:-/tmp/kvlite-kvbench-results}"
usage="usage: ./run-docker.sh [light|medium|large] [--engines=kvlite,bbolt,redis] [--workloads=focused|reads|writes|deletes|mixed|all] [--storage=tmpfs|volume] [--case=durable:BenchmarkName/scale=light/...] [--profile]"
benchmark_scale="light"
engine_option="--engines=kvlite,bbolt,redis"
workload_option=""
storage_option=""
case_option=""
profile_enabled=0
scale_set=0
engine_set=0
workload_set=0
storage_set=0
case_set=0
for argument in "$@"; do
	case "${argument}" in
	light | medium | large)
		if ((scale_set)); then
			echo "select one scale" >&2
			exit 1
		fi
		benchmark_scale="${argument}"
		scale_set=1
		;;
	--engines=*)
		if ((engine_set)); then
			echo "select the engines once" >&2
			exit 1
		fi
		engine_option="${argument}"
		engine_set=1
		;;
	--workloads=*)
		if ((workload_set)); then
			echo "select the workloads once" >&2
			exit 1
		fi
		workload_option="${argument}"
		workload_set=1
		;;
	--storage=*)
		if ((storage_set)); then
			echo "select the storage once" >&2
			exit 1
		fi
		storage_option="${argument}"
		storage_set=1
		;;
	--case=*)
		if ((case_set)); then
			echo "select the case once" >&2
			exit 1
		fi
		case_option="${argument#--case=}"
		case_set=1
		;;
	--profile)
		if ((profile_enabled)); then
			echo "select profiling once" >&2
			exit 1
		fi
		profile_enabled=1
		;;
	*)
		echo "${usage}" >&2
		exit 1
		;;
	esac
done
readonly benchmark_scale
readonly engine_option
case "${benchmark_scale}" in
light)
	measured_count=3
	readonly samples_per_round=2
	readonly warmup_count=1
	;;
medium)
	measured_count=10
	readonly samples_per_round=1
	readonly warmup_count=1
	;;
large)
	measured_count=15
	readonly samples_per_round=1
	readonly warmup_count=1
	;;
*)
	echo "scale must be light, medium, or large" >&2
	exit 1
	;;
esac
readonly measured_count
if [[ -z "${workload_option}" ]]; then
	if ((case_set)); then
		workload_option="--workloads=all"
	elif [[ "${benchmark_scale}" == "light" ]]; then
		workload_option="--workloads=focused"
	else
		workload_option="--workloads=all"
	fi
fi
readonly workload_option
if [[ -z "${storage_option}" ]]; then
	if [[ "${benchmark_scale}" == "light" ]]; then
		storage_option="--storage=tmpfs"
	else
		storage_option="--storage=volume"
	fi
fi
readonly storage_option
readonly benchmark_storage="${storage_option#--storage=}"
case "${benchmark_storage}" in
tmpfs | volume) ;;
*)
	echo "storage must be tmpfs or volume" >&2
	exit 1
	;;
esac
readonly benchmark_workload="${workload_option#--workloads=}"
case "${benchmark_workload}" in
focused | reads | writes | deletes | mixed | all) ;;
*)
	echo "workloads must be focused, reads, writes, deletes, mixed, or all" >&2
	exit 1
	;;
esac
readonly engine_csv="${engine_option#--engines=}"
if [[ -z "${engine_csv}" ]]; then
	echo "select at least one engine" >&2
	exit 1
fi
declare -a requested_engines
IFS=',' read -r -a requested_engines <<<"${engine_csv}"
selected_engines=""
uses_redis=0
for engine in "${requested_engines[@]}"; do
	case "${engine}" in
	kvlite | bbolt | redis) ;;
	*)
		echo "engines must be kvlite, bbolt, or redis" >&2
		exit 1
		;;
	esac
	if [[ " ${selected_engines} " == *" ${engine} "* ]]; then
		echo "engines must not contain duplicates" >&2
		exit 1
	fi
	selected_engines+="${selected_engines:+ }${engine}"
	if [[ "${engine}" == "redis" ]]; then
		uses_redis=1
	fi
done
if [[ -z "${selected_engines}" ]]; then
	echo "select at least one engine" >&2
	exit 1
fi
readonly selected_engines
readonly uses_redis

selected_case_mode=""
selected_case_name=""
if ((case_set)); then
	selected_case_mode="${case_option%%:*}"
	selected_case_name="${case_option#*:}"
	if [[ "${selected_case_mode}" != "durable" && "${selected_case_mode}" != "no-commit-sync" ]] ||
		[[ ! "${selected_case_name}" =~ ^Benchmark[A-Za-z0-9_=/\-]+$ ]] ||
		[[ "${selected_case_name}" != *"/scale=${benchmark_scale}/"* ]]; then
		echo "case must be MODE:BenchmarkName/scale=${benchmark_scale}/... with mode durable or no-commit-sync" >&2
		exit 1
	fi
	case "${selected_case_name}" in
	*/kvlite | */bbolt | */redis) selected_case_name="${selected_case_name%/*}" ;;
	esac
	IFS='/' read -r -a case_parts <<<"${selected_case_name}"
	benchmark_filter=""
	for part in "${case_parts[@]}"; do
		benchmark_filter+="${benchmark_filter:+/}^${part}$"
	done
elif [[ "${benchmark_workload}" == "focused" ]]; then
	readonly benchmark_filter='^Benchmark(AcknowledgedOperations|FocusedWrites|DeleteOperations)$'
else
	case "${benchmark_workload}" in
	reads)
		readonly benchmark_filter='^Benchmark(AcknowledgedOperations|ReadTransactions|Enumeration|OrderedOperations|ScaleAndAccessDistribution|Latency|Collections)$'
		;;
	writes)
		readonly benchmark_filter='^Benchmark(AcknowledgedOperations|DeleteOperations|DeleteBuckets|DeleteVisibility|PageReuse|PageReusePlateau|TailReclamation|ReopenAfterDelete|Latency|Collections)$'
		;;
	deletes)
		readonly benchmark_filter='^Benchmark(DeleteOperations|DeleteBuckets|DeleteVisibility|PageReuse|PageReusePlateau|TailReclamation|ReopenAfterDelete)$'
		;;
	mixed)
		readonly benchmark_filter='^Benchmark(AcknowledgedOperations|MixedTransactions|Latency)$'
		;;
	all)
		readonly benchmark_filter='^Benchmark'
		;;
	esac
fi
readonly selected_case_mode
readonly selected_case_name
readonly benchmark_filter
docker_cpu_count="$(docker info --format '{{.NCPU}}')"
if [[ ! "${docker_cpu_count}" =~ ^[1-9][0-9]*$ ]]; then
	echo "Docker reported an invalid CPU count: ${docker_cpu_count}" >&2
	exit 1
fi
cpu_last=$((docker_cpu_count - 1))
if [[ "${benchmark_workload}" == "focused" ]]; then
	cpu_last=0
elif ((cpu_last > 3)); then
	cpu_last=3
fi
if ((cpu_last == 0)); then
	default_cpu_set="0"
else
	default_cpu_set="0-${cpu_last}"
fi
readonly cpu_set="${KVBENCH_CPUSET:-${default_cpu_set}}"
declare -a benchmark_data_mount
if [[ "${benchmark_storage}" == "tmpfs" ]]; then
	benchmark_data_mount=(--tmpfs "/benchmark-data:rw,exec,size=512m")
else
	benchmark_data_mount=(--volume "${volume_name}:/benchmark-data")
fi

cleanup() {
	docker rm --force "${go_name}" >/dev/null 2>&1 || true
	docker rm --force "${redis_name}" >/dev/null 2>&1 || true
	docker network rm "${network_name}" >/dev/null 2>&1 || true
	docker volume rm "${volume_name}" >/dev/null 2>&1 || true
}

start_go() {
	docker rm --force "${go_name}" >/dev/null 2>&1 || true
	docker run --detach --name "${go_name}" \
		--cpuset-cpus "${cpu_set}" \
		--env GOFLAGS=-buildvcs=false \
		--network "${network_name}" \
		"${benchmark_data_mount[@]}" \
		--volume "${cache_volume_name}:/go-cache" \
		--mount "type=bind,source=${results_dir},target=/benchmark-results" \
		--mount "type=bind,source=${repository_dir},target=/workspace,readonly" \
		--workdir /workspace/benchmarks/kvbench \
		"${go_image}" \
		sleep infinity >/dev/null
}
trap cleanup EXIT

wait_for_redis() {
	for ((attempt = 0; attempt < 100; attempt++)); do
		if docker exec "${redis_name}" redis-cli ping >/dev/null 2>&1; then
			return
		fi
		sleep 0.1
	done
	docker logs "${redis_name}"
	return 1
}

start_redis() {
	docker rm --force "${redis_name}" >/dev/null 2>&1 || true
	docker run --detach --name "${redis_name}" \
		--cpuset-cpus "${cpu_set}" \
		--network "${network_name}" \
		"${benchmark_data_mount[@]}" \
		"${redis_image}" \
		redis-server \
		--dir /benchmark-data \
		--appendonly yes \
		--appendfsync always \
		--save "" \
		--auto-aof-rewrite-percentage 0 \
		--no-appendfsync-on-rewrite no >/dev/null
	wait_for_redis
}

mkdir -p "${results_dir}"
results_dir="$(cd "${results_dir}" && pwd -P)"
readonly results_dir
rm -f "${results_dir}/profiles/index.tsv"
docker network create "${network_name}" >/dev/null
if [[ "${benchmark_storage}" == "volume" ]]; then
	docker volume create "${volume_name}" >/dev/null
fi
docker volume create "${cache_volume_name}" >/dev/null
start_go
docker exec "${go_name}" chmod 0777 /benchmark-data
docker exec \
	--env GOCACHE=/go-cache/build \
	--env GOMODCACHE=/go-cache/mod \
	--env TMPDIR=/benchmark-data/tmp \
	"${go_name}" \
	sh -c 'mkdir -p "$GOCACHE" "$GOMODCACHE" "$TMPDIR" && go build -o /benchmark-data/kvbench-report ./cmd/report'
redis_address=""
if ((uses_redis)); then
	start_redis
	redis_address="${redis_name}:6379"
fi
readonly redis_address

run_go() {
	local mode="$1"
	local engine="$2"
	shift 2
	docker exec \
		--workdir /workspace/benchmarks/kvbench \
		--env GOCACHE=/go-cache/build \
		--env GOMODCACHE=/go-cache/mod \
		--env TMPDIR=/benchmark-data/tmp \
		--env KVBENCH_DURABILITY="${mode}" \
		--env KVBENCH_ENGINE="${engine}" \
		--env KVBENCH_SCALE="${benchmark_scale}" \
		--env KVBENCH_WORKLOAD="${benchmark_workload}" \
		--env KVBENCH_REDIS_ADDR="${redis_address}" \
		--env KVBENCH_REDIS_FLUSHDB=1 \
		"${go_name}" \
		sh -c 'mkdir -p "$GOCACHE" "$GOMODCACHE" "$TMPDIR" && exec go test "$@"' sh "$@"
}

run_root_tests() {
	docker exec \
		--workdir /workspace \
		--env GOCACHE=/go-cache/build \
		--env GOMODCACHE=/go-cache/mod \
		--env TMPDIR=/tmp/kvlite-tests \
		"${go_name}" \
		sh -c 'mkdir -p "$GOCACHE" "$GOMODCACHE" "$TMPDIR" && exec go test -count=1 ./...'
}

engine_order() {
	local round="$1"
	local engine_count="${#requested_engines[@]}"
	local offset
	for ((offset = 0; offset < engine_count; offset++)); do
		echo "${requested_engines[$(((round + offset) % engine_count))]}"
	done
}

run_round() {
	local mode="$1"
	local round="$2"
	local output_file="${results_dir}/${mode}.txt"
	local engine
	for engine in $(engine_order "${round}"); do
		run_go "${mode}" "${engine}" -run '^$' -bench "${benchmark_filter}" -benchtime=1x -count="${samples_per_round}" | tee -a "${output_file}"
	done
}

run_warmup_round() {
	local mode="$1"
	local round="$2"
	local engine
	local output
	for engine in $(engine_order "${round}"); do
		if ! output="$(run_go "${mode}" "${engine}" -run '^$' -bench "${benchmark_filter}" -benchtime=1x -count=1)"; then
			echo "${output}" >&2
			return 1
		fi
		if [[ -n "${selected_case_name}" && "${output}" != *"${selected_case_name}/${engine}"* ]]; then
			echo "${selected_case_mode}:${selected_case_name} produced no result for ${engine}; check the workload and engine" >&2
			return 1
		fi
		printf '%s\n' "${output}" >> "${results_dir}/warmup-${mode}.txt"
	done
}

run_profile_round() {
	local round="$1"
	local command="$2"
	if [[ -n "${selected_case_mode}" ]]; then
		"${command}" "${selected_case_mode}" "${round}"
		return
	fi
	if [[ "${benchmark_workload}" == "reads" ]]; then
		"${command}" durable "${round}"
		return
	fi
	if ((round % 2 == 0)); then
		"${command}" durable "${round}"
		"${command}" no-commit-sync "${round}"
	else
		"${command}" no-commit-sync "${round}"
		"${command}" durable "${round}"
	fi
}

verify_redis_reopen_after_kill() {
	local key="kvbench-durable-reopen-probe"
	local value
	value="$(date -u '+%Y%m%dT%H%M%SZ')"
	docker exec "${redis_name}" redis-cli FLUSHDB >/dev/null
	docker exec "${redis_name}" redis-cli CONFIG SET appendfsync always >/dev/null
	if [[ "$(docker exec "${redis_name}" redis-cli SET "${key}" "${value}")" != "OK" ]]; then
		echo "Redis did not acknowledge the durable reopen probe." >&2
		exit 1
	fi
	docker kill --signal KILL "${redis_name}" >/dev/null
	start_redis
	if [[ "$(docker exec "${redis_name}" redis-cli GET "${key}")" != "${value}" ]]; then
		echo "Redis did not recover the acknowledged reopen probe." >&2
		exit 1
	fi
}

{
	echo "Run date: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
	echo "KVLite commit: $(git -C "${repository_dir}" rev-parse HEAD)"
	echo "Benchmark scale: ${benchmark_scale}"
	echo "Engines: ${selected_engines}"
	echo "Workloads: ${benchmark_workload}"
	echo "Selected case: ${case_option:-all}"
	echo "Warm-up count: ${warmup_count}"
	echo "Measured rounds: ${measured_count}"
	echo "Samples per round: ${samples_per_round}"
	echo "Measured samples: $((measured_count * samples_per_round))"
	echo "Profile every selected case: ${profile_enabled}"
	echo "Benchmark filter: ${benchmark_filter}"
	echo "CPU set: ${cpu_set}"
	echo "Benchmark data storage: ${benchmark_storage}"
	echo "Working tree:"
	git -C "${repository_dir}" status --short
	docker exec "${go_name}" go version
	if ((uses_redis)); then
		docker exec "${redis_name}" redis-server --version
	else
		echo "Redis: not selected"
	fi
	docker info --format 'Container OS: {{.OperatingSystem}}; kernel: {{.KernelVersion}}; storage driver: {{.Driver}}; CPUs: {{.NCPU}}'
} | tee "${results_dir}/environment.txt"

git -C "${repository_dir}" diff --binary >"${results_dir}/working-tree.patch"
git -C "${repository_dir}" ls-files --others --exclude-standard -- . \
	':(exclude)benchmarks/kvbench/benchmark-results*.md' >"${results_dir}/untracked-files.txt"
if [[ -s "${results_dir}/untracked-files.txt" ]]; then
	tar -C "${repository_dir}" -czf "${results_dir}/untracked-files.tar.gz" -T "${results_dir}/untracked-files.txt"
else
	rm -f "${results_dir}/untracked-files.tar.gz"
fi
(
	cd "${repository_dir}"
	{
		git ls-files -z -- . ':(exclude)benchmarks/kvbench/benchmark-results*.md'
		git ls-files --others --exclude-standard -z -- . ':(exclude)benchmarks/kvbench/benchmark-results*.md'
	} | sort -z | xargs -0 shasum -a 256
) >"${results_dir}/source-sha256.txt"

if [[ "${benchmark_scale}" != "light" ]]; then
	run_root_tests
	run_go durable "" -count=1 ./...
	if ((uses_redis)) && [[ "${benchmark_workload}" != "reads" ]]; then
		if [[ "${benchmark_storage}" == "volume" ]]; then
			verify_redis_reopen_after_kill
		else
			echo "Skip the Redis restart durability probe because tmpfs does not survive a container restart."
		fi
	fi
fi

: >"${results_dir}/durable.txt"
: >"${results_dir}/no-commit-sync.txt"
: >"${results_dir}/warmup-durable.txt"
: >"${results_dir}/warmup-no-commit-sync.txt"

for ((round = 0; round < warmup_count; round++)); do
	echo "Warm-up round $((round + 1)) of ${warmup_count}"
	run_profile_round "${round}" run_warmup_round
done

for ((round = 0; round < measured_count; round++)); do
	echo "Measured round $((round + 1)) of ${measured_count}"
	run_profile_round "${round}" run_round
done

echo "Raw results: ${results_dir}"
if ((profile_enabled)); then
	profile_dir="${results_dir}/profiles"
	mkdir -p "${profile_dir}"
	docker exec "${go_name}" /benchmark-data/kvbench-report cases /benchmark-results >"${profile_dir}/index.tsv"
	while IFS=$'\t' read -r case_id case_mode case_name case_engine case_filter case_rounds; do
		case_dir="${profile_dir}/${case_id}"
		container_case_dir="/benchmark-results/profiles/${case_id}"
		mkdir -p "${case_dir}"
		rm -f "${case_dir}"/{cpu.pprof,memory.pprof,trace.out,run.txt,cpu-top.txt,cpu-cum.txt,alloc-top.txt,alloc-cum.txt,net.pprof,net-top.txt,sync.pprof,sync-top.txt,syscall.pprof,syscall-top.txt,sched.pprof,sched-top.txt}
		echo "Profile ${case_id}: ${case_mode} ${case_name} (${case_rounds} repeats)"
		run_go "${case_mode}" "${case_engine}" \
			-run '^$' -bench "${case_filter}" -benchtime=1x -count="${case_rounds}" -timeout=0 \
			-o /benchmark-data/kvbench.test \
			-cpuprofile="${container_case_dir}/cpu.pprof" \
			-memprofile="${container_case_dir}/memory.pprof" \
			-trace="${container_case_dir}/trace.out" | tee "${case_dir}/run.txt"
		docker exec "${go_name}" /benchmark-data/kvbench-report check-profile \
			"${container_case_dir}/run.txt" "${case_name}" "${case_rounds}"
		for view in cpu alloc; do
			if [[ "${view}" == cpu ]]; then
				profile_file="${container_case_dir}/cpu.pprof"
				view_option=()
			else
				profile_file="${container_case_dir}/memory.pprof"
				view_option=(-alloc_space)
			fi
			docker exec "${go_name}" go tool pprof -top -nodecount=0 -divide_by="${case_rounds}" \
				"${view_option[@]}" /benchmark-data/kvbench.test "${profile_file}" >"${case_dir}/${view}-top.txt"
		done
		docker exec "${go_name}" go tool pprof -top -cum -nodecount=0 -divide_by="${case_rounds}" \
			/benchmark-data/kvbench.test "${container_case_dir}/cpu.pprof" >"${case_dir}/cpu-cum.txt"
		docker exec "${go_name}" go tool pprof -top -cum -alloc_space -nodecount=0 -divide_by="${case_rounds}" \
			/benchmark-data/kvbench.test "${container_case_dir}/memory.pprof" >"${case_dir}/alloc-cum.txt"
		for wait_kind in net sync syscall sched; do
			if docker exec "${go_name}" sh -c 'go tool trace -pprof="$1" "$2" > "$3"' sh \
				"${wait_kind}" "${container_case_dir}/trace.out" "${container_case_dir}/${wait_kind}.pprof"; then
				if ! docker exec "${go_name}" go tool pprof -top -nodecount=15 -divide_by="${case_rounds}" \
					/benchmark-data/kvbench.test "${container_case_dir}/${wait_kind}.pprof" >"${case_dir}/${wait_kind}-top.txt"; then
					rm -f "${case_dir}/${wait_kind}-top.txt"
				fi
			fi
		done
	done <"${profile_dir}/index.tsv"
	docker exec "${go_name}" cat /benchmark-data/kvbench.test >"${profile_dir}/kvbench.test"
fi
docker exec "${go_name}" /benchmark-data/kvbench-report report /benchmark-results
echo "Report: ${results_dir}/report.md"
