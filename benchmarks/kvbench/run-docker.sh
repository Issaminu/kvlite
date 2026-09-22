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
readonly results_dir="${KVBENCH_RESULTS_DIR:-/tmp/kvlite-kvbench-results}"
usage="usage: ./run-docker.sh [light|medium|large] [--engines=kvlite,bbolt,redis] [--workloads=reads|writes|mixed|all]"
benchmark_profile="light"
engine_option="--engines=kvlite,bbolt,redis"
workload_option="--workloads=all"
profile_set=0
engine_set=0
workload_set=0
for argument in "$@"; do
	case "${argument}" in
	light | medium | large)
		if ((profile_set)); then
			echo "select one profile" >&2
			exit 1
		fi
		benchmark_profile="${argument}"
		profile_set=1
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
	*)
		echo "${usage}" >&2
		exit 1
		;;
	esac
done
readonly benchmark_profile
readonly engine_option
readonly workload_option
case "${benchmark_profile}" in
light)
	readonly measured_count=1
	readonly warmup_count=0
	;;
medium)
	readonly measured_count=5
	readonly warmup_count=1
	;;
large)
	readonly measured_count=10
	readonly warmup_count=1
	;;
*)
	echo "profile must be light, medium, or large" >&2
	exit 1
	;;
esac
readonly benchmark_workload="${workload_option#--workloads=}"
case "${benchmark_workload}" in
reads | writes | mixed | all) ;;
*)
	echo "workloads must be reads, writes, mixed, or all" >&2
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

case "${benchmark_workload}" in
reads)
	readonly benchmark_filter='^Benchmark(AcknowledgedOperations|ReadTransactions|Enumeration|OrderedOperations|ScaleAndAccessDistribution|Latency|Collections)$'
	;;
writes)
	readonly benchmark_filter='^Benchmark(AcknowledgedOperations|Latency|Collections)$'
	;;
mixed)
	readonly benchmark_filter='^Benchmark(AcknowledgedOperations|MixedTransactions|Latency)$'
	;;
all)
	readonly benchmark_filter='^Benchmark(AcknowledgedOperations|ReadTransactions|MixedTransactions|Enumeration|OrderedOperations|ScaleAndAccessDistribution|Latency|Collections|Lifecycle)$'
	;;
esac
docker_cpu_count="$(docker info --format '{{.NCPU}}')"
if [[ ! "${docker_cpu_count}" =~ ^[1-9][0-9]*$ ]]; then
	echo "Docker reported an invalid CPU count: ${docker_cpu_count}" >&2
	exit 1
fi
cpu_last=$((docker_cpu_count - 1))
if ((cpu_last > 3)); then
	cpu_last=3
fi
if ((cpu_last == 0)); then
	default_cpu_set="0"
else
	default_cpu_set="0-${cpu_last}"
fi
readonly cpu_set="${KVBENCH_CPUSET:-${default_cpu_set}}"

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
		--network "${network_name}" \
		--volume "${volume_name}:/benchmark-data" \
		--volume "${cache_volume_name}:/go-cache" \
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
		--volume "${volume_name}:/benchmark-data" \
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
docker network create "${network_name}" >/dev/null
docker volume create "${volume_name}" >/dev/null
docker volume create "${cache_volume_name}" >/dev/null
start_go
docker exec "${go_name}" chmod 0777 /benchmark-data
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
		--env KVBENCH_FIXED_WORK=1 \
		--env KVBENCH_PROFILE="${benchmark_profile}" \
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
		run_go "${mode}" "${engine}" -run '^$' -bench "${benchmark_filter}" -benchtime=1x -count=1 | tee -a "${output_file}"
	done
}

run_warmup_round() {
	local mode="$1"
	local round="$2"
	local engine
	for engine in $(engine_order "${round}"); do
		run_go "${mode}" "${engine}" -run '^$' -bench "${benchmark_filter}" -benchtime=1x -count=1 >/dev/null
	done
}

run_profile_round() {
	local round="$1"
	local command="$2"
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
	echo "Benchmark profile: ${benchmark_profile}"
	echo "Engines: ${selected_engines}"
	echo "Workloads: ${benchmark_workload}"
	echo "Warm-up count: ${warmup_count}"
	echo "Measured count: ${measured_count}"
	echo "Benchmark filter: ${benchmark_filter}"
	echo "CPU set: ${cpu_set}"
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

if [[ "${benchmark_profile}" != "light" ]]; then
	run_root_tests
	run_go durable "" -count=1 ./...
	if ((uses_redis)) && [[ "${benchmark_workload}" != "reads" ]]; then
		verify_redis_reopen_after_kill
	fi
fi

: >"${results_dir}/durable.txt"
: >"${results_dir}/no-commit-sync.txt"

for ((round = 0; round < warmup_count; round++)); do
	echo "Warm-up round $((round + 1)) of ${warmup_count}"
	run_profile_round "${round}" run_warmup_round
done

for ((round = 0; round < measured_count; round++)); do
	echo "Measured round $((round + 1)) of ${measured_count}"
	run_profile_round "${round}" run_round
done

echo "Raw results: ${results_dir}"
