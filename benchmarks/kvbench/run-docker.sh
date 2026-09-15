#!/usr/bin/env bash

set -euo pipefail

readonly go_image="golang@sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2"
readonly redis_image="redis@sha256:027002f3d4161eb427643e0cc71023b582fb461c6199e699f46db843f0b39fb1"
readonly run_id="kvlite-kvbench-$$"
readonly network_name="${run_id}-network"
readonly volume_name="${run_id}-data"
readonly redis_name="${run_id}-redis"
suite_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly suite_dir
repository_dir="$(cd "${suite_dir}/../.." && pwd)"
readonly repository_dir
readonly results_dir="${KVBENCH_RESULTS_DIR:-/tmp/kvlite-kvbench-results}"
readonly measured_count="${KVBENCH_COUNT:-10}"
readonly benchmark_filter="${KVBENCH_BENCH:-^BenchmarkAcknowledgedOperations$}"
if [[ ! "${measured_count}" =~ ^[1-9][0-9]*$ ]]; then
	echo "KVBENCH_COUNT must be a positive integer." >&2
	exit 1
fi
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
	docker rm --force "${redis_name}" >/dev/null 2>&1 || true
	docker network rm "${network_name}" >/dev/null 2>&1 || true
	docker volume rm "${volume_name}" >/dev/null 2>&1 || true
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
docker run --rm --volume "${volume_name}:/benchmark-data" --entrypoint sh "${redis_image}" -c 'chmod 0777 /benchmark-data'
start_redis

run_go() {
	local mode="$1"
	local engine="$2"
	shift 2
	docker run --rm \
		--cpuset-cpus "${cpu_set}" \
		--network "${network_name}" \
		--volume "${volume_name}:/benchmark-data" \
		--mount "type=bind,source=${repository_dir},target=/workspace,readonly" \
		--workdir /workspace/benchmarks/kvbench \
		--env GOCACHE=/benchmark-data/go-cache \
		--env GOMODCACHE=/benchmark-data/go-mod-cache \
		--env TMPDIR=/benchmark-data/tmp \
		--env KVBENCH_DURABILITY="${mode}" \
		--env KVBENCH_ENGINE="${engine}" \
		--env KVBENCH_FIXED_WORK=1 \
		--env KVBENCH_REDIS_ADDR="${redis_name}:6379" \
		--env KVBENCH_REDIS_FLUSHDB=1 \
		"${go_image}" \
		sh -c 'mkdir -p "$GOCACHE" "$GOMODCACHE" "$TMPDIR" && exec go test "$@"' sh "$@"
}

run_root_tests() {
	docker run --rm \
		--cpuset-cpus "${cpu_set}" \
		--volume "${volume_name}:/benchmark-data" \
		--mount "type=bind,source=${repository_dir},target=/workspace,readonly" \
		--workdir /workspace \
		--env GOCACHE=/benchmark-data/go-cache \
		--env GOMODCACHE=/benchmark-data/go-mod-cache \
		--env TMPDIR=/tmp/kvlite-tests \
		"${go_image}" \
		sh -c 'mkdir -p "$GOCACHE" "$GOMODCACHE" "$TMPDIR" && exec go test ./...'
}

engine_order() {
	case $(($1 % 3)) in
	0) echo "kvlite bbolt redis" ;;
	1) echo "bbolt redis kvlite" ;;
	2) echo "redis kvlite bbolt" ;;
	esac
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
	echo "Measured count: ${measured_count}"
	echo "Benchmark filter: ${benchmark_filter}"
	echo "CPU set: ${cpu_set}"
	echo "Working tree:"
	git -C "${repository_dir}" status --short
	docker run --rm "${go_image}" go version
	docker run --rm "${redis_image}" redis-server --version
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

run_root_tests
run_go durable "" ./...
run_go no-commit-sync "" ./...
verify_redis_reopen_after_kill

: >"${results_dir}/durable.txt"
: >"${results_dir}/no-commit-sync.txt"
for engine in kvlite bbolt redis; do
	run_go durable "${engine}" -run '^$' -bench "${benchmark_filter}" -benchtime=1x -count=1 >/dev/null
	run_go no-commit-sync "${engine}" -run '^$' -bench "${benchmark_filter}" -benchtime=1x -count=1 >/dev/null
done

for ((round = 0; round < measured_count; round++)); do
	if ((round % 2 == 0)); then
		run_round durable "${round}"
		run_round no-commit-sync "${round}"
	else
		run_round no-commit-sync "${round}"
		run_round durable "${round}"
	fi
done

echo "Raw results: ${results_dir}"
