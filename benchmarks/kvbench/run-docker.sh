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
readonly measured_count="${KVBENCH_COUNT:-5}"
readonly benchmark_filter="${KVBENCH_BENCH:-^BenchmarkAcknowledgedOperations$}"

cleanup() {
	docker rm --force "${redis_name}" >/dev/null 2>&1 || true
	docker network rm "${network_name}" >/dev/null 2>&1 || true
	docker volume rm "${volume_name}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

mkdir -p "${results_dir}"
docker network create "${network_name}" >/dev/null
docker volume create "${volume_name}" >/dev/null
docker run --rm --volume "${volume_name}:/benchmark-data" --entrypoint sh "${redis_image}" -c 'chmod 0777 /benchmark-data'

docker run --detach --name "${redis_name}" \
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

for ((attempt = 0; attempt < 100; attempt++)); do
	if docker exec "${redis_name}" redis-cli ping >/dev/null 2>&1; then
		break
	fi
	sleep 0.1
done
if ! docker exec "${redis_name}" redis-cli ping >/dev/null 2>&1; then
	docker logs "${redis_name}"
	exit 1
fi

run_go() {
	local mode="$1"
	shift
	docker run --rm \
		--network "${network_name}" \
		--volume "${volume_name}:/benchmark-data" \
		--mount "type=bind,source=${repository_dir},target=/workspace,readonly" \
		--workdir /workspace/benchmarks/kvbench \
		--env GOCACHE=/benchmark-data/go-cache \
		--env GOMODCACHE=/benchmark-data/go-mod-cache \
		--env TMPDIR=/benchmark-data/tmp \
		--env KVBENCH_DURABILITY="${mode}" \
		--env KVBENCH_REDIS_ADDR="${redis_name}:6379" \
		--env KVBENCH_REDIS_FLUSHDB=1 \
		"${go_image}" \
		sh -c 'mkdir -p "$GOCACHE" "$GOMODCACHE" "$TMPDIR" && exec go test "$@"' sh "$@"
}

run_mode() {
	local mode="$1"
	local output_file="${results_dir}/${mode}.txt"

	run_go "${mode}" -run '^$' -bench "${benchmark_filter}" -benchtime=1x -count=1 >/dev/null
	run_go "${mode}" -run '^$' -bench "${benchmark_filter}" -benchmem -count="${measured_count}" | tee "${output_file}"
}

{
	echo "Run date: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
	echo "KVLite commit: $(git -C "${repository_dir}" rev-parse HEAD)"
	echo "Measured count: ${measured_count}"
	echo "Benchmark filter: ${benchmark_filter}"
	echo "Working tree:"
	git -C "${repository_dir}" status --short
	docker run --rm "${go_image}" go version
	docker run --rm "${redis_image}" redis-server --version
	docker info --format 'Container OS: {{.OperatingSystem}}; kernel: {{.KernelVersion}}; storage driver: {{.Driver}}'
} | tee "${results_dir}/environment.txt"

run_go durable ./...
run_mode durable
docker exec "${redis_name}" redis-cli CONFIG SET appendfsync no >/dev/null
run_mode no-commit-sync

echo "Raw results: ${results_dir}"
