#!/usr/bin/env bash
set -euo pipefail

# These tests delete the complete TiKV keyspace. Use only new, labelled,
# loopback-bound containers on an ephemeral GitHub-hosted runner.
test "${GITHUB_ACTIONS:-}" = true
test "${RUNNER_ENVIRONMENT:-}" = github-hosted
test -n "${RUNNER_TEMP:-}"
test -n "${GITHUB_RUN_ID:-}"
test -n "${GITHUB_RUN_ATTEMPT:-}"
name="native-quota-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
pd="${name}-pd"
config="${RUNNER_TEMP}/${name}-tikv.toml"

cleanup() {
	for container in "${name}-tikv-0" "${name}-tikv-1" "${name}-tikv-2" "$pd"; do
		if [ "$(docker inspect -f '{{ index .Config.Labels "native-quota-ci" }}' "$container" 2>/dev/null || true)" = "$name" ]; then
			docker rm -f "$container" >/dev/null 2>&1 || true
		fi
	done
}

case "${1:-}" in
	stop)
		cleanup
		;;
	start)
		trap 'cleanup' ERR
		if ss -H -ltn '( sport = :2379 or sport = :20160 or sport = :20161 or sport = :20162 )' | grep -q .; then
			echo 'TiKV test ports are occupied; refusing destructive tests.' >&2
			exit 1
		fi
		cat > "$config" <<'EOF'
memory-usage-limit = "768MB"
[storage]
scheduler-concurrency = 1024
scheduler-worker-pool-size = 2
txn-status-cache-capacity = 10000
api-version = 1
[rocksdb]
max-open-files = 1024
[raftdb]
max-open-files = 1024
[rocksdb.defaultcf]
block-cache-size = "64MB"
[rocksdb.writecf]
block-cache-size = "64MB"
[rocksdb.lockcf]
block-cache-size = "32MB"
[raftstore]
store-pool-size = 1
apply-pool-size = 1
[readpool.unified]
min-thread-count = 1
max-thread-count = 2
[raft-engine]
memory-limit = "64MB"
[server]
grpc-concurrency = 2
end-point-memory-quota = "64MB"
[memory]
enable-heap-profiling = false
enable-thread-exclusive-arena = false
EOF
		docker run --rm -d --name "$pd" --label "native-quota-ci=$name" \
			-p 127.0.0.1:2379:2379 -p 127.0.0.1:20160:20160 \
			-p 127.0.0.1:20161:20161 -p 127.0.0.1:20162:20162 \
			'pingcap/pd@sha256:c26dbbaaf95f2d5e15402f40f829f8dc26f5b2a39d8d90d74c30a9de7689521b' \
			--name=disposable-pd --client-urls=http://0.0.0.0:2379 \
			--advertise-client-urls=http://127.0.0.1:2379 --peer-urls=http://127.0.0.1:2380 \
			--advertise-peer-urls=http://127.0.0.1:2380 \
			--initial-cluster=disposable-pd=http://127.0.0.1:2380 --data-dir=/data/pd >/dev/null
		for i in 0 1 2; do
			port=$((20160 + i)); status=$((20260 + i))
			docker run --rm -d --name "${name}-tikv-$i" --label "native-quota-ci=$name" \
				--network "container:$pd" -v "$config:/config/tikv.toml:ro" \
				'pingcap/tikv@sha256:8d0b99d78b9b6fe5a9389d3680835386a3afb572168fc87bf7ddb27ab1359919' \
				"--addr=0.0.0.0:$port" "--advertise-addr=127.0.0.1:$port" \
				"--status-addr=0.0.0.0:$status" --pd=127.0.0.1:2379 \
				--data-dir=/data/tikv --config=/config/tikv.toml >/dev/null
		done
		test "$(docker port "$pd" 2379/tcp)" = '127.0.0.1:2379'
		for _ in $(seq 1 90); do
			if curl -fsS --max-time 2 http://127.0.0.1:2379/pd/api/v1/stores 2>/dev/null \
				| jq -e '[.stores[] | select(.store.state_name == "Up")] | length == 3' >/dev/null; then
				trap - ERR
				exit 0
			fi
			sleep 2
		done
		echo 'Disposable TiKV cluster did not become ready.' >&2
		cleanup
		exit 1
		;;
	*)
		echo 'Usage: disposable_tikv_ci.sh start|stop' >&2
		exit 2
		;;
esac
