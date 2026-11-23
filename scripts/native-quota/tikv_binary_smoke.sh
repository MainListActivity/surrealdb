#!/usr/bin/env bash
set -euo pipefail

binary="${1:?path to the compiled surreal binary is required}"
test -x "$binary"
test -z "$(ss -H -ltn 'sport = :18080')"
endpoint=http://127.0.0.1:18080
cluster="$(dirname "$0")/disposable_tikv_ci.sh"
log="$(mktemp)"
pid=
cluster_started=

cleanup() {
	if [[ -n "$pid" ]]; then
		kill "$pid" 2>/dev/null || true
		wait "$pid" || true
	fi
	if [[ -n "$cluster_started" ]]; then
		bash "$cluster" stop
	fi
	rm -f "$log"
}
trap cleanup EXIT
bash "$cluster" start
cluster_started=1

start() {
	"$binary" start --bind 127.0.0.1:18080 --user root --pass root \
		tikv://127.0.0.1:2379 >>"$log" 2>&1 &
	pid=$!
	for _ in $(seq 1 90); do
		kill -0 "$pid" 2>/dev/null || break
		if curl -fsS --max-time 2 "$endpoint/ready?require=native-quota-v1" >/dev/null 2>&1; then
			return
		fi
		sleep 1
	done
	cat "$log" >&2
	return 1
}

sql() {
	local query="$1"
	shift
	curl -fsS --max-time 10 -u root:root -H 'Accept: application/json' \
		-H 'content-type: text/plain' "$@" --data-binary "$query" "$endpoint/sql"
}

db_sql() {
	sql "$1" -H 'surreal-ns: rehearsal' -H 'surreal-db: smoke'
}

expect_ok() {
	jq -e 'length == 1 and .[0].status == "OK"' >/dev/null
}

start
curl -fsS --max-time 5 "$endpoint/capabilities" | jq -e '
	.storage.backend == "tikv" and .storage.ready and .backend.production
	and .backend.hard_quota_certified and .backend.persistent_restart_certified
' >/dev/null
sql 'DEFINE NAMESPACE rehearsal;' | expect_ok
sql 'DEFINE DATABASE smoke;' -H 'surreal-ns: rehearsal' | expect_ok
db_sql 'DEFINE QUOTA ON DATABASE smoke RULE probe_limit FOR RECORD MATCH EXACT probe LIMIT 1;' | expect_ok
db_sql 'CREATE probe:one SET ok = true;' | expect_ok
kill "$pid"
wait "$pid" || true
pid=
start
db_sql 'CREATE probe:two;' |
	jq -e '.[0] | .status == "ERR" and (.result | tostring | contains("probe_limit"))' >/dev/null
db_sql 'SELECT VALUE ok FROM probe;' |
	jq -e '.[0] | .status == "OK" and .result == [true]' >/dev/null
