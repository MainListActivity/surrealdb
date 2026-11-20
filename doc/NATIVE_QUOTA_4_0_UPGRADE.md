# 4.0.1 upgrade validation status

This development tree merges the original native-quota fork with upstream
4.0.1 and the atomic LIVE event-fence followup. It is not a release candidate.
The active certified-release manifest is deliberately unchanged; readiness and
capability checks must fail closed when its identity differs from this binary.
Do not relabel the old 3.3 certification as 4.0 certification.

## Implemented and locally exercised

- The genuine RocksDB provider is integrated across the KVS workspace, core,
  local engine, SDK typed/Any endpoints, and server storage feature. RocksDB
  endpoints no longer open SurrealKV through an alias.
- Writable native cursors rebase after savepoint rollback. Borrowed affinity
  tasks are synchronously scoped; parallel count workers retain their snapshot
  until joined.
- The quota backend integration target reuses the existing assertions without
  requiring the oversized core unit-test executable. Test hooks are optional
  and are not enabled by ordinary release builds. The v4 held/encoded write
  path participates in business-mutation fault injection.
- The RocksDB crash child selects the exact test name after removing only the
  crate-name prefix, preventing an empty successful child test run.
- Server and Rust remote SDK builds, disposable RocksDB/TiKV conformance,
  TiKV node-loss and full-cluster crash recovery, and JavaScript SDK protocol
  acceptance have been exercised. Detailed counts and failures belong to the
  accompanying handoff evidence rather than being inferred from compilation.
- SDK 2.0.8 rejects server 4.0.1. SDK 2.2.2 supports the tested HTTP and WebSocket
  paths, including atomic quota rejection and LIVE notification. Downstream
  dependency and exact-SDK acceptance pins must move together.

## Outstanding release gates

A debug build using a cached debug native archive is local validation only.
It cannot establish the optimized RocksDB workload/memory regression budget.
Before release, complete the exact-source protected-branch CI, optimized
fixed-workload benchmark on the required macOS runner, amd64 and arm64 CLI/image
builds and smoke tests, matching capability/format/SDK manifest, SBOM,
vulnerability gate, provenance, signatures, and signed immutable candidate.
Canary/staging receipts and downstream signed acceptance are required before
production promotion. No production data or VPS may be used for destructive
backend tests; a different database on an API1 TiKV cluster is not isolation.

The local TiKV tests use an independently created three-node TiKV 8.5.6 API1
cluster with loopback-only published endpoints. Destructive quota tests require
both explicit opt-in and the exact loopback PD endpoint. They must not be
repointed at a deployed store.
