# Extracted RocksDB engine

Source-only extraction from the fork commit `e797f3cfc`, originally under
`surrealdb/core/src/kvs/rocksdb`. The original owned-key transaction and cursor
implementation is kept behind a private compatibility layer. The crate adapter bridges the SurrealDB 4.0.1 KVS API and is registered through
the optional `kv-rocksdb` / `storage-rocksdb` feature chain. The `rocksdb:`
endpoint opens this provider directly; it is no longer a SurrealKV alias.

The extraction preserves RocksDB options, disk-space guarding, flush/commit
coordination, versioned comparators and prefix extraction, memory accounting,
range sharding and native cursors. Configuration and common system helpers are
rebound to the split workspace crates. Other-backend error conversions and typed
catalog serialization are excluded. The original core-dependent integration
suite is retained in `src/kvs/rocksdb/tests.rs` as inactive porting reference;
independent helper unit tests remain active.

The migration adds an explicit optimistic `getu` read dependency operation and
a conservative commit-outcome error: only RocksDB Busy/TryAgain failures remain
retryable; other commit failures and failures following commit are classified
as outcome unknown. Legacy savepoint release remains a no-op; logical release
tracking in the adapter does not free the engine savepoint allocation before
the transaction ends.

Local debug builds and disposable-store tests now exercise this implementation.
No production data or VPS is used. See LICENSE for source license terms.

## Integration and release gates

The provider is a workspace member and is wired through core, engine-local,
SDK typed/Any endpoints, and the server. A focused integration test checks the
transaction kind and native RocksDB `CURRENT` file.

Borrowed work sent to affinitypool is consumed in a synchronous scope, including
unwinding, so the unsafe `spawn_local` future cannot outlive its borrows.
Current-thread runtimes run that work inline. Read-only parallel counts join
scoped workers before releasing their snapshot guard. Writable cursors track a
view generation and resume after their last consumed key when savepoint rollback
invalidates the native iterator view.

The adapter reuses v4 SavepointStack, bridges owned legacy keys and native cursor
lifetimes, and copies `next_batch` buffers while leaving visitor callbacks on the
native streaming path. It retains original visitor errors. These allocations
and retained released savepoints require performance and memory benchmarks.

Before release, compile all affected SDK/server feature combinations, run the v4
KVS conformance suite using newly created disposable local directories, and run
native quota concurrent fencing, nested savepoint, restart/WAL sync, disk-full,
fault-injection and uncertain-commit tests. Audit the default timestamp and
safe-timestamp semantics for LIVE/watermark use. The trait capability flags are
implementation assertions, not certification results. The local debug conformance run passed 59 unit tests and 115 shared KVS tests
(37 shared cases are ignored by the suite). Final quota and restart certification
is recorded separately in the handoff evidence. These results do not certify an
optimized multi-architecture release. Existing release gates, including the
fixed-workload benchmark, remain enforced.
