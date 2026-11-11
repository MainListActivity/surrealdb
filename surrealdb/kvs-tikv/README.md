# surrealdb-kvs-tikv

The TiKV key-value store backend for SurrealDB.

> [!WARNING]
> This crate is SurrealDB internal API. It does not adhere to SemVer and its
> API is free to change and break code even between patch versions. If you are
> looking for a stable interface to the SurrealDB library please have a look at
> the [Rust SDK](https://crates.io/crates/surrealdb).

## Vendored upstream provenance

This backend is vendored from [surrealdb/surrealdb-tikv](https://github.com/surrealdb/surrealdb-tikv/tree/7b3c3102acb3d704607e0e4b9f9c13ba0d58fe81/crates/kvs-tikv), commit `7b3c3102acb3d704607e0e4b9f9c13ba0d58fe81`.

The upstream LICENSE is preserved alongside this file. Cargo metadata and engine dependencies inherit this workspace; the TiKV client dependency remains `surrealdb-tikv-client` 0.5.0 with default features disabled. Upstream's lockfile uses engine 4.0.0. This import targets the local 4.0.1 interfaces and must pass compilation and live-cluster conformance tests before production use.

## LIVE Router restriction

This fork rejects `live_query_engine=router` on TiKV. A fresh PD timestamp orders
allocation, but does not close the LIVE event log: event keys are stamped before
commit, and a router reads an older MVCC snapshot. Advancing a cursor to that
stamp can permanently skip a later-visible event. Returning the transaction's
start timestamp alone does not solve pre-commit stamping.

The provider rejects Router before connecting. The transaction's
`safe_timestamp()` also fails explicitly for callers that supply the backend
directly. There is no silent switch of engines. The default Inline engine is
unchanged; this mitigation does not certify cross-node ordering for Inline.
A host-managed External engine must supply its own committed change stream.

See [the regression and validation handoff](../../doc/TIKV_LIVE_ORDERING.md).

## Destructive test safety

**The `kvs` integration suite wipes the ENTIRE cluster keyspace before each test. Never use a production or shared cluster.** Use a dedicated disposable cluster only. Both live suites require `SURREAL_TIKV_TEST_ENDPOINT` and the separate explicit opt-in `SURREAL_TIKV_TEST_ALLOW_DESTRUCTIVE=1` before connecting. An endpoint without that opt-in fails. The `ops` suite also runs garbage collection and lock cleanup, and remains ignored by default.

With no endpoint the `kvs` suite skips; a skipped result is not backend certification. Offline unit tests use `cargo test --locked -p surrealdb-kvs-tikv --lib` and need neither variable.

Local safety changes from the pinned upstream import: `tests/kvs.rs` and `tests/ops.rs` now refuse live operations without the destructive-test opt-in. This protects against accidentally reusing a production endpoint, but does not make a production target safe.
