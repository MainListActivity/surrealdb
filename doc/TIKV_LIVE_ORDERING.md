# TiKV LIVE ordering regression and mitigation

Base: isolated upgrade snapshot `bdf68290d20ee3918d31788ee5125d807b9909de`.
This change only concerns LIVE ordering and its tests. It does not access a TiKV
cluster or change production data.

## Failure mechanism

`Transaction::store_changes()` stamps LIVE keys before the native commit.
TiKV previously inherited `safe_timestamp()` from the KVS trait, which calls
`timestamp()` and obtains a fresh PD TSO. The router opens a transaction first,
then reads this watermark and advances its cursor after scanning.

Two schedules violate the required closed-log contract:

1. A writer stamps event 10 and pauses before commit. A router snapshot sees no
   event, obtains watermark 20, and advances. Event 10 commits afterward and is
   permanently below the cursor.
2. A router opens snapshot 10. A writer stamps and commits an event after that
   snapshot but before the router obtains watermark 20. The snapshot cannot see
   the event, but the cursor still advances past it.

A transaction-start timestamp only addresses the second schedule. A fixed delay,
a process-local mutex, or disabling async commit does not establish a distributed
closed watermark for the first schedule.

## Containment in this patch

- The TiKV provider rejects an explicit Router selection before constructing a
  client. Its offline test uses an empty endpoint and verifies the specific,
  non-retryable configuration error, including mixed-case selections.
- TiKV overrides `safe_timestamp()` with the same explicit error. Direct users
  of the backend cannot fall back to a fresh PD timestamp. The shared KVS suite
  replaces TiKV's numeric watermark-success check with an explicit refusal test
  on the real transaction; it does not silently omit the contract.
- Baseline establishment and router passes cancel the read transaction if the
  watermark fails, preserving the original error and the router's cursor/lap.
- The KVS documentation now requires visibility in the reader's snapshot and
  excludes future lower-stamped events; it no longer claims a monotonic oracle
  alone proves safety.

Normal startup rejects Router at baseline establishment before startup writes.
A builder that defers startup still refuses it when the baseline is established;
this is not a claim that every constructor variant rejects immediately.

This is containment, not an implementation of ordered TiKV Router delivery.
Inline remains unchanged and is not certified for distributed ordering here.

## Deterministic regression

`lq::subscriber::tests::router_delivers_an_event_committed_after_an_interleaved_pass`
uses the real in-memory datastore, event buffer, native commit path and router.
Maintenance tasks are disabled. The existing one-shot `before_commit` hook runs
a complete router pass after `store_changes()` but before the writer commits.
The test then proves a LIVE event is retained and requires exactly one delivery
on the next pass, with no duplicate on another pass. The timeout is a hang
watchdog, not a probabilistic race generator.

The default watermark used by the memory backend shares the pre-commit gap.
Consequently this regression is explicitly ignored as a **known failure**, with
its correct delivery assertion intact. The TiKV-specific guard does not make it
pass. It must be run explicitly; an ignored test is not validation. A future
protocol that makes readers wait for pending writers will need a concurrent
barrier form of this test rather than running the reader inside the writer hook.

## Focused integration entry

`surrealdb/core/tests/live_ordering.rs` runs the same delivery contract in a
separate `live_ordering` target, linking the ordinary core library without
compiling all core unit-test modules. It requires `kv-mem`; no additional
production API or test feature is introduced. The datastore dev dependency
already enables the one-shot commit hook for integration tests.

Only the Router job is enabled, through the existing public host maintenance
API. An execution observer requires each pass to report success, no backlog and
exactly one pass event, preserving the original progress/error assertions. The
interleaved pass must process zero items. SQL setup and write results, retained
event count, exact subscription id, exact record id, CREATE action, single
notification and absence of duplicates are all checked. An uninterrupted-commit
control exercises this same harness; only the commit interleaving differs.
The focused interleaved test also runs that control first internally, even when
selected alone, and does not reach the interleaved scenario unless the entire
control (including exact delivery, duplicate checks and shutdown) passes.

The upgrade snapshot has a separate database quota-ledger initialization defect
being fixed by the upgrade worker. A failure in setup or in the normal-commit
control is **not evidence of LIVE ordering loss**. Require the log marker
`LIVE ordering control passed; starting interleaved scenario`, then check that
the later failure is the expected delivery assertion with a retained event.
Do not bypass quota initialization or weaken the setup assertions to get there.

The reproduction remains explicitly ignored as a known failure. Run the third
command below to enable it; a default test run that skips it does not validate
ordering. Scheduler sleeps only wait for a due job while the writer remains
paused at the hook, so the forced interleaving is deterministic.

## Validation handoff

The upgrade worker owns the build window. No builds or downloads were performed
while preparing this patch. Rustfmt parsed the modified Rust files and
`git diff --check` passed; these are static checks, not compilation or runtime
test results. These commands operate locally and need no TiKV
endpoint; never set a production endpoint for any test:

```sh
cargo test --locked -p surrealdb-kvs-tikv --lib provider::tests
cargo test --locked -p surrealdb-core --no-default-features --features kv-mem --test live_ordering router_delivers_an_uninterrupted_commit -- --exact --nocapture
cargo test --locked -p surrealdb-core --no-default-features --features kv-mem --test live_ordering router_delivers_an_event_committed_after_an_interleaved_pass -- --ignored --exact --nocapture
```

After the independent quota initialization defect is fixed, expected before a
general watermark fix: the provider and uninterrupted-commit control commands pass; the explicitly enabled interleaved regression fails at
the delivery assertion with the retained event present. These are
expectations from source review, not recorded runtime results. The TiKV live
conformance suites erase cluster data and are outside this handoff.

## Requirements for enabling Router

A candidate is a durable feed fence atomically committed with every stamped
event and read through the router's own snapshot. Write conflicts must prevent
a delayed writer from committing below an already visible fence. This introduces
a serialization point and requires explicit retry, crash, unknown-commit,
retention and upgrade analysis. All nodes must use the same protocol: mixed old
writers would bypass it. A versioned rollout and isolated multi-node tests are
required before removing the TiKV restriction.
