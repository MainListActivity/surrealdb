# LIVE ordering protocol and validation handoff

Base: isolated upgrade snapshot `bdf68290d20ee3918d31788ee5125d807b9909de`.
All work is local source work. No production service or data has been touched.

## Reproduced failure

`Transaction::store_changes()` stamps LIVE event keys before the native commit.
The old router used a fresh `safe_timestamp()` oracle value to advance its cursor.
That is unsafe in both of these deterministic schedules:

1. A writer stamps event 10 and pauses before commit. A router sees no event,
   obtains watermark 20 and advances. Event 10 commits below the cursor.
2. A reader opens snapshot 10. Another transaction commits before the reader
   obtains watermark 20. The event is invisible in the reader's older snapshot,
   but its cursor still advances past it.

The upgrade worker ran the first schedule after fixing an independent database
quota-ledger initialization defect. Its normal-commit control passed. The
interleaved case retained the event but delivered zero notifications instead of
one (`live-interleaved-runtime.log`). This is behavioral evidence of the old
bug; it is not validation of the new protocol.

## Atomic event fence

Every transaction with buffered LIVE events now performs a conflict-tracked
`getu` of the global `/!lw` key before allocating its event timestamp. The
new timestamp must strictly exceed the previously committed fence. The same
native transaction writes that fence and all its LIVE event rows. A failed or
cancelled transaction publishes neither. The router reads the fence through
the same snapshot it uses to scan event rows; it never uses a fresh oracle as
the delivery bound. The startup baseline uses this same committed fence.

For a reader that sees fence W, any later successful event writer either sees
W and must allocate a greater stamp, or uses an older snapshot and conflicts
with the committed fence. Therefore an event at or below W cannot become
visible after that reader. `getu` is essential: relying on blind-write conflict
behavior would not cover every supported backend. Conflicting transactions
must retry in a new snapshot. A nonadvancing clock fails with TimestampInvalid
instead of silently losing an event.

The key is global because each router has one cursor across all databases.
This adds one read and one write to event-producing transactions and serializes
successful LIVE event commits, including otherwise unrelated tables/databases.
Existing transaction retry/error propagation handles conflicts; explicit
transactions can return a retryable conflict to their caller. Transactions
without LIVE events do not access the fence. Changefeed-only ordering is not
changed by this protocol.

An idle fence does not represent current time. Retention reporting continues
to use a separate fresh timestamp, and subscription capture/registration times
retain their existing semantics. The behind-retention counter remains a
conservative risk signal: after startup or idle, an expired cursor range can
be empty and fresh work can still deliver successfully. The warning now makes
that explicit. Replacing its reference with last-scan time could hide a delayed
writer whose event was already aged before committing. This protocol prevents
cursor loss; it does not make notification transport durable or extend retention.

## Focused behavioral tests

`surrealdb/core/tests/live_ordering.rs` links the ordinary core library without
compiling the complete core unit-test suite. It uses an in-memory datastore
and the public host maintenance API, with every job except Router disabled.
An observer requires each requested Router pass to report success, completion
and exactly one execution. No live endpoint is involved.

The tests require:

- Normal SQL commit delivers the exact subscription, CREATE action and record
  once, with no duplicate on the next pass.
- A Router pass forced between event staging and commit processes no event;
  the later pass delivers the committed event once. This test always runs the
  normal-commit control first. Neither case is ignored.
- An old snapshot first asked for its fence after a newer commit still sees
  its old fence/event set; a fresh snapshot sees both committed events.
- Two event writers with distinct event keys conflict through the shared
  fence. The later-stamped winner commits and is routed from inside the older
  writer's pre-commit hook. The older commit must fail retryably; a fresh
  retry must deliver its event exactly once.
- Cancelling after both fence and event writes are staged leaves both unchanged.
- A deliberately future fence rejects a nonadvancing timestamp and publishes
  no event.
- The generated typed keyspace map equals its checked-in snapshot.

Synthetic events isolate the protocol cases from document quota/write conflicts;
they use the real buffer, commit, router and subscriber. The two SQL scenarios
cover the end-to-end path. Commit hooks force the schedules, and timeouts are
hang watchdogs. Scheduler sleeps only select a due job while the writer remains
paused. The original core unit regression is also enabled.

## Validation handoff

The upgrade worker owns the build window. This patch has only been checked by
Rustfmt parsing/formatting and `git diff --check`; no build or runtime success
is claimed for the new protocol. Integrate it with the worker's quota fix first.
The old isolated snapshot alone still has the unrelated quota initialization
bug, so bypassing that setup would invalidate the control.

Run locally using the worker's existing toolchain/cache and disk budget:

```sh
cargo test --locked -p surrealdb-core --no-default-features --features kv-mem --test live_ordering -- --nocapture
cargo test --locked -p surrealdb-kvs-tikv --lib provider::tests
```

Expected: all focused tests pass, including the exact previously failing
interleaving. The provider tests are offline configuration tests. This handoff
does not request the destructive TiKV conformance suites or any production
endpoint. Broader existing Router retention/GC and two-node ownership tests
should run when the full core unit-test build is available.

## TiKV rollout remains blocked

The TiKV provider still rejects explicit Router configuration before connecting,
the datastore fence API also rejects direct TiKV backend builders, and TiKV's
backend `safe_timestamp()` continues to reject the unsafe oracle contract.
The datastore protocol is separate from that backend API.

A fence cannot constrain old binaries that do not write it. Enabling Router on
an existing/mixed-version cluster requires a coordinated, versioned rollout:
drain old writers, make the storage/release compatibility gate reject them,
resolve retained pre-protocol history and cursor baselines, and require every
writer to participate. Absence of `/!lw` returns the earliest stamp; this is
not a migration guarantee for older retained history. The release marker used
by the parallel quota upgrade must be coordinated rather than changed here.

Keep the TiKV Router restriction until disposable multi-node tests cover this
rollout, reverse commit order, stale reader snapshots, aborts, restarts and
unknown-commit outcomes. Inline behavior is unchanged and no cross-node Inline
ordering certification is implied. Production release remains blocked on
integration/runtime validation and the coordinated compatibility decision.
