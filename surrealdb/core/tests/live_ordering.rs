//! Focused LIVE ordering regression. Links the ordinary core library instead
//! of compiling the complete core unit-test binary. Uses only an in-memory
//! datastore and explicitly driven maintenance; no endpoint or network access.
#![cfg(feature = "kv-mem")]
#![recursion_limit = "256"]
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use surrealdb_catalog::DatabaseDefinition;
use surrealdb_catalog::providers::CatalogProvider;
use surrealdb_cnf::ConfigMap;
use surrealdb_core::channel::{self, Receiver};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::kvs::tasks::{Job, Progress};
use surrealdb_core::options::EngineOptions;
use surrealdb_datastore::key::schema::{KEYSPACE_MAP, LiveEventWatermarkKey, LiveEventsPrefix};
use surrealdb_datastore::tx::Transaction;
use surrealdb_datastore::{is_retryable_transaction_conflict, storage_error};
use surrealdb_expr::val::{RecordId as StoredRecordId, TableName, Value as StoredValue};
use surrealdb_kvs::TransactionType::{Read, Write};
use surrealdb_kvs::timestamp::MAX_TIMESTAMP_BYTES;
use surrealdb_observe::{ExecutionObserver, Outcome, TaskPassEvent, TaskPassEventSafe};
use surrealdb_rpc::capabilities::Capabilities;
use surrealdb_types::{Action, Notification, RecordId, Uuid, Value};
use tokio::time::{sleep, timeout};

#[derive(Default)]
struct PassRecorder(Mutex<Vec<TaskPassEventSafe>>);

impl ExecutionObserver for PassRecorder {
	fn on_task_pass(&self, event: &TaskPassEvent) {
		self.0.lock().unwrap().push(event.safe);
	}
}

/// The public scheduler reports completion even on a job error. Observe its
/// outcome as well so a failed router pass cannot masquerade as a successful
/// empty scan. Only Router is scheduled, and there are no background tasks.
async fn router_pass(ds: &Arc<Datastore>, recorder: &PassRecorder) -> TaskPassEventSafe {
	let before = recorder.0.lock().unwrap().len();
	timeout(Duration::from_secs(5), async {
		loop {
			let slice = ds.run_maintenance_slice().await.unwrap();
			match slice.job {
				Some(job) => {
					assert_eq!(job, Job::LiveQueryRouter, "no other maintenance may run");
					assert_eq!(slice.progress, Progress::Done);
					break;
				}
				None => {
					let wait = slice.next_due.expect("Router must remain scheduled");
					sleep(wait).await;
				}
			}
		}
	})
	.await
	.expect("the requested router pass must finish");
	let passes = recorder.0.lock().unwrap();
	assert_eq!(passes.len(), before + 1, "exactly one pass must have run");
	let pass = passes[before];
	assert_eq!(pass.job, "live_query_router");
	assert_eq!(pass.outcome, Outcome::Success, "the router must not hide an error");
	assert!(!pass.backlog, "the pass must complete its lap");
	pass
}

fn router_only_options() -> EngineOptions {
	EngineOptions {
		node_membership_refresh_interval: Duration::ZERO,
		node_membership_check_interval: Duration::ZERO,
		node_membership_cleanup_interval: Duration::ZERO,
		changefeed_gc_interval: Duration::ZERO,
		live_query_gc_interval: Duration::ZERO,
		index_compaction_interval: Duration::ZERO,
		index_build_resume_interval: Duration::ZERO,
		event_processing_interval: Duration::ZERO,
		graph_fold_interval: Duration::ZERO,
		table_migration_interval: Duration::ZERO,
		live_query_router_interval: Duration::from_millis(1),
		reclaim_interval: Duration::ZERO,
		mvcc_gc_interval: Duration::ZERO,
		lock_cleanup_interval: Duration::ZERO,
		rpc_session_gc_interval: Duration::ZERO,
		system_metrics_refresh_interval: Duration::ZERO,
		diskann_writer_lease_release_interval: Duration::ZERO,
		..EngineOptions::default()
	}
}

async fn drain(recv: &Receiver<Notification>) -> Vec<Notification> {
	let mut out = Vec::new();
	while let Ok(Ok(notification)) = timeout(Duration::from_millis(200), recv.recv()).await {
		out.push(notification);
	}
	out
}

struct Fixture {
	recorder: Arc<PassRecorder>,
	recv: Receiver<Notification>,
	ds: Arc<Datastore>,
	database: Arc<DatabaseDefinition>,
	owner: Session,
	subscription: Uuid,
}

async fn fixture() -> Fixture {
	let recorder = Arc::new(PassRecorder::default());
	let observer = Arc::clone(&recorder);
	let (send, recv) = channel::bounded(1000);
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_engine_options(router_only_options())
		.with_observer(observer)
		.with_capabilities(Capabilities::all())
		.with_auth(false)
		.with_notify(send)
		.with_config(ConfigMap::empty().with_key_value("live_query_engine", "router"))
		.build_with_path("memory")
		.await
		.unwrap();
	ds.check_version().await.unwrap();
	assert!(!ds.maintenance_tasks_running());

	let (ns, db) = ("test", "test");
	let tx = ds.transaction(Write).await.unwrap();
	let database = tx.ensure_ns_db(None, ns, db).await.unwrap();
	tx.commit().await.unwrap();
	let owner = Session::owner().with_ns(ns).with_db(db);
	let mut setup = ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
	assert_eq!(setup.len(), 1);
	setup.remove(0).result.unwrap();
	let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
	let mut registered = ds.execute("LIVE SELECT * FROM doc", &live, None).await.unwrap();
	assert_eq!(registered.len(), 1);
	let Value::Uuid(subscription) = registered.remove(0).result.unwrap() else {
		panic!("LIVE must return a subscription id");
	};
	assert!(recv.try_recv().is_err());

	Fixture {
		recorder,
		recv,
		ds,
		database,
		owner,
		subscription,
	}
}

async fn delivery_scenario(interleave: bool) {
	let Fixture {
		recorder,
		recv,
		ds,
		database,
		owner,
		subscription,
	} = fixture().await;

	let ran = Arc::new(AtomicBool::new(false));
	if interleave {
		let observed = Arc::clone(&ran);
		let during_commit = Arc::clone(&ds);
		let during_pass = Arc::clone(&recorder);
		ds.commit_triggers().set_before_commit_hook(Box::new(move || {
			Box::pin(async move {
				// The hook fixes the interleaving: store_changes has stamped
				// the event but native commit has not started. Scheduler waits
				// only select a pass; the writer stays paused throughout it.
				let pass = router_pass(&during_commit, &during_pass).await;
				assert_eq!(pass.items, 0, "the write is still uncommitted");
				observed.store(true, Ordering::Release);
			})
		}));
	}

	let mut created = ds.execute("CREATE doc:1 SET n = 1", &owner, None).await.unwrap();
	assert_eq!(created.len(), 1);
	created.remove(0).result.unwrap();
	assert_eq!(ran.load(Ordering::Acquire), interleave, "the requested hook must run");

	// Distinguish delivery loss from failed writes or missing event capture.
	let tx = ds.transaction(Read).await.unwrap();
	let events = tx
		.scan_raw(
			LiveEventsPrefix {
				ns: database.namespace_id,
				db: database.database_id,
			}
			.range()
			.unwrap(),
			2,
			0,
			None,
		)
		.await
		.unwrap();
	tx.cancel().await.unwrap();
	assert_eq!(events.len(), 1, "the committed event must be retained");

	router_pass(&ds, &recorder).await;
	let notifs = drain(&recv).await;
	assert_eq!(
		notifs.len(),
		1,
		"the router must deliver the retained event, including after an interleaved pass",
	);
	assert_eq!(notifs[0].id, subscription);
	assert_eq!(notifs[0].action, Action::Create);
	assert_eq!(notifs[0].record, Value::RecordId(RecordId::new("doc", 1_i64)));

	router_pass(&ds, &recorder).await;
	assert!(drain(&recv).await.is_empty(), "deliver the event exactly once");
	ds.shutdown().await.unwrap();
}

/// Control: verifies the public maintenance driver and observer can deliver the
/// same retained event when no pass intervenes before its commit.
#[tokio::test]
async fn router_delivers_an_uninterrupted_commit() {
	delivery_scenario(false).await;
}

/// Same delivery assertions as the control; only the forced interleaving differs.
/// Run the control first even when this test is selected alone, so an unrelated
/// setup failure (such as quota-ledger initialization) is not mistaken for loss.
#[tokio::test]
async fn router_delivers_an_event_committed_after_an_interleaved_pass() {
	eprintln!("LIVE ordering phase: normal-commit control");
	delivery_scenario(false).await;
	eprintln!("LIVE ordering control passed; starting interleaved scenario");
	delivery_scenario(true).await;
}

/// Synthetic event capture isolates the commit-order protocol from document
/// quotas and write contention. The full SQL path is covered above. Each event
/// still passes through the actual LIVE buffer, commit, router and subscriber.
async fn buffer_create(tx: &Transaction, database: &DatabaseDefinition, key: i64) {
	let table = TableName::from("doc");
	let id = StoredRecordId::new(table.clone(), key);
	let current = StoredValue::from(BTreeMap::from([
		("id", StoredValue::RecordId(id.clone())),
		("n", StoredValue::from(key)),
	]));
	let captured = tx.timestamp().await.unwrap().as_versionstamp();
	tx.live_event_buffer_record_change(
		database.namespace_id,
		database.database_id,
		&table,
		&id,
		StoredValue::None,
		current,
		captured,
	);
}

async fn event_count(tx: &Transaction, database: &DatabaseDefinition) -> usize {
	tx.scan_raw(
		LiveEventsPrefix {
			ns: database.namespace_id,
			db: database.database_id,
		}
		.range()
		.unwrap(),
		100,
		0,
		None,
	)
	.await
	.unwrap()
	.len()
}

fn notifications_are(notifs: &[Notification], subscription: Uuid, keys: &[i64]) {
	assert_eq!(notifs.len(), keys.len(), "one notification per committed event: {notifs:?}");
	for (notif, key) in notifs.iter().zip(keys) {
		assert_eq!(notif.id, subscription);
		assert_eq!(notif.action, Action::Create);
		assert_eq!(notif.record, Value::RecordId(RecordId::new("doc", *key)));
	}
}

/// The watermark must come from exactly the snapshot that scans the events,
/// even when another writer commits before the reader asks for its watermark.
#[tokio::test]
async fn a_reader_keeps_its_snapshot_watermark_after_a_newer_commit() {
	let Fixture {
		ds,
		database,
		recorder,
		recv,
		subscription,
		..
	} = fixture().await;
	let first = ds.transaction(Write).await.unwrap();
	buffer_create(&first, &database, 1).await;
	first.commit().await.unwrap();

	let before = ds.transaction(Read).await.unwrap();
	let first_watermark = before.live_query_watermark().await.unwrap().as_versionstamp();
	before.cancel().await.unwrap();
	let stale = ds.transaction(Read).await.unwrap();

	let second = ds.transaction(Write).await.unwrap();
	buffer_create(&second, &database, 2).await;
	second.commit().await.unwrap();

	// Deliberately make the first watermark read on this old snapshot after
	// the second commit. A fresh oracle timestamp would cross its visibility.
	assert_eq!(stale.live_query_watermark().await.unwrap().as_versionstamp(), first_watermark);
	assert_eq!(event_count(&stale, &database).await, 1);
	stale.cancel().await.unwrap();

	let fresh = ds.transaction(Read).await.unwrap();
	assert!(fresh.live_query_watermark().await.unwrap().as_versionstamp() > first_watermark);
	assert_eq!(event_count(&fresh, &database).await, 2);
	fresh.cancel().await.unwrap();

	router_pass(&ds, &recorder).await;
	notifications_are(&drain(&recv).await, subscription, &[1, 2]);
	router_pass(&ds, &recorder).await;
	assert!(drain(&recv).await.is_empty(), "old snapshots must not cause duplicate delivery");
	ds.shutdown().await.unwrap();
}

/// A later-stamped writer can commit first, but a stale writer must then fail
/// retryably instead of landing a smaller stamp below the router's cursor.
#[tokio::test]
async fn a_stale_writer_conflicts_and_a_fresh_retry_delivers_once() {
	let Fixture {
		ds,
		database,
		recorder,
		recv,
		subscription,
		..
	} = fixture().await;
	let stale = ds.transaction(Write).await.unwrap();
	buffer_create(&stale, &database, 1).await;
	let winner = ds.transaction(Write).await.unwrap();
	buffer_create(&winner, &database, 2).await;

	let ran = Arc::new(AtomicBool::new(false));
	let observed = Arc::clone(&ran);
	let during_commit = Arc::clone(&ds);
	let during_pass = Arc::clone(&recorder);
	ds.commit_triggers().set_before_commit_hook(Box::new(move || {
		Box::pin(async move {
			// The stale writer already buffered its fence/event atomically.
			// This commit has a distinct event key; only the shared fence
			// supplies the write conflict that must reject the stale writer.
			winner.commit().await.unwrap();
			let pass = router_pass(&during_commit, &during_pass).await;
			assert_eq!(pass.items, 1, "only the winning event is visible");
			observed.store(true, Ordering::Release);
		})
	}));
	let error = stale.commit().await.expect_err("the older fence writer must conflict");
	assert!(ran.load(Ordering::Acquire), "the winning commit must have interleaved");
	assert!(is_retryable_transaction_conflict(&error), "expected retryable conflict: {error:#}");
	notifications_are(&drain(&recv).await, subscription, &[2]);
	let committed = ds.transaction(Read).await.unwrap();
	let winner_watermark = committed.live_query_watermark().await.unwrap().as_versionstamp();
	assert_eq!(event_count(&committed, &database).await, 1, "the failed event must roll back");
	committed.cancel().await.unwrap();

	let retry = ds.transaction(Write).await.unwrap();
	buffer_create(&retry, &database, 1).await;
	retry.commit().await.unwrap();
	let committed = ds.transaction(Read).await.unwrap();
	assert!(committed.live_query_watermark().await.unwrap().as_versionstamp() > winner_watermark);
	assert_eq!(event_count(&committed, &database).await, 2);
	committed.cancel().await.unwrap();
	router_pass(&ds, &recorder).await;
	notifications_are(&drain(&recv).await, subscription, &[1]);
	router_pass(&ds, &recorder).await;
	assert!(drain(&recv).await.is_empty(), "neither winner nor retry is delivered twice");
	ds.shutdown().await.unwrap();
}

/// Cancelling a writer must leave both the durable fence and the retained event
/// set untouched; an empty pass must not advance on the cancelled capture.
#[tokio::test]
async fn a_cancelled_event_does_not_advance_the_watermark() {
	let Fixture {
		ds,
		database,
		recorder,
		recv,
		subscription,
		..
	} = fixture().await;
	let first = ds.transaction(Write).await.unwrap();
	buffer_create(&first, &database, 1).await;
	first.commit().await.unwrap();
	router_pass(&ds, &recorder).await;
	notifications_are(&drain(&recv).await, subscription, &[1]);
	let before = ds.transaction(Read).await.unwrap();
	let watermark = before.live_query_watermark().await.unwrap().as_versionstamp();
	before.cancel().await.unwrap();

	let cancelled = ds.transaction(Write).await.unwrap();
	buffer_create(&cancelled, &database, 2).await;
	// Exercise rollback after both the fence and event row have been staged.
	cancelled.store_changes().await.unwrap();
	cancelled.cancel().await.unwrap();
	let after = ds.transaction(Read).await.unwrap();
	assert_eq!(after.live_query_watermark().await.unwrap().as_versionstamp(), watermark);
	assert_eq!(event_count(&after, &database).await, 1);
	after.cancel().await.unwrap();
	assert_eq!(router_pass(&ds, &recorder).await.items, 0);
	assert!(drain(&recv).await.is_empty(), "a cancelled event is never delivered");
	ds.shutdown().await.unwrap();
}

/// A process restart or clock regression must fail closed if its next timestamp
/// does not exceed the durable fence, without committing or delivering an event.
#[tokio::test]
async fn a_nonadvancing_timestamp_is_rejected_without_publishing_an_event() {
	let Fixture {
		ds,
		database,
		recorder,
		recv,
		..
	} = fixture().await;
	let seeded = ds.transaction(Write).await.unwrap();
	let future = seeded
		.timestamp_impl()
		.create_from_versionstamp(u128::from(u64::MAX - 1))
		.expect("the memory backend supports a 64-bit fence");
	let expected = future.as_versionstamp();
	let mut bytes = [0; MAX_TIMESTAMP_BYTES];
	seeded.set_key(&LiveEventWatermarkKey {}, &future.encode(&mut bytes).to_vec()).await.unwrap();
	seeded.commit().await.unwrap();

	let writer = ds.transaction(Write).await.unwrap();
	buffer_create(&writer, &database, 1).await;
	let error = writer.commit().await.expect_err("a timestamp behind the fence must be refused");
	assert!(
		matches!(storage_error(&error), Some(surrealdb_kvs::Error::TimestampInvalid(_))),
		"expected a timestamp failure, not a hidden setup error: {error:#}",
	);
	assert!(!is_retryable_transaction_conflict(&error));

	let after = ds.transaction(Read).await.unwrap();
	assert_eq!(after.live_query_watermark().await.unwrap().as_versionstamp(), expected);
	assert_eq!(event_count(&after, &database).await, 0, "the rejected event must not commit");
	after.cancel().await.unwrap();
	assert_eq!(router_pass(&ds, &recorder).await.items, 0);
	assert!(drain(&recv).await.is_empty(), "the rejected event must not be delivered");
	ds.shutdown().await.unwrap();
}

/// Keep the generated typed-key documentation in sync without linking the
/// datastore's complete unit-test binary into this focused regression run.
#[test]
fn typed_keyspace_map_matches_its_snapshot() {
	assert_eq!(KEYSPACE_MAP, include_str!("../../datastore/src/key/keyspace.map"),);
}
