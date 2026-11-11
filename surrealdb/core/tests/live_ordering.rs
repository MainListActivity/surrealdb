//! Focused LIVE ordering regression. Links the ordinary core library instead
//! of compiling the complete core unit-test binary. Uses only an in-memory
//! datastore and explicitly driven maintenance; no endpoint or network access.
#![cfg(feature = "kv-mem")]
#![recursion_limit = "256"]
#![allow(clippy::unwrap_used)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use surrealdb_catalog::providers::CatalogProvider;
use surrealdb_cnf::ConfigMap;
use surrealdb_core::channel::{self, Receiver};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::kvs::tasks::{Job, Progress};
use surrealdb_core::options::EngineOptions;
use surrealdb_datastore::key::schema::LiveEventsPrefix;
use surrealdb_kvs::TransactionType::{Read, Write};
use surrealdb_observe::{ExecutionObserver, Outcome, TaskPassEvent, TaskPassEventSafe};
use surrealdb_rpc::capabilities::Capabilities;
use surrealdb_types::{Action, Notification, RecordId, Value};
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

async fn delivery_scenario(interleave: bool) {
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
#[ignore = "known failure: default safe_timestamp can pass an uncommitted event; pending general watermark protocol"]
async fn router_delivers_an_event_committed_after_an_interleaved_pass() {
	eprintln!("LIVE ordering phase: normal-commit control");
	delivery_scenario(false).await;
	eprintln!("LIVE ordering control passed; starting interleaved scenario");
	delivery_scenario(true).await;
}
