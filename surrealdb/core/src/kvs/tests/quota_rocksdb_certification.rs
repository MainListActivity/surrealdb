use temp_dir::TempDir;
use uuid::Uuid;

use crate::CommunityComposer;
use crate::catalog::QuotaUsageState;
use crate::catalog::providers::DatabaseProvider;
use crate::dbs::Session;
use crate::kvs::{Datastore, LockType, TransactionType};
use crate::val::TableName;

const CRASH_CHILD_PATH: &str = "SURREAL_QUOTA_ROCKSDB_CRASH_CHILD_PATH";
const CERTIFICATION_TEST: &str = "kvs::tests::quota_rocksdb_certification::quota_persistent_restart_recovers_interrupted_rebuild_without_epoch_flip";

async fn open(path: &str) -> Datastore {
	Datastore::builder()
		.with_id(Uuid::new_v4())
		.build_with_factory_path(path, CommunityComposer())
		.await
		.unwrap()
}

async fn statement_result(ds: &Datastore, sql: &str, session: &Session) -> anyhow::Result<()> {
	match ds.execute(sql, session, None).await {
		Ok(responses) => {
			for response in responses {
				response.result?;
			}
			Ok(())
		}
		Err(error) => Err(error.into()),
	}
}

async fn statement_error(ds: &Datastore, sql: &str, session: &Session) -> String {
	statement_result(ds, sql, session).await.unwrap_err().to_string()
}

async fn database_ids(ds: &Datastore) -> (crate::catalog::NamespaceId, crate::catalog::DatabaseId) {
	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let db = tx.get_db_by_name("tenant", "app", None).await.unwrap().unwrap();
	let ids = (db.namespace_id, db.database_id);
	tx.cancel().await.unwrap();
	ids
}

async fn record_count(
	ds: &Datastore,
	ns: crate::catalog::NamespaceId,
	db: crate::catalog::DatabaseId,
) -> u64 {
	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let count = tx.quota_usage(ns, db).record_count(&TableName::from("ent_restart")).await.unwrap();
	tx.cancel().await.unwrap();
	count
}

async fn seed_interrupted_rebuild(path: &str) {
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	let ds = open(path).await;
	statement_result(&ds, "DEFINE NAMESPACE tenant", &Session::owner()).await.unwrap();
	statement_result(&ds, "DEFINE DATABASE app", &namespace_owner).await.unwrap();
	statement_result(
		&ds,
		"DEFINE QUOTA ON DATABASE app \
		 RULE restart_records FOR RECORD MATCH EXACT ent_restart LIMIT 3",
		&namespace_owner,
	)
	.await
	.unwrap();
	statement_result(
		&ds,
		"DEFINE TABLE ent_restart; \
		 CREATE ent_restart:one; \
		 CREATE ent_restart:two",
		&database_owner,
	)
	.await
	.unwrap();
	let (ns, db) = database_ids(&ds).await;
	assert_eq!(record_count(&ds, ns, db).await, 2);

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let quota = tx.quota_usage(ns, db);
	assert_eq!(quota.begin_rebuild().await.unwrap(), 2);
	quota.set_staged_record_count(&TableName::from("ent_restart"), 99).await.unwrap();
	tx.commit().await.unwrap();
	// The certification child deliberately exits without cancelling the
	// finished transaction or shutting down the datastore. The parent process
	// must be able to reopen only the state RocksDB made durable before death.
	std::process::exit(0);
}

#[tokio::test]
async fn quota_persistent_restart_recovers_interrupted_rebuild_without_epoch_flip() {
	if let Ok(path) = std::env::var(CRASH_CHILD_PATH) {
		seed_interrupted_rebuild(&path).await;
		unreachable!("crash certification child exits from seed_interrupted_rebuild");
	}

	let directory = TempDir::new().unwrap();
	let path = format!("rocksdb:{}", directory.path().to_string_lossy());
	let status = std::process::Command::new(std::env::current_exe().unwrap())
		.args(["--exact", CERTIFICATION_TEST, "--nocapture"])
		.env(CRASH_CHILD_PATH, &path)
		.status()
		.unwrap();
	assert!(status.success(), "crash certification child failed: {status}");

	let reopened = open(&path).await;
	let (ns, db) = database_ids(&reopened).await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	let tx = reopened.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let quota = tx.quota_usage(ns, db);
	let meta = quota.meta().await.unwrap();
	assert_eq!(meta.state, QuotaUsageState::Rebuilding);
	assert_eq!(meta.active_epoch, Some(1));
	assert_eq!(meta.staged_epoch, Some(2));
	assert_eq!(quota.record_count(&TableName::from("ent_restart")).await.unwrap(), 2);
	tx.cancel().await.unwrap();

	let fenced = statement_error(&reopened, "CREATE ent_restart:fenced", &database_owner).await;
	assert!(fenced.contains("ledger is rebuilding"), "{fenced}");

	statement_result(&reopened, "REBUILD QUOTA ON DATABASE app", &namespace_owner).await.unwrap();
	assert_eq!(record_count(&reopened, ns, db).await, 2);
	statement_result(&reopened, "CREATE ent_restart:three", &database_owner).await.unwrap();
	let exceeded = statement_error(&reopened, "CREATE ent_restart:four", &database_owner).await;
	assert!(exceeded.contains("restart_records"), "{exceeded}");
	assert_eq!(record_count(&reopened, ns, db).await, 3);

	reopened.shutdown().await.unwrap();
}
