use std::sync::Arc;

use tokio::sync::Barrier;
use uuid::Uuid;

use super::CreateDs;
use crate::catalog::QuotaUsageState;
use crate::catalog::providers::DatabaseProvider;
use crate::dbs::Session;
use crate::kvs::quota::QuotaUsageSnapshot;
use crate::kvs::testing::{QuotaFaultSite, inject_quota_fault};
use crate::kvs::{Datastore, LockType, TransactionType};
use crate::val::TableName;

async fn setup(new_ds: impl CreateDs, node_id: Uuid) -> Datastore {
	let (ds, _) = new_ds.create_ds(node_id).await;
	ds.execute("DEFINE NAMESPACE tenant", &Session::owner(), None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	ds.execute("DEFINE DATABASE app", &Session::owner().with_ns("tenant"), None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	ds
}

fn namespace_owner() -> Session {
	Session::owner().with_ns("tenant")
}

fn database_owner() -> Session {
	Session::owner().with_ns("tenant").with_db("app")
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

async fn record_count(ds: &Datastore, table: &str) -> u64 {
	let (ns, db) = database_ids(ds).await;
	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let count = tx.quota_usage(ns, db).record_count(&TableName::from(table)).await.unwrap();
	tx.cancel().await.unwrap();
	count
}

async fn field_count(ds: &Datastore, table: &str) -> u64 {
	let (ns, db) = database_ids(ds).await;
	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let count = tx.quota_usage(ns, db).field_count(&TableName::from(table)).await.unwrap();
	tx.cancel().await.unwrap();
	count
}

async fn table_bucket_count(ds: &Datastore, generation: u64, rule: &str) -> u64 {
	let (ns, db) = database_ids(ds).await;
	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let count = tx.quota_usage(ns, db).table_bucket_count(generation, rule).await.unwrap();
	tx.cancel().await.unwrap();
	count
}

async fn physical_record_count(ds: &Datastore, table: &str) -> i64 {
	let sql = format!("SELECT count() FROM {table} GROUP ALL");
	let mut responses = ds.execute(&sql, &database_owner(), None).await.unwrap();
	let surrealdb_types::Value::Array(rows) = responses.remove(0).result.unwrap() else {
		panic!("count query did not return an array");
	};
	let Some(surrealdb_types::Value::Object(row)) = rows.first() else {
		return 0;
	};
	let Some(surrealdb_types::Value::Number(count)) = row.get("count") else {
		panic!("count query did not return a numeric count");
	};
	count.to_int().unwrap()
}

fn is_expected_contention(error: &anyhow::Error) -> bool {
	let message = error.to_string();
	message.contains("Transaction conflict")
		|| message.contains("failed transaction")
		|| message.contains("Quota admission conflicted")
		|| message.contains("Condition not met")
}

pub async fn no_policy_metering_and_regex_contract(new_ds: impl CreateDs) {
	let ds = setup(new_ds, Uuid::new_v4()).await;
	let ns_owner = namespace_owner();
	let db_owner = database_owner();

	statement_result(
		&ds,
		"DEFINE TABLE ent_user SCHEMAFULL; \
		 DEFINE FIELD name ON ent_user TYPE string; \
		 CREATE ent_user:one SET name = 'one'",
		&db_owner,
	)
	.await
	.unwrap();
	assert_eq!(field_count(&ds, "ent_user").await, 1);
	assert_eq!(record_count(&ds, "ent_user").await, 1);

	statement_result(
		&ds,
		"DEFINE QUOTA ON DATABASE app \
		 RULE ent_tables FOR TABLE MATCH REGEX /^ent_/ LIMIT 2 \
		 RULE exact_user_table FOR TABLE MATCH EXACT ent_user LIMIT 1 \
		 RULE ent_fields FOR FIELD MATCH REGEX /^ent_/ LIMIT 2 \
		 RULE exact_user_records FOR RECORD MATCH EXACT ent_user LIMIT 2 \
		 RULE ent_records FOR RECORD MATCH REGEX /^ent_/ LIMIT 3",
		&ns_owner,
	)
	.await
	.unwrap();

	statement_result(&ds, "DEFINE FIELD email ON ent_user TYPE string", &db_owner).await.unwrap();
	let field_error =
		statement_error(&ds, "DEFINE FIELD phone ON ent_user TYPE string", &db_owner).await;
	assert!(field_error.contains("ent_fields"), "{field_error}");

	statement_result(
		&ds,
		"INSERT INTO ent_user { id: ent_user:two, name: 'two', email: 'two@example.test' }",
		&db_owner,
	)
	.await
	.unwrap();
	let record_error = statement_error(
		&ds,
		"INSERT INTO ent_user [\
		 { id: ent_user:three, name: 'three', email: 'three@example.test' }, \
		 { id: ent_user:four, name: 'four', email: 'four@example.test' }]",
		&db_owner,
	)
	.await;
	assert!(record_error.contains("exact_user_records"), "{record_error}");

	statement_result(&ds, "DEFINE TABLE ent_order", &db_owner).await.unwrap();
	let table_error = statement_error(&ds, "DEFINE TABLE ent_invoice", &db_owner).await;
	assert!(table_error.contains("ent_tables"), "{table_error}");

	assert_eq!(table_bucket_count(&ds, 1, "exact_user_table").await, 1);
	assert_eq!(table_bucket_count(&ds, 1, "ent_tables").await, 2);
	assert_eq!(field_count(&ds, "ent_user").await, 2);
	assert_eq!(record_count(&ds, "ent_user").await, 2);
	assert_eq!(physical_record_count(&ds, "ent_user").await, 2);
}

pub async fn multi_node_mixed_contention_contract(new_ds: impl CreateDs) {
	const LIMIT: usize = 24;
	const CLIENTS: usize = 72;

	let primary_id = Uuid::new_v4();
	let primary = Arc::new(setup(new_ds, primary_id).await);
	let secondary = Arc::new(primary.fork_for_test_with_node_id(Uuid::new_v4()));
	let ns_owner = namespace_owner();
	let db_owner = database_owner();
	statement_result(
		&primary,
		&format!(
			"DEFINE QUOTA ON DATABASE app \
			 RULE mixed_records FOR RECORD MATCH EXACT ent_mix LIMIT {LIMIT}"
		),
		&ns_owner,
	)
	.await
	.unwrap();
	statement_result(&primary, "DEFINE TABLE ent_mix", &db_owner).await.unwrap();

	let barrier = Arc::new(Barrier::new(CLIENTS + 1));
	let mut clients = Vec::with_capacity(CLIENTS);
	for index in 0..CLIENTS {
		let ds = if index % 2 == 0 {
			Arc::clone(&primary)
		} else {
			Arc::clone(&secondary)
		};
		let barrier = Arc::clone(&barrier);
		let session = db_owner.clone();
		clients.push(tokio::spawn(async move {
			let sql = match index % 3 {
				0 => format!("CREATE ent_mix:row_{index} SET source = 'create'"),
				1 => format!("INSERT INTO ent_mix {{ id: ent_mix:row_{index}, source: 'insert' }}"),
				_ => format!("UPSERT ent_mix:row_{index} SET source = 'upsert'"),
			};
			barrier.wait().await;
			for _ in 0..512 {
				match statement_result(&ds, &sql, &session).await {
					Ok(()) => return true,
					Err(error) if error.to_string().contains("mixed_records") => return false,
					Err(error) if is_expected_contention(&error) => {
						tokio::task::yield_now().await;
					}
					Err(error) => panic!("unexpected mixed contention error: {error:#}"),
				}
			}
			panic!("mixed multi-node quota admission did not converge");
		}));
	}
	barrier.wait().await;

	let mut successes = 0;
	for client in clients {
		successes += usize::from(client.await.unwrap());
	}
	assert_eq!(successes, LIMIT);
	assert_eq!(record_count(&primary, "ent_mix").await, LIMIT as u64);
	assert_eq!(physical_record_count(&primary, "ent_mix").await, LIMIT as i64);
}

async fn execute_in_transaction(
	ds: &Datastore,
	sql: &str,
) -> (Arc<crate::kvs::Transaction>, anyhow::Result<()>) {
	let tx = Arc::new(ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap());
	let result =
		match ds.execute_with_transaction(sql, &database_owner(), None, Arc::clone(&tx)).await {
			Ok(responses) => {
				let mut result = Ok(());
				for response in responses {
					if let Err(error) = response.result {
						result = Err(error.into());
						break;
					}
				}
				result
			}
			Err(error) => Err(error.into()),
		};
	(tx, result)
}

pub async fn atomic_fault_and_commit_unknown_contract(new_ds: impl CreateDs) {
	let node_id = Uuid::new_v4();
	let ds = setup(new_ds, node_id).await;
	statement_result(
		&ds,
		"DEFINE QUOTA ON DATABASE app \
		 RULE fault_records FOR RECORD MATCH EXACT ent_fault LIMIT 20",
		&namespace_owner(),
	)
	.await
	.unwrap();
	statement_result(&ds, "DEFINE TABLE ent_fault", &database_owner()).await.unwrap();

	for (index, site) in
		[QuotaFaultSite::BeforeBusinessMutation, QuotaFaultSite::AfterBusinessMutation]
			.into_iter()
			.enumerate()
	{
		let _fault = inject_quota_fault(site, node_id);
		let (tx, result) =
			execute_in_transaction(&ds, &format!("CREATE ent_fault:business_{index}")).await;
		assert!(result.unwrap_err().to_string().contains("injected quota fault"));
		tx.cancel().await.unwrap();
		assert_eq!(record_count(&ds, "ent_fault").await, 0);
		assert_eq!(physical_record_count(&ds, "ent_fault").await, 0);
	}

	for (index, site) in [
		QuotaFaultSite::BeforeCounterWrite,
		QuotaFaultSite::AfterCounterWrite,
		QuotaFaultSite::BeforeCommit,
	]
	.into_iter()
	.enumerate()
	{
		let (tx, result) =
			execute_in_transaction(&ds, &format!("CREATE ent_fault:commit_{index}")).await;
		result.unwrap();
		let _fault = inject_quota_fault(site, node_id);
		let error = tx.commit().await.unwrap_err().to_string();
		assert!(error.contains("injected quota fault"), "{error}");
		assert_eq!(record_count(&ds, "ent_fault").await, 0);
		assert_eq!(physical_record_count(&ds, "ent_fault").await, 0);
	}

	let (tx, result) =
		execute_in_transaction(&ds, "INSERT INTO ent_fault { id: ent_fault:unknown, attempt: 1 }")
			.await;
	result.unwrap();
	let _fault = inject_quota_fault(QuotaFaultSite::CommitOutcomeUnknown, node_id);
	let error = tx.commit().await.unwrap_err().to_string();
	assert!(error.contains("injected quota fault"), "{error}");
	assert_eq!(record_count(&ds, "ent_fault").await, 1);
	assert_eq!(physical_record_count(&ds, "ent_fault").await, 1);

	statement_result(
		&ds,
		"INSERT INTO ent_fault { id: ent_fault:unknown, attempt: 2 } \
		 ON DUPLICATE KEY UPDATE attempt = $input.attempt",
		&database_owner(),
	)
	.await
	.unwrap();
	assert_eq!(record_count(&ds, "ent_fault").await, 1);
	assert_eq!(physical_record_count(&ds, "ent_fault").await, 1);
}

pub async fn generation_and_rebuild_epoch_contract(new_ds: impl CreateDs) {
	let ds = setup(new_ds, Uuid::new_v4()).await;
	let ns_owner = namespace_owner();
	let db_owner = database_owner();
	statement_result(
		&ds,
		"DEFINE QUOTA ON DATABASE app \
		 RULE generation_records FOR RECORD MATCH EXACT ent_generation LIMIT 10",
		&ns_owner,
	)
	.await
	.unwrap();
	statement_result(&ds, "DEFINE TABLE ent_generation", &db_owner).await.unwrap();

	let writer =
		Arc::new(ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap());
	let responses = ds
		.execute_with_transaction(
			"CREATE ent_generation:stale",
			&db_owner,
			None,
			Arc::clone(&writer),
		)
		.await
		.unwrap();
	for response in responses {
		response.result.unwrap();
	}
	statement_result(
		&ds,
		"ALTER QUOTA ON DATABASE app EXPECT GENERATION 1 \
		 SET RULE generation_records FOR RECORD MATCH EXACT ent_generation LIMIT 0",
		&ns_owner,
	)
	.await
	.unwrap();
	assert!(writer.commit().await.is_err());
	assert_eq!(record_count(&ds, "ent_generation").await, 0);
	assert_eq!(physical_record_count(&ds, "ent_generation").await, 0);

	let (ns, db) = database_ids(&ds).await;
	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let quota = tx.quota_usage(ns, db);
	assert_eq!(quota.begin_rebuild().await.unwrap(), 2);
	quota.set_staged_record_count(&TableName::from("ent_generation"), 7).await.unwrap();
	tx.commit().await.unwrap();

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let quota = tx.quota_usage(ns, db);
	let mut expected = QuotaUsageSnapshot::default();
	expected.set_record_count(&TableName::from("ent_generation"), 7);
	let validated = quota.validate_staged_epoch(&expected).await.unwrap();
	quota.activate_validated_epoch(validated).await.unwrap();
	tx.cancel().await.unwrap();

	let restarted = ds.restart();
	let tx = restarted.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let quota = tx.quota_usage(ns, db);
	let meta = quota.meta().await.unwrap();
	assert_eq!(meta.state, QuotaUsageState::Rebuilding);
	assert_eq!(meta.active_epoch, Some(1));
	assert_eq!(meta.staged_epoch, Some(2));
	assert_eq!(quota.record_count(&TableName::from("ent_generation")).await.unwrap(), 0);
	tx.cancel().await.unwrap();
	let fenced = statement_error(&restarted, "CREATE ent_generation:fenced", &db_owner).await;
	assert!(fenced.contains("ledger is rebuilding"), "{fenced}");

	statement_result(&restarted, "REBUILD QUOTA ON DATABASE app", &ns_owner).await.unwrap();
	let tx = restarted.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let quota = tx.quota_usage(ns, db);
	let meta = quota.meta().await.unwrap();
	assert_eq!(meta.state, QuotaUsageState::Ready);
	assert_eq!(meta.staged_epoch, None);
	assert_eq!(quota.record_count(&TableName::from("ent_generation")).await.unwrap(), 0);
	tx.cancel().await.unwrap();
}

macro_rules! define_tests {
	($new_ds:ident) => {
		#[tokio::test]
		async fn quota_no_policy_metering_and_regex_contract() {
			super::quota_backend_contract::no_policy_metering_and_regex_contract($new_ds).await;
		}

		#[tokio::test(flavor = "multi_thread")]
		async fn quota_multi_node_mixed_contention_contract() {
			super::quota_backend_contract::multi_node_mixed_contention_contract($new_ds).await;
		}

		#[tokio::test]
		async fn quota_atomic_fault_and_commit_unknown_contract() {
			super::quota_backend_contract::atomic_fault_and_commit_unknown_contract($new_ds).await;
		}

		#[tokio::test]
		async fn quota_generation_and_rebuild_epoch_contract() {
			super::quota_backend_contract::generation_and_rebuild_epoch_contract($new_ds).await;
		}
	};
}

pub(crate) use define_tests;
