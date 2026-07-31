use crate::catalog::QuotaUsageState;
use crate::catalog::providers::DatabaseProvider;
use crate::dbs::Session;
use crate::key::database::qub::QuotaTableBucket;
use crate::key::database::qur::QuotaRecordUsage;
use crate::kvs::quota::QuotaUsageSnapshot;
use crate::kvs::{Datastore, KVKey, LockType, TransactionType};
use crate::types::PublicValue;
use crate::val::TableName;

async fn setup() -> (Datastore, crate::catalog::NamespaceId, crate::catalog::DatabaseId) {
	let ds = Datastore::new("memory").await.unwrap();
	let root = Session::owner();
	let namespace_owner = Session::owner().with_ns("tenant");
	ds.execute("DEFINE NAMESPACE tenant", &root, None).await.unwrap();
	ds.execute("DEFINE DATABASE app", &namespace_owner, None).await.unwrap();

	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let db = tx.get_db_by_name("tenant", "app", None).await.unwrap().unwrap();
	let ids = (db.namespace_id, db.database_id);
	tx.cancel().await.unwrap();
	(ds, ids.0, ids.1)
}

async fn statement_error(ds: &Datastore, sql: &str, session: &Session) -> String {
	match ds.execute(sql, session, None).await {
		Ok(mut responses) => responses.remove(responses.len() - 1).result.unwrap_err().to_string(),
		Err(error) => error.to_string(),
	}
}

#[tokio::test]
async fn new_database_starts_with_ready_empty_usage_ledger() {
	let (ds, ns, db) = setup().await;
	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let quota = tx.quota_usage(ns, db);
	let meta = quota.meta().await.unwrap();

	assert_eq!(meta.state, QuotaUsageState::Ready);
	assert_eq!(meta.active_epoch, Some(1));
	assert_eq!(meta.staged_epoch, None);
	assert_eq!(quota.record_count(&TableName::from("user")).await.unwrap(), 0);
	assert_eq!(quota.field_count(&TableName::from("user")).await.unwrap(), 0);
	assert_eq!(quota.table_bucket_count(7, "ent-tables").await.unwrap(), 0);
	tx.cancel().await.unwrap();
}

#[tokio::test]
async fn relation_redefinition_keeps_implicit_field_usage_consistent() {
	let (ds, ns, db) = setup().await;
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	let table = TableName::from("likes");

	ds.execute("DEFINE TABLE likes TYPE RELATION IN person OUT person", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	assert_eq!(tx.quota_usage(ns, db).field_count(&table).await.unwrap(), 2);
	tx.cancel().await.unwrap();

	ds.execute("REMOVE TABLE likes", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	ds.execute(
		"DEFINE TABLE likes TYPE RELATION IN person OUT person | thing",
		&database_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	ds.execute("REMOVE FIELD out ON TABLE likes", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();

	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	assert_eq!(tx.quota_usage(ns, db).field_count(&table).await.unwrap(), 1);
	tx.cancel().await.unwrap();
}

#[tokio::test]
async fn usage_counter_and_business_kv_share_commit_cancel_and_savepoint_boundaries() {
	let (ds, ns, db) = setup().await;
	let table = TableName::from("user");
	let committed_key = b"quota-test/committed".to_vec();
	let cancelled_key = b"quota-test/cancelled".to_vec();
	let savepoint_key = b"quota-test/savepoint".to_vec();

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	tx.set(&committed_key, &b"yes".to_vec()).await.unwrap();
	tx.quota_usage(ns, db).increment_record_count(&table, 1).await.unwrap();
	tx.commit().await.unwrap();

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	tx.set(&cancelled_key, &b"no".to_vec()).await.unwrap();
	tx.quota_usage(ns, db).increment_record_count(&table, 2).await.unwrap();
	tx.cancel().await.unwrap();

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(&savepoint_key, &b"no".to_vec()).await.unwrap();
	tx.quota_usage(ns, db).increment_record_count(&table, 4).await.unwrap();
	tx.rollback_to_save_point().await.unwrap();
	tx.commit().await.unwrap();

	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	assert_eq!(tx.get(&committed_key, None).await.unwrap(), Some(b"yes".to_vec()));
	assert_eq!(tx.get(&cancelled_key, None).await.unwrap(), None);
	assert_eq!(tx.get(&savepoint_key, None).await.unwrap(), None);
	assert_eq!(tx.quota_usage(ns, db).record_count(&table).await.unwrap(), 1);
	tx.cancel().await.unwrap();
}

#[tokio::test]
async fn staged_rebuild_remains_fenced_until_validated_epoch_switch_commits() {
	let (ds, ns, db) = setup().await;
	let table = TableName::from("user");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute("DEFINE TABLE user", &database_owner, None).await.unwrap().remove(0).result.unwrap();

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let quota = tx.quota_usage(ns, db);
	quota.increment_record_count(&table, 3).await.unwrap();
	quota.increment_field_count(&table, 4).await.unwrap();
	quota.increment_table_bucket_count(7, "ent-tables", 1).await.unwrap();
	tx.commit().await.unwrap();

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let quota = tx.quota_usage(ns, db);
	let staged_epoch = quota.begin_rebuild().await.unwrap();
	assert_eq!(staged_epoch, 2);
	quota.set_staged_record_count(&table, 9).await.unwrap();
	quota.set_staged_field_count(&table, 10).await.unwrap();
	quota.set_staged_table_bucket_count(7, "ent-tables", 2).await.unwrap();
	tx.commit().await.unwrap();

	let error = statement_error(&ds, "CREATE user:one SET name = 'blocked'", &database_owner).await;
	assert!(error.contains("ledger is rebuilding"), "{error}");
	ds.execute("SELECT * FROM user", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	ds.execute("INFO FOR DB", &database_owner, None).await.unwrap().remove(0).result.unwrap();

	// A failed activation transaction must leave the active epoch and fence unchanged.
	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let quota = tx.quota_usage(ns, db);
	let mut expected = QuotaUsageSnapshot::default();
	expected.set_record_count(&table, 9);
	expected.set_field_count(&table, 10);
	expected.set_table_bucket_count(7, "ent-tables", 2);
	let validated = quota.validate_staged_epoch(&expected).await.unwrap();
	quota.activate_validated_epoch(validated).await.unwrap();
	tx.cancel().await.unwrap();

	let ds = ds.restart();
	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let quota = tx.quota_usage(ns, db);
	let meta = quota.meta().await.unwrap();
	assert_eq!(meta.state, QuotaUsageState::Rebuilding);
	assert_eq!(meta.active_epoch, Some(1));
	assert_eq!(meta.staged_epoch, Some(2));
	assert_eq!(quota.record_count(&table).await.unwrap(), 3);
	tx.cancel().await.unwrap();
	let error =
		statement_error(&ds, "CREATE user:one SET name = 'still blocked'", &database_owner).await;
	assert!(error.contains("ledger is rebuilding"), "{error}");

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let quota = tx.quota_usage(ns, db);
	let validated = quota.validate_staged_epoch(&expected).await.unwrap();
	quota.activate_validated_epoch(validated).await.unwrap();
	quota.clear_inactive_epoch(1).await.unwrap();
	tx.commit().await.unwrap();

	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let quota = tx.quota_usage(ns, db);
	let meta = quota.meta().await.unwrap();
	assert_eq!(meta.state, QuotaUsageState::Ready);
	assert_eq!(meta.active_epoch, Some(2));
	assert_eq!(meta.staged_epoch, None);
	assert_eq!(quota.record_count(&table).await.unwrap(), 9);
	assert_eq!(quota.field_count(&table).await.unwrap(), 10);
	assert_eq!(quota.table_bucket_count(7, "ent-tables").await.unwrap(), 2);
	tx.cancel().await.unwrap();
	ds.execute("CREATE user:one SET name = 'allowed'", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
}

#[tokio::test]
async fn external_write_maintenance_fences_before_raw_install_until_rebuild() {
	let (ds, ns, db) = setup().await;
	let database_owner = Session::owner().with_ns("tenant").with_db("app");

	// A quota-aware restore/copy tool commits the fence before installing any
	// external bytes. The raw install itself intentionally bypasses the facade.
	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	tx.quota_usage(ns, db).begin_external_write_maintenance().await.unwrap();
	tx.commit().await.unwrap();
	let restored_key = b"raw-restore/business-key".to_vec();
	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	tx.set(&restored_key, &b"restored".to_vec()).await.unwrap();
	tx.commit().await.unwrap();

	let error = statement_error(&ds, "CREATE user:one", &database_owner).await;
	assert!(error.contains("ledger is uninitialized"), "{error}");
	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let meta = tx.quota_usage(ns, db).meta().await.unwrap();
	assert_eq!(meta.state, QuotaUsageState::Uninitialized);
	assert_eq!(meta.active_epoch, None);
	assert_eq!(tx.get(&restored_key, None).await.unwrap(), Some(b"restored".to_vec()));
	tx.cancel().await.unwrap();
}

#[tokio::test]
async fn corrupt_usage_state_fences_writes_but_allows_rebuild_recovery() {
	let (ds, ns, db) = setup().await;
	let database_owner = Session::owner().with_ns("tenant").with_db("app");

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	tx.quota_usage(ns, db).mark_corrupt().await.unwrap();
	tx.commit().await.unwrap();

	let error = statement_error(&ds, "CREATE user:one", &database_owner).await;
	assert!(error.contains("ledger is corrupt"), "{error}");
	ds.execute("INFO FOR DB", &database_owner, None).await.unwrap().remove(0).result.unwrap();

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let staged = tx.quota_usage(ns, db).begin_rebuild().await.unwrap();
	assert_eq!(staged, 2);
	tx.commit().await.unwrap();
}

#[tokio::test]
async fn rebuilding_database_rejects_parent_scoped_policy_mutation() {
	let (ds, ns, db) = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE records FOR RECORD MATCH EXACT user LIMIT 10",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	tx.quota_usage(ns, db).begin_rebuild().await.unwrap();
	tx.commit().await.unwrap();

	let error =
		statement_error(&ds, "REMOVE QUOTA ON DATABASE app EXPECT GENERATION 1", &namespace_owner)
			.await;
	assert!(error.contains("ledger is rebuilding"), "{error}");

	let error = statement_error(
		&ds,
		"DEFINE DATABASE OVERWRITE app COMMENT 'blocked during rebuild'",
		&namespace_owner,
	)
	.await;
	assert!(error.contains("ledger is rebuilding"), "{error}");
}

#[tokio::test]
async fn counter_release_is_atomic_and_never_underflows() {
	let (ds, ns, db) = setup().await;
	let table = TableName::from("ent_user");

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let quota = tx.quota_usage(ns, db);
	quota.increment_record_count(&table, 5).await.unwrap();
	quota.increment_field_count(&table, 4).await.unwrap();
	quota.increment_table_bucket_count(1, "ent", 1).await.unwrap();
	quota.decrement_record_count(&table, 2).await.unwrap();
	quota.decrement_field_count(&table, 1).await.unwrap();
	quota.decrement_table_bucket_count(1, "ent", 1).await.unwrap();
	tx.commit().await.unwrap();

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let error = tx.quota_usage(ns, db).decrement_record_count(&table, 4).await.unwrap_err();
	assert!(error.to_string().contains("counter underflow"), "{error}");
	tx.cancel().await.unwrap();

	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let quota = tx.quota_usage(ns, db);
	assert_eq!(quota.record_count(&table).await.unwrap(), 3);
	assert_eq!(quota.field_count(&table).await.unwrap(), 3);
	assert_eq!(quota.table_bucket_count(1, "ent").await.unwrap(), 0);
	assert_eq!(
		tx.get(&QuotaTableBucket::new(ns, db, 1, 1, "ent"), None).await.unwrap(),
		None,
		"zero-valued counters must be physically removed"
	);
	tx.cancel().await.unwrap();
}

#[tokio::test]
async fn competing_counter_updates_use_backend_conditional_writes() {
	let (ds, ns, db) = setup().await;
	let table = TableName::from("user");
	let first = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let second = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();

	first.quota_usage(ns, db).increment_record_count(&table, 1).await.unwrap();
	second.quota_usage(ns, db).increment_record_count(&table, 1).await.unwrap();
	let first_result = first.commit().await;
	let second_result = second.commit().await;
	assert_ne!(
		first_result.is_ok(),
		second_result.is_ok(),
		"exactly one competing conditional counter update must commit"
	);

	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	assert_eq!(tx.quota_usage(ns, db).record_count(&table).await.unwrap(), 1);
	tx.cancel().await.unwrap();
}

#[tokio::test]
async fn active_counter_writer_conflicts_with_concurrent_rebuild_fence() {
	let (ds, ns, db) = setup().await;
	let table = TableName::from("user");
	let writer = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let maintenance = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();

	writer.quota_usage(ns, db).increment_record_count(&table, 1).await.unwrap();
	assert_eq!(maintenance.quota_usage(ns, db).begin_rebuild().await.unwrap(), 2);
	maintenance.commit().await.unwrap();
	assert!(
		writer.commit().await.is_err(),
		"a counter writer that observed Ready must conflict with the committed rebuild fence"
	);

	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let quota = tx.quota_usage(ns, db);
	assert_eq!(quota.meta().await.unwrap().state, QuotaUsageState::Rebuilding);
	assert_eq!(quota.record_count(&table).await.unwrap(), 0);
	tx.cancel().await.unwrap();
}

#[tokio::test]
async fn staged_counter_writer_conflicts_with_concurrent_epoch_activation() {
	let (ds, ns, db) = setup().await;
	let table = TableName::from("user");
	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	assert_eq!(tx.quota_usage(ns, db).begin_rebuild().await.unwrap(), 2);
	tx.commit().await.unwrap();

	let writer = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let activation = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	writer.quota_usage(ns, db).set_staged_record_count(&table, 1).await.unwrap();
	let validated = activation
		.quota_usage(ns, db)
		.validate_staged_epoch(&QuotaUsageSnapshot::default())
		.await
		.unwrap();
	activation.quota_usage(ns, db).activate_validated_epoch(validated).await.unwrap();
	activation.commit().await.unwrap();
	assert!(
		writer.commit().await.is_err(),
		"a staged writer must not mutate an epoch after its validated activation commits"
	);

	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let quota = tx.quota_usage(ns, db);
	assert_eq!(quota.meta().await.unwrap().state, QuotaUsageState::Ready);
	assert_eq!(quota.record_count(&table).await.unwrap(), 0);
	tx.cancel().await.unwrap();
}

#[tokio::test]
async fn dynamic_use_target_cannot_write_through_a_maintenance_fence() {
	let (ds, ns, db) = setup().await;
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute(
		"DEFINE TABLE audit; \
		DEFINE FUNCTION fn::writes_and_returns_db() { \
			CREATE audit:one; \
			RETURN 'app'; \
		}",
		&database_owner,
		None,
	)
	.await
	.unwrap()
	.remove(1)
	.result
	.unwrap();

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	tx.quota_usage(ns, db).begin_rebuild().await.unwrap();
	tx.commit().await.unwrap();

	let error = statement_error(&ds, "USE DB fn::writes_and_returns_db()", &database_owner).await;
	assert!(error.contains("ledger is rebuilding"), "{error}");
	let records = ds
		.execute("SELECT * FROM audit", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	assert_eq!(records, PublicValue::Array(surrealdb_types::array![]));
}

#[tokio::test]
async fn malformed_staged_epoch_never_activates_or_opens_writes() {
	let (ds, ns, db) = setup().await;
	let table = TableName::from("user");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let staged = tx.quota_usage(ns, db).begin_rebuild().await.unwrap();
	tx.commit().await.unwrap();

	// Model a crashed/offline rebuild writer that left an undecodable staged counter.
	let malformed_key = QuotaRecordUsage::new(ns, db, staged, &table).encode_key().unwrap();
	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	tx.set(&malformed_key, &vec![1]).await.unwrap();
	tx.commit().await.unwrap();

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let error = tx
		.quota_usage(ns, db)
		.validate_staged_epoch(&QuotaUsageSnapshot::default())
		.await
		.unwrap_err();
	assert!(error.to_string().contains("invalid counter"), "{error}");
	tx.cancel().await.unwrap();

	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let meta = tx.quota_usage(ns, db).meta().await.unwrap();
	assert_eq!(meta.state, QuotaUsageState::Rebuilding);
	assert_eq!(meta.active_epoch, Some(1));
	assert_eq!(meta.staged_epoch, Some(staged));
	tx.cancel().await.unwrap();
	let error = statement_error(&ds, "CREATE user:one", &database_owner).await;
	assert!(error.contains("ledger is rebuilding"), "{error}");
}

#[tokio::test]
async fn incomplete_staged_epoch_never_activates() {
	let (ds, ns, db) = setup().await;
	let table = TableName::from("user");

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let quota = tx.quota_usage(ns, db);
	quota.begin_rebuild().await.unwrap();
	quota.set_staged_record_count(&table, 9).await.unwrap();
	tx.commit().await.unwrap();

	let mut expected = QuotaUsageSnapshot::default();
	expected.set_record_count(&table, 9);
	expected.set_field_count(&table, 4);
	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let error = tx.quota_usage(ns, db).validate_staged_epoch(&expected).await.unwrap_err();
	assert!(error.to_string().contains("does not match trusted rebuild snapshot"), "{error}");
	tx.cancel().await.unwrap();

	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let meta = tx.quota_usage(ns, db).meta().await.unwrap();
	assert_eq!(meta.state, QuotaUsageState::Rebuilding);
	assert_eq!(meta.active_epoch, Some(1));
	tx.cancel().await.unwrap();
}
