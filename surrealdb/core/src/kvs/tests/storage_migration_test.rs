use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{ForkMigrationState, ForkStorageFormat, QuotaUsageState};
use crate::dbs::Session;
use crate::key::database::qm::Qm;
use crate::key::format::StorageFormat;
use crate::key::version::Version;
use crate::kvs::version::MajorVersion;
use crate::kvs::{
	Datastore, LockType, NativeQuotaMigrationOptions, NativeQuotaStorageState, TransactionType,
};

async fn legacy_datastore() -> Datastore {
	let ds = Datastore::new("memory").await.unwrap();
	let root = Session::owner();
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute("DEFINE NAMESPACE tenant", &root, None).await.unwrap();
	ds.execute("DEFINE DATABASE app", &namespace_owner, None).await.unwrap();
	ds.execute(
		"
		DEFINE TABLE ent_user SCHEMAFULL;
		DEFINE FIELD name ON ent_user TYPE string;
		CREATE ent_user:one SET name = 'one';
		CREATE ent_user:two SET name = 'two';
		",
		&database_owner,
		None,
	)
	.await
	.unwrap()
	.into_iter()
	.for_each(|result| {
		result.result.unwrap();
	});

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let db = tx.get_db_by_name("tenant", "app", None).await.unwrap().unwrap();
	tx.del(&Qm::new(db.namespace_id, db.database_id)).await.unwrap();
	tx.replace(&Version::new(), &MajorVersion::upstream_latest()).await.unwrap();
	tx.commit().await.unwrap();
	ds
}

#[tokio::test]
async fn explicit_migration_backfills_usage_and_is_idempotent() {
	let ds = legacy_datastore().await;
	let before = ds.native_quota_storage_status().await.unwrap();
	assert_eq!(before.state, NativeQuotaStorageState::MigrationRequired);
	assert!(before.migration_required);
	assert!(!before.ready);

	let report = ds
		.migrate_native_quota_datastore(NativeQuotaMigrationOptions {
			snapshot_reference: "snapshot:test-before-native-quota".to_owned(),
			offline: true,
		})
		.await
		.unwrap();
	assert!(report.changed);
	assert_eq!(report.databases, 1);
	assert_eq!(report.tables, 1);
	assert_eq!(report.fields, 1);
	assert_eq!(report.records, 2);
	assert_eq!(report.after.state, NativeQuotaStorageState::Ready);
	assert!(report.after.ready);
	ds.check_version().await.unwrap();

	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let db = tx.get_db_by_name("tenant", "app", None).await.unwrap().unwrap();
	let quota = tx.quota_usage(db.namespace_id, db.database_id);
	assert_eq!(quota.meta().await.unwrap().state, QuotaUsageState::Ready);
	assert_eq!(quota.field_count(&"ent_user".into()).await.unwrap(), 1);
	assert_eq!(quota.record_count(&"ent_user".into()).await.unwrap(), 2);
	tx.cancel().await.unwrap();

	let noop = ds
		.migrate_native_quota_datastore(NativeQuotaMigrationOptions {
			snapshot_reference: "snapshot:test-before-native-quota".to_owned(),
			offline: true,
		})
		.await
		.unwrap();
	assert!(!noop.changed);
	assert_eq!(noop.databases, 0);
	assert_eq!(noop.after.state, NativeQuotaStorageState::Ready);
}

#[tokio::test]
async fn interrupted_global_fence_is_resumable_and_normal_startup_refuses_it() {
	let ds = legacy_datastore().await;
	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let mut marker = ForkStorageFormat::current();
	marker.migration_state = ForkMigrationState::InProgress;
	tx.replace(&Version::new(), &MajorVersion::latest()).await.unwrap();
	tx.replace(&StorageFormat::new(), &marker).await.unwrap();
	tx.commit().await.unwrap();

	let status = ds.native_quota_storage_status().await.unwrap();
	assert_eq!(status.state, NativeQuotaStorageState::Migrating);
	assert!(ds.check_version().await.is_err());

	let report = ds
		.migrate_native_quota_datastore(NativeQuotaMigrationOptions {
			snapshot_reference: "snapshot:resume".to_owned(),
			offline: true,
		})
		.await
		.unwrap();
	assert!(report.changed);
	assert_eq!(report.after.state, NativeQuotaStorageState::Ready);
	ds.check_version().await.unwrap();
}

#[tokio::test]
async fn migration_requires_snapshot_offline_confirmation_and_known_format() {
	let ds = legacy_datastore().await;
	assert!(
		ds.migrate_native_quota_datastore(NativeQuotaMigrationOptions {
			snapshot_reference: String::new(),
			offline: true,
		})
		.await
		.unwrap_err()
		.to_string()
		.contains("snapshot")
	);
	assert!(
		ds.migrate_native_quota_datastore(NativeQuotaMigrationOptions {
			snapshot_reference: "snapshot:test".to_owned(),
			offline: false,
		})
		.await
		.unwrap_err()
		.to_string()
		.contains("offline")
	);

	let ds = Datastore::new("memory").await.unwrap();
	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let mut marker = ForkStorageFormat::current();
	marker.quota_usage_format_revision += 1;
	tx.replace(&Version::new(), &MajorVersion::latest()).await.unwrap();
	tx.replace(&StorageFormat::new(), &marker).await.unwrap();
	tx.commit().await.unwrap();
	assert!(ds.native_quota_storage_status().await.is_err());
}
