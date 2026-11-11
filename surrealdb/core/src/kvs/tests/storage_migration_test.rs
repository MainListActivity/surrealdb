use crate::catalog::QuotaUsageState;
use crate::catalog::providers::DatabaseProvider;
use crate::dbs::Session;
use crate::key::schema::ForkStorageKey;
use crate::key::schema::QuotaUsageMetaKey;
use crate::key::schema::VersionKey;
use crate::key::schema::{FieldKey, NativeQuotaMigrationKey};
use crate::kvs::{Datastore, NativeQuotaMigrationOptions, NativeQuotaStorageState};
use std::borrow::Cow;
use surrealdb_datastore::fork_storage::{ForkMigrationState, ForkStorageFormat};
use surrealdb_datastore::version::MajorVersion;
use surrealdb_kvs::TransactionType;
use surrealdb_kvs::key::KVKey;

async fn legacy_datastore() -> std::sync::Arc<Datastore> {
	synthetic_datastore(false).await
}

// In-memory fixtures only; the snapshot attestations below describe these
// disposable test inputs, never a production backup.
async fn synthetic_datastore(previous_fork: bool) -> std::sync::Arc<Datastore> {
	let builder = if previous_fork {
		Datastore::builder().without_startup_transactions()
	} else {
		Datastore::builder().without_maintenance_tasks()
	};
	let ds = builder.build_with_path("memory").await.unwrap();
	if previous_fork {
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		let mut marker = ForkStorageFormat::current();
		marker.upstream_storage_major = 3;
		marker.minimum_compatible_fork_release = "3.3.0-native-quota.1".to_owned();
		tx.set_key(&VersionKey {}, &MajorVersion::from(0x8003)).await.unwrap();
		tx.set_key(&ForkStorageKey {}, &marker).await.unwrap();
		tx.commit().await.unwrap();
	}
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

	if previous_fork {
		return ds;
	}
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	let db = tx.get_db_by_name("tenant", "app", None).await.unwrap().unwrap();
	tx.del_key(&QuotaUsageMetaKey {
		ns: db.namespace_id,
		db: db.database_id,
	})
	.await
	.unwrap();
	tx.del_key(&ForkStorageKey {}).await.unwrap();
	tx.set_key(&VersionKey {}, &MajorVersion::upstream_latest()).await.unwrap();
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
			snapshot_verified: true,
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

	let tx = ds.transaction(TransactionType::Read).await.unwrap();
	let db = tx.get_db_by_name("tenant", "app", None).await.unwrap().unwrap();
	let quota = tx.quota_usage(db.namespace_id, db.database_id);
	assert_eq!(quota.meta().await.unwrap().state, QuotaUsageState::Ready);
	assert_eq!(quota.field_count(&"ent_user".into()).await.unwrap(), 1);
	assert_eq!(quota.record_count(&"ent_user".into()).await.unwrap(), 2);
	tx.cancel().await.unwrap();

	let noop = ds
		.migrate_native_quota_datastore(NativeQuotaMigrationOptions {
			snapshot_reference: "snapshot:test-before-native-quota".to_owned(),
			snapshot_verified: true,
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
	let ds = synthetic_datastore(true).await;
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	let db = tx.get_db_by_name("tenant", "app", None).await.unwrap().unwrap();
	let (ns, db) = (db.namespace_id, db.database_id);
	let table = surrealdb_strand::TableName::from("ent_user");
	let key = FieldKey::new(ns, db, Cow::Borrowed(&table), Cow::Borrowed("name"));
	let field = tx.get_key_raw(&key, None).await.unwrap().unwrap();
	// Upstream field migrations skip undecodable definitions. The quota scan
	// must then fail after the migrator has durably installed its own journal.
	tx.set(key.encode_key().unwrap(), vec![255]).await.unwrap();
	tx.commit().await.unwrap();
	let options = NativeQuotaMigrationOptions {
		snapshot_reference: "synthetic:resume-before-native-quota".to_owned(),
		snapshot_verified: true,
		offline: true,
	};
	ds.migrate_native_quota_datastore(options.clone()).await.unwrap_err();
	let status = ds.native_quota_storage_status().await.unwrap();
	assert_eq!(status.state, NativeQuotaStorageState::Migrating);
	assert_eq!(status.storage_version, Some(0x8003));
	assert!(ds.check_version().await.is_err());
	let tx = ds.transaction(TransactionType::Read).await.unwrap();
	let journal = tx.get_key(&NativeQuotaMigrationKey {}, None).await.unwrap().unwrap();
	assert_eq!(tx.get_key(&VersionKey {}, None).await.unwrap(), Some(MajorVersion::from(0x8003)));
	let marker = tx.get_key(&ForkStorageKey {}, None).await.unwrap().unwrap();
	assert_eq!(marker.migration_state, ForkMigrationState::InProgress);
	tx.cancel().await.unwrap();

	let mut wrong_snapshot = options.clone();
	wrong_snapshot.snapshot_reference = "synthetic:different-backup".to_owned();
	let error = ds.migrate_native_quota_datastore(wrong_snapshot).await.unwrap_err();
	assert!(error.to_string().contains("snapshot"), "{error}");
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	assert_eq!(tx.get_key(&NativeQuotaMigrationKey {}, None).await.unwrap(), Some(journal.clone()));
	// A journal does not authorize ordinary writes, even when restoring known test bytes.
	assert!(tx.set(key.encode_key().unwrap(), field.clone()).await.is_err());
	tx.cancel().await.unwrap();

	let other = Datastore::builder()
		.without_startup_transactions()
		.build_with_path("memory")
		.await
		.unwrap();
	let wrong_store = other.transaction(TransactionType::Write).await.unwrap();
	assert!(
		wrong_store
			.admit_native_quota_catalog_migration(MajorVersion::from(0x8003), &marker, &journal,)
			.await
			.is_err()
	);
	wrong_store.cancel().await.unwrap();
	assert_eq!(
		other.native_quota_storage_status().await.unwrap().state,
		NativeQuotaStorageState::Empty
	);

	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	assert!(
		tx.admit_native_quota_catalog_migration(
			MajorVersion::from(0x8003),
			&marker,
			b"a different journal",
		)
		.await
		.is_err()
	);
	tx.cancel().await.unwrap();

	// Explicit catalog-only maintenance reasserts the exact committed recovery fence.
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	tx.admit_native_quota_catalog_migration(MajorVersion::from(0x8003), &marker, &journal)
		.await
		.unwrap();
	tx.set(key.encode_key().unwrap(), field).await.unwrap();
	tx.commit().await.unwrap();

	let report = ds.migrate_native_quota_datastore(options.clone()).await.unwrap();
	assert!(report.changed);
	assert_eq!((report.databases, report.tables, report.fields, report.records), (1, 1, 1, 2));
	assert_eq!(report.after.state, NativeQuotaStorageState::Ready);
	assert_eq!(report.after.storage_version, Some(MajorVersion::LATEST));
	ds.check_version().await.unwrap();
	let tx = ds.transaction(TransactionType::Read).await.unwrap();
	assert!(tx.get_key(&NativeQuotaMigrationKey {}, None).await.unwrap().is_none());
	let quota = tx.quota_usage(ns, db);
	assert_eq!(quota.meta().await.unwrap().state, QuotaUsageState::Ready);
	assert_eq!(quota.record_count(&table).await.unwrap(), 2);
	assert_eq!(quota.field_count(&table).await.unwrap(), 1);
	tx.cancel().await.unwrap();
	let noop = ds.migrate_native_quota_datastore(options).await.unwrap();
	assert!(!noop.changed);
	assert_eq!(noop.after.state, NativeQuotaStorageState::Ready);
}

#[tokio::test]
async fn fabricated_target_fence_without_journal_is_not_resumable() {
	let ds = legacy_datastore().await;
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	let mut marker = ForkStorageFormat::current();
	marker.migration_state = ForkMigrationState::InProgress;
	tx.set_key(&VersionKey {}, &MajorVersion::latest()).await.unwrap();
	tx.set_key(&ForkStorageKey {}, &marker).await.unwrap();
	tx.commit().await.unwrap();
	assert!(ds.native_quota_storage_status().await.is_err());
	assert!(ds.check_version().await.is_err());
	let error = ds
		.migrate_native_quota_datastore(NativeQuotaMigrationOptions {
			snapshot_reference: "synthetic:untrusted-fence".to_owned(),
			snapshot_verified: true,
			offline: true,
		})
		.await
		.unwrap_err();
	assert!(error.to_string().contains("journal"), "{error}");
}

#[tokio::test]
async fn migration_requires_snapshot_offline_confirmation_and_known_format() {
	let ds = legacy_datastore().await;
	let before = ds.native_quota_storage_status().await.unwrap();
	let missing: NativeQuotaMigrationOptions = serde_json::from_value(serde_json::json!({
		"snapshot_reference": "snapshot:unverified-synthetic",
		"offline": true
	}))
	.unwrap();
	assert!(!missing.snapshot_verified, "omitted verification must default to refusal");
	for options in [
		missing,
		NativeQuotaMigrationOptions {
			snapshot_reference: "snapshot:unverified-synthetic".to_owned(),
			snapshot_verified: false,
			offline: true,
		},
	] {
		let error = ds.migrate_native_quota_datastore(options).await.unwrap_err();
		assert!(error.to_string().contains("verified"), "{error}");
		assert_eq!(
			ds.native_quota_storage_status().await.unwrap(),
			before,
			"unverified snapshots must not install a fence or change source identity"
		);
	}
	assert!(
		ds.migrate_native_quota_datastore(NativeQuotaMigrationOptions {
			snapshot_reference: String::new(),
			snapshot_verified: false,
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
			snapshot_verified: true,
			offline: false,
		})
		.await
		.unwrap_err()
		.to_string()
		.contains("offline")
	);

	let ds =
		Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap();
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	let mut marker = ForkStorageFormat::current();
	marker.quota_usage_format_revision += 1;
	tx.set_key(&VersionKey {}, &MajorVersion::latest()).await.unwrap();
	tx.set_key(&ForkStorageKey {}, &marker).await.unwrap();
	tx.commit().await.unwrap();
	assert!(ds.native_quota_storage_status().await.is_err());
}
