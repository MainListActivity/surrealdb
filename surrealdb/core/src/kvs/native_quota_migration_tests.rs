//! Synthetic in-memory stores only. These fixtures are not production migration certification.
use std::borrow::Cow;
use std::sync::Arc;

use super::*;
use crate::dbs::Session;
use crate::key::KVKey;
use crate::key::schema::{FieldKey, MigrationKey, StoreDefaultsKey};
use crate::kvs::version::{MigrationRecord, StoreDefaults};
use surrealdb_strand::TableName;

fn options() -> NativeQuotaMigrationOptions {
	NativeQuotaMigrationOptions {
		snapshot_reference: "synthetic:verified-before-4.0".to_owned(),
		snapshot_verified: true,
		offline: true,
	}
}

async fn empty() -> Arc<Datastore> {
	Datastore::builder().without_startup_transactions().build_with_path("memory").await.unwrap()
}

async fn execute(ds: &Datastore, sql: &str, session: &Session) {
	for result in ds.execute(sql, session, None).await.unwrap() {
		result.result.unwrap();
	}
}

async fn previous() -> Arc<Datastore> {
	let ds = empty().await;
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	tx.set_key(&VersionKey {}, &PREVIOUS_FORK_STORAGE_VERSION.into()).await.unwrap();
	tx.set_key(&ForkStorageKey {}, &previous_marker()).await.unwrap();
	tx.commit().await.unwrap();
	assert!(ds.feature_cell().level().is_legacy());

	execute(&ds, "DEFINE NAMESPACE tenant", &Session::owner()).await;
	let ns_owner = Session::owner().with_ns("tenant");
	execute(&ds, "DEFINE DATABASE app COMPATIBILITY 3", &ns_owner).await;
	execute(
		&ds,
		"DEFINE QUOTA ON DATABASE app \
		 RULE records FOR RECORD MATCH EXACT ent_user LIMIT 2 \
		 RULE tables FOR TABLE MATCH EXACT ent_user LIMIT 1",
		&ns_owner,
	)
	.await;
	execute(
		&ds,
		"DEFINE TABLE ent_user SCHEMAFULL; \
		 DEFINE FIELD name ON ent_user TYPE string; \
		 CREATE ent_user:one SET name = 'one'; \
		 CREATE ent_user:two SET name = 'two'",
		&Session::owner().with_ns("tenant").with_db("app"),
	)
	.await;
	ds
}

async fn dump(ds: &Datastore) -> Vec<(Vec<u8>, Vec<u8>)> {
	let tx = ds.transaction(TransactionType::Read).await.unwrap();
	let result = tx
		.scan_raw(RawRange::every_key_after(&Key::from(&b""[..])), u32::MAX, 0, None)
		.await
		.unwrap();
	tx.cancel().await.unwrap();
	result
}

async fn database(ds: &Datastore) -> (NamespaceId, DatabaseId) {
	let tx = ds.transaction(TransactionType::Read).await.unwrap();
	let database = tx.get_db_by_name("tenant", "app", None).await.unwrap().unwrap();
	tx.cancel().await.unwrap();
	(database.namespace_id, database.database_id)
}

#[tokio::test]
async fn previous_fork_migration_runs_upstream_work_and_preserves_policy_and_usage() {
	let ds = previous().await;
	let (ns, db) = database(&ds).await;
	let table = TableName::from("ent_user");
	let canonical = FieldKey::new(ns, db, Cow::Borrowed(&table), Cow::Borrowed("name"));
	let drifted = FieldKey::new(ns, db, Cow::Borrowed(&table), Cow::Borrowed("`name`"));
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	let field = tx.get_key_raw(&canonical, None).await.unwrap().unwrap();
	tx.set(drifted.encode_key().unwrap(), field.clone()).await.unwrap();
	tx.del_key(&canonical).await.unwrap();
	let protected = capture_databases(&tx).await.unwrap();
	tx.commit().await.unwrap();

	let before = ds.native_quota_storage_status().await.unwrap();
	assert_eq!(before.state, NativeQuotaStorageState::MigrationRequired);
	assert_eq!(before.storage_version, Some(PREVIOUS_FORK_STORAGE_VERSION));
	let unchanged = dump(&ds).await;
	assert!(ds.check_version().await.is_err());
	assert_eq!(dump(&ds).await, unchanged);

	let report = ds.migrate_native_quota_datastore(options()).await.unwrap();
	assert!(report.changed);
	assert_eq!((report.databases, report.tables, report.fields, report.records), (1, 1, 1, 2));
	assert_eq!(report.after.state, NativeQuotaStorageState::Ready);
	assert_eq!(report.after.storage_version, Some(MajorVersion::LATEST));
	assert!(migration::settled(&ds).await.unwrap());
	let tx = ds.transaction(TransactionType::Read).await.unwrap();
	check_protected_database(&tx, &protected[0]).await.unwrap();
	assert_eq!(tx.get_key_raw(&canonical, None).await.unwrap(), Some(field));
	assert!(tx.get_key_raw(&drifted, None).await.unwrap().is_none());
	assert!(tx.get_key(&NativeQuotaMigrationKey {}, None).await.unwrap().is_none());
	assert_eq!(tx.get_key(&StoreDefaultsKey {}, None).await.unwrap(), Some(StoreDefaults::LEGACY));
	assert!(tx.get_key(&FeatureLevelKey {}, None).await.unwrap().is_none());
	let quota = tx.quota_usage(ns, db);
	assert_eq!(quota.record_count(&table).await.unwrap(), 2);
	assert_eq!(quota.field_count(&table).await.unwrap(), 1);
	assert_eq!(quota.table_bucket_count(1, "tables").await.unwrap(), 1);
	assert!(quota.meta().await.unwrap().epoch_high_water > protected[0].epoch_high_water);
	tx.cancel().await.unwrap();

	let after = dump(&ds).await;
	let noop = ds.migrate_native_quota_datastore(options()).await.unwrap();
	assert!(!noop.changed);
	assert_eq!(dump(&ds).await, after);
	ds.check_version().await.unwrap();
	let session = Session::owner().with_ns("tenant").with_db("app");
	execute(&ds, "SELECT * FROM ent_user", &session).await;
	let responses =
		ds.execute("CREATE ent_user:three SET name = 'three'", &session, None).await.unwrap();
	assert!(responses.into_iter().any(|result| result.result.is_err()));
}

#[tokio::test]
async fn crashed_rebuild_resumes_with_same_snapshot_and_new_epoch() {
	let ds = previous().await;
	let journal = ds.begin_native_quota_migration(&options().snapshot_reference).await.unwrap();
	let (ns, db) = database(&ds).await;
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	let abandoned_epoch = tx.quota_usage(ns, db).begin_rebuild().await.unwrap();
	tx.commit().await.unwrap();
	let status = ds.native_quota_storage_status().await.unwrap();
	assert_eq!(status.state, NativeQuotaStorageState::Migrating);
	assert_eq!(status.storage_version, Some(PREVIOUS_FORK_STORAGE_VERSION));
	assert!(ds.check_version().await.is_err());
	let before = dump(&ds).await;
	let mut wrong = options();
	wrong.snapshot_reference = "synthetic:a-different-snapshot".to_owned();
	assert!(ds.migrate_native_quota_datastore(wrong).await.is_err());
	assert_eq!(dump(&ds).await, before);

	let report = ds.migrate_native_quota_datastore(options()).await.unwrap();
	assert_eq!(report.operation_id, journal.operation_id.to_string());
	assert!(report.after.ready);
	let tx = ds.transaction(TransactionType::Read).await.unwrap();
	let meta = tx.quota_usage(ns, db).meta().await.unwrap();
	assert!(meta.active_epoch.unwrap() > abandoned_epoch);
	assert_eq!(tx.quota_usage(ns, db).record_count(&"ent_user".into()).await.unwrap(), 2);
	tx.cancel().await.unwrap();
}

#[tokio::test]
async fn interrupted_upstream_completion_keeps_source_identity_fenced() {
	let ds = previous().await;
	let journal = ds.begin_native_quota_migration(&options().snapshot_reference).await.unwrap();
	ds.refresh_feature_level().await.unwrap();
	NATIVE_QUOTA_MIGRATION_PERMIT
		.scope(migration_permit(&ds, &journal).unwrap(), migration::run(&ds, false))
		.await
		.unwrap();
	assert_eq!(ds.storage_version().await.unwrap(), Some(StorageVersion::current()));
	assert!(ds.finish_native_quota_migration(&journal, &[]).await.is_err());
	assert_eq!(
		ds.native_quota_storage_status().await.unwrap().storage_version,
		Some(PREVIOUS_FORK_STORAGE_VERSION)
	);
	assert!(ds.check_version().await.is_err());
	let report = ds.migrate_native_quota_datastore(options()).await.unwrap();
	assert_eq!(report.operation_id, journal.operation_id.to_string());
	assert!(report.after.ready);
}

#[tokio::test]
async fn prerequisite_refusals_leave_all_bytes_unchanged() {
	let ds = previous().await;
	let before = dump(&ds).await;
	let mut cases = Vec::new();
	let mut no_snapshot = options();
	no_snapshot.snapshot_reference = "  ".to_owned();
	cases.push(no_snapshot);
	let mut unverified = options();
	unverified.snapshot_verified = false;
	cases.push(unverified);
	let mut online = options();
	online.offline = false;
	cases.push(online);
	for options in cases {
		assert!(ds.migrate_native_quota_datastore(options).await.is_err());
		assert_eq!(dump(&ds).await, before);
	}
	let omitted: NativeQuotaMigrationOptions =
		serde_json::from_str(r#"{"snapshot_reference":"synthetic:backup","offline":true}"#)
			.unwrap();
	assert!(!omitted.snapshot_verified);
}

#[tokio::test]
async fn orphan_and_unknown_journals_never_bootstrap_or_migrate() {
	for payload in [vec![255], b"not-a-migration-journal".to_vec()] {
		let ds = empty().await;
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		tx.set_key(&NativeQuotaMigrationKey {}, &payload).await.unwrap();
		tx.commit().await.unwrap();
		let before = dump(&ds).await;
		assert!(ds.native_quota_storage_status().await.is_err());
		assert!(ds.check_version().await.is_err());
		assert!(ds.migrate_native_quota_datastore(options()).await.is_err());
		assert_eq!(dump(&ds).await, before);
	}
	let ds = previous().await;
	let journal = ds.begin_native_quota_migration(&options().snapshot_reference).await.unwrap();
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	tx.del_key(&VersionKey {}).await.unwrap();
	tx.del_key(&ForkStorageKey {}).await.unwrap();
	assert_eq!(read_journal(&tx).await.unwrap(), Some(journal));
	tx.commit().await.unwrap();
	let before = dump(&ds).await;
	assert!(ds.native_quota_storage_status().await.is_err());
	assert!(ds.check_version().await.is_err());
	assert_eq!(dump(&ds).await, before);
}

#[tokio::test]
async fn unknown_markers_and_unjournaled_fences_are_refused_without_writes() {
	let mut foreign = previous_marker();
	foreign.fork_id = "another/fork".to_owned();
	let mut release = previous_marker();
	release.minimum_compatible_fork_release = "3.3.1-native-quota.1".to_owned();
	let mut revision = previous_marker();
	revision.quota_usage_format_revision = 2;
	let mut unfinished = previous_marker();
	unfinished.migration_state = ForkMigrationState::InProgress;
	for (version, marker) in [
		(PREVIOUS_FORK_STORAGE_VERSION, Some(foreign)),
		(PREVIOUS_FORK_STORAGE_VERSION, Some(release)),
		(PREVIOUS_FORK_STORAGE_VERSION, Some(revision)),
		(PREVIOUS_FORK_STORAGE_VERSION, Some(unfinished)),
		(PREVIOUS_FORK_STORAGE_VERSION, Some(ForkStorageFormat::current())),
		(MajorVersion::LATEST, Some(previous_marker())),
		(MajorVersion::LATEST + 1, Some(ForkStorageFormat::current())),
		(MajorVersion::LATEST, None),
	] {
		let ds = empty().await;
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		tx.set_key(&VersionKey {}, &version.into()).await.unwrap();
		if let Some(marker) = marker {
			tx.set_key(&ForkStorageKey {}, &marker).await.unwrap();
		}
		tx.commit().await.unwrap();
		let before = dump(&ds).await;
		assert!(ds.native_quota_storage_status().await.is_err());
		assert!(ds.migrate_native_quota_datastore(options()).await.is_err());
		assert_eq!(dump(&ds).await, before);
	}
}

#[tokio::test]
async fn unknown_upstream_ledger_is_refused_before_installing_fence() {
	let ds = previous().await;
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	tx.set_key(
		&MigrationKey::new(5),
		&MigrationRecord::applied("unreadable-format", (3, 4, 0), ds.id(), 0),
	)
	.await
	.unwrap();
	tx.commit().await.unwrap();
	let before = dump(&ds).await;
	assert!(ds.migrate_native_quota_datastore(options()).await.is_err());
	assert_eq!(dump(&ds).await, before);
}

#[tokio::test]
async fn resumed_policy_tampering_never_cleans_global_fence() {
	let ds = previous().await;
	let journal = ds.begin_native_quota_migration(&options().snapshot_reference).await.unwrap();
	let (ns, db) = database(&ds).await;
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	let mut policy = tx
		.get_key(
			&QuotaPolicyKey {
				ns,
				db,
			},
			None,
		)
		.await
		.unwrap()
		.unwrap();
	policy.generation += 1;
	tx.set_key(
		&QuotaPolicyKey {
			ns,
			db,
		},
		&policy,
	)
	.await
	.unwrap();
	tx.set_key(
		&QuotaGenerationKey {
			ns,
			db,
		},
		&policy.generation,
	)
	.await
	.unwrap();
	tx.commit().await.unwrap();
	assert!(ds.migrate_native_quota_datastore(options()).await.is_err());
	let status = ds.native_quota_storage_status().await.unwrap();
	assert_eq!(status.state, NativeQuotaStorageState::Migrating);
	assert_eq!(status.storage_version, Some(journal.source_version));
	assert!(ds.check_version().await.is_err());
}

#[tokio::test]
async fn upstream_four_store_is_not_mistaken_for_a_new_store() {
	let ds = empty().await;
	ds.check_version().await.unwrap();
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	tx.set_key(&VersionKey {}, &MajorVersion::upstream_latest()).await.unwrap();
	tx.del_key(&ForkStorageKey {}).await.unwrap();
	tx.commit().await.unwrap();
	let report = ds.migrate_native_quota_datastore(options()).await.unwrap();
	assert!(report.after.ready);
	assert_eq!(report.databases, 0);
	assert!(migration::settled(&ds).await.unwrap());
}

#[tokio::test]
async fn migration_permit_is_scoped_to_its_datastore_and_future() {
	let ds = previous().await;
	let other = empty().await;
	let journal = ds.begin_native_quota_migration(&options().snapshot_reference).await.unwrap();
	let permit = migration_permit(&ds, &journal).unwrap();
	let result = NATIVE_QUOTA_MIGRATION_PERMIT
		.scope(permit.clone(), async {
			assert!(migration_transaction(&other, TransactionType::Write).await.is_err());
			let ordinary = ds.transaction(TransactionType::Write).await.unwrap();
			assert!(
				ordinary
					.quota_usage(
						NamespaceId(journal.databases[0].ns),
						DatabaseId(journal.databases[0].db)
					)
					.ensure_writable_for_update()
					.await
					.is_err()
			);
			ordinary.cancel().await.unwrap();
			let migrated = migration_transaction(&ds, TransactionType::Write).await.unwrap();
			migrated.cancel().await.unwrap();
			let spawned =
				tokio::spawn(async { NATIVE_QUOTA_MIGRATION_PERMIT.try_with(|_| ()).is_err() })
					.await
					.unwrap();
			assert!(spawned);
		})
		.await;
	assert_eq!(result, ());
	assert!(NATIVE_QUOTA_MIGRATION_PERMIT.try_with(|_| ()).is_err());
	let pending = NATIVE_QUOTA_MIGRATION_PERMIT.scope(permit, std::future::pending::<()>());
	tokio::pin!(pending);
	tokio::select! {
		_ = &mut pending => unreachable!(),
		_ = tokio::task::yield_now() => {}
	}
	drop(pending);
	assert!(NATIVE_QUOTA_MIGRATION_PERMIT.try_with(|_| ()).is_err());
}
