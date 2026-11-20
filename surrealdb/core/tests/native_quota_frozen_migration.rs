//! Frozen upstream 3.3 bytes in a disposable memory store, not a deployed-fork restore test.
#![cfg(feature = "kv-mem")]

use std::borrow::Cow;

use surrealdb_catalog::{
	DatabaseId, NamespaceId, QuotaPolicyDefinition, QuotaUsageMeta, QuotaUsageState,
};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::{Datastore, NativeQuotaMigrationOptions, NativeQuotaStorageState};
use surrealdb_datastore::fork_storage::ForkStorageFormat;
use surrealdb_datastore::key::schema::{
	DatabaseKey, FeatureLevelKey, FieldKey, ForkStorageKey, NamespaceKey, NativeQuotaMigrationKey,
	QuotaGenerationKey, QuotaPolicyKey, QuotaUsageMetaKey, RecordKey, StorageVersionKey,
	StoreDefaultsKey, TableKey, VersionKey,
};
use surrealdb_datastore::version::{MajorVersion, StorageVersion, StoreDefaults};
use surrealdb_expr::val::RecordIdKey;
use surrealdb_kvs::TransactionType;
use surrealdb_kvs::key::KVKey;
use surrealdb_strand::{Strand, TableName};
use surrealdb_types::ToSql;

#[allow(dead_code)]
#[rustfmt::skip]
#[path = "../src/kvs/compat/v3_3_0.rs"]
mod v3;

#[tokio::test]
async fn migrates_frozen_v3_catalog_and_record_bytes_without_restamping_them_as_converted() {
	let ds = Datastore::builder()
		.without_startup_transactions()
		.build_with_path("memory")
		.await
		.unwrap();
	let ns = NamespaceId(1);
	let db = DatabaseId(1);
	let table = TableName::from("users");
	let id = RecordIdKey::String(Strand::new_static("fixture"));
	let record = RecordKey::new(ns, db, Cow::Borrowed(&table), Cow::Borrowed(&id));
	let field = FieldKey::new(ns, db, Cow::Borrowed(&table), Cow::Borrowed("name"));
	let drifted = FieldKey::new(ns, db, Cow::Borrowed(&table), Cow::Borrowed("`name`"));
	let mut marker = ForkStorageFormat::current();
	marker.upstream_storage_major = 3;
	marker.minimum_compatible_fork_release = "3.3.0-native-quota.1".to_owned();
	let policy = QuotaPolicyDefinition::new("test".into(), 7, Vec::new()).unwrap();

	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	tx.set_key(&VersionKey {}, &MajorVersion::from(0x8003)).await.unwrap();
	tx.set_key(&ForkStorageKey {}, &marker).await.unwrap();
	tx.set(
		NamespaceKey::new(Cow::Borrowed("test")).encode_key().unwrap(),
		v3::NAMESPACE_BASIC.to_vec(),
	)
	.await
	.unwrap();
	tx.set(
		DatabaseKey::new(ns, Cow::Borrowed("test")).encode_key().unwrap(),
		v3::DATABASE_BASIC.to_vec(),
	)
	.await
	.unwrap();
	tx.set_key(
		&QuotaUsageMetaKey {
			ns,
			db,
		},
		&QuotaUsageMeta::ready_empty(),
	)
	.await
	.unwrap();
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
		&7,
	)
	.await
	.unwrap();
	tx.set(
		TableKey::new(ns, db, Cow::Borrowed(&table)).encode_key().unwrap(),
		v3::TABLE_BASIC.to_vec(),
	)
	.await
	.unwrap();
	tx.set(drifted.encode_key().unwrap(), v3::FIELD_BASIC.to_vec()).await.unwrap();
	// The frozen record's embedded id names compat_test:fixture. Its key is deliberately
	// different so the migration also proves it does not rewrite the stored record body.
	tx.set(record.encode_key().unwrap(), v3::RECORD_OBJECT.to_vec()).await.unwrap();
	let policy_bytes = tx
		.get_key_raw(
			&QuotaPolicyKey {
				ns,
				db,
			},
			None,
		)
		.await
		.unwrap();
	tx.commit().await.unwrap();

	assert!(ds.storage_version().await.unwrap().is_none());
	assert!(ds.applied_migrations().await.unwrap().is_empty());
	assert!(ds.check_version().await.is_err());
	let report = ds
		.migrate_native_quota_datastore(NativeQuotaMigrationOptions {
			snapshot_reference: "synthetic:frozen-v3-fixtures".to_owned(),
			snapshot_verified: true,
			offline: true,
		})
		.await
		.unwrap();
	assert_eq!(report.before.state, NativeQuotaStorageState::MigrationRequired);
	assert!(report.after.ready);
	assert_eq!((report.databases, report.tables, report.fields, report.records), (1, 1, 1, 1));
	let tx = ds.transaction(TransactionType::Read).await.unwrap();
	assert_eq!(tx.get_key(&VersionKey {}, None).await.unwrap(), Some(MajorVersion::latest()));
	assert_eq!(
		tx.get_key(&StorageVersionKey {}, None).await.unwrap(),
		Some(StorageVersion::current())
	);
	assert_eq!(tx.get_key(&StoreDefaultsKey {}, None).await.unwrap(), Some(StoreDefaults::LEGACY));
	assert!(tx.get_key(&FeatureLevelKey {}, None).await.unwrap().is_none());
	assert!(tx.get_key(&NativeQuotaMigrationKey {}, None).await.unwrap().is_none());
	assert!(tx.get_key_raw(&drifted, None).await.unwrap().is_none());
	let rewritten_field = tx.get_key_raw(&field, None).await.unwrap().unwrap();
	assert_ne!(rewritten_field, v3::FIELD_BASIC);
	assert!(tx.get_key(&field, None).await.unwrap().is_some());
	assert_eq!(tx.get_key_raw(&record, None).await.unwrap().unwrap(), v3::RECORD_OBJECT);
	assert_eq!(
		tx.get_key_raw(
			&QuotaPolicyKey {
				ns,
				db
			},
			None
		)
		.await
		.unwrap(),
		policy_bytes
	);
	assert_eq!(
		tx.get_key(
			&QuotaGenerationKey {
				ns,
				db
			},
			None
		)
		.await
		.unwrap(),
		Some(7)
	);
	let quota = tx.quota_usage(ns, db);
	assert_eq!(quota.meta().await.unwrap().state, QuotaUsageState::Ready);
	assert_eq!(quota.record_count(&table).await.unwrap(), 1);
	assert_eq!(quota.field_count(&table).await.unwrap(), 1);
	tx.cancel().await.unwrap();

	let applied = ds.applied_migrations().await.unwrap();
	assert!(applied.iter().any(|(id, _)| *id == 4));
	assert!(applied.iter().any(|(id, _)| *id == 6));
	ds.check_version().await.unwrap();
	let results = ds
		.execute(
			"SELECT name, age, active FROM users",
			&Session::owner().with_ns("test").with_db("test"),
			None,
		)
		.await
		.unwrap();
	let value = results.into_iter().next().unwrap().result.unwrap().to_sql();
	assert!(value.contains("Alice"), "{value}");
	assert!(value.contains("30"), "{value}");
	assert!(value.contains("true"), "{value}");
}
