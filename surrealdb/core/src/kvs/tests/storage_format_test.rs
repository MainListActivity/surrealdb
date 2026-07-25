use crate::catalog::{ForkMigrationState, ForkStorageFormat};
use crate::err::Error;
use crate::key::format::StorageFormat;
use crate::key::version::Version;
use crate::kvs::version::MajorVersion;
use crate::kvs::{Datastore, KVKey, KVValue, LockType, TransactionType};

async fn write_format(ds: &Datastore, version: MajorVersion, marker: Option<ForkStorageFormat>) {
	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	tx.replace(&Version::new(), &version).await.unwrap();
	if let Some(marker) = marker {
		tx.replace(&StorageFormat::new(), &marker).await.unwrap();
	}
	tx.commit().await.unwrap();
}

#[tokio::test]
async fn new_datastore_atomically_writes_fork_version_and_structured_marker() {
	let ds = Datastore::new("memory").await.unwrap();
	let (version, is_new) = ds.check_version().await.unwrap();
	assert!(is_new);
	assert_eq!(version, MajorVersion::latest());
	assert!(version.requires_quota_fork());

	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let marker = tx.get(&StorageFormat::new(), None).await.unwrap().unwrap();
	assert_eq!(marker, ForkStorageFormat::current());
	tx.cancel().await.unwrap();
}

#[tokio::test]
async fn upstream_storage_requires_explicit_migration_without_silent_writes() {
	let ds = Datastore::new("memory").await.unwrap();
	write_format(&ds, MajorVersion::upstream_latest(), None).await;

	let error = ds.check_version().await.unwrap_err();
	assert!(crate::err::is_storage_compatibility_error(&error));
	assert!(
		matches!(
			error.downcast_ref::<Error>(),
			Some(Error::ForkStorageMigrationRequired {
				actual
			}) if *actual == MajorVersion::UPSTREAM_LATEST
		),
		"{error}"
	);

	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	assert_eq!(tx.get(&Version::new(), None).await.unwrap(), Some(MajorVersion::upstream_latest()));
	assert_eq!(tx.get(&StorageFormat::new(), None).await.unwrap(), None);
	tx.cancel().await.unwrap();
}

#[tokio::test]
async fn fork_version_without_marker_and_incomplete_migration_fail_closed() {
	let ds = Datastore::new("memory").await.unwrap();
	write_format(&ds, MajorVersion::latest(), None).await;
	let error = ds.check_version().await.unwrap_err();
	assert!(crate::err::is_storage_compatibility_error(&error));
	assert!(error.to_string().contains("format marker is missing"), "{error}");

	let ds = Datastore::new("memory").await.unwrap();
	let mut marker = ForkStorageFormat::current();
	marker.migration_state = ForkMigrationState::InProgress;
	write_format(&ds, MajorVersion::latest(), Some(marker)).await;
	let error = ds.check_version().await.unwrap_err();
	assert!(crate::err::is_storage_compatibility_error(&error));
	assert!(
		matches!(error.downcast_ref::<Error>(), Some(Error::ForkStorageMigrationRequired { .. })),
		"{error}"
	);
}

#[tokio::test]
async fn unknown_newer_quota_format_fails_closed() {
	let ds = Datastore::new("memory").await.unwrap();
	let mut marker = ForkStorageFormat::current();
	marker.quota_usage_format_revision += 1;
	write_format(&ds, MajorVersion::latest(), Some(marker)).await;
	let error = ds.check_version().await.unwrap_err();
	assert!(crate::err::is_storage_compatibility_error(&error));
	assert!(error.to_string().contains("quota usage format revision"), "{error}");
}

#[tokio::test]
async fn older_known_format_requires_migration_and_release_range_is_ordered() {
	let ds = Datastore::new("memory").await.unwrap();
	let mut marker = ForkStorageFormat::current();
	marker.quota_usage_format_revision = 0;
	write_format(&ds, MajorVersion::latest(), Some(marker)).await;
	let error = ds.check_version().await.unwrap_err();
	assert!(
		matches!(error.downcast_ref::<Error>(), Some(Error::ForkStorageMigrationRequired { .. })),
		"{error}"
	);

	let ds = Datastore::new("memory").await.unwrap();
	let mut marker = ForkStorageFormat::current();
	marker.minimum_compatible_fork_release = "3.3.0-native-quota.0".to_owned();
	write_format(&ds, MajorVersion::latest(), Some(marker)).await;
	ds.check_version().await.unwrap();

	let ds = Datastore::new("memory").await.unwrap();
	let mut marker = ForkStorageFormat::current();
	marker.minimum_compatible_fork_release = "3.3.0-native-quota.2".to_owned();
	write_format(&ds, MajorVersion::latest(), Some(marker)).await;
	let error = ds.check_version().await.unwrap_err();
	assert!(
		matches!(error.downcast_ref::<Error>(), Some(Error::ForkStorageFormatIncompatible { .. })),
		"{error}"
	);
}

#[tokio::test]
async fn undecodable_storage_version_and_marker_fail_immediately() {
	let ds = Datastore::new("memory").await.unwrap();
	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	tx.set(&Version::new().encode_key().unwrap(), &vec![1]).await.unwrap();
	tx.commit().await.unwrap();
	let error = ds.check_version().await.unwrap_err();
	assert!(matches!(error.downcast_ref::<Error>(), Some(Error::InvalidStorageVersion)));
	assert!(crate::err::is_storage_compatibility_error(&error));

	let ds = Datastore::new("memory").await.unwrap();
	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	tx.replace(&Version::new(), &MajorVersion::latest()).await.unwrap();
	tx.set(&StorageFormat::new().encode_key().unwrap(), &vec![1]).await.unwrap();
	tx.commit().await.unwrap();
	let error = ds.check_version().await.unwrap_err();
	assert!(
		matches!(error.downcast_ref::<Error>(), Some(Error::ForkStorageFormatIncompatible { .. })),
		"{error}"
	);
	assert!(crate::err::is_storage_compatibility_error(&error));
}

#[tokio::test]
async fn unversioned_existing_data_is_not_modified_by_startup_check() {
	let ds = Datastore::new("memory").await.unwrap();
	let existing_key = b"existing/data".to_vec();
	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	tx.set(&existing_key, &b"keep".to_vec()).await.unwrap();
	tx.commit().await.unwrap();

	let error = ds.check_version().await.unwrap_err();
	assert!(crate::err::is_storage_compatibility_error(&error));
	assert!(error.to_string().contains("out-of-date"), "{error}");

	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	assert_eq!(tx.get(&existing_key, None).await.unwrap(), Some(b"keep".to_vec()));
	assert_eq!(tx.get(&Version::new(), None).await.unwrap(), None);
	assert_eq!(tx.get(&StorageFormat::new(), None).await.unwrap(), None);
	tx.cancel().await.unwrap();
}

#[test]
fn fork_required_version_is_rejected_by_vanilla_and_pre_marker_forks() {
	let encoded = MajorVersion::latest().kv_encode_value().unwrap();
	assert_eq!(encoded, [0x80, 0x03], "fork-required version bytes are a frozen format fence");
	let raw = u16::from_be_bytes(encoded.try_into().unwrap());
	assert_ne!(raw, MajorVersion::UPSTREAM_LATEST);
	assert_eq!(raw & MajorVersion::FORK_REQUIRED_FLAG, MajorVersion::FORK_REQUIRED_FLAG);
	assert!(
		raw != 3,
		"an upstream or old fork binary expecting version 3 must reject the fork-required version"
	);
}
