//! Public operational contract for the fork-required native quota datastore format.

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::version::MajorVersion;
use super::{Datastore, LockType, TransactionType};
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::catalog::{
	CURRENT_FORK_RELEASE, FORK_STORAGE_FORMAT_REVISION, ForkMigrationState, ForkStorageFormat,
	MINIMUM_COMPATIBLE_FORK_RELEASE, NATIVE_QUOTA_FORK_ID, QUOTA_POLICY_FORMAT_REVISION,
	QUOTA_USAGE_FORMAT_REVISION, QuotaUsageState,
};
use crate::err::Error;
use crate::key::format::StorageFormat;
use crate::key::version::Version;

/// Capability name required by native quota-aware clients and readiness probes.
pub const NATIVE_QUOTA_CAPABILITY: &str = "native-quota-v1";
/// Stable public INFO DTO major.
pub const NATIVE_QUOTA_INFO_FORMAT_VERSION: u16 = 1;
/// Stable structured quota error contract major.
pub const NATIVE_QUOTA_ERROR_FORMAT_VERSION: u16 = 1;
/// Stable datastore status/report DTO major.
pub const NATIVE_QUOTA_STORAGE_STATUS_FORMAT_VERSION: u16 = 1;

/// Machine-readable native quota datastore lifecycle.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeQuotaStorageState {
	/// No version or user data exists; ordinary startup will initialise the datastore.
	Empty,
	/// Unversioned legacy data must first pass through the upstream migration path.
	LegacyUnversioned,
	/// A known upstream or older fork format requires explicit native quota migration.
	MigrationRequired,
	/// The global migration fence is committed and migration can be resumed.
	Migrating,
	/// The datastore marker and every native quota format are current and clean.
	Ready,
}

/// Redacted public projection of the protected `!vf` storage marker.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NativeQuotaStorageMarker {
	/// Marker payload format revision.
	pub format_revision: u16,
	/// Stable fork identity.
	pub fork_id: String,
	/// Upstream storage major on which the fork format is based.
	pub upstream_storage_major: u16,
	/// Native quota policy catalog format revision.
	pub quota_policy_format_revision: u16,
	/// Native quota usage catalog format revision.
	pub quota_usage_format_revision: u16,
	/// Oldest fork release allowed to open the format.
	pub minimum_compatible_fork_release: String,
	/// Stored migration lifecycle.
	pub migration_state: String,
}

impl From<&ForkStorageFormat> for NativeQuotaStorageMarker {
	fn from(marker: &ForkStorageFormat) -> Self {
		Self {
			format_revision: marker.format_revision,
			fork_id: marker.fork_id.clone(),
			upstream_storage_major: marker.upstream_storage_major,
			quota_policy_format_revision: marker.quota_policy_format_revision,
			quota_usage_format_revision: marker.quota_usage_format_revision,
			minimum_compatible_fork_release: marker.minimum_compatible_fork_release.clone(),
			migration_state: match marker.migration_state {
				ForkMigrationState::Clean => "clean",
				ForkMigrationState::InProgress => "in_progress",
			}
			.to_owned(),
		}
	}
}

/// Stable status returned by the CLI and embedded capability document.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NativeQuotaStorageStatus {
	/// Public status document major.
	pub format_version: u16,
	/// Current backend name without paths, credentials, or endpoints.
	pub backend: String,
	/// Raw storage version when present.
	pub storage_version: Option<u16>,
	/// Current lifecycle.
	pub state: NativeQuotaStorageState,
	/// Whether ordinary native-quota service is safe.
	pub ready: bool,
	/// Whether the explicit migrator can advance this known format.
	pub migration_required: bool,
	/// Redacted marker, when present and decodable.
	pub marker: Option<NativeQuotaStorageMarker>,
}

/// Explicit prerequisites for an offline native quota datastore migration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NativeQuotaMigrationOptions {
	/// Operator-provided reference to a recoverable pre-migration snapshot.
	pub snapshot_reference: String,
	/// Explicit confirmation that all servers and writers are stopped.
	pub offline: bool,
}

/// Stable result of a status-preserving, resumable datastore migration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NativeQuotaMigrationReport {
	/// Public report major.
	pub format_version: u16,
	/// Unique migration attempt identifier.
	pub operation_id: String,
	/// Whether this invocation advanced or rebuilt protected state.
	pub changed: bool,
	/// Operator-provided snapshot reference.
	pub snapshot_reference: String,
	/// Status observed before migration.
	pub before: NativeQuotaStorageStatus,
	/// Status committed after migration.
	pub after: NativeQuotaStorageStatus,
	/// Databases rebuilt during this invocation.
	pub databases: u64,
	/// Tables scanned during this invocation.
	pub tables: u64,
	/// Explicit fields scanned during this invocation.
	pub fields: u64,
	/// Records scanned during this invocation.
	pub records: u64,
}

impl Datastore {
	/// Return a read-only, redacted view of the native quota datastore format.
	pub async fn native_quota_storage_status(&self) -> Result<NativeQuotaStorageStatus> {
		let tx = self.transaction(TransactionType::Read, LockType::Optimistic).await?;
		let version = tx.get(&Version::new(), None).await?;
		let marker = tx.get(&StorageFormat::new(), None).await?;
		let has_any_data = if version.is_none() {
			!tx.keys(vec![0x00]..vec![0xff], 1, 0, None).await?.is_empty()
		} else {
			true
		};
		tx.cancel().await?;

		let backend = self.to_string();
		let Some(version) = version else {
			let state = if has_any_data {
				NativeQuotaStorageState::LegacyUnversioned
			} else {
				NativeQuotaStorageState::Empty
			};
			return Ok(NativeQuotaStorageStatus {
				format_version: NATIVE_QUOTA_STORAGE_STATUS_FORMAT_VERSION,
				backend,
				storage_version: None,
				state,
				ready: false,
				migration_required: has_any_data,
				marker: None,
			});
		};

		if version == MajorVersion::upstream_latest() {
			if marker.is_some() {
				bail!(Error::ForkStorageFormatIncompatible {
					reason: "upstream storage version unexpectedly has a native-quota marker"
						.to_owned(),
				});
			}
			return Ok(NativeQuotaStorageStatus {
				format_version: NATIVE_QUOTA_STORAGE_STATUS_FORMAT_VERSION,
				backend,
				storage_version: Some(version.into()),
				state: NativeQuotaStorageState::MigrationRequired,
				ready: false,
				migration_required: true,
				marker: None,
			});
		}
		if version != MajorVersion::latest() {
			bail!(Error::ForkStorageFormatIncompatible {
				reason: format!("unsupported datastore storage version {}", u16::from(version)),
			});
		}

		let marker = marker.ok_or_else(|| Error::ForkStorageFormatIncompatible {
			reason: "fork-required format marker is missing".to_owned(),
		})?;
		validate_marker_ceiling(&marker)?;
		let current = ForkStorageFormat::current();
		let state = if marker.migration_state == ForkMigrationState::InProgress {
			NativeQuotaStorageState::Migrating
		} else if marker == current {
			NativeQuotaStorageState::Ready
		} else {
			NativeQuotaStorageState::MigrationRequired
		};
		Ok(NativeQuotaStorageStatus {
			format_version: NATIVE_QUOTA_STORAGE_STATUS_FORMAT_VERSION,
			backend,
			storage_version: Some(version.into()),
			state,
			ready: state == NativeQuotaStorageState::Ready,
			migration_required: state != NativeQuotaStorageState::Ready,
			marker: Some(NativeQuotaStorageMarker::from(&marker)),
		})
	}

	/// Explicitly migrate an offline upstream/older datastore into the protected quota format.
	///
	/// The first commit installs the fork-required high-bit version and an `in_progress`
	/// marker. Every database is then rebuilt into a validated usage epoch. The final commit
	/// advances the marker to `clean`. A crash at any earlier point remains fail-closed and the
	/// same command can be run again.
	pub async fn migrate_native_quota_datastore(
		&self,
		options: NativeQuotaMigrationOptions,
	) -> Result<NativeQuotaMigrationReport> {
		let snapshot_reference = options.snapshot_reference.trim();
		if snapshot_reference.is_empty() {
			bail!("a recoverable pre-migration snapshot reference is required");
		}
		if !options.offline {
			bail!("native quota datastore migration requires explicit offline confirmation");
		}

		let before = self.native_quota_storage_status().await?;
		let operation_id = Uuid::now_v7().to_string();
		if before.state == NativeQuotaStorageState::Ready {
			return Ok(NativeQuotaMigrationReport {
				format_version: NATIVE_QUOTA_STORAGE_STATUS_FORMAT_VERSION,
				operation_id,
				changed: false,
				snapshot_reference: snapshot_reference.to_owned(),
				before: before.clone(),
				after: before,
				databases: 0,
				tables: 0,
				fields: 0,
				records: 0,
			});
		}
		if matches!(
			before.state,
			NativeQuotaStorageState::Empty | NativeQuotaStorageState::LegacyUnversioned
		) {
			bail!(Error::ForkStorageFormatIncompatible {
				reason: format!(
					"datastore state {:?} cannot be migrated directly to native quota",
					before.state
				),
			});
		}

		self.begin_native_quota_datastore_migration().await?;

		let databases = {
			let tx = self.transaction(TransactionType::Read, LockType::Optimistic).await?;
			let mut databases = Vec::new();
			for namespace in tx.all_ns(None).await?.iter() {
				for database in tx.all_db(namespace.namespace_id, None).await?.iter() {
					databases.push((namespace.namespace_id, database.database_id));
				}
			}
			tx.cancel().await?;
			databases
		};

		let mut scanned_tables = 0u64;
		let mut scanned_fields = 0u64;
		let mut scanned_records = 0u64;
		for (namespace, database) in &databases {
			let fence = self.transaction(TransactionType::Write, LockType::Optimistic).await?;
			fence.quota_usage(*namespace, *database).begin_rebuild().await?;
			fence.commit().await?;

			let scan_tx = self.transaction(TransactionType::Read, LockType::Optimistic).await?;
			let scan = scan_tx.scan_quota_usage(*namespace, *database).await?;
			scan_tx.cancel().await?;

			let activate = self.transaction(TransactionType::Write, LockType::Optimistic).await?;
			let quota = activate.quota_usage(*namespace, *database);
			quota.stage_rebuild_scan(&scan).await?;
			let validated = quota.validate_staged_epoch(&scan.snapshot).await?;
			quota.activate_validated_epoch(validated).await?;
			activate.commit().await?;

			scanned_tables =
				scanned_tables.checked_add(scan.tables).ok_or_else(migration_count_overflow)?;
			scanned_fields =
				scanned_fields.checked_add(scan.fields).ok_or_else(migration_count_overflow)?;
			scanned_records =
				scanned_records.checked_add(scan.records).ok_or_else(migration_count_overflow)?;
		}

		self.finish_native_quota_datastore_migration().await?;
		let after = self.native_quota_storage_status().await?;
		Ok(NativeQuotaMigrationReport {
			format_version: NATIVE_QUOTA_STORAGE_STATUS_FORMAT_VERSION,
			operation_id,
			changed: true,
			snapshot_reference: snapshot_reference.to_owned(),
			before,
			after,
			databases: u64::try_from(databases.len()).map_err(|_| migration_count_overflow())?,
			tables: scanned_tables,
			fields: scanned_fields,
			records: scanned_records,
		})
	}

	async fn begin_native_quota_datastore_migration(&self) -> Result<()> {
		let tx = self.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let version = tx.get(&Version::new(), None).await?.ok_or_else(|| {
			Error::ForkStorageFormatIncompatible {
				reason: "cannot migrate an unversioned datastore directly".to_owned(),
			}
		})?;
		let marker = tx.get(&StorageFormat::new(), None).await?;
		let mut target = ForkStorageFormat::current();
		target.migration_state = ForkMigrationState::InProgress;

		if version == MajorVersion::upstream_latest() {
			if marker.is_some() {
				bail!(Error::ForkStorageFormatIncompatible {
					reason: "upstream storage version unexpectedly has a native-quota marker"
						.to_owned(),
				});
			}
			tx.putc(&Version::new(), &MajorVersion::latest(), Some(&version)).await?;
			tx.putc(&StorageFormat::new(), &target, None).await?;
			return tx.commit().await;
		}
		if version != MajorVersion::latest() {
			bail!(Error::ForkStorageFormatIncompatible {
				reason: format!("unsupported datastore storage version {}", u16::from(version)),
			});
		}
		let marker = marker.ok_or_else(|| Error::ForkStorageFormatIncompatible {
			reason: "fork-required format marker is missing".to_owned(),
		})?;
		validate_marker_ceiling(&marker)?;
		if marker == target {
			tx.cancel().await?;
			return Ok(());
		}
		tx.putc(&StorageFormat::new(), &target, Some(&marker)).await?;
		tx.commit().await
	}

	async fn finish_native_quota_datastore_migration(&self) -> Result<()> {
		let tx = self.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let namespaces = tx.all_ns(None).await?;
		for namespace in namespaces.iter() {
			for database in tx.all_db(namespace.namespace_id, None).await?.iter() {
				let meta =
					tx.quota_usage(namespace.namespace_id, database.database_id).meta().await?;
				if meta.state != QuotaUsageState::Ready {
					bail!(Error::QuotaUsageNotReady {
						state: format!("{:?}", meta.state).to_ascii_lowercase(),
					});
				}
			}
		}

		let version = tx.get(&Version::new(), None).await?.ok_or_else(|| {
			Error::ForkStorageFormatIncompatible {
				reason: "fork-required storage version disappeared during migration".to_owned(),
			}
		})?;
		if version != MajorVersion::latest() {
			bail!(Error::ForkStorageFormatIncompatible {
				reason: "fork-required storage version changed during migration".to_owned(),
			});
		}
		tx.putc(&Version::new(), &version, Some(&version)).await?;
		let marker = tx.get(&StorageFormat::new(), None).await?.ok_or_else(|| {
			Error::ForkStorageFormatIncompatible {
				reason: "native quota marker disappeared during migration".to_owned(),
			}
		})?;
		let mut expected = ForkStorageFormat::current();
		expected.migration_state = ForkMigrationState::InProgress;
		if marker != expected {
			bail!(Error::ForkStorageFormatIncompatible {
				reason: "native quota marker changed during migration".to_owned(),
			});
		}
		tx.putc(&StorageFormat::new(), &ForkStorageFormat::current(), Some(&marker)).await?;
		tx.commit().await
	}
}

fn validate_marker_ceiling(marker: &ForkStorageFormat) -> Result<()> {
	if marker.fork_id != NATIVE_QUOTA_FORK_ID {
		bail!(Error::ForkStorageFormatIncompatible {
			reason: format!("unknown datastore fork id '{}'", marker.fork_id),
		});
	}
	if marker.upstream_storage_major != MajorVersion::UPSTREAM_LATEST {
		bail!(Error::ForkStorageFormatIncompatible {
			reason: format!("unsupported upstream storage major {}", marker.upstream_storage_major),
		});
	}
	let minimum =
		semver::Version::parse(&marker.minimum_compatible_fork_release).map_err(|error| {
			Error::ForkStorageFormatIncompatible {
				reason: format!(
					"invalid minimum compatible fork release '{}': {error}",
					marker.minimum_compatible_fork_release
				),
			}
		})?;
	let current = semver::Version::parse(CURRENT_FORK_RELEASE).map_err(|error| {
		Error::ForkStorageFormatIncompatible {
			reason: format!("invalid current fork release '{CURRENT_FORK_RELEASE}': {error}"),
		}
	})?;
	if current < minimum {
		bail!(Error::ForkStorageFormatIncompatible {
			reason: format!(
				"datastore requires fork release {} but this binary is {}",
				marker.minimum_compatible_fork_release, CURRENT_FORK_RELEASE
			),
		});
	}
	if marker.format_revision > FORK_STORAGE_FORMAT_REVISION
		|| marker.quota_policy_format_revision > QUOTA_POLICY_FORMAT_REVISION
		|| marker.quota_usage_format_revision > QUOTA_USAGE_FORMAT_REVISION
	{
		bail!(Error::ForkStorageFormatIncompatible {
			reason: "datastore uses a newer native quota format".to_owned(),
		});
	}
	Ok(())
}

fn migration_count_overflow() -> Error {
	Error::QuotaUsageInvalid {
		reason: "native quota datastore migration count overflow".to_owned(),
	}
}

/// Stable build-time fork identity used by capability documents and matching CLIs.
pub fn native_quota_fork_contract() -> (&'static str, &'static str, &'static str) {
	(NATIVE_QUOTA_FORK_ID, CURRENT_FORK_RELEASE, MINIMUM_COMPATIBLE_FORK_RELEASE)
}

/// Stable protected format revisions used by capability documents and manifests.
pub fn native_quota_format_contract() -> (u16, u16, u16, u16) {
	(
		FORK_STORAGE_FORMAT_REVISION,
		MajorVersion::UPSTREAM_LATEST,
		QUOTA_POLICY_FORMAT_REVISION,
		QUOTA_USAGE_FORMAT_REVISION,
	)
}

/// Stable fork-required storage major used by compatibility manifests and matching CLIs.
pub fn native_quota_storage_version_contract() -> u16 {
	MajorVersion::latest().into()
}
