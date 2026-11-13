//! Public native quota identity and explicit, fenced offline storage migration.
use anyhow::{Result, bail};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use surrealdb_catalog::providers::{DatabaseProvider, NamespaceProvider};
use surrealdb_catalog::{
	DatabaseId, Error, NamespaceId, QUOTA_POLICY_FORMAT_REVISION, QUOTA_USAGE_FORMAT_REVISION,
	QuotaUsageState,
};
use surrealdb_datastore::fork_storage::*;
use surrealdb_datastore::key::schema::{
	FeatureLevelKey, ForkStorageKey, NativeQuotaMigrationKey, QuotaGenerationKey,
	QuotaLatestChangeKey, QuotaPolicyKey, QuotaUsageMetaKey, VersionKey,
};
use surrealdb_datastore::version::{MajorVersion, StorageVersion};
use surrealdb_kvs::Key;
use surrealdb_kvs::key::RawRange;
use uuid::Uuid;

use super::{Datastore, Transaction, TransactionType, migration};
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
	/// Caller attestation that the referenced snapshot is verified and recoverable.
	/// This flag does not perform or prove a restore test. The caller must verify
	/// backup integrity and rollback independently before setting it.
	#[serde(default)]
	pub snapshot_verified: bool,
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

const PREVIOUS_FORK_STORAGE_VERSION: u16 = 0x8003;
const PREVIOUS_FORK_RELEASE: &str = "3.3.0-native-quota.1";

/// Immutable recovery binding, stored at `!nqm` with the global fence.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq)]
struct NativeQuotaMigrationJournal {
	source_version: u16,
	target_release: String,
	operation_id: Uuid,
	snapshot_reference: String,
	databases: Vec<NativeQuotaMigrationDatabase>,
}

/// Permission carried only by the offline migration future, never by ordinary transactions.
struct NativeQuotaMigrationPermit {
	datastore_id: Uuid,
	source_version: MajorVersion,
	source_marker: ForkStorageFormat,
	journal: Vec<u8>,
}

tokio::task_local! {
	static NATIVE_QUOTA_MIGRATION_PERMIT: std::sync::Arc<NativeQuotaMigrationPermit>;
}

fn migration_permit(
	ds: &Datastore,
	journal: &NativeQuotaMigrationJournal,
) -> Result<std::sync::Arc<NativeQuotaMigrationPermit>> {
	Ok(std::sync::Arc::new(NativeQuotaMigrationPermit {
		datastore_id: ds.id(),
		source_version: journal.source_version.into(),
		source_marker: migration_marker(journal.source_version)?,
		journal: revision::to_vec(journal)?,
	}))
}

/// Opens only an upstream migration transaction. Ordinary SQL and datastore transaction
/// constructors never consult this scope. The permit is not inherited by spawned tasks and is
/// discarded when the scoped future completes or is canceled.
pub(super) async fn migration_transaction(
	ds: &Datastore,
	kind: TransactionType,
) -> Result<Transaction> {
	let permit = NATIVE_QUOTA_MIGRATION_PERMIT.try_with(std::sync::Arc::clone).ok();
	if permit.as_ref().is_some_and(|permit| permit.datastore_id != ds.id()) {
		bail!(incompatible("native quota migration permit belongs to another datastore"));
	}
	let tx = ds.transaction(kind).await?;
	if matches!(kind, TransactionType::Write)
		&& let Some(permit) = permit
	{
		catch!(
			tx,
			tx.admit_native_quota_catalog_migration(
				permit.source_version,
				&permit.source_marker,
				&permit.journal,
			)
			.await
		);
	}
	Ok(tx)
}

/// Protected policy bytes and monotonic epoch floor that migration must retain.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq)]
struct NativeQuotaMigrationDatabase {
	ns: u32,
	db: u32,
	policy: Option<Vec<u8>>,
	generation: Option<Vec<u8>>,
	latest_change: Option<Vec<u8>>,
	epoch_high_water: u64,
}

fn incompatible(reason: &str) -> Error {
	Error::ForkStorageFormatIncompatible {
		reason: reason.to_owned(),
	}
}

fn previous_marker() -> ForkStorageFormat {
	ForkStorageFormat {
		format_revision: 1,
		fork_id: NATIVE_QUOTA_FORK_ID.to_owned(),
		upstream_storage_major: 3,
		quota_policy_format_revision: 1,
		quota_usage_format_revision: 1,
		minimum_compatible_fork_release: PREVIOUS_FORK_RELEASE.to_owned(),
		migration_state: ForkMigrationState::Clean,
	}
}

fn migration_marker(source_version: u16) -> Result<ForkStorageFormat> {
	let mut marker = match source_version {
		PREVIOUS_FORK_STORAGE_VERSION => previous_marker(),
		MajorVersion::UPSTREAM_LATEST => ForkStorageFormat::current(),
		_ => bail!(incompatible("unsupported native quota migration source")),
	};
	marker.migration_state = ForkMigrationState::InProgress;
	Ok(marker)
}

async fn read_journal(tx: &Transaction) -> Result<Option<NativeQuotaMigrationJournal>> {
	let Some(bytes) = tx.get_key(&NativeQuotaMigrationKey {}, None).await? else {
		return Ok(None);
	};
	let journal: NativeQuotaMigrationJournal = revision::from_slice(&bytes)
		.map_err(|_| incompatible("unrecognized native quota migration journal"))?;
	if revision::to_vec(&journal)? != bytes
		|| journal.target_release != CURRENT_FORK_RELEASE
		|| journal.operation_id.is_nil()
		|| journal.snapshot_reference.trim().is_empty()
	{
		bail!(incompatible("invalid native quota migration journal"));
	}
	migration_marker(journal.source_version)?;
	let mut scopes = std::collections::BTreeSet::new();
	if journal.databases.iter().any(|db| !scopes.insert((db.ns, db.db))) {
		bail!(incompatible("duplicate database in native quota migration journal"));
	}
	Ok(Some(journal))
}

fn storage_state(
	version: Option<MajorVersion>,
	marker: Option<&ForkStorageFormat>,
	journal: Option<&NativeQuotaMigrationJournal>,
	has_data: bool,
) -> Result<NativeQuotaStorageState> {
	if let Some(journal) = journal {
		if version != Some(journal.source_version.into())
			|| marker != Some(&migration_marker(journal.source_version)?)
		{
			bail!(incompatible("native quota migration journal and global fence disagree"));
		}
		return Ok(NativeQuotaStorageState::Migrating);
	}
	match (version.map(u16::from), marker) {
		(None, None) if !has_data => Ok(NativeQuotaStorageState::Empty),
		(None, None) => Ok(NativeQuotaStorageState::LegacyUnversioned),
		(Some(PREVIOUS_FORK_STORAGE_VERSION), Some(marker)) if *marker == previous_marker() => {
			Ok(NativeQuotaStorageState::MigrationRequired)
		}
		(Some(MajorVersion::UPSTREAM_LATEST), None) => {
			Ok(NativeQuotaStorageState::MigrationRequired)
		}
		(Some(MajorVersion::LATEST), Some(marker)) if *marker == ForkStorageFormat::current() => {
			Ok(NativeQuotaStorageState::Ready)
		}
		_ => bail!(incompatible(
			"unknown storage identity or incomplete migration without a matching journal"
		)),
	}
}

async fn capture_databases(tx: &Transaction) -> Result<Vec<NativeQuotaMigrationDatabase>> {
	let mut databases = Vec::new();
	for namespace in tx.all_ns(None).await?.iter() {
		for database in tx.all_db(namespace.namespace_id, None).await?.iter() {
			let ns = namespace.namespace_id;
			let db = database.database_id;
			if !tx.all_db_models(ns, db, None).await?.is_empty() {
				bail!(incompatible("stored model definitions are unsupported by this release"));
			}
			tx.ensure_quota_rebuild_supported(ns, db).await?;
			let meta = tx.quota_usage(ns, db).meta().await?;
			meta.validate()?;
			if let Some(policy) = tx.get_db_quota(ns, db, None).await? {
				let mut validated = (*policy).clone();
				validated.normalize_and_validate()?;
				if tx
					.get_key(
						&QuotaGenerationKey {
							ns,
							db,
						},
						None,
					)
					.await? != Some(policy.generation)
				{
					bail!(incompatible("quota policy and generation fence disagree"));
				}
			}
			databases.push(NativeQuotaMigrationDatabase {
				ns: ns.0,
				db: db.0,
				policy: tx
					.get_key_raw(
						&QuotaPolicyKey {
							ns,
							db,
						},
						None,
					)
					.await?,
				generation: tx
					.get_key_raw(
						&QuotaGenerationKey {
							ns,
							db,
						},
						None,
					)
					.await?,
				latest_change: tx
					.get_key_raw(
						&QuotaLatestChangeKey {
							ns,
							db,
						},
						None,
					)
					.await?,
				epoch_high_water: meta.epoch_high_water,
			});
		}
	}
	Ok(databases)
}

async fn check_protected_database(
	tx: &Transaction,
	expected: &NativeQuotaMigrationDatabase,
) -> Result<()> {
	let ns = NamespaceId(expected.ns);
	let db = DatabaseId(expected.db);
	if tx
		.get_key_raw(
			&QuotaPolicyKey {
				ns,
				db,
			},
			None,
		)
		.await?
		!= expected.policy
		|| tx
			.get_key_raw(
				&QuotaGenerationKey {
					ns,
					db,
				},
				None,
			)
			.await? != expected.generation
		|| tx
			.get_key_raw(
				&QuotaLatestChangeKey {
					ns,
					db,
				},
				None,
			)
			.await? != expected.latest_change
		|| tx.quota_usage(ns, db).meta().await?.epoch_high_water < expected.epoch_high_water
	{
		bail!(incompatible("protected quota state changed during migration"));
	}
	Ok(())
}

/// Reassert the immutable recovery binding and both identity keys in a write transaction.
async fn guard_migration(tx: &Transaction, journal: &NativeQuotaMigrationJournal) -> Result<()> {
	let version = MajorVersion::from(journal.source_version);
	let marker = migration_marker(journal.source_version)?;
	let bytes = revision::to_vec(journal)?;
	if read_journal(tx).await?.as_ref() != Some(journal)
		|| tx.get_key(&VersionKey {}, None).await? != Some(version)
		|| tx.get_key(&ForkStorageKey {}, None).await?.as_ref() != Some(&marker)
	{
		bail!(incompatible("native quota migration fence changed"));
	}
	tx.put_compare_key(&VersionKey {}, &version, Some(&version)).await?;
	tx.put_compare_key(&ForkStorageKey {}, &marker, Some(&marker)).await?;
	tx.put_compare_key(&NativeQuotaMigrationKey {}, &bytes, Some(&bytes)).await?;
	Ok(())
}

impl Datastore {
	/// Inspect identity without changing the datastore or completing migrations.
	#[cfg_attr(feature = "observability", tracing::instrument(skip_all, err))]
	pub async fn native_quota_storage_status(&self) -> Result<NativeQuotaStorageStatus> {
		let tx = self.transaction(TransactionType::Read).await?;
		let result = async {
			let version = tx.get_key(&VersionKey {}, None).await?;
			let marker = tx.get_key(&ForkStorageKey {}, None).await?;
			let journal = read_journal(&tx).await?;
			let has_data = if version.is_none() {
				!tx.keys_raw(RawRange::every_key_after(&Key::from(&b""[..])), 1, 0, None)
					.await?
					.is_empty()
			} else {
				true
			};
			let state = storage_state(version, marker.as_ref(), journal.as_ref(), has_data)?;
			Ok(NativeQuotaStorageStatus {
				format_version: NATIVE_QUOTA_STORAGE_STATUS_FORMAT_VERSION,
				backend: self.to_string(),
				storage_version: version.map(u16::from),
				state,
				ready: state == NativeQuotaStorageState::Ready,
				migration_required: matches!(
					state,
					NativeQuotaStorageState::MigrationRequired
						| NativeQuotaStorageState::Migrating
						| NativeQuotaStorageState::LegacyUnversioned
				),
				marker: marker.as_ref().map(NativeQuotaStorageMarker::from),
			})
		}
		.await;
		tx.cancel().await?;
		result
	}

	/// Migrate only an explicitly offline, known datastore with a verified backup attestation.
	///
	/// Open the datastore with `Builder::without_startup_transactions` and stop every other
	/// writer first. The durable source identity stays fenced until upstream migrations and
	/// quota epoch validation complete. A crash retains that fence and the snapshot binding;
	/// resuming requires the same snapshot reference. No backend conversion is performed.
	#[cfg_attr(feature = "observability", tracing::instrument(skip_all, err))]
	pub async fn migrate_native_quota_datastore(
		&self,
		options: NativeQuotaMigrationOptions,
	) -> Result<NativeQuotaMigrationReport> {
		let snapshot = options.snapshot_reference.trim();
		if snapshot.is_empty() {
			bail!("a recoverable pre-migration snapshot reference is required");
		}
		if !options.snapshot_verified {
			bail!("explicit attestation of a verified, recoverable snapshot is required");
		}
		if !options.offline || self.maintenance_tasks_running() {
			bail!(
				"native quota migration requires offline confirmation and stopped maintenance tasks"
			);
		}
		let before = self.native_quota_storage_status().await?;
		if before.ready {
			return Ok(NativeQuotaMigrationReport {
				format_version: NATIVE_QUOTA_STORAGE_STATUS_FORMAT_VERSION,
				operation_id: Uuid::now_v7().to_string(),
				changed: false,
				snapshot_reference: snapshot.to_owned(),
				before: before.clone(),
				after: before,
				databases: 0,
				tables: 0,
				fields: 0,
				records: 0,
			});
		}
		if !matches!(
			before.state,
			NativeQuotaStorageState::MigrationRequired | NativeQuotaStorageState::Migrating
		) {
			bail!(incompatible("this datastore cannot be migrated directly to native quota"));
		}
		let source =
			before.storage_version.ok_or_else(|| incompatible("missing source version"))?;
		self.preflight_native_quota_migration(
			source,
			before.state == NativeQuotaStorageState::Migrating,
		)
		.await?;
		let journal = self.begin_native_quota_migration(snapshot).await?;
		let verify = self.transaction(TransactionType::Read).await?;
		let checked = async {
			let actual = capture_databases(&verify).await?;
			if actual.iter().map(|db| (db.ns, db.db)).collect::<Vec<_>>()
				!= journal.databases.iter().map(|db| (db.ns, db.db)).collect::<Vec<_>>()
			{
				bail!(incompatible("database catalog changed during offline migration"));
			}
			for database in &journal.databases {
				check_protected_database(&verify, database).await?;
			}
			Ok::<_, anyhow::Error>(())
		}
		.await;
		verify.cancel().await?;
		checked?;

		// The durable feature level, never the target identity, decides how upstream migrations
		// encode. In particular, an unstamped 3.x store must not be baselined as a new v4 store.
		self.refresh_feature_level().await?;
		NATIVE_QUOTA_MIGRATION_PERMIT
			.scope(migration_permit(self, &journal)?, migration::run(self, false))
			.await?;
		if self.is_read_only() || !migration::settled(self).await? {
			bail!(incompatible("upstream storage migrations did not settle"));
		}

		let mut tables = 0u64;
		let mut fields = 0u64;
		let mut records = 0u64;
		let mut epochs = Vec::new();
		for database in &journal.databases {
			let ns = NamespaceId(database.ns);
			let db = DatabaseId(database.db);
			let fence = self.transaction(TransactionType::Write).await?.enclose();
			let epoch = run!(
				fence,
				async {
					guard_migration(&fence, &journal).await?;
					check_protected_database(&fence, database).await?;
					fence.quota_usage(ns, db).begin_rebuild().await
				}
				.await
			)?;

			let scan_tx = self.transaction(TransactionType::Read).await?;
			let scanned = scan_tx.scan_quota_usage(ns, db).await;
			scan_tx.cancel().await?;
			let scan = scanned?;
			let activate = self.transaction(TransactionType::Write).await?.enclose();
			run!(
				activate,
				async {
					guard_migration(&activate, &journal).await?;
					check_protected_database(&activate, database).await?;
					let quota = activate.quota_usage(ns, db);
					if quota.meta().await?.staged_epoch != Some(epoch) {
						bail!(incompatible("quota rebuild epoch changed during migration"));
					}
					quota.stage_rebuild_scan(&scan).await?;
					let validated = quota.validate_staged_epoch(&scan.snapshot).await?;
					quota.activate_validated_epoch(validated).await
				}
				.await
			)?;
			epochs.push(epoch);
			tables = tables.checked_add(scan.tables).ok_or_else(migration_count_overflow)?;
			fields = fields.checked_add(scan.fields).ok_or_else(migration_count_overflow)?;
			records = records.checked_add(scan.records).ok_or_else(migration_count_overflow)?;
		}
		self.finish_native_quota_migration(&journal, &epochs).await?;
		Ok(NativeQuotaMigrationReport {
			format_version: NATIVE_QUOTA_STORAGE_STATUS_FORMAT_VERSION,
			operation_id: journal.operation_id.to_string(),
			changed: true,
			snapshot_reference: journal.snapshot_reference,
			before,
			after: self.native_quota_storage_status().await?,
			databases: u64::try_from(journal.databases.len())
				.map_err(|_| migration_count_overflow())?,
			tables,
			fields,
			records,
		})
	}

	async fn preflight_native_quota_migration(&self, source: u16, resuming: bool) -> Result<()> {
		migration_marker(source)?;
		if self.is_read_only() {
			bail!(incompatible("a read-only datastore cannot be migrated"));
		}
		let current = StorageVersion::current();
		if let Some(stored) = self.storage_version().await?
			&& (stored.triple() > current.triple()
				|| (source == PREVIOUS_FORK_STORAGE_VERSION
					&& stored.triple() != (3, 3, 0)
					&& !(resuming && stored == current))
				|| (source == MajorVersion::UPSTREAM_LATEST && stored.triple().0 != 4))
		{
			bail!(incompatible("semantic storage version does not match the migration source"));
		}
		for (id, record) in self.applied_migration_ids().await? {
			if !migration::MIGRATIONS.iter().any(|known| {
				known.id == id
					&& record
						.as_ref()
						.is_some_and(|record| record.version.triple() == known.version)
			}) {
				bail!(incompatible("unknown or unreadable upstream migration ledger entry"));
			}
		}
		let tx = self.transaction(TransactionType::Read).await?;
		let result = async {
			let feature = tx.get_key(&FeatureLevelKey {}, None).await?;
			if feature
				.as_ref()
				.is_some_and(|feature| feature.level() > surrealdb_kvs::Release::CURRENT)
				|| (source == PREVIOUS_FORK_STORAGE_VERSION && feature.is_some())
			{
				bail!(incompatible("unsupported feature level for native quota migration source"));
			}
			capture_databases(&tx).await?;
			Ok(())
		}
		.await;
		tx.cancel().await?;
		result
	}

	async fn begin_native_quota_migration(
		&self,
		snapshot: &str,
	) -> Result<NativeQuotaMigrationJournal> {
		let tx = self.transaction(TransactionType::Write).await?.enclose();
		run!(
			tx,
			async {
				let version = tx.get_key(&VersionKey {}, None).await?;
				let marker = tx.get_key(&ForkStorageKey {}, None).await?;
				let prior = read_journal(&tx).await?;
				let state = storage_state(version, marker.as_ref(), prior.as_ref(), true)?;
				if let Some(journal) = prior {
					if journal.snapshot_reference != snapshot {
						bail!(incompatible(
							"resume requires the original verified snapshot reference"
						));
					}
					return Ok(journal);
				}
				if state != NativeQuotaStorageState::MigrationRequired {
					bail!(incompatible("migration source changed before the fence was installed"));
				}
				let version =
					version.ok_or_else(|| incompatible("missing migration source version"))?;
				let journal = NativeQuotaMigrationJournal {
					source_version: version.into(),
					target_release: CURRENT_FORK_RELEASE.to_owned(),
					operation_id: Uuid::now_v7(),
					snapshot_reference: snapshot.to_owned(),
					databases: capture_databases(&tx).await?,
				};
				tx.put_compare_key(&VersionKey {}, &version, Some(&version)).await?;
				tx.put_compare_key(
					&ForkStorageKey {},
					&migration_marker(version.into())?,
					marker.as_ref(),
				)
				.await?;
				tx.put_compare_key(&NativeQuotaMigrationKey {}, &revision::to_vec(&journal)?, None)
					.await?;
				Ok(journal)
			}
			.await
		)
	}

	async fn finish_native_quota_migration(
		&self,
		journal: &NativeQuotaMigrationJournal,
		epochs: &[u64],
	) -> Result<()> {
		if !migration::settled(self).await? || epochs.len() != journal.databases.len() {
			bail!(incompatible("migration has not completed every required stage"));
		}
		let tx = self.transaction(TransactionType::Write).await?.enclose();
		run!(
			tx,
			async {
				guard_migration(&tx, journal).await?;
				let actual = capture_databases(&tx).await?;
				if actual.iter().map(|db| (db.ns, db.db)).collect::<Vec<_>>()
					!= journal.databases.iter().map(|db| (db.ns, db.db)).collect::<Vec<_>>()
				{
					bail!(incompatible("database catalog changed during offline migration"));
				}
				for (database, epoch) in journal.databases.iter().zip(epochs) {
					check_protected_database(&tx, database).await?;
					let ns = NamespaceId(database.ns);
					let db = DatabaseId(database.db);
					let meta = tx.quota_usage(ns, db).meta().await?;
					if meta.state != QuotaUsageState::Ready || meta.active_epoch != Some(*epoch) {
						bail!(incompatible(
							"a migrated database does not have its validated quota epoch"
						));
					}
					tx.put_compare_key(
						&QuotaUsageMetaKey {
							ns,
							db,
						},
						&meta,
						Some(&meta),
					)
					.await?;
				}
				tx.put_compare_key(
					&VersionKey {},
					&MajorVersion::latest(),
					Some(&journal.source_version.into()),
				)
				.await?;
				tx.put_compare_key(
					&ForkStorageKey {},
					&ForkStorageFormat::current(),
					Some(&migration_marker(journal.source_version)?),
				)
				.await?;
				tx.del_compare_key(&NativeQuotaMigrationKey {}, Some(&revision::to_vec(journal)?))
					.await?;
				Ok::<_, anyhow::Error>(())
			}
			.await
		)
	}
}

fn migration_count_overflow() -> Error {
	incompatible("native quota migration scan count overflow")
}

#[cfg(all(test, feature = "kv-mem"))]
#[path = "native_quota_migration_tests.rs"]
mod migration_tests;

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
