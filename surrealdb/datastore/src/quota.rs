//! Transaction-scoped access to protected native quota usage state.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::{Result, bail};

use super::Transaction;
use crate::NORMAL_BATCH_SIZE;
use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::catalog::{
	DatabaseId, NamespaceId, QuotaLimit, QuotaPolicyDefinition, QuotaResource, QuotaSelector,
	QuotaUsageMeta, QuotaUsageState, TableDefinition, TableType,
};
use crate::key::schema::{
	QuotaEpochRoot, QuotaFieldUsageKey as QuotaFieldUsage, QuotaGenerationKey as Qg,
	QuotaLatestChangeKey as Ql, QuotaRecordUsageKey as QuotaRecordUsage,
	QuotaTableBucketKey as QuotaTableBucket, QuotaUsageMetaKey as Qm,
};
use crate::val::{Datetime, Object, TableName, Value};
use surrealdb_catalog::{Error, QuotaExceededError, QuotaViolation};
use surrealdb_collections::map;
use surrealdb_kvs::key::{KVKey, KVKeyDecode, Resumable};
use surrealdb_kvs::value::KVValue;

#[cfg(any(test, feature = "test-hooks"))]
use crate::testing::{QuotaFaultSite, maybe_inject_quota_fault};

const MAX_QUOTA_VIOLATIONS: usize = 64;

#[derive(Clone, Debug)]
struct BoundQuotaDatabase {
	observed_generation: Option<u64>,
	meta: QuotaUsageMeta,
	policy: Option<Arc<QuotaPolicyDefinition>>,
}

#[derive(Clone, Debug, Default)]
struct QuotaTransactionSnapshot {
	databases: BTreeMap<(NamespaceId, DatabaseId), BoundQuotaDatabase>,
	table_deltas: BTreeMap<(NamespaceId, DatabaseId, TableName), i128>,
	field_deltas: BTreeMap<(NamespaceId, DatabaseId, TableName), i128>,
	record_deltas: BTreeMap<(NamespaceId, DatabaseId, TableName), i128>,
	reset_tables: BTreeSet<(NamespaceId, DatabaseId, TableName)>,
	// Original existence at the first raw mutation since the last flush.
	record_transitions: BTreeMap<Vec<u8>, (NamespaceId, DatabaseId, TableName, bool)>,
	catalog_transitions: BTreeMap<Vec<u8>, (NamespaceId, DatabaseId, TableName, bool, bool)>,
}

/// Transaction-local quota intents. The final signed delta is applied once at commit.
#[derive(Debug, Default)]
pub struct QuotaTransactionState {
	current: QuotaTransactionSnapshot,
	savepoints: Vec<QuotaTransactionSnapshot>,
	flushed_admission: bool,
}

impl QuotaTransactionState {
	pub(crate) fn has_pending(&self) -> bool {
		self.flushed_admission
			|| !self.current.databases.is_empty()
			|| !self.current.record_transitions.is_empty()
			|| !self.current.catalog_transitions.is_empty()
	}

	pub fn new_save_point(&mut self) {
		self.savepoints.push(self.current.clone());
	}

	pub fn release_last_save_point(&mut self) {
		self.savepoints.pop();
	}

	pub fn rollback_to_save_point(&mut self) {
		if let Some(snapshot) = self.savepoints.pop() {
			self.current = snapshot;
		}
	}
}

/// Quota operations bound to the same transaction as the protected business write.
pub struct QuotaTransaction<'a> {
	tx: &'a Transaction,
	ns: NamespaceId,
	db: DatabaseId,
}

/// Canonical non-zero counters produced by a trusted rebuild scan.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QuotaUsageSnapshot {
	table_buckets: BTreeMap<(u64, String), u64>,
	field_counts: BTreeMap<TableName, u64>,
	record_counts: BTreeMap<TableName, u64>,
}

/// Counts and canonical counters produced by one trusted catalog/record scan.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QuotaRebuildScan {
	pub snapshot: QuotaUsageSnapshot,
	pub tables: u64,
	pub fields: u64,
	pub records: u64,
}

impl QuotaUsageSnapshot {
	pub fn set_table_bucket_count(&mut self, generation: u64, rule: &str, count: u64) {
		let key = (generation, rule.to_owned());
		if count == 0 {
			self.table_buckets.remove(&key);
		} else {
			self.table_buckets.insert(key, count);
		}
	}

	pub fn set_field_count(&mut self, table: &TableName, count: u64) {
		if count == 0 {
			self.field_counts.remove(table);
		} else {
			self.field_counts.insert(table.clone(), count);
		}
	}

	pub fn set_record_count(&mut self, table: &TableName, count: u64) {
		if count == 0 {
			self.record_counts.remove(table);
		} else {
			self.record_counts.insert(table.clone(), count);
		}
	}

	fn counter_count(&self) -> usize {
		self.table_buckets.len() + self.field_counts.len() + self.record_counts.len()
	}
}

/// Proof that a staged epoch exactly matched a trusted rebuild snapshot.
#[derive(Debug)]
pub struct ValidatedQuotaEpoch {
	ns: NamespaceId,
	db: DatabaseId,
	epoch: u64,
	snapshot: QuotaUsageSnapshot,
}

impl Transaction {
	pub(crate) async fn ensure_no_native_quota_migration(&self) -> Result<()> {
		if self.get_key(&crate::key::schema::NativeQuotaMigrationKey {}, None).await?.is_some() {
			bail!(Error::QuotaUsageNotReady {
				state: "offline migration in progress".to_owned()
			});
		}
		Ok(())
	}

	/// Capture first-touch existence before every public raw record mutation.
	/// Commit observes final existence, so repeated writes, batch duplicates,
	/// held creates and provider writes are charged exactly once.
	pub(crate) fn track_quota_record_key<'a>(
		&'a self,
		key: &'a [u8],
		deleting: bool,
	) -> crate::catalog::providers::BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			if self.quota_catalog_migration.load(std::sync::atomic::Ordering::Relaxed)
				&& crate::key::schema::FieldKey::decode_key(key).is_ok()
			{
				return Ok(());
			}
			// Table keys share the generated namespace/database prefix. Quota
			// catalog keys are outside it, so fencing below cannot recurse.
			if key.len() < 12 || !key.starts_with(b"/*") || key[6] != b'*' || key[11] != b'*' {
				return Ok(());
			}
			let ns = NamespaceId(u32::from_be_bytes(
				key[2..6].try_into().expect("four namespace bytes"),
			));
			let db =
				DatabaseId(u32::from_be_bytes(key[7..11].try_into().expect("four database bytes")));
			self.guard_lightweight_quota_writes(ns, db).await?;
			let Ok(record) = crate::key::schema::RecordKey::decode_key(key) else {
				return Ok(());
			};
			let table = record.tb.into_owned();
			{
				let state = self.quota_state.lock().await;
				if state.current.record_transitions.contains_key(key)
					|| (deleting && state.current.reset_tables.contains(&(ns, db, table.clone())))
				{
					return Ok(());
				}
			}
			self.quota_usage(ns, db).ensure_writable_for_update().await?;
			self.quota_usage(ns, db).bind_database().await?;
			let existed = self.exists(surrealdb_kvs::Key::from(key.to_vec()), None).await?;
			self.quota_state
				.lock()
				.await
				.current
				.record_transitions
				.entry(key.to_vec())
				.or_insert((ns, db, table, existed));
			Ok(())
		})
	}

	/// Lightweight edges have no RecordKey. Until canonical adjacency
	/// accounting exists, reject table mutations when an active record rule
	/// could be bypassed through them. Policy maintenance remains possible.
	// These are storage admission reads, not compiler dependencies. Declare only
	// the actual catalog loads unsealed, retaining normal read recording.
	async fn guard_lightweight_quota_writes(&self, ns: NamespaceId, db: DatabaseId) -> Result<()> {
		let tables = crate::catalog_reads::sealed(false, self.all_tb(ns, db, None)).await?;
		let lightweight = tables
			.iter()
			.filter(|table| {
				matches!(&table.table_type,
			TableType::Relation(relation) if relation.lightweight)
			})
			.collect::<Vec<_>>();
		if lightweight.is_empty() {
			return Ok(());
		}
		self.quota_usage(ns, db).ensure_writable_for_update().await?;
		let generation_key = Qg {
			ns,
			db,
		};
		let generation = self.get_key(&generation_key, None).await?;
		self.put_compare_key(&generation_key, &generation.unwrap_or(0), generation.as_ref())
			.await?;
		if let Some(policy) = self.get_db_quota(ns, db, None).await? {
			for table in lightweight {
				if policy.rules.iter().any(|rule| {
					rule.resource == QuotaResource::Record
						&& selector_matches(&rule.selector, &table.name)
				}) {
					bail!(Error::QuotaUsageInvalid {
						reason: format!(
							"record quota accounting for lightweight relation '{}' is unsupported; remove its matching RECORD rule before table writes",
							table.name
						),
					});
				}
			}
		}
		Ok(())
	}

	pub(crate) fn track_quota_catalog_key<'a>(
		&'a self,
		key: &'a [u8],
		ns: NamespaceId,
		db: DatabaseId,
		table: &'a TableName,
		is_table: bool,
		deleting: bool,
	) -> crate::catalog::providers::BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			self.quota_usage(ns, db).ensure_writable_for_update().await?;
			{
				let state = self.quota_state.lock().await;
				if state.current.catalog_transitions.contains_key(key)
					|| (!is_table
						&& deleting && state.current.reset_tables.contains(&(
						ns,
						db,
						table.clone(),
					))) {
					return Ok(());
				}
			}
			self.quota_usage(ns, db).bind_database().await?;
			let existed = self.billable_catalog_key_exists(key, is_table).await?;
			self.quota_state
				.lock()
				.await
				.current
				.catalog_transitions
				.entry(key.to_vec())
				.or_insert((ns, db, table.clone(), is_table, existed));
			Ok(())
		})
	}

	/// Create a database-scoped quota facade over this transaction.
	pub fn quota_usage(&self, ns: NamespaceId, db: DatabaseId) -> QuotaTransaction<'_> {
		QuotaTransaction {
			tx: self,
			ns,
			db,
		}
	}

	/// Resolve all transaction-local quota intents and stage one conditional
	/// counter update per affected bucket.
	pub async fn flush_quota_usage(&self) -> Result<()> {
		self.flush_quota_usage_inner().await.map_err(|error| {
			if crate::is_retryable_transaction_conflict(&error)
				|| matches!(
					crate::storage_error(&error),
					Some(
						surrealdb_kvs::Error::TransactionConditionNotMet
							| surrealdb_kvs::Error::TransactionKeyAlreadyExists
					)
				) {
				Error::QuotaConflict.into()
			} else {
				error
			}
		})
	}

	// Relation endpoint definitions may be written before their table definition.
	// Resolve eligibility from the final catalog at flush, not at write order.
	async fn billable_catalog_key_exists(&self, key: &[u8], is_table: bool) -> Result<bool> {
		if !self.exists(surrealdb_kvs::Key::from(key.to_vec()), None).await? {
			return Ok(false);
		}
		if !is_table {
			let field = crate::key::schema::FieldKey::decode_key(key)?;
			if matches!(field.fd.as_ref(), "in" | "out") {
				let table = self
					.get_key(
						&crate::key::schema::TableKey {
							ns: field.ns,
							db: field.db,
							tb: field.tb,
						},
						None,
					)
					.await?;
				if table.is_some_and(|table| matches!(table.table_type, TableType::Relation(_))) {
					return Ok(false);
				}
			}
		}
		Ok(true)
	}

	async fn resolve_quota_transitions(
		&self,
		snapshot: &mut QuotaTransactionSnapshot,
	) -> Result<()> {
		for (key, (ns, db, table, existed)) in &snapshot.record_transitions {
			let exists = self.exists(surrealdb_kvs::Key::from(key.clone()), None).await?;
			let delta = i128::from(exists) - i128::from(*existed);
			let entry = snapshot.record_deltas.entry((*ns, *db, table.clone())).or_default();
			*entry = entry.checked_add(delta).ok_or_else(|| Error::QuotaUsageInvalid {
				reason: "transactional record quota delta overflow".to_owned(),
			})?;
		}
		for (key, (ns, db, table, is_table, existed)) in &snapshot.catalog_transitions {
			let exists = self.billable_catalog_key_exists(key, *is_table).await?;
			let delta = i128::from(exists) - i128::from(*existed);
			let deltas = if *is_table {
				&mut snapshot.table_deltas
			} else {
				&mut snapshot.field_deltas
			};
			let entry = deltas.entry((*ns, *db, table.clone())).or_default();
			*entry = entry.checked_add(delta).ok_or_else(|| Error::QuotaUsageInvalid {
				reason: "transactional catalog quota delta overflow".to_owned(),
			})?;
		}
		Ok(())
	}

	async fn projected_quota_snapshot(&self) -> Result<QuotaTransactionSnapshot> {
		let mut snapshot = self.quota_state.lock().await.current.clone();
		self.resolve_quota_transitions(&mut snapshot).await?;
		Ok(snapshot)
	}

	async fn flush_quota_usage_inner(&self) -> Result<()> {
		// Keep registrations and savepoint changes serialized with the flush.
		// Transaction execution is normally sequential, but `Transaction` is
		// shareable and must not lose a late intent between snapshot and clear.
		let mut quota_state = self.quota_state.lock().await;
		let mut snapshot = quota_state.current.clone();
		self.resolve_quota_transitions(&mut snapshot).await?;
		if snapshot.table_deltas.values().all(|delta| *delta == 0)
			&& snapshot.field_deltas.values().all(|delta| *delta == 0)
			&& snapshot.record_deltas.values().all(|delta| *delta == 0)
			&& snapshot.reset_tables.is_empty()
		{
			quota_state.current = QuotaTransactionSnapshot::default();
			return Ok(());
		}
		#[cfg(any(test, feature = "test-hooks"))]
		maybe_inject_quota_fault(QuotaFaultSite::BeforeCounterWrite, self.sequences().node_id())?;
		let mut violations = Vec::new();
		let mut violations_truncated = false;

		for ((ns, db), bound) in &snapshot.databases {
			let facade = self.quota_usage(*ns, *db);
			// Reassert both observed fences. A concurrent usage rebuild or policy
			// generation switch must make this business transaction conflict.
			facade.reassert_meta(&bound.meta).await?;
			let epoch = bound.meta.active_epoch.ok_or_else(|| Error::QuotaUsageInvalid {
				reason: "ready quota usage metadata has no active epoch".to_owned(),
			})?;
			let generation_key = Qg {
				ns: *ns,
				db: *db,
			};
			match bound.observed_generation {
				Some(generation) => {
					self.put_compare_key(&generation_key, &generation, Some(&generation)).await?;
				}
				None => {
					self.put_compare_key(&generation_key, &0, None).await?;
				}
			}

			let mut buckets = BTreeMap::<String, (i128, QuotaLimit, TableName)>::new();
			if let Some(policy) = &bound.policy {
				for ((delta_ns, delta_db, table), delta) in &snapshot.table_deltas {
					if delta_ns != ns || delta_db != db || *delta == 0 {
						continue;
					}
					for rule in policy.rules.iter().filter(|rule| {
						rule.resource == QuotaResource::Table
							&& selector_matches(&rule.selector, table)
					}) {
						let bucket = buckets.entry(rule.id.to_string()).or_insert((
							0,
							rule.limit,
							table.clone(),
						));
						if *delta > 0 {
							bucket.2 = table.clone();
						}
						bucket.0 = bucket.0.checked_add(*delta).ok_or_else(|| {
							Error::QuotaUsageInvalid {
								reason: "transactional table quota delta overflow".to_owned(),
							}
						})?;
					}
				}

				for (rule, (delta, limit, table)) in buckets {
					if delta == 0 {
						continue;
					}
					let key = QuotaTableBucket {
						ns: *ns,
						db: *db,
						epoch,
						generation: policy.generation,
						rule: rule.as_str().into(),
					};
					let current = self.get_key(&key, None).await?;
					let current_value = current.unwrap_or(0);
					let projected = project_counter(current_value, delta)?;
					push_violation(
						&mut violations,
						&mut violations_truncated,
						limit_violation(
							limit,
							&rule,
							"table",
							&table,
							current_value,
							delta,
							projected,
						),
					);
					if projected == 0 {
						self.del_compare_key(&key, current.as_ref()).await?;
					} else {
						self.put_compare_key(&key, &projected, current.as_ref()).await?;
					}
				}
			}

			let field_tables = snapshot
				.field_deltas
				.keys()
				.filter(|(delta_ns, delta_db, _)| delta_ns == ns && delta_db == db)
				.map(|(_, _, table)| table.clone())
				.chain(
					snapshot
						.reset_tables
						.iter()
						.filter(|(reset_ns, reset_db, _)| reset_ns == ns && reset_db == db)
						.map(|(_, _, table)| table.clone()),
				)
				.collect::<BTreeSet<_>>();
			for table in field_tables {
				let delta =
					snapshot.field_deltas.get(&(*ns, *db, table.clone())).copied().unwrap_or(0);
				let reset = snapshot.reset_tables.contains(&(*ns, *db, table.clone()));
				let key = QuotaFieldUsage {
					ns: *ns,
					db: *db,
					epoch,
					table: Cow::Borrowed(&table),
				};
				let current = self.get_key(&key, None).await?;
				let current_value = current.unwrap_or(0);
				let projected = project_counter(
					if reset {
						0
					} else {
						current_value
					},
					delta,
				)?;
				let effective_delta = i128::from(projected) - i128::from(current_value);
				if let Some((rule, limit)) =
					bound.policy.as_deref().and_then(|policy| effective_field_rule(policy, &table))
				{
					push_violation(
						&mut violations,
						&mut violations_truncated,
						limit_violation(
							limit,
							&rule,
							"field",
							&table,
							current_value,
							effective_delta,
							projected,
						),
					);
				}
				if projected == 0 {
					if current.is_some() {
						self.del_compare_key(&key, current.as_ref()).await?;
					}
				} else {
					self.put_compare_key(&key, &projected, current.as_ref()).await?;
				}
			}

			let record_tables = snapshot
				.record_deltas
				.keys()
				.filter(|(delta_ns, delta_db, _)| delta_ns == ns && delta_db == db)
				.map(|(_, _, table)| table.clone())
				.chain(
					snapshot
						.reset_tables
						.iter()
						.filter(|(reset_ns, reset_db, _)| reset_ns == ns && reset_db == db)
						.map(|(_, _, table)| table.clone()),
				)
				.collect::<BTreeSet<_>>();
			for table in record_tables {
				let delta =
					snapshot.record_deltas.get(&(*ns, *db, table.clone())).copied().unwrap_or(0);
				let reset = snapshot.reset_tables.contains(&(*ns, *db, table.clone()));
				let key = QuotaRecordUsage {
					ns: *ns,
					db: *db,
					epoch,
					table: Cow::Borrowed(&table),
				};
				let current = self.get_key(&key, None).await?;
				let current_value = current.unwrap_or(0);
				let projected = project_counter(
					if reset {
						0
					} else {
						current_value
					},
					delta,
				)?;
				let effective_delta = i128::from(projected) - i128::from(current_value);
				if let Some((rule, limit)) =
					bound.policy.as_deref().and_then(|policy| effective_record_rule(policy, &table))
				{
					push_violation(
						&mut violations,
						&mut violations_truncated,
						limit_violation(
							limit,
							&rule,
							"record",
							&table,
							current_value,
							effective_delta,
							projected,
						),
					);
				}
				if projected == 0 {
					if current.is_some() {
						self.del_compare_key(&key, current.as_ref()).await?;
					}
				} else {
					self.put_compare_key(&key, &projected, current.as_ref()).await?;
				}
			}
		}

		#[cfg(any(test, feature = "test-hooks"))]
		maybe_inject_quota_fault(QuotaFaultSite::AfterCounterWrite, self.sequences().node_id())?;
		if !violations.is_empty() {
			let policy = snapshot
				.databases
				.values()
				.find_map(|bound| bound.policy.as_deref())
				.expect("quota violations require an active policy");
			bail!(Error::QuotaExceeded(Box::new(QuotaExceededError {
				database: policy.database.to_string(),
				generation: policy.generation,
				violations,
				truncated: violations_truncated,
			})));
		}
		quota_state.current = QuotaTransactionSnapshot::default();
		quota_state.flushed_admission = true;
		Ok(())
	}

	/// Whether this transaction staged quota admission fences or counters.
	pub async fn has_flushed_quota_admission(&self) -> bool {
		self.quota_state.lock().await.flushed_admission
	}

	/// Check before installing a rebuild maintenance fence. The scan repeats
	/// this check in its fenced snapshot, so schema changes cannot evade it.
	pub async fn ensure_quota_rebuild_supported(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<()> {
		for table in crate::catalog_reads::sealed(false, self.all_tb(ns, db, None)).await?.iter() {
			ensure_record_scan_supported(&table.name, &table.table_type)?;
		}
		Ok(())
	}

	/// Scan the complete database catalog and record keyspace into canonical
	/// quota counters. The caller owns the surrounding consistent read
	/// transaction and must run this only after committing the maintenance fence.
	pub async fn scan_quota_usage(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<QuotaRebuildScan> {
		let policy = self.get_db_quota(ns, db, None).await?;
		let tables = crate::catalog_reads::sealed(false, self.all_tb(ns, db, None)).await?;
		for table in tables.iter() {
			ensure_record_scan_supported(&table.name, &table.table_type)?;
		}
		let mut scan = QuotaRebuildScan {
			tables: u64::try_from(tables.len()).map_err(|_| Error::QuotaUsageInvalid {
				reason: "table catalog size does not fit in quota rebuild count".to_owned(),
			})?,
			..QuotaRebuildScan::default()
		};

		if let Some(policy) = policy.as_deref() {
			for rule in policy.rules.iter().filter(|rule| rule.resource == QuotaResource::Table) {
				let count = tables
					.iter()
					.filter(|table| selector_matches(&rule.selector, &table.name))
					.count();
				let count = u64::try_from(count).map_err(|_| Error::QuotaUsageInvalid {
					reason: "table quota bucket size does not fit in rebuild counter".to_owned(),
				})?;
				scan.snapshot.set_table_bucket_count(policy.generation, rule.id.as_str(), count);
			}
		}

		for table in tables.iter() {
			let fields =
				crate::catalog_reads::sealed(false, self.all_tb_fields(ns, db, &table.name, None))
					.await?;
			let field_count = u64::try_from(
				fields
					.iter()
					.filter(|field| {
						field.key_part.is_none()
							&& (!matches!(&table.table_type, TableType::Relation(_))
								|| !matches!(field.name.to_raw_string().as_str(), "in" | "out"))
					})
					.count(),
			)
			.map_err(|_| Error::QuotaUsageInvalid {
				reason: format!(
					"field catalog size for table '{}' does not fit in rebuild counter",
					table.name
				),
			})?;
			scan.fields =
				scan.fields.checked_add(field_count).ok_or_else(|| Error::QuotaUsageInvalid {
					reason: "quota rebuild field scan count overflow".to_owned(),
				})?;
			scan.snapshot.set_field_count(&table.name, field_count);

			// Materialized document and packed rows both occupy exactly one
			// RecordKey. Layout generations live inside its value, while graph,
			// view-state and layout metadata live outside this typed prefix.
			// Lightweight relations are rejected above: they have no record key.
			let records = crate::key::schema::RecordPrefix {
				ns,
				db,
				tb: Cow::Borrowed(&table.name),
			}
			.range()?;
			let record_count = u64::try_from(self.count(records, None).await?).map_err(|_| {
				Error::QuotaUsageInvalid {
					reason: format!(
						"record count for table '{}' does not fit in rebuild counter",
						table.name
					),
				}
			})?;
			scan.records =
				scan.records.checked_add(record_count).ok_or_else(|| Error::QuotaUsageInvalid {
					reason: "quota rebuild record scan count overflow".to_owned(),
				})?;
			scan.snapshot.set_record_count(&table.name, record_count);
		}
		Ok(scan)
	}
}

impl QuotaTransaction<'_> {
	/// Build the stable `INFO FOR QUOTA ... STRUCTURE` representation from one
	/// transaction-consistent policy, ledger, catalog, and counter snapshot.
	pub async fn info_structure(
		&self,
		database: &str,
		policy: Option<&QuotaPolicyDefinition>,
		tables: &[TableDefinition],
	) -> Result<Value> {
		let meta = self.meta().await?;
		let usage_trusted = meta.state == QuotaUsageState::Ready;
		let policy_value = policy.map_or(Value::None, quota_policy_value);
		let usage = if usage_trusted {
			self.info_usage(policy, tables).await?
		} else {
			Value::None
		};
		let latest_change = self
			.tx
			.get_key(
				&Ql {
					ns: self.ns,
					db: self.db,
				},
				None,
			)
			.await?
			.map_or(Value::None, |change| {
				Value::Object(Object::from(map! {
					"action" => change.action.into(),
					"actor" => change.actor.into(),
					"changed_at" => Value::Datetime(change.changed_at),
					"generation" => change.generation.into(),
					"operation_id" => change.operation_id.into(),
				}))
			});
		Ok(Value::Object(Object::from(map! {
			"database" => database.into(),
			"format_version" => 1u64.into(),
			"latest_change" => latest_change,
			"ledger" => Value::Object(Object::from(map! {
				"active_epoch" => meta.active_epoch.map_or(Value::None, Value::from),
				"state" => quota_usage_state_name(meta.state).into(),
				"usage_trusted" => usage_trusted.into(),
			})),
			"observed_at" => Value::Datetime(Datetime::now()),
			"policy" => policy_value,
			"usage" => usage,
		})))
	}

	/// Install every non-zero counter from a trusted scan into the currently
	/// staged epoch.
	pub async fn stage_rebuild_scan(&self, scan: &QuotaRebuildScan) -> Result<()> {
		for ((generation, rule), count) in &scan.snapshot.table_buckets {
			self.set_staged_table_bucket_count(*generation, rule, *count).await?;
		}
		for (table, count) in &scan.snapshot.field_counts {
			self.set_staged_field_count(table, *count).await?;
		}
		for (table, count) in &scan.snapshot.record_counts {
			self.set_staged_record_count(table, *count).await?;
		}
		Ok(())
	}

	async fn info_usage(
		&self,
		policy: Option<&QuotaPolicyDefinition>,
		tables: &[TableDefinition],
	) -> Result<Value> {
		let mut tables = tables.iter().collect::<Vec<_>>();
		tables.sort_by(|left, right| left.name.cmp(&right.name));

		let mut table_buckets = Vec::new();
		if let Some(policy) = policy {
			for rule in policy.rules.iter().filter(|rule| rule.resource == QuotaResource::Table) {
				let used = self.table_bucket_count(policy.generation, rule.id.as_str()).await?;
				table_buckets.push(Value::Object(Object::from(map! {
					"exceeded" => quota_exceeded(used, rule.limit).into(),
					"limit" => quota_limit_value(rule.limit),
					"remaining" => quota_remaining(used, rule.limit),
					"rule_id" => rule.id.to_string().into(),
					"used" => used.into(),
				})));
			}
		}

		let mut unmatched_table = Vec::new();
		let mut unmatched_field = Vec::new();
		let mut unmatched_record = Vec::new();
		let mut table_usage = Vec::with_capacity(tables.len());
		for table in tables {
			let table_name = &table.name;
			let table_matched = matching_rules(policy, QuotaResource::Table, table_name);
			if table_matched.is_empty() {
				unmatched_table.push(Value::from(table_name.to_string()));
			}

			let field_matched = matching_rules(policy, QuotaResource::Field, table_name);
			if field_matched.is_empty() {
				unmatched_field.push(Value::from(table_name.to_string()));
			}
			let field_used = self.field_count(table_name).await?;
			let field = effective_usage_value(field_used, &field_matched);

			let record_matched = matching_rules(policy, QuotaResource::Record, table_name);
			if record_matched.is_empty() {
				unmatched_record.push(Value::from(table_name.to_string()));
			}
			let record_used = self.record_count(table_name).await?;
			let record = effective_usage_value(record_used, &record_matched);

			table_usage.push(Value::Object(Object::from(map! {
				"field" => field,
				"record" => record,
				"table" => table_name.to_string().into(),
			})));
		}

		Ok(Value::Object(Object::from(map! {
			"table_buckets" => table_buckets.into(),
			"tables" => table_usage.into(),
			"unmatched" => Value::Object(Object::from(map! {
				"field" => unmatched_field.into(),
				"record" => unmatched_record.into(),
				"table" => unmatched_table.into(),
			})),
		})))
	}

	/// Materialise table-rule buckets for a newly installed policy generation.
	///
	/// Policy assignment may intentionally place a database over its new limit,
	/// so this seeds observed usage without applying admission checks.
	pub async fn initialize_policy_table_buckets(
		&self,
		policy: &QuotaPolicyDefinition,
		tables: &[TableDefinition],
	) -> Result<()> {
		let meta = self.ensure_writable_for_update().await?;
		let epoch = meta.active_epoch.expect("ready metadata has an active epoch");
		for rule in policy.rules.iter().filter(|rule| rule.resource == QuotaResource::Table) {
			let count =
				tables.iter().filter(|table| selector_matches(&rule.selector, &table.name)).count();
			let count = u64::try_from(count).map_err(|_| Error::QuotaUsageInvalid {
				reason: "table catalog count does not fit in quota counter".to_owned(),
			})?;
			if count == 0 {
				continue;
			}
			let key = QuotaTableBucket {
				ns: self.ns,
				db: self.db,
				epoch,
				generation: policy.generation,
				rule: rule.id.as_str().into(),
			};
			self.tx.put_compare_key(&key, &count, None).await?;
		}
		Ok(())
	}

	async fn bind_database(&self) -> Result<()> {
		if self.tx.quota_state.lock().await.current.databases.contains_key(&(self.ns, self.db)) {
			return Ok(());
		}
		let meta = self.ensure_writable().await?;
		let observed_generation = self
			.tx
			.get_key(
				&Qg {
					ns: self.ns,
					db: self.db,
				},
				None,
			)
			.await?;
		let policy = self.tx.get_db_quota(self.ns, self.db, None).await?;
		if let Some(policy) = &policy
			&& Some(policy.generation) != observed_generation
		{
			bail!(Error::QuotaPolicyChanged {
				database: policy.database.to_string(),
				expected: observed_generation.unwrap_or(0),
				actual: policy.generation,
			});
		}
		self.tx.quota_state.lock().await.current.databases.entry((self.ns, self.db)).or_insert(
			BoundQuotaDatabase {
				observed_generation,
				meta,
				policy,
			},
		);
		Ok(())
	}

	/// Record one physical-table existence transition in the transaction-local ledger.
	pub async fn register_table_delta(&self, table: &TableName, delta: i128) -> Result<()> {
		if delta == 0 {
			return Ok(());
		}
		self.bind_database().await?;
		let mut state = self.tx.quota_state.lock().await;
		let entry =
			state.current.table_deltas.entry((self.ns, self.db, table.clone())).or_default();
		*entry = entry.checked_add(delta).ok_or_else(|| Error::QuotaUsageInvalid {
			reason: "transactional table quota delta overflow".to_owned(),
		})?;
		Ok(())
	}

	/// Record a table deletion and reset all per-table usage without scanning records.
	pub async fn register_table_removal(&self, table: &TableName) -> Result<()> {
		self.register_table_delta(table, -1).await?;
		self.reset_table_usage(table).await
	}

	pub(crate) async fn reset_table_usage(&self, table: &TableName) -> Result<()> {
		let mut state = self.tx.quota_state.lock().await;
		let key = (self.ns, self.db, table.clone());
		// Earlier per-field deltas are subsumed by the table deletion. Only
		// fields and records created after a possible recreation contribute to
		// the new table.
		state.current.field_deltas.remove(&key);
		state.current.record_deltas.remove(&key);
		state
			.current
			.record_transitions
			.retain(|_, (ns, db, table, _)| (*ns, *db, table.clone()) != key);
		state.current.catalog_transitions.retain(|_, (ns, db, table, is_table, _)| {
			*is_table || (*ns, *db, table.clone()) != key
		});
		state.current.reset_tables.insert(key);
		Ok(())
	}

	/// Record one explicit field-definition existence transition.
	pub async fn register_field_delta(&self, table: &TableName, delta: i128) -> Result<()> {
		if delta == 0 {
			return Ok(());
		}
		self.bind_database().await?;
		let mut state = self.tx.quota_state.lock().await;
		let entry =
			state.current.field_deltas.entry((self.ns, self.db, table.clone())).or_default();
		*entry = entry.checked_add(delta).ok_or_else(|| Error::QuotaUsageInvalid {
			reason: "transactional field quota delta overflow".to_owned(),
		})?;
		Ok(())
	}

	/// Record one logical record existence transition.
	pub async fn register_record_delta(&self, table: &TableName, delta: i128) -> Result<()> {
		if delta == 0 {
			return Ok(());
		}
		self.bind_database().await?;
		let mut state = self.tx.quota_state.lock().await;
		let entry =
			state.current.record_deltas.entry((self.ns, self.db, table.clone())).or_default();
		*entry = entry.checked_add(delta).ok_or_else(|| Error::QuotaUsageInvalid {
			reason: "transactional record quota delta overflow".to_owned(),
		})?;
		Ok(())
	}

	/// Initialise the protected empty ledger for a newly-created database.
	pub async fn initialize_new_database(&self) -> Result<()> {
		let key = Qm {
			ns: self.ns,
			db: self.db,
		};
		self.tx.put_key(&key, &QuotaUsageMeta::ready_empty()).await
	}

	/// Read and validate usage metadata. A missing marker is an uninitialised legacy database.
	pub async fn meta(&self) -> Result<QuotaUsageMeta> {
		Ok(self.stored_meta().await?.unwrap_or_else(QuotaUsageMeta::uninitialized))
	}

	async fn stored_meta(&self) -> Result<Option<QuotaUsageMeta>> {
		let meta = self
			.tx
			.get_key(
				&Qm {
					ns: self.ns,
					db: self.db,
				},
				None,
			)
			.await?;
		if let Some(meta) = &meta {
			meta.validate()?;
		}
		Ok(meta)
	}

	/// Reject normal writes unless the active ledger has been validated.
	pub async fn ensure_writable(&self) -> Result<QuotaUsageMeta> {
		self.tx.ensure_no_native_quota_migration().await?;
		let meta = self.meta().await?;
		if meta.state != QuotaUsageState::Ready {
			bail!(Error::QuotaUsageNotReady {
				state: format!("{:?}", meta.state).to_ascii_lowercase(),
			});
		}
		Ok(meta)
	}

	/// Reassert observed metadata so this transaction conflicts with a
	/// concurrent maintenance-state transition at commit time.
	async fn reassert_meta(&self, meta: &QuotaUsageMeta) -> Result<()> {
		self.tx
			.put_compare_key(
				&Qm {
					ns: self.ns,
					db: self.db,
				},
				meta,
				Some(meta),
			)
			.await
	}

	/// Guard a protected metadata mutation against a concurrent maintenance
	/// fence without making ordinary, non-quota writes contend on `Qm`.
	pub async fn ensure_writable_for_update(&self) -> Result<QuotaUsageMeta> {
		let meta = self.ensure_writable().await?;
		self.reassert_meta(&meta).await?;
		Ok(meta)
	}

	/// Persist the maintenance fence and allocate a new staged epoch.
	pub async fn begin_rebuild(&self) -> Result<u64> {
		let key = Qm {
			ns: self.ns,
			db: self.db,
		};
		let current = self.stored_meta().await?;
		let mut meta = current.clone().unwrap_or_else(QuotaUsageMeta::uninitialized);
		let staged_epoch =
			meta.epoch_high_water.checked_add(1).ok_or_else(|| Error::QuotaUsageInvalid {
				reason: "quota usage epoch overflow".to_owned(),
			})?;
		meta.state = QuotaUsageState::Rebuilding;
		meta.staged_epoch = Some(staged_epoch);
		meta.epoch_high_water = staged_epoch;
		meta.validate()?;
		self.tx.put_compare_key(&key, &meta, current.as_ref()).await?;
		Ok(staged_epoch)
	}

	/// Persist the read-only fence before a raw restore, prefix copy, or offline mutation begins.
	///
	/// The caller must commit this state before installing external data. Only a
	/// later trusted rebuild may return the database to `Ready`.
	pub async fn begin_external_write_maintenance(&self) -> Result<()> {
		let key = Qm {
			ns: self.ns,
			db: self.db,
		};
		let current = self.stored_meta().await?;
		let meta = QuotaUsageMeta {
			epoch_high_water: current.as_ref().map_or(0, |meta| meta.epoch_high_water),
			..QuotaUsageMeta::uninitialized()
		};
		meta.validate()?;
		self.tx.put_compare_key(&key, &meta, current.as_ref()).await
	}

	/// Mark the ledger corrupt while retaining its last active epoch for diagnostics.
	pub async fn mark_corrupt(&self) -> Result<()> {
		let key = Qm {
			ns: self.ns,
			db: self.db,
		};
		let current = self.stored_meta().await?;
		let previous = current.clone().unwrap_or_else(QuotaUsageMeta::uninitialized);
		let meta = QuotaUsageMeta {
			state: QuotaUsageState::Corrupt,
			staged_epoch: None,
			..previous
		};
		meta.validate()?;
		self.tx.put_compare_key(&key, &meta, current.as_ref()).await
	}

	async fn staged_epoch(&self) -> Result<u64> {
		let meta = self.meta().await?;
		if meta.state != QuotaUsageState::Rebuilding {
			bail!(Error::QuotaUsageInvalid {
				reason: "quota usage ledger is not rebuilding".to_owned(),
			});
		}
		meta.staged_epoch.ok_or_else(|| {
			Error::QuotaUsageInvalid {
				reason: "rebuilding quota usage has no staged epoch".to_owned(),
			}
			.into()
		})
	}

	async fn staged_epoch_for_update(&self) -> Result<u64> {
		let meta = self.meta().await?;
		if meta.state != QuotaUsageState::Rebuilding {
			bail!(Error::QuotaUsageInvalid {
				reason: "quota usage ledger is not rebuilding".to_owned(),
			});
		}
		let epoch = meta.staged_epoch.ok_or_else(|| Error::QuotaUsageInvalid {
			reason: "rebuilding quota usage has no staged epoch".to_owned(),
		})?;
		self.reassert_meta(&meta).await?;
		Ok(epoch)
	}

	/// Replace one staged record counter with the rebuilt snapshot count.
	pub async fn set_staged_record_count(&self, table: &TableName, count: u64) -> Result<()> {
		let epoch = self.staged_epoch_for_update().await?;
		let key = QuotaRecordUsage {
			ns: self.ns,
			db: self.db,
			epoch,
			table: Cow::Borrowed(table),
		};
		let current = self.tx.get_key(&key, None).await?;
		if count == 0 {
			self.tx.del_compare_key(&key, current.as_ref()).await
		} else {
			self.tx.put_compare_key(&key, &count, current.as_ref()).await
		}
	}

	/// Replace one staged field counter with the rebuilt snapshot count.
	pub async fn set_staged_field_count(&self, table: &TableName, count: u64) -> Result<()> {
		let epoch = self.staged_epoch_for_update().await?;
		let key = QuotaFieldUsage {
			ns: self.ns,
			db: self.db,
			epoch,
			table: Cow::Borrowed(table),
		};
		let current = self.tx.get_key(&key, None).await?;
		if count == 0 {
			self.tx.del_compare_key(&key, current.as_ref()).await
		} else {
			self.tx.put_compare_key(&key, &count, current.as_ref()).await
		}
	}

	/// Replace one staged table-rule bucket with the rebuilt snapshot count.
	pub async fn set_staged_table_bucket_count(
		&self,
		generation: u64,
		rule: &str,
		count: u64,
	) -> Result<()> {
		let epoch = self.staged_epoch_for_update().await?;
		let key = QuotaTableBucket {
			ns: self.ns,
			db: self.db,
			epoch,
			generation,
			rule: (rule).into(),
		};
		let current = self.tx.get_key(&key, None).await?;
		if count == 0 {
			self.tx.del_compare_key(&key, current.as_ref()).await
		} else {
			self.tx.put_compare_key(&key, &count, current.as_ref()).await
		}
	}

	async fn read_epoch_snapshot(&self, epoch: u64) -> Result<QuotaUsageSnapshot> {
		let mut snapshot = QuotaUsageSnapshot::default();
		let mut next = Some(
			QuotaEpochRoot {
				ns: self.ns,
				db: self.db,
				epoch,
			}
			.range()?,
		);
		while let Some(range) = next {
			let batch = self.tx.batch_keys_vals_raw(range.clone(), NORMAL_BATCH_SIZE, None).await?;
			next = if batch.next.is_some() {
				batch
					.result
					.last()
					.map(|(key, _)| range.resume_after(key, crate::Direction::Forward))
			} else {
				None
			};
			for (key, value) in batch.result {
				let count =
					u64::kv_decode_value(&value, ()).map_err(|error| Error::QuotaUsageInvalid {
						reason: format!(
							"invalid counter in staged quota usage epoch {epoch}: {error}"
						),
					})?;
				if count == 0 {
					bail!(Error::QuotaUsageInvalid {
						reason: format!("zero counter stored in staged quota usage epoch {epoch}"),
					});
				}
				if let Ok(decoded) = QuotaTableBucket::decode_key(&key) {
					if decoded.ns != self.ns || decoded.db != self.db || decoded.epoch != epoch {
						bail!(Error::QuotaUsageInvalid {
							reason: format!("counter escaped staged quota usage epoch {epoch}"),
						});
					}
					snapshot.set_table_bucket_count(decoded.generation, &decoded.rule, count);
				} else if let Ok(decoded) = QuotaFieldUsage::decode_key(&key) {
					if decoded.ns != self.ns || decoded.db != self.db || decoded.epoch != epoch {
						bail!(Error::QuotaUsageInvalid {
							reason: format!("counter escaped staged quota usage epoch {epoch}"),
						});
					}
					snapshot.set_field_count(&decoded.table, count);
				} else if let Ok(decoded) = QuotaRecordUsage::decode_key(&key) {
					if decoded.ns != self.ns || decoded.db != self.db || decoded.epoch != epoch {
						bail!(Error::QuotaUsageInvalid {
							reason: format!("counter escaped staged quota usage epoch {epoch}"),
						});
					}
					snapshot.set_record_count(&decoded.table, count);
				} else {
					bail!(Error::QuotaUsageInvalid {
						reason: format!("unknown counter key in staged quota usage epoch {epoch}"),
					});
				}
			}
		}
		Ok(snapshot)
	}

	/// Compare every staged counter with a trusted catalog/record scan and return activation proof.
	pub async fn validate_staged_epoch(
		&self,
		expected: &QuotaUsageSnapshot,
	) -> Result<ValidatedQuotaEpoch> {
		let epoch = self.staged_epoch().await?;
		let actual = self.read_epoch_snapshot(epoch).await?;
		if actual != *expected {
			bail!(Error::QuotaUsageInvalid {
				reason: format!(
					"staged quota usage epoch {epoch} does not match trusted rebuild snapshot \
					 (expected {} counters, found {})",
					expected.counter_count(),
					actual.counter_count()
				),
			});
		}
		Ok(ValidatedQuotaEpoch {
			ns: self.ns,
			db: self.db,
			epoch,
			snapshot: expected.clone(),
		})
	}

	/// Atomically make a validated staged epoch active and remove the maintenance fence.
	pub async fn activate_validated_epoch(&self, validated: ValidatedQuotaEpoch) -> Result<()> {
		if validated.ns != self.ns || validated.db != self.db {
			bail!(Error::QuotaUsageInvalid {
				reason: "validated quota epoch belongs to another database".to_owned(),
			});
		}
		let key = Qm {
			ns: self.ns,
			db: self.db,
		};
		let current = self.stored_meta().await?.ok_or_else(|| Error::QuotaUsageInvalid {
			reason: "validated quota epoch has no persisted usage metadata".to_owned(),
		})?;
		let mut meta = current.clone();
		if meta.state != QuotaUsageState::Rebuilding || meta.staged_epoch != Some(validated.epoch) {
			bail!(Error::QuotaUsageInvalid {
				reason: "validated quota epoch is stale".to_owned(),
			});
		}
		// Re-scan at activation so mutations after proof creation cannot change
		// the complete counter set or values before the epoch switch.
		let actual = self.read_epoch_snapshot(validated.epoch).await?;
		if actual != validated.snapshot {
			bail!(Error::QuotaUsageInvalid {
				reason: format!(
					"validated quota usage epoch {} changed before activation",
					validated.epoch
				),
			});
		}
		meta.state = QuotaUsageState::Ready;
		meta.active_epoch = Some(validated.epoch);
		meta.staged_epoch = None;
		meta.validate()?;
		self.tx.put_compare_key(&key, &meta, Some(&current)).await
	}

	/// Remove a non-active, non-staged epoch after a successful switch.
	pub async fn clear_inactive_epoch(&self, epoch: u64) -> Result<()> {
		let meta = self.meta().await?;
		if meta.active_epoch == Some(epoch) || meta.staged_epoch == Some(epoch) {
			bail!(Error::QuotaUsageInvalid {
				reason: format!("cannot clear referenced quota usage epoch {epoch}"),
			});
		}
		self.tx
			.delr(
				QuotaEpochRoot {
					ns: self.ns,
					db: self.db,
					epoch,
				}
				.range()?,
			)
			.await
	}

	async fn active_epoch_for_read(&self) -> Result<u64> {
		self.meta().await?.active_epoch.ok_or_else(|| {
			Error::QuotaUsageInvalid {
				reason: "quota usage ledger does not have an active epoch".to_owned(),
			}
			.into()
		})
	}

	async fn active_epoch_for_update(&self) -> Result<u64> {
		let meta = self.ensure_writable_for_update().await?;
		Ok(meta.active_epoch.expect("ready metadata has an active epoch"))
	}

	async fn increment<K>(&self, key: K, amount: u64) -> Result<u64>
	where
		K: KVKey<Value = u64> + std::fmt::Debug,
	{
		let current = self.tx.get_key(&key, None).await?;
		let current_value = current.unwrap_or(0);
		let next = current_value.checked_add(amount).ok_or_else(|| Error::QuotaUsageInvalid {
			reason: "quota usage counter overflow".to_owned(),
		})?;
		if amount != 0 {
			self.tx.put_compare_key(&key, &next, current.as_ref()).await?;
		}
		Ok(next)
	}

	async fn decrement<K>(&self, key: K, amount: u64) -> Result<u64>
	where
		K: KVKey<Value = u64> + std::fmt::Debug,
	{
		let current = self.tx.get_key(&key, None).await?;
		let current_value = current.unwrap_or(0);
		let next = current_value.checked_sub(amount).ok_or_else(|| Error::QuotaUsageInvalid {
			reason: format!(
				"quota usage counter underflow: cannot release {amount} from {current_value}"
			),
		})?;
		if amount != 0 {
			if next == 0 {
				self.tx.del_compare_key(&key, current.as_ref()).await?;
			} else {
				self.tx.put_compare_key(&key, &next, current.as_ref()).await?;
			}
		}
		Ok(next)
	}

	/// Return the active record count for a physical table.
	pub async fn record_count(&self, table: &TableName) -> Result<u64> {
		let epoch = self.active_epoch_for_read().await?;
		let stored = self
			.tx
			.get_key(
				&QuotaRecordUsage {
					ns: self.ns,
					db: self.db,
					epoch,
					table: Cow::Borrowed(table),
				},
				None,
			)
			.await?
			.unwrap_or(0);
		let snapshot = self.tx.projected_quota_snapshot().await?;
		let key = (self.ns, self.db, table.clone());
		let base = if snapshot.reset_tables.contains(&key) {
			0
		} else {
			stored
		};
		project_counter(base, snapshot.record_deltas.get(&key).copied().unwrap_or(0))
	}

	/// Increment the active record count in this transaction.
	pub async fn increment_record_count(&self, table: &TableName, amount: u64) -> Result<u64> {
		let epoch = self.active_epoch_for_update().await?;
		self.increment(
			QuotaRecordUsage {
				ns: self.ns,
				db: self.db,
				epoch,
				table: Cow::Borrowed(table),
			},
			amount,
		)
		.await
	}

	/// Decrement the active record count in this transaction.
	pub async fn decrement_record_count(&self, table: &TableName, amount: u64) -> Result<u64> {
		let epoch = self.active_epoch_for_update().await?;
		self.decrement(
			QuotaRecordUsage {
				ns: self.ns,
				db: self.db,
				epoch,
				table: Cow::Borrowed(table),
			},
			amount,
		)
		.await
	}

	/// Return the active field count for a physical table.
	pub async fn field_count(&self, table: &TableName) -> Result<u64> {
		let epoch = self.active_epoch_for_read().await?;
		let stored = self
			.tx
			.get_key(
				&QuotaFieldUsage {
					ns: self.ns,
					db: self.db,
					epoch,
					table: Cow::Borrowed(table),
				},
				None,
			)
			.await?
			.unwrap_or(0);
		let snapshot = self.tx.projected_quota_snapshot().await?;
		let key = (self.ns, self.db, table.clone());
		let base = if snapshot.reset_tables.contains(&key) {
			0
		} else {
			stored
		};
		project_counter(base, snapshot.field_deltas.get(&key).copied().unwrap_or(0))
	}

	/// Increment the active field count in this transaction.
	pub async fn increment_field_count(&self, table: &TableName, amount: u64) -> Result<u64> {
		let epoch = self.active_epoch_for_update().await?;
		self.increment(
			QuotaFieldUsage {
				ns: self.ns,
				db: self.db,
				epoch,
				table: Cow::Borrowed(table),
			},
			amount,
		)
		.await
	}

	/// Decrement the active field count in this transaction.
	pub async fn decrement_field_count(&self, table: &TableName, amount: u64) -> Result<u64> {
		let epoch = self.active_epoch_for_update().await?;
		self.decrement(
			QuotaFieldUsage {
				ns: self.ns,
				db: self.db,
				epoch,
				table: Cow::Borrowed(table),
			},
			amount,
		)
		.await
	}

	/// Return the active table count charged to a policy generation and rule.
	pub async fn table_bucket_count(&self, generation: u64, rule: &str) -> Result<u64> {
		let epoch = self.active_epoch_for_read().await?;
		let stored = self
			.tx
			.get_key(
				&QuotaTableBucket {
					ns: self.ns,
					db: self.db,
					epoch,
					generation,
					rule: (rule).into(),
				},
				None,
			)
			.await?
			.unwrap_or(0);
		let snapshot = self.tx.projected_quota_snapshot().await?;
		let mut delta = 0i128;
		if let Some(policy) = self.tx.get_db_quota(self.ns, self.db, None).await?
			&& policy.generation == generation
			&& let Some(rule) = policy
				.rules
				.iter()
				.find(|entry| entry.id.as_str() == rule && entry.resource == QuotaResource::Table)
		{
			for ((ns, db, table), change) in &snapshot.table_deltas {
				if (*ns, *db) == (self.ns, self.db) && selector_matches(&rule.selector, table) {
					delta = delta.checked_add(*change).ok_or_else(|| Error::QuotaUsageInvalid {
						reason: "projected table count overflow".to_owned(),
					})?;
				}
			}
		}
		project_counter(stored, delta)
	}

	/// Increment an active table-rule bucket in this transaction.
	pub async fn increment_table_bucket_count(
		&self,
		generation: u64,
		rule: &str,
		amount: u64,
	) -> Result<u64> {
		let epoch = self.active_epoch_for_update().await?;
		self.increment(
			QuotaTableBucket {
				ns: self.ns,
				db: self.db,
				epoch,
				generation,
				rule: (rule).into(),
			},
			amount,
		)
		.await
	}

	/// Decrement an active table-rule bucket in this transaction.
	pub async fn decrement_table_bucket_count(
		&self,
		generation: u64,
		rule: &str,
		amount: u64,
	) -> Result<u64> {
		let epoch = self.active_epoch_for_update().await?;
		self.decrement(
			QuotaTableBucket {
				ns: self.ns,
				db: self.db,
				epoch,
				generation,
				rule: (rule).into(),
			},
			amount,
		)
		.await
	}
}

// A lightweight edge exists in adjacency storage without an edge-side record.
// Counting RecordPrefix would silently certify zero usage, so fail closed until
// a canonical logical-edge scanner is implemented for both adjacency formats.
fn ensure_record_scan_supported(table: &TableName, table_type: &TableType) -> Result<()> {
	if matches!(table_type, TableType::Relation(relation) if relation.lightweight) {
		bail!(Error::QuotaUsageInvalid {
			reason: format!(
				"quota usage rebuild cannot certify lightweight relation '{table}': its logical edges are stored outside the record keyspace"
			),
		});
	}
	Ok(())
}

fn quota_usage_state_name(state: QuotaUsageState) -> &'static str {
	match state {
		QuotaUsageState::Uninitialized => "uninitialized",
		QuotaUsageState::Rebuilding => "rebuilding",
		QuotaUsageState::Ready => "ready",
		QuotaUsageState::Corrupt => "corrupt",
	}
}

/// Lightweight database INFO discovery summary. It intentionally omits rules
/// and usage so broad schema discovery does not become a high-cardinality scan.
pub fn quota_summary_value(
	policy: Option<&QuotaPolicyDefinition>,
	generation: Option<u64>,
) -> Value {
	Value::Object(Object::from(map! {
		"defined" => policy.is_some().into(),
		"generation" => generation
			.filter(|generation| *generation > 0)
			.map_or(Value::None, Value::from),
	}))
}

fn quota_policy_value(policy: &QuotaPolicyDefinition) -> Value {
	let rules = policy.rules.iter().map(quota_rule_value).collect::<Vec<_>>();
	Value::Object(Object::from(map! {
		"generation" => policy.generation.into(),
		"rules" => rules.into(),
	}))
}

fn quota_rule_value(rule: &crate::catalog::QuotaRuleDefinition) -> Value {
	let resource = match rule.resource {
		QuotaResource::Table => "table",
		QuotaResource::Field => "field",
		QuotaResource::Record => "record",
	};
	let selector = match &rule.selector {
		QuotaSelector::Exact(table) => Value::Object(Object::from(map! {
			"kind" => "exact".into(),
			"table" => table.to_string().into(),
		})),
		QuotaSelector::Regex(regex) => Value::Object(Object::from(map! {
			"kind" => "regex".into(),
			"pattern" => regex.inner().as_str().into(),
		})),
	};
	Value::Object(Object::from(map! {
		"limit" => quota_limit_value(rule.limit),
		"resource" => resource.into(),
		"rule_id" => rule.id.to_string().into(),
		"selector" => selector,
	}))
}

fn quota_limit_value(limit: QuotaLimit) -> Value {
	match limit {
		QuotaLimit::Finite(value) => Value::Object(Object::from(map! {
			"kind" => "finite".into(),
			"value" => value.into(),
		})),
		QuotaLimit::Unlimited => Value::Object(Object::from(map! {
			"kind" => "unlimited".into(),
		})),
	}
}

fn quota_remaining(used: u64, limit: QuotaLimit) -> Value {
	match limit {
		QuotaLimit::Finite(limit) => limit.saturating_sub(used).into(),
		QuotaLimit::Unlimited => Value::None,
	}
}

fn quota_exceeded(used: u64, limit: QuotaLimit) -> bool {
	matches!(limit, QuotaLimit::Finite(limit) if used > limit)
}

fn matching_rules<'a>(
	policy: Option<&'a QuotaPolicyDefinition>,
	resource: QuotaResource,
	table: &TableName,
) -> Vec<&'a crate::catalog::QuotaRuleDefinition> {
	policy
		.into_iter()
		.flat_map(|policy| policy.rules.iter())
		.filter(|rule| rule.resource == resource && selector_matches(&rule.selector, table))
		.collect()
}

fn effective_usage_value(used: u64, matched: &[&crate::catalog::QuotaRuleDefinition]) -> Value {
	let matched_rule_ids =
		matched.iter().map(|rule| Value::from(rule.id.to_string())).collect::<Vec<_>>();
	let exact =
		matched.iter().copied().find(|rule| matches!(rule.selector, QuotaSelector::Exact(_)));
	let (effective, limit, limit_origin) = if let Some(exact) = exact {
		let origin = if exact.limit == QuotaLimit::Unlimited {
			"explicit_unlimited"
		} else {
			"exact"
		};
		(vec![exact], exact.limit, origin)
	} else {
		let minimum = matched.iter().filter_map(|rule| match rule.limit {
			QuotaLimit::Finite(limit) => Some(limit),
			QuotaLimit::Unlimited => None,
		});
		if let Some(minimum) = minimum.min() {
			(
				matched
					.iter()
					.copied()
					.filter(|rule| rule.limit == QuotaLimit::Finite(minimum))
					.collect(),
				QuotaLimit::Finite(minimum),
				"regex_min",
			)
		} else if matched.is_empty() {
			(Vec::new(), QuotaLimit::Unlimited, "unmatched")
		} else {
			(matched.to_owned(), QuotaLimit::Unlimited, "explicit_unlimited")
		}
	};
	let effective_rule_ids =
		effective.iter().map(|rule| Value::from(rule.id.to_string())).collect::<Vec<_>>();
	Value::Object(Object::from(map! {
		"effective_rule_ids" => effective_rule_ids.into(),
		"exceeded" => quota_exceeded(used, limit).into(),
		"limit" => quota_limit_value(limit),
		"limit_origin" => limit_origin.into(),
		"matched_rule_ids" => matched_rule_ids.into(),
		"remaining" => quota_remaining(used, limit),
		"used" => used.into(),
	}))
}

fn selector_matches(selector: &QuotaSelector, table: &TableName) -> bool {
	match selector {
		QuotaSelector::Exact(exact) => exact.as_str() == table.as_str(),
		QuotaSelector::Regex(regex) => regex.inner().is_match(table.as_str()),
	}
}

fn effective_field_rule(
	policy: &QuotaPolicyDefinition,
	table: &TableName,
) -> Option<(String, QuotaLimit)> {
	let matching = policy.rules.iter().filter(|rule| {
		rule.resource == QuotaResource::Field && selector_matches(&rule.selector, table)
	});
	let mut exact = None;
	let mut regex_unlimited = None;
	let mut regex_finite = None::<(&str, u64)>;
	for rule in matching {
		match (&rule.selector, rule.limit) {
			(QuotaSelector::Exact(_), limit) => {
				exact = Some((rule.id.to_string(), limit));
			}
			(QuotaSelector::Regex(_), QuotaLimit::Finite(limit)) => {
				if regex_finite.is_none_or(|(_, current)| limit < current) {
					regex_finite = Some((rule.id.as_str(), limit));
				}
			}
			(QuotaSelector::Regex(_), QuotaLimit::Unlimited) => {
				if regex_unlimited.is_none() {
					regex_unlimited = Some(rule.id.as_str());
				}
			}
		}
	}
	exact.or_else(|| {
		regex_finite
			.map(|(id, limit)| (id.to_owned(), QuotaLimit::Finite(limit)))
			.or_else(|| regex_unlimited.map(|id| (id.to_owned(), QuotaLimit::Unlimited)))
	})
}

fn effective_record_rule(
	policy: &QuotaPolicyDefinition,
	table: &TableName,
) -> Option<(String, QuotaLimit)> {
	let matching = policy.rules.iter().filter(|rule| {
		rule.resource == QuotaResource::Record && selector_matches(&rule.selector, table)
	});
	let mut exact = None;
	let mut regex_unlimited = None;
	let mut regex_finite = None::<(&str, u64)>;
	for rule in matching {
		match (&rule.selector, rule.limit) {
			(QuotaSelector::Exact(_), limit) => {
				exact = Some((rule.id.to_string(), limit));
			}
			(QuotaSelector::Regex(_), QuotaLimit::Finite(limit)) => {
				if regex_finite.is_none_or(|(_, current)| limit < current) {
					regex_finite = Some((rule.id.as_str(), limit));
				}
			}
			(QuotaSelector::Regex(_), QuotaLimit::Unlimited) => {
				if regex_unlimited.is_none() {
					regex_unlimited = Some(rule.id.as_str());
				}
			}
		}
	}
	exact.or_else(|| {
		regex_finite
			.map(|(id, limit)| (id.to_owned(), QuotaLimit::Finite(limit)))
			.or_else(|| regex_unlimited.map(|id| (id.to_owned(), QuotaLimit::Unlimited)))
	})
}

fn project_counter(current: u64, delta: i128) -> Result<u64> {
	let projected =
		i128::from(current).checked_add(delta).ok_or_else(|| Error::QuotaUsageInvalid {
			reason: "quota usage counter arithmetic overflow".to_owned(),
		})?;
	u64::try_from(projected).map_err(|_| {
		Error::QuotaUsageInvalid {
			reason: format!(
				"quota usage counter underflow: cannot apply delta {delta} to {current}"
			),
		}
		.into()
	})
}

fn limit_violation(
	limit: QuotaLimit,
	rule: &str,
	resource: &str,
	table: &TableName,
	current: u64,
	delta: i128,
	projected: u64,
) -> Option<QuotaViolation> {
	let QuotaLimit::Finite(limit) = limit else {
		return None;
	};
	if projected <= limit || (current > limit && projected <= current) {
		return None;
	}
	Some(QuotaViolation {
		rule: rule.to_owned(),
		resource: resource.to_owned(),
		table: table.to_string(),
		current,
		delta,
		projected,
		limit,
		over_by: projected.saturating_sub(limit),
	})
}

fn push_violation(
	violations: &mut Vec<QuotaViolation>,
	truncated: &mut bool,
	violation: Option<QuotaViolation>,
) {
	let Some(violation) = violation else {
		return;
	};
	violations.push(violation);
	violations.sort_unstable_by(|left, right| {
		let rank = |resource: &str| match resource {
			"table" => 0,
			"field" => 1,
			"record" => 2,
			_ => 3,
		};
		(rank(&left.resource), &left.table, &left.rule).cmp(&(
			rank(&right.resource),
			&right.table,
			&right.rule,
		))
	});
	if violations.len() > MAX_QUOTA_VIOLATIONS {
		violations.pop();
		*truncated = true;
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn lightweight_edges_cannot_be_certified_by_record_key_counts() {
		let name = TableName::from("edges");
		let mut relation = crate::catalog::Relation {
			from: Vec::new(),
			to: Vec::new(),
			enforced: true,
			lightweight: false,
		};
		assert!(
			ensure_record_scan_supported(&name, &TableType::Relation(relation.clone())).is_ok()
		);
		relation.lightweight = true;
		assert!(ensure_record_scan_supported(&name, &TableType::Relation(relation)).is_err());
	}

	#[test]
	fn quota_counter_arithmetic_fails_closed() {
		assert_eq!(project_counter(7, -7).unwrap(), 0);
		assert!(project_counter(0, -1).is_err());
		assert!(project_counter(u64::MAX, 1).is_err());
	}

	#[test]
	fn quota_over_limit_usage_may_only_decrease() {
		let table = TableName::from("items");
		assert!(
			limit_violation(QuotaLimit::Finite(10), "r", "record", &table, 12, -1, 11).is_none()
		);
		assert!(
			limit_violation(QuotaLimit::Finite(10), "r", "record", &table, 12, 1, 13).is_some()
		);
	}

	#[test]
	fn quota_savepoints_restore_deltas_and_resets() {
		let mut state = QuotaTransactionState::default();
		let key = (NamespaceId(1), DatabaseId(2), TableName::from("items"));
		state.current.record_deltas.insert(key.clone(), 1);
		state.current.record_transitions.insert(vec![1], (key.0, key.1, key.2.clone(), false));
		state.new_save_point();
		state.current.record_deltas.insert(key.clone(), 3);
		state.current.reset_tables.insert(key.clone());
		state.current.record_transitions.insert(vec![2], (key.0, key.1, key.2.clone(), true));
		state.rollback_to_save_point();
		assert_eq!(state.current.record_deltas[&key], 1);
		assert!(state.current.reset_tables.is_empty());
		assert_eq!(state.current.record_transitions.len(), 1);
		assert!(!state.current.record_transitions[&vec![1]].3);
		state.new_save_point();
		state.current.record_deltas.insert(key.clone(), 4);
		state.release_last_save_point();
		assert_eq!(state.current.record_deltas[&key], 4);
	}

	#[test]
	fn bare_commit_rejects_flushed_admission() {
		let mut state = QuotaTransactionState::default();
		assert!(!state.has_pending());
		state.flushed_admission = true;
		assert!(state.has_pending());
	}
}

#[cfg(test)]
mod memory_transaction_tests {
	use super::*;
	use crate::key::schema::{QuotaPolicyKey, RecordKey};
	use crate::sequences::Sequences;
	use crate::tx::WriteBatch;
	use crate::{TransactionFactory, TransactionType};
	use surrealdb_cnf::ConfigMap;
	use surrealdb_expr::val::RecordIdKey;
	use surrealdb_kvs::Key;

	const NS: NamespaceId = NamespaceId(1);
	const DB: DatabaseId = DatabaseId(2);

	fn table() -> TableName {
		TableName::from("items")
	}

	fn key(id: i64) -> Key<'static> {
		RecordKey {
			ns: NS,
			db: DB,
			tb: Cow::Owned(table()),
			id: Cow::Owned(RecordIdKey::Number(id)),
		}
		.encode_key()
		.unwrap()
	}

	// Deliberately use opaque raw values: these tests exercise physical write
	// entry points and admission, without importing the SQL VM or row decoder.
	async fn empty_factory() -> (TransactionFactory, Sequences) {
		let builder = surrealdb_kvs_any::Backends::community()
			.new_transaction_builder("mem://", Default::default(), ConfigMap::default())
			.await
			.unwrap();
		let factory = TransactionFactory::new(
			Arc::new(crate::triggers::CommitTriggers::new()),
			builder,
			Arc::new(Default::default()),
		);
		let sequences = Sequences::new(factory.clone(), uuid::Uuid::new_v4());
		(factory, sequences)
	}

	async fn setup(limit: u64) -> (TransactionFactory, Sequences) {
		let (factory, sequences) = empty_factory().await;
		let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		tx.quota_usage(NS, DB).initialize_new_database().await.unwrap();
		tx.set_key(
			&Qg {
				ns: NS,
				db: DB,
			},
			&1,
		)
		.await
		.unwrap();
		tx.set_key(
			&QuotaPolicyKey {
				ns: NS,
				db: DB,
			},
			&QuotaPolicyDefinition {
				format_revision: crate::catalog::QUOTA_POLICY_FORMAT_REVISION,
				generation: 1,
				database: "db".into(),
				rules: vec![crate::catalog::QuotaRuleDefinition {
					id: "records".into(),
					resource: QuotaResource::Record,
					selector: QuotaSelector::Exact("items".into()),
					limit: QuotaLimit::Finite(limit),
				}],
			},
		)
		.await
		.unwrap();
		tx.commit().await.unwrap();
		(factory, sequences)
	}

	#[tokio::test]
	async fn direct_database_catalog_write_bootstraps_only_new_databases() {
		use crate::catalog::{DatabaseCompatibility, DatabaseDefinition};
		use crate::key::schema::DatabaseKey;
		use surrealdb_expr::val::stored::schema::Unrevisioned;
		let (factory, sequences) = empty_factory().await;
		let key = DatabaseKey {
			ns: NS,
			db: Cow::Borrowed("db"),
		};
		let mut definition = DatabaseDefinition {
			namespace_id: NS,
			database_id: DB,
			name: "db".into(),
			comment: None,
			changefeed: None,
			strict: false,
			compatibility: Unrevisioned(DatabaseCompatibility::default()),
		};
		let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		tx.put_key(&key, &definition).await.unwrap();
		assert_eq!(tx.quota_usage(NS, DB).meta().await.unwrap().state, QuotaUsageState::Ready);
		tx.commit().await.unwrap();
		let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		definition.comment = Some("update must not insert the ledger twice".to_owned());
		tx.set_key(&key, &definition).await.unwrap();
		tx.commit().await.unwrap();
		// Model an existing legacy/corrupt database, not a new catalog creation.
		let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		tx.del_key(&Qm {
			ns: NS,
			db: DB,
		})
		.await
		.unwrap();
		tx.commit().await.unwrap();
		let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		assert!(tx.set_key(&key, &definition).await.is_err());
		assert_eq!(
			tx.quota_usage(NS, DB).meta().await.unwrap().state,
			QuotaUsageState::Uninitialized
		);
		tx.cancel().await.unwrap();
	}

	#[tokio::test]
	async fn direct_table_catalog_writes_are_counted_once() {
		use crate::catalog::{StoredTableDefinition, TableId};
		use crate::key::schema::TableKey;
		let (factory, sequences) = setup(10).await;
		let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		let policy_key = QuotaPolicyKey {
			ns: NS,
			db: DB,
		};
		let mut policy = tx.get_key(&policy_key, None).await.unwrap().unwrap();
		policy.rules.push(crate::catalog::QuotaRuleDefinition {
			id: "tables".into(),
			resource: QuotaResource::Table,
			selector: QuotaSelector::Exact("items".into()),
			limit: QuotaLimit::Finite(1),
		});
		tx.set_key(&policy_key, &policy).await.unwrap();
		let key = TableKey {
			ns: NS,
			db: DB,
			tb: Cow::Owned(table()),
		};
		let definition = StoredTableDefinition::new(NS, DB, TableId(1), table());
		tx.set_key(&key, &definition).await.unwrap();
		tx.set_key(&key, &definition).await.unwrap();
		tx.commit().await.unwrap();
		let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		assert_eq!(tx.quota_usage(NS, DB).table_bucket_count(1, "tables").await.unwrap(), 1);
		tx.del_key(&key).await.unwrap();
		tx.commit().await.unwrap();
		let tx = factory.transaction(TransactionType::Read, sequences.clone()).await.unwrap();
		assert_eq!(tx.quota_usage(NS, DB).table_bucket_count(1, "tables").await.unwrap(), 0);
		tx.cancel().await.unwrap();
	}

	#[tokio::test]
	async fn migration_admission_is_explicit_exact_and_catalog_only() {
		use crate::catalog::{StoredTableDefinition, TableId};
		use crate::fork_storage::{ForkMigrationState, ForkStorageFormat};
		use crate::key::schema::{ForkStorageKey, NativeQuotaMigrationKey, TableKey, VersionKey};
		let (factory, sequences) = setup(10).await;
		let version = crate::version::MajorVersion::upstream_latest();
		let mut marker = ForkStorageFormat::current();
		marker.migration_state = ForkMigrationState::InProgress;
		let journal = b"exact durable offline migration journal".to_vec();
		let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		tx.set_key(&VersionKey {}, &version).await.unwrap();
		tx.set_key(&ForkStorageKey {}, &marker).await.unwrap();
		tx.set_key(&NativeQuotaMigrationKey {}, &journal).await.unwrap();
		tx.commit().await.unwrap();
		let table_key = TableKey {
			ns: NS,
			db: DB,
			tb: Cow::Owned(table()),
		};
		let definition = StoredTableDefinition::new(NS, DB, TableId(1), table());
		let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		assert!(tx.set_key(&table_key, &definition).await.is_err());
		assert!(tx.put(key(1), vec![1]).await.is_err());
		assert!(
			tx.admit_native_quota_catalog_migration(version, &marker, b"another datastore journal")
				.await
				.is_err()
		);
		assert!(!tx.quota_catalog_migration.load(std::sync::atomic::Ordering::Relaxed));
		tx.cancel().await.unwrap();
		let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		tx.new_save_point().await.unwrap();
		assert!(tx.admit_native_quota_catalog_migration(version, &marker, &journal).await.is_err());
		tx.cancel().await.unwrap();
		let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		tx.admit_native_quota_catalog_migration(version, &marker, &journal).await.unwrap();
		tx.set_key(&table_key, &definition).await.unwrap();
		assert!(!tx.quota_state.lock().await.has_pending());
		assert!(tx.put(key(1), vec![1]).await.is_err());
		tx.commit().await.unwrap();
		let tx = factory.transaction(TransactionType::Read, sequences.clone()).await.unwrap();
		assert!(tx.get_key(&table_key, None).await.unwrap().is_some());
		assert!(!tx.exists(key(1), None).await.unwrap());
		tx.cancel().await.unwrap();
	}

	async fn count(factory: &TransactionFactory, sequences: &Sequences) -> u64 {
		let tx = factory.transaction(TransactionType::Read, sequences.clone()).await.unwrap();
		let count = tx.quota_usage(NS, DB).record_count(&table()).await.unwrap();
		tx.cancel().await.unwrap();
		count
	}

	async fn write(tx: &Transaction, path: &str, id: i64) {
		match path {
			"put" => tx.put(key(id), vec![1]).await.unwrap(),
			"set" => tx.set(key(id), vec![1]).await.unwrap(),
			"held" => tx.put_held(key(id), vec![1]).await.unwrap(),
			"cas" => tx.putc(key(id), vec![1], None).await.unwrap(),
			"replace" => tx.replace(key(id), vec![1]).await.unwrap(),
			"setm" => tx.setm(vec![(key(id), vec![1]), (key(id), vec![2])]).await.unwrap(),
			"batch" => {
				let mut batch = WriteBatch::default();
				batch.set_record(table(), RecordIdKey::Number(id), key(id), vec![1], true);
				batch.set_record(table(), RecordIdKey::Number(id), key(id), vec![2], false);
				tx.write_batch(NS, DB, batch).await.unwrap();
			}
			_ => panic!("unknown write path"),
		}
	}

	#[tokio::test]
	async fn every_raw_create_path_counts_once_and_rejects_over_limit_atomically() {
		for path in ["put", "set", "held", "cas", "replace", "setm", "batch"] {
			let (factory, sequences) = setup(1).await;
			let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
			write(&tx, path, 1).await;
			tx.set(key(1), vec![3]).await.unwrap();
			tx.commit().await.unwrap();
			assert_eq!(count(&factory, &sequences).await, 1, "{path}");

			let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
			write(&tx, path, 2).await;
			let error = tx.commit().await.expect_err(path);
			assert!(
				matches!(error.downcast_ref::<Error>(), Some(Error::QuotaExceeded(_))),
				"{path}: {error}"
			);
			let read = factory.transaction(TransactionType::Read, sequences.clone()).await.unwrap();
			assert!(read.get(key(2), None).await.unwrap().is_none(), "{path}");
			assert_eq!(read.get(key(1), None).await.unwrap(), Some(vec![3]), "{path}");
			assert_eq!(read.quota_usage(NS, DB).record_count(&table()).await.unwrap(), 1, "{path}");
			read.cancel().await.unwrap();
		}
	}

	#[tokio::test]
	async fn insert_delete_and_delete_reinsert_have_exact_net_counts() {
		let (factory, sequences) = setup(1).await;
		let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		tx.put_held(key(1), vec![1]).await.unwrap();
		tx.del(key(1)).await.unwrap();
		tx.commit().await.unwrap();
		assert_eq!(count(&factory, &sequences).await, 0);

		let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		tx.put(key(1), vec![1]).await.unwrap();
		tx.commit().await.unwrap();
		let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		tx.delm(vec![key(1), key(1)]).await.unwrap();
		tx.put(key(1), vec![2]).await.unwrap();
		tx.commit().await.unwrap();
		assert_eq!(count(&factory, &sequences).await, 1);
		let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		tx.delc(key(1), Some(&[2])).await.unwrap();
		tx.commit().await.unwrap();
		assert_eq!(count(&factory, &sequences).await, 0);
	}

	#[tokio::test]
	async fn rejected_flush_rolls_back_counters_and_records_to_savepoint() {
		let (factory, sequences) = setup(1).await;
		let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		tx.put(key(1), vec![1]).await.unwrap();
		tx.commit().await.unwrap();

		let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		tx.new_save_point().await.unwrap();
		tx.put(key(2), vec![2]).await.unwrap();
		let error = tx.flush_quota_usage().await.unwrap_err();
		assert!(matches!(error.downcast_ref::<Error>(), Some(Error::QuotaExceeded(_))));
		tx.rollback_to_save_point().await.unwrap();
		assert!(tx.get(key(2), None).await.unwrap().is_none());
		tx.set(key(1), vec![3]).await.unwrap();
		tx.commit().await.unwrap();
		assert_eq!(count(&factory, &sequences).await, 1);
	}

	#[tokio::test]
	async fn counter_overflow_cancels_business_write_without_wrapping() {
		let (factory, sequences) = setup(u64::MAX).await;
		let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		let epoch = tx.quota_usage(NS, DB).meta().await.unwrap().active_epoch.unwrap();
		tx.set_key(
			&QuotaRecordUsage {
				ns: NS,
				db: DB,
				epoch,
				table: Cow::Owned(table()),
			},
			&u64::MAX,
		)
		.await
		.unwrap();
		tx.commit().await.unwrap();

		let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		tx.put(key(1), vec![1]).await.unwrap();
		let error = tx.commit().await.unwrap_err();
		assert!(matches!(error.downcast_ref::<Error>(), Some(Error::QuotaUsageInvalid { .. })));
		let read = factory.transaction(TransactionType::Read, sequences.clone()).await.unwrap();
		assert!(read.get(key(1), None).await.unwrap().is_none());
		assert_eq!(read.quota_usage(NS, DB).record_count(&table()).await.unwrap(), u64::MAX);
		read.cancel().await.unwrap();
	}

	#[tokio::test]
	async fn bare_commit_cannot_drop_raw_admission_or_flushed_admission() {
		let (factory, sequences) = setup(1).await;
		let tx = factory.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		tx.put(key(1), vec![1]).await.unwrap();
		assert!(tx.commit_bare().await.is_err());
		tx.flush_quota_usage().await.unwrap();
		assert!(tx.commit_bare().await.is_err());
		tx.commit().await.unwrap();
		assert_eq!(count(&factory, &sequences).await, 1);
	}
}
