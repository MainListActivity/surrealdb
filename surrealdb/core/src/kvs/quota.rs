//! Transaction-scoped access to protected native quota usage state.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::{Result, bail};

use super::Transaction;
use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::catalog::{
	DatabaseId, NamespaceId, QuotaLimit, QuotaPolicyDefinition, QuotaResource, QuotaSelector,
	QuotaUsageMeta, QuotaUsageState, TableDefinition,
};
use crate::err::{Error, QuotaExceededError, QuotaViolation};
use crate::key::database::qg::Qg;
use crate::key::database::ql::Ql;
use crate::key::database::qm::Qm;
use crate::key::database::qub::QuotaTableBucket;
use crate::key::database::que::QuotaEpochRoot;
use crate::key::database::quf::QuotaFieldUsage;
use crate::key::database::qur::QuotaRecordUsage;
#[cfg(test)]
use crate::kvs::testing::{QuotaFaultSite, maybe_inject_quota_fault};
use crate::kvs::{KVKey, KVValue, NORMAL_BATCH_SIZE};
use crate::val::{Datetime, Object, TableName, Value};

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
}

/// Transaction-local quota intents. The final signed delta is applied once at commit.
#[derive(Debug, Default)]
pub(crate) struct QuotaTransactionState {
	current: QuotaTransactionSnapshot,
	savepoints: Vec<QuotaTransactionSnapshot>,
	flushed_admission: bool,
}

impl QuotaTransactionState {
	pub(crate) fn new_save_point(&mut self) {
		self.savepoints.push(self.current.clone());
	}

	pub(crate) fn release_last_save_point(&mut self) {
		self.savepoints.pop();
	}

	pub(crate) fn rollback_to_save_point(&mut self) {
		if let Some(snapshot) = self.savepoints.pop() {
			self.current = snapshot;
		}
	}
}

/// Quota operations bound to the same transaction as the protected business write.
pub(crate) struct QuotaTransaction<'a> {
	tx: &'a Transaction,
	ns: NamespaceId,
	db: DatabaseId,
}

/// Canonical non-zero counters produced by a trusted rebuild scan.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct QuotaUsageSnapshot {
	table_buckets: BTreeMap<(u64, String), u64>,
	field_counts: BTreeMap<TableName, u64>,
	record_counts: BTreeMap<TableName, u64>,
}

/// Counts and canonical counters produced by one trusted catalog/record scan.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct QuotaRebuildScan {
	pub(crate) snapshot: QuotaUsageSnapshot,
	pub(crate) tables: u64,
	pub(crate) fields: u64,
	pub(crate) records: u64,
}

impl QuotaUsageSnapshot {
	pub(crate) fn set_table_bucket_count(&mut self, generation: u64, rule: &str, count: u64) {
		let key = (generation, rule.to_owned());
		if count == 0 {
			self.table_buckets.remove(&key);
		} else {
			self.table_buckets.insert(key, count);
		}
	}

	pub(crate) fn set_field_count(&mut self, table: &TableName, count: u64) {
		if count == 0 {
			self.field_counts.remove(table);
		} else {
			self.field_counts.insert(table.clone(), count);
		}
	}

	pub(crate) fn set_record_count(&mut self, table: &TableName, count: u64) {
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
pub(crate) struct ValidatedQuotaEpoch {
	ns: NamespaceId,
	db: DatabaseId,
	epoch: u64,
	snapshot: QuotaUsageSnapshot,
}

impl Transaction {
	/// Create a database-scoped quota facade over this transaction.
	pub(crate) fn quota_usage(&self, ns: NamespaceId, db: DatabaseId) -> QuotaTransaction<'_> {
		QuotaTransaction {
			tx: self,
			ns,
			db,
		}
	}

	/// Resolve all transaction-local quota intents and stage one conditional
	/// counter update per affected bucket.
	pub(crate) async fn flush_quota_usage(&self) -> Result<()> {
		// Keep registrations and savepoint changes serialized with the flush.
		// Transaction execution is normally sequential, but `Transaction` is
		// shareable and must not lose a late intent between snapshot and clear.
		let mut quota_state = self.quota_state.lock().await;
		let snapshot = quota_state.current.clone();
		if snapshot.table_deltas.values().all(|delta| *delta == 0)
			&& snapshot.field_deltas.values().all(|delta| *delta == 0)
			&& snapshot.record_deltas.values().all(|delta| *delta == 0)
			&& snapshot.reset_tables.is_empty()
		{
			quota_state.current = QuotaTransactionSnapshot::default();
			return Ok(());
		}
		#[cfg(test)]
		maybe_inject_quota_fault(QuotaFaultSite::BeforeCounterWrite, self.node_id_for_test())?;
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
			let generation_key = Qg::new(*ns, *db);
			match bound.observed_generation {
				Some(generation) => {
					self.putc(&generation_key, &generation, Some(&generation)).await?;
				}
				None => {
					self.putc(&generation_key, &0, None).await?;
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
					let key = QuotaTableBucket::new(*ns, *db, epoch, policy.generation, &rule);
					let current = self.get(&key, None).await?;
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
						self.delc(&key, current.as_ref()).await?;
					} else {
						self.putc(&key, &projected, current.as_ref()).await?;
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
				let key = QuotaFieldUsage::new(*ns, *db, epoch, &table);
				let current = self.get(&key, None).await?;
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
						self.delc(&key, current.as_ref()).await?;
					}
				} else {
					self.putc(&key, &projected, current.as_ref()).await?;
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
				let key = QuotaRecordUsage::new(*ns, *db, epoch, &table);
				let current = self.get(&key, None).await?;
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
						self.delc(&key, current.as_ref()).await?;
					}
				} else {
					self.putc(&key, &projected, current.as_ref()).await?;
				}
			}
		}

		#[cfg(test)]
		maybe_inject_quota_fault(QuotaFaultSite::AfterCounterWrite, self.node_id_for_test())?;
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
	pub(crate) async fn has_flushed_quota_admission(&self) -> bool {
		self.quota_state.lock().await.flushed_admission
	}

	/// Scan the complete database catalog and record keyspace into canonical
	/// quota counters. The caller owns the surrounding consistent read
	/// transaction and must run this only after committing the maintenance fence.
	pub(crate) async fn scan_quota_usage(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<QuotaRebuildScan> {
		let policy = self.get_db_quota(ns, db, None).await?;
		let tables = self.all_tb(ns, db, None).await?;
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
			let fields = self.all_tb_fields(ns, db, &table.name, None).await?;
			let field_count =
				u64::try_from(fields.len()).map_err(|_| Error::QuotaUsageInvalid {
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

			let beg = crate::key::record::prefix(ns, db, &table.name)?;
			let end = crate::key::record::suffix(ns, db, &table.name)?;
			let record_count = u64::try_from(self.count(beg..end, None).await?).map_err(|_| {
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
	pub(crate) async fn info_structure(
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
		let latest_change =
			self.tx.get(&Ql::new(self.ns, self.db), None).await?.map_or(Value::None, |change| {
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
	pub(crate) async fn stage_rebuild_scan(&self, scan: &QuotaRebuildScan) -> Result<()> {
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
	pub(crate) async fn initialize_policy_table_buckets(
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
			let key =
				QuotaTableBucket::new(self.ns, self.db, epoch, policy.generation, rule.id.as_str());
			self.tx.putc(&key, &count, None).await?;
		}
		Ok(())
	}

	async fn bind_database(&self) -> Result<()> {
		if self.tx.quota_state.lock().await.current.databases.contains_key(&(self.ns, self.db)) {
			return Ok(());
		}
		let meta = self.ensure_writable().await?;
		let observed_generation = self.tx.get(&Qg::new(self.ns, self.db), None).await?;
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
	pub(crate) async fn register_table_delta(&self, table: &TableName, delta: i128) -> Result<()> {
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
	pub(crate) async fn register_table_removal(&self, table: &TableName) -> Result<()> {
		self.register_table_delta(table, -1).await?;
		let mut state = self.tx.quota_state.lock().await;
		let key = (self.ns, self.db, table.clone());
		// Earlier per-field deltas are subsumed by the table deletion. Only
		// fields and records created after a possible recreation contribute to
		// the new table.
		state.current.field_deltas.remove(&key);
		state.current.record_deltas.remove(&key);
		state.current.reset_tables.insert(key);
		Ok(())
	}

	/// Record one explicit field-definition existence transition.
	pub(crate) async fn register_field_delta(&self, table: &TableName, delta: i128) -> Result<()> {
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
	pub(crate) async fn register_record_delta(&self, table: &TableName, delta: i128) -> Result<()> {
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
	pub(crate) async fn initialize_new_database(&self) -> Result<()> {
		let key = Qm::new(self.ns, self.db);
		self.tx.put(&key, &QuotaUsageMeta::ready_empty()).await
	}

	/// Read and validate usage metadata. A missing marker is an uninitialised legacy database.
	pub(crate) async fn meta(&self) -> Result<QuotaUsageMeta> {
		Ok(self.stored_meta().await?.unwrap_or_else(QuotaUsageMeta::uninitialized))
	}

	async fn stored_meta(&self) -> Result<Option<QuotaUsageMeta>> {
		let meta = self.tx.get(&Qm::new(self.ns, self.db), None).await?;
		if let Some(meta) = &meta {
			meta.validate()?;
		}
		Ok(meta)
	}

	/// Reject normal writes unless the active ledger has been validated.
	pub(crate) async fn ensure_writable(&self) -> Result<QuotaUsageMeta> {
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
		self.tx.putc(&Qm::new(self.ns, self.db), meta, Some(meta)).await
	}

	/// Guard a protected metadata mutation against a concurrent maintenance
	/// fence without making ordinary, non-quota writes contend on `Qm`.
	pub(crate) async fn ensure_writable_for_update(&self) -> Result<QuotaUsageMeta> {
		let meta = self.ensure_writable().await?;
		self.reassert_meta(&meta).await?;
		Ok(meta)
	}

	/// Persist the maintenance fence and allocate a new staged epoch.
	pub(crate) async fn begin_rebuild(&self) -> Result<u64> {
		let key = Qm::new(self.ns, self.db);
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
		self.tx.putc(&key, &meta, current.as_ref()).await?;
		Ok(staged_epoch)
	}

	/// Persist the read-only fence before a raw restore, prefix copy, or offline mutation begins.
	///
	/// The caller must commit this state before installing external data. Only a
	/// later trusted rebuild may return the database to `Ready`.
	pub(crate) async fn begin_external_write_maintenance(&self) -> Result<()> {
		let key = Qm::new(self.ns, self.db);
		let current = self.stored_meta().await?;
		let meta = QuotaUsageMeta {
			epoch_high_water: current.as_ref().map_or(0, |meta| meta.epoch_high_water),
			..QuotaUsageMeta::uninitialized()
		};
		meta.validate()?;
		self.tx.putc(&key, &meta, current.as_ref()).await
	}

	/// Mark the ledger corrupt while retaining its last active epoch for diagnostics.
	pub(crate) async fn mark_corrupt(&self) -> Result<()> {
		let key = Qm::new(self.ns, self.db);
		let current = self.stored_meta().await?;
		let previous = current.clone().unwrap_or_else(QuotaUsageMeta::uninitialized);
		let meta = QuotaUsageMeta {
			state: QuotaUsageState::Corrupt,
			staged_epoch: None,
			..previous
		};
		meta.validate()?;
		self.tx.putc(&key, &meta, current.as_ref()).await
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
	pub(crate) async fn set_staged_record_count(
		&self,
		table: &TableName,
		count: u64,
	) -> Result<()> {
		let epoch = self.staged_epoch_for_update().await?;
		let key = QuotaRecordUsage::new(self.ns, self.db, epoch, table);
		let current = self.tx.get(&key, None).await?;
		if count == 0 {
			self.tx.delc(&key, current.as_ref()).await
		} else {
			self.tx.putc(&key, &count, current.as_ref()).await
		}
	}

	/// Replace one staged field counter with the rebuilt snapshot count.
	pub(crate) async fn set_staged_field_count(&self, table: &TableName, count: u64) -> Result<()> {
		let epoch = self.staged_epoch_for_update().await?;
		let key = QuotaFieldUsage::new(self.ns, self.db, epoch, table);
		let current = self.tx.get(&key, None).await?;
		if count == 0 {
			self.tx.delc(&key, current.as_ref()).await
		} else {
			self.tx.putc(&key, &count, current.as_ref()).await
		}
	}

	/// Replace one staged table-rule bucket with the rebuilt snapshot count.
	pub(crate) async fn set_staged_table_bucket_count(
		&self,
		generation: u64,
		rule: &str,
		count: u64,
	) -> Result<()> {
		let epoch = self.staged_epoch_for_update().await?;
		let key = QuotaTableBucket::new(self.ns, self.db, epoch, generation, rule);
		let current = self.tx.get(&key, None).await?;
		if count == 0 {
			self.tx.delc(&key, current.as_ref()).await
		} else {
			self.tx.putc(&key, &count, current.as_ref()).await
		}
	}

	async fn read_epoch_snapshot(&self, epoch: u64) -> Result<QuotaUsageSnapshot> {
		let mut snapshot = QuotaUsageSnapshot::default();
		let prefix = QuotaEpochRoot::new(self.ns, self.db, epoch).encode_key()?;
		let mut next = Some(crate::kvs::util::to_prefix_range(&prefix)?);
		while let Some(range) = next {
			let batch = self.tx.batch_keys_vals(range, NORMAL_BATCH_SIZE, None).await?;
			next = batch.next;
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
	pub(crate) async fn validate_staged_epoch(
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
	pub(crate) async fn activate_validated_epoch(
		&self,
		validated: ValidatedQuotaEpoch,
	) -> Result<()> {
		if validated.ns != self.ns || validated.db != self.db {
			bail!(Error::QuotaUsageInvalid {
				reason: "validated quota epoch belongs to another database".to_owned(),
			});
		}
		let key = Qm::new(self.ns, self.db);
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
		self.tx.putc(&key, &meta, Some(&current)).await
	}

	/// Remove a non-active, non-staged epoch after a successful switch.
	pub(crate) async fn clear_inactive_epoch(&self, epoch: u64) -> Result<()> {
		let meta = self.meta().await?;
		if meta.active_epoch == Some(epoch) || meta.staged_epoch == Some(epoch) {
			bail!(Error::QuotaUsageInvalid {
				reason: format!("cannot clear referenced quota usage epoch {epoch}"),
			});
		}
		self.tx.delp(&QuotaEpochRoot::new(self.ns, self.db, epoch)).await
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
		K: KVKey<ValueType = u64> + std::fmt::Debug,
	{
		let current = self.tx.get(&key, None).await?;
		let current_value = current.unwrap_or(0);
		let next = current_value.checked_add(amount).ok_or_else(|| Error::QuotaUsageInvalid {
			reason: "quota usage counter overflow".to_owned(),
		})?;
		if amount != 0 {
			self.tx.putc(&key, &next, current.as_ref()).await?;
		}
		Ok(next)
	}

	async fn decrement<K>(&self, key: K, amount: u64) -> Result<u64>
	where
		K: KVKey<ValueType = u64> + std::fmt::Debug,
	{
		let current = self.tx.get(&key, None).await?;
		let current_value = current.unwrap_or(0);
		let next = current_value.checked_sub(amount).ok_or_else(|| Error::QuotaUsageInvalid {
			reason: format!(
				"quota usage counter underflow: cannot release {amount} from {current_value}"
			),
		})?;
		if amount != 0 {
			if next == 0 {
				self.tx.delc(&key, current.as_ref()).await?;
			} else {
				self.tx.putc(&key, &next, current.as_ref()).await?;
			}
		}
		Ok(next)
	}

	/// Return the active record count for a physical table.
	pub(crate) async fn record_count(&self, table: &TableName) -> Result<u64> {
		let epoch = self.active_epoch_for_read().await?;
		Ok(self
			.tx
			.get(&QuotaRecordUsage::new(self.ns, self.db, epoch, table), None)
			.await?
			.unwrap_or(0))
	}

	/// Increment the active record count in this transaction.
	pub(crate) async fn increment_record_count(
		&self,
		table: &TableName,
		amount: u64,
	) -> Result<u64> {
		let epoch = self.active_epoch_for_update().await?;
		self.increment(QuotaRecordUsage::new(self.ns, self.db, epoch, table), amount).await
	}

	/// Decrement the active record count in this transaction.
	pub(crate) async fn decrement_record_count(
		&self,
		table: &TableName,
		amount: u64,
	) -> Result<u64> {
		let epoch = self.active_epoch_for_update().await?;
		self.decrement(QuotaRecordUsage::new(self.ns, self.db, epoch, table), amount).await
	}

	/// Return the active field count for a physical table.
	pub(crate) async fn field_count(&self, table: &TableName) -> Result<u64> {
		let epoch = self.active_epoch_for_read().await?;
		Ok(self
			.tx
			.get(&QuotaFieldUsage::new(self.ns, self.db, epoch, table), None)
			.await?
			.unwrap_or(0))
	}

	/// Increment the active field count in this transaction.
	pub(crate) async fn increment_field_count(
		&self,
		table: &TableName,
		amount: u64,
	) -> Result<u64> {
		let epoch = self.active_epoch_for_update().await?;
		self.increment(QuotaFieldUsage::new(self.ns, self.db, epoch, table), amount).await
	}

	/// Decrement the active field count in this transaction.
	pub(crate) async fn decrement_field_count(
		&self,
		table: &TableName,
		amount: u64,
	) -> Result<u64> {
		let epoch = self.active_epoch_for_update().await?;
		self.decrement(QuotaFieldUsage::new(self.ns, self.db, epoch, table), amount).await
	}

	/// Return the active table count charged to a policy generation and rule.
	pub(crate) async fn table_bucket_count(&self, generation: u64, rule: &str) -> Result<u64> {
		let epoch = self.active_epoch_for_read().await?;
		Ok(self
			.tx
			.get(&QuotaTableBucket::new(self.ns, self.db, epoch, generation, rule), None)
			.await?
			.unwrap_or(0))
	}

	/// Increment an active table-rule bucket in this transaction.
	pub(crate) async fn increment_table_bucket_count(
		&self,
		generation: u64,
		rule: &str,
		amount: u64,
	) -> Result<u64> {
		let epoch = self.active_epoch_for_update().await?;
		self.increment(QuotaTableBucket::new(self.ns, self.db, epoch, generation, rule), amount)
			.await
	}

	/// Decrement an active table-rule bucket in this transaction.
	pub(crate) async fn decrement_table_bucket_count(
		&self,
		generation: u64,
		rule: &str,
		amount: u64,
	) -> Result<u64> {
		let epoch = self.active_epoch_for_update().await?;
		self.decrement(QuotaTableBucket::new(self.ns, self.db, epoch, generation, rule), amount)
			.await
	}
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
pub(crate) fn quota_summary_value(
	policy: Option<&QuotaPolicyDefinition>,
	generation: Option<u64>,
) -> Value {
	Value::Object(Object::from(map! {
		"defined" => policy.is_some().into(),
		"generation" => generation.map_or(Value::None, Value::from),
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
