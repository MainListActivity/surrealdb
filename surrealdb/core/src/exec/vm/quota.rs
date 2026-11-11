//! Native quota control-plane persistence and rebuild operations.

use std::collections::{BTreeMap, HashSet};

use anyhow::{Result, anyhow, bail};
use surrealdb_types::ToSql;
use uuid::Uuid;
use web_time::Instant;

use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::catalog::{
	Error, QuotaPolicyChange, QuotaPolicyDefinition, QuotaRuleDefinition, QuotaUsageMeta,
	QuotaUsageState,
};
use crate::exec::ExecutionContext;
use crate::expr::Base;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::quota::{AlterQuotaClause, QuotaRule};
use crate::iam::{Action, ResourceKind};
use crate::key::schema::{QuotaGenerationKey, QuotaLatestChangeKey, QuotaPolicyKey};
use crate::kvs::{Transaction, TransactionType};
use crate::observe::{
	QuotaEvent, QuotaEventCtx, QuotaEventKind, QuotaEventOutcome, QuotaEventSafe,
};
use crate::val::{Datetime, Object, Value};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum QuotaCommand {
	Define {
		kind: DefineKind,
		expected_generation: Option<u64>,
		rules: Vec<QuotaRule>,
	},
	Alter {
		if_exists: bool,
		expected_generation: u64,
		clauses: Vec<AlterQuotaClause>,
	},
	Remove {
		if_exists: bool,
		expected_generation: u64,
	},
	Rebuild {
		if_needed: bool,
	},
	Info {
		structure: bool,
	},
}

pub(super) async fn execute(
	ctx: &ExecutionContext,
	database: &str,
	command: &QuotaCommand,
) -> Result<Value> {
	match command {
		QuotaCommand::Define {
			kind,
			expected_generation,
			rules,
		} => define(ctx, database, kind.clone(), *expected_generation, rules).await,
		QuotaCommand::Alter {
			if_exists,
			expected_generation,
			clauses,
		} => alter(ctx, database, *if_exists, *expected_generation, clauses).await,
		QuotaCommand::Remove {
			if_exists,
			expected_generation,
		} => remove(ctx, database, *if_exists, *expected_generation).await,
		QuotaCommand::Rebuild {
			if_needed,
		} => rebuild(ctx, database, *if_needed).await,
		QuotaCommand::Info {
			structure,
		} => info(ctx, database, *structure).await,
	}
}

/// The lightweight quota field returned by an already-authorized INFO FOR DB.
/// Like the fork's database INFO, this reads current quota metadata even when
/// the rest of the schema information requests a historical version.
pub(super) async fn database_summary(ctx: &ExecutionContext) -> Result<Value> {
	let database = ctx.database()?;
	let ns = database.ns_ctx.ns.namespace_id;
	let db = database.db.database_id;
	let txn = ctx.txn();
	let policy = txn.get_db_quota(ns, db, None).await?;
	let generation = txn
		.get_key(
			&QuotaGenerationKey {
				ns,
				db,
			},
			None,
		)
		.await?;
	Ok(surrealdb_datastore::quota::quota_summary_value(policy.as_deref(), generation))
}

async fn define(
	ctx: &ExecutionContext,
	database: &str,
	kind: DefineKind,
	expected_generation: Option<u64>,
	rules: &[QuotaRule],
) -> Result<Value> {
	let opt = ctx.options().ok_or_else(|| anyhow!("quota statement requires session options"))?;
	let database = database.to_owned();
	ctx.is_allowed(Action::Edit, ResourceKind::Quota, Base::Ns)?;
	if opt.import {
		bail!(Error::QuotaImportNotAllowed);
	}
	let namespace = opt.ns()?;
	let txn = ctx.txn();
	let db =
		txn.get_db_by_name(namespace, &database, None).await?.ok_or_else(|| Error::DbNotFound {
			name: database.clone(),
		})?;
	let operation = QuotaOperation::new("define", database.clone());
	txn.flush_quota_usage().await?;
	let meta =
		txn.quota_usage(db.namespace_id, db.database_id).ensure_writable_for_update().await?;
	let quota_key = QuotaPolicyKey {
		ns: db.namespace_id,
		db: db.database_id,
	};
	let generation_key = QuotaGenerationKey {
		ns: db.namespace_id,
		db: db.database_id,
	};
	let stored_generation = txn.get_key(&generation_key, None).await?;
	let current = txn.get_db_quota(db.namespace_id, db.database_id, None).await?;
	let policy = match current.as_deref() {
		Some(current) => match kind {
			DefineKind::Default => {
				bail!(Error::QuotaAlreadyExists {
					database,
				});
			}
			DefineKind::IfNotExists => {
				return Ok(operation.result(
					false,
					Some(current.generation),
					Some(current.generation),
					&meta,
				));
			}
			DefineKind::Overwrite => {
				let Some(expected) = expected_generation else {
					bail!(Error::QuotaGenerationRequired {
						database,
					});
				};
				if current.generation != expected {
					bail!(Error::QuotaGenerationMismatch {
						database,
						expected,
						actual: current.generation,
					});
				}
				let mut policy = QuotaPolicyDefinition::new(
					database.clone().into(),
					current.generation,
					rules.to_vec(),
				)?;
				if current.rules == policy.rules {
					return Ok(operation.result(
						false,
						Some(current.generation),
						Some(current.generation),
						&meta,
					));
				}
				policy.generation =
					current.generation.checked_add(1).ok_or_else(|| Error::QuotaPolicyInvalid {
						reason: "quota policy generation overflow".to_owned(),
					})?;
				policy
			}
		},
		None => {
			if let Some(expected) = expected_generation {
				bail!(Error::QuotaGenerationMismatch {
					database,
					expected,
					actual: 0,
				});
			}
			let generation = stored_generation.unwrap_or(0).checked_add(1).ok_or_else(|| {
				Error::QuotaPolicyInvalid {
					reason: "quota policy generation overflow".to_owned(),
				}
			})?;
			QuotaPolicyDefinition::new(database.clone().into(), generation, rules.to_vec())?
		}
	};

	txn.put_compare_key(&quota_key, &policy, current.as_deref()).await?;
	txn.put_compare_key(&generation_key, &policy.generation, stored_generation.as_ref()).await?;
	let tables = txn.all_tb(db.namespace_id, db.database_id, None).await?;
	txn.quota_usage(db.namespace_id, db.database_id)
		.initialize_policy_table_buckets(&policy, &tables)
		.await?;
	let change = operation.latest_change(opt.auth.id().to_string(), policy.generation);
	txn.set_key(
		&QuotaLatestChangeKey {
			ns: db.namespace_id,
			db: db.database_id,
		},
		&change,
	)
	.await?;
	txn.queue_quota_event(operation.audit_event(
		QuotaEventKind::Define,
		namespace,
		opt.auth.id().to_string(),
		QuotaEventOutcome::Changed,
	))
	.await;
	txn.clear_cache();
	Ok(operation.result(
		true,
		current.as_ref().map(|policy| policy.generation),
		Some(policy.generation),
		&meta,
	))
}

async fn alter(
	ctx: &ExecutionContext,
	database: &str,
	if_exists: bool,
	expected_generation: u64,
	clauses: &[AlterQuotaClause],
) -> Result<Value> {
	let opt = ctx.options().ok_or_else(|| anyhow!("quota statement requires session options"))?;
	let database = database.to_owned();
	ctx.is_allowed(Action::Edit, ResourceKind::Quota, Base::Ns)?;
	if opt.import {
		bail!(Error::QuotaImportNotAllowed);
	}
	let namespace = opt.ns()?;
	let txn = ctx.txn();
	let db =
		txn.get_db_by_name(namespace, &database, None).await?.ok_or_else(|| Error::DbNotFound {
			name: database.clone(),
		})?;
	let operation = QuotaOperation::new("alter", database.clone());
	txn.flush_quota_usage().await?;
	let meta =
		txn.quota_usage(db.namespace_id, db.database_id).ensure_writable_for_update().await?;
	let generation_key = QuotaGenerationKey {
		ns: db.namespace_id,
		db: db.database_id,
	};
	let stored_generation = txn.get_key(&generation_key, None).await?;
	let Some(current) = txn.get_db_quota(db.namespace_id, db.database_id, None).await? else {
		if if_exists {
			return Ok(operation.result(false, stored_generation, stored_generation, &meta));
		}
		bail!(Error::QuotaNotFound {
			database,
		});
	};
	if current.generation != expected_generation {
		bail!(Error::QuotaGenerationMismatch {
			database,
			expected: expected_generation,
			actual: current.generation,
		});
	}

	let mut rules = current
		.rules
		.iter()
		.cloned()
		.map(|rule| (rule.id.clone(), rule))
		.collect::<BTreeMap<_, _>>();
	let mut operated = HashSet::with_capacity(clauses.len());
	for clause in clauses {
		let id = match clause {
			AlterQuotaClause::Set(rule) => &rule.id,
			AlterQuotaClause::Drop {
				id,
				..
			} => id,
		};
		if !operated.insert(id.clone()) {
			bail!(Error::QuotaPolicyInvalid {
				reason: format!("quota rule '{}' is operated on more than once", id),
			});
		}
		match clause {
			AlterQuotaClause::Set(rule) => {
				rules.insert(rule.id.clone(), QuotaRuleDefinition::try_from(rule.clone())?);
			}
			AlterQuotaClause::Drop {
				id,
				if_exists,
			} => {
				if rules.remove(id).is_none() && !if_exists {
					bail!(Error::QuotaRuleNotFound {
						id: id.to_string(),
					});
				}
			}
		}
	}
	let next_rules = rules.into_values().map(Into::into).collect();
	let mut policy = QuotaPolicyDefinition::new(database.into(), current.generation, next_rules)?;
	if current.rules == policy.rules {
		return Ok(operation.result(
			false,
			Some(current.generation),
			Some(current.generation),
			&meta,
		));
	}
	policy.generation =
		current.generation.checked_add(1).ok_or_else(|| Error::QuotaPolicyInvalid {
			reason: "quota policy generation overflow".to_owned(),
		})?;
	txn.put_compare_key(
		&QuotaPolicyKey {
			ns: db.namespace_id,
			db: db.database_id,
		},
		&policy,
		Some(current.as_ref()),
	)
	.await?;
	txn.put_compare_key(&generation_key, &policy.generation, stored_generation.as_ref()).await?;
	let tables = txn.all_tb(db.namespace_id, db.database_id, None).await?;
	txn.quota_usage(db.namespace_id, db.database_id)
		.initialize_policy_table_buckets(&policy, &tables)
		.await?;
	let change = operation.latest_change(opt.auth.id().to_string(), policy.generation);
	txn.set_key(
		&QuotaLatestChangeKey {
			ns: db.namespace_id,
			db: db.database_id,
		},
		&change,
	)
	.await?;
	txn.queue_quota_event(operation.audit_event(
		QuotaEventKind::Alter,
		namespace,
		opt.auth.id().to_string(),
		QuotaEventOutcome::Changed,
	))
	.await;
	txn.clear_cache();
	Ok(operation.result(true, Some(current.generation), Some(policy.generation), &meta))
}

async fn remove(
	ctx: &ExecutionContext,
	database: &str,
	if_exists: bool,
	expected_generation: u64,
) -> Result<Value> {
	let opt = ctx.options().ok_or_else(|| anyhow!("quota statement requires session options"))?;
	let database = database.to_owned();
	ctx.is_allowed(Action::Edit, ResourceKind::Quota, Base::Ns)?;
	if opt.import {
		bail!(Error::QuotaImportNotAllowed);
	}
	let namespace = opt.ns()?;
	let txn = ctx.txn();
	let db =
		txn.get_db_by_name(namespace, &database, None).await?.ok_or_else(|| Error::DbNotFound {
			name: database.clone(),
		})?;
	let operation = QuotaOperation::new("remove", database.clone());
	txn.flush_quota_usage().await?;
	let meta =
		txn.quota_usage(db.namespace_id, db.database_id).ensure_writable_for_update().await?;
	let generation_key = QuotaGenerationKey {
		ns: db.namespace_id,
		db: db.database_id,
	};
	let stored_generation = txn.get_key(&generation_key, None).await?;
	let Some(current) = txn.get_db_quota(db.namespace_id, db.database_id, None).await? else {
		if if_exists {
			return Ok(operation.result(false, stored_generation, stored_generation, &meta));
		}
		bail!(Error::QuotaNotFound {
			database,
		});
	};
	if current.generation != expected_generation {
		bail!(Error::QuotaGenerationMismatch {
			database,
			expected: expected_generation,
			actual: current.generation,
		});
	}
	let tombstone_generation =
		current.generation.checked_add(1).ok_or_else(|| Error::QuotaPolicyInvalid {
			reason: "quota policy generation overflow".to_owned(),
		})?;
	txn.put_compare_key(&generation_key, &tombstone_generation, stored_generation.as_ref()).await?;
	txn.del_compare_key(
		&QuotaPolicyKey {
			ns: db.namespace_id,
			db: db.database_id,
		},
		Some(current.as_ref()),
	)
	.await?;
	let change = operation.latest_change(opt.auth.id().to_string(), tombstone_generation);
	txn.set_key(
		&QuotaLatestChangeKey {
			ns: db.namespace_id,
			db: db.database_id,
		},
		&change,
	)
	.await?;
	txn.queue_quota_event(operation.audit_event(
		QuotaEventKind::Remove,
		namespace,
		opt.auth.id().to_string(),
		QuotaEventOutcome::Changed,
	))
	.await;
	txn.clear_cache();
	Ok(operation.result(true, Some(current.generation), Some(tombstone_generation), &meta))
}

async fn rebuild(ctx: &ExecutionContext, database: &str, if_needed: bool) -> Result<Value> {
	let opt = ctx.options().ok_or_else(|| anyhow!("quota statement requires session options"))?;
	let database = database.to_owned();
	let started = Instant::now();
	ctx.is_allowed(Action::Edit, ResourceKind::Quota, Base::Ns)?;
	if opt.import {
		return Err(Error::QuotaImportNotAllowed.into());
	}
	let namespace = opt.ns()?;
	let db = ctx.txn().get_db_by_name(namespace, &database, None).await?.ok_or_else(|| {
		Error::DbNotFound {
			name: database.clone(),
		}
	})?;
	let factory = ctx.ctx().try_get_transaction_factory()?;
	let sequences = ctx.ctx().try_get_sequences()?.clone();
	let operation_id = Uuid::now_v7().to_string();
	let event = RebuildEventGuard::new(
		ctx.txn(),
		operation_id.clone(),
		namespace.to_owned(),
		database.clone(),
		opt.auth.id().to_owned(),
		started,
	);

	// Persist the maintenance fence before any scan. A crash or disconnect
	// after this commit leaves the database read-only in Rebuilding.
	let fence = factory.transaction(TransactionType::Write, sequences.clone()).await?;
	let old_meta = fence.quota_usage(db.namespace_id, db.database_id).meta().await?;
	let generation = fence
		.get_key(
			&QuotaGenerationKey {
				ns: db.namespace_id,
				db: db.database_id,
			},
			None,
		)
		.await?;
	if if_needed && old_meta.state == QuotaUsageState::Ready {
		fence.cancel().await?;
		event.complete(QuotaEventOutcome::Noop);
		return Ok(rebuild_result(
			&operation_id,
			&database,
			false,
			generation,
			&old_meta,
			&old_meta,
			0,
			0,
			0,
			started,
		));
	}
	// Reject unsupported layouts before persisting the maintenance fence. The scan repeats
	// this check in its fenced snapshot to detect concurrent schema changes.
	if let Err(error) = fence.ensure_quota_rebuild_supported(db.namespace_id, db.database_id).await
	{
		fence.cancel().await?;
		return Err(error);
	}
	fence.quota_usage(db.namespace_id, db.database_id).begin_rebuild().await?;
	fence.commit().await?;

	// Build the trusted snapshot while the committed fence rejects all
	// catalog and record mutations for this database.
	let scan_tx = factory.transaction(TransactionType::Read, sequences.clone()).await?;
	let scan = scan_tx.scan_quota_usage(db.namespace_id, db.database_id).await?;
	scan_tx.cancel().await?;

	// Stage, validate, and switch the epoch atomically.
	let activate = factory.transaction(TransactionType::Write, sequences.clone()).await?;
	let quota = activate.quota_usage(db.namespace_id, db.database_id);
	quota.stage_rebuild_scan(&scan).await?;
	let validated = quota.validate_staged_epoch(&scan.snapshot).await?;
	quota.activate_validated_epoch(validated).await?;
	let new_meta = quota.meta().await?;
	activate.commit().await?;

	// Inactive epochs are not authoritative. Cleanup is best-effort after
	// the successful switch so a cleanup failure can never undo recovery.
	if let Some(old_epoch) = old_meta.active_epoch
		&& Some(old_epoch) != new_meta.active_epoch
	{
		let cleanup = factory.transaction(TransactionType::Write, sequences).await?;
		if let Err(error) = cleanup
			.quota_usage(db.namespace_id, db.database_id)
			.clear_inactive_epoch(old_epoch)
			.await
		{
			warn!(
				operation_id = %operation_id,
				epoch = old_epoch,
				error = %error,
				"quota rebuild left an inactive epoch for later cleanup"
			);
			cleanup.cancel().await?;
		} else if let Err(error) = cleanup.commit().await {
			warn!(
				operation_id = %operation_id,
				epoch = old_epoch,
				error = %error,
				"quota rebuild cleanup commit failed; active epoch remains valid"
			);
		}
	}

	event.complete(QuotaEventOutcome::Changed);
	Ok(rebuild_result(
		&operation_id,
		&database,
		true,
		generation,
		&old_meta,
		&new_meta,
		scan.tables,
		scan.fields,
		scan.records,
		started,
	))
}

async fn info(ctx: &ExecutionContext, database: &str, structure: bool) -> Result<Value> {
	let opt = ctx.options().ok_or_else(|| anyhow!("quota statement requires session options"))?;
	let database = database.to_owned();
	let namespace = opt.ns()?;
	let base = if opt.db().is_ok_and(|selected| selected == database) {
		Base::Db
	} else {
		Base::Ns
	};
	ctx.is_allowed(Action::View, ResourceKind::Quota, base)?;
	let txn = ctx.txn();
	let db =
		txn.get_db_by_name(namespace, &database, None).await?.ok_or_else(|| Error::DbNotFound {
			name: database.clone(),
		})?;
	let policy = txn.get_db_quota(db.namespace_id, db.database_id, None).await?;
	if structure {
		let tables = txn.all_tb(db.namespace_id, db.database_id, None).await?;
		txn.quota_usage(db.namespace_id, db.database_id)
			.info_structure(&database, policy.as_deref(), &tables)
			.await
	} else {
		Ok(policy.as_deref().map(|policy| Value::from(policy.to_sql())).unwrap_or(Value::None))
	}
}

struct RebuildEventGuard {
	tx: std::sync::Arc<Transaction>,
	operation_id: String,
	namespace: String,
	database: String,
	actor: String,
	started: Instant,
	completed: bool,
}

impl RebuildEventGuard {
	fn new(
		tx: std::sync::Arc<Transaction>,
		operation_id: String,
		namespace: String,
		database: String,
		actor: String,
		started: Instant,
	) -> Self {
		Self {
			tx,
			operation_id,
			namespace,
			database,
			actor,
			started,
			completed: false,
		}
	}

	fn complete(mut self, outcome: QuotaEventOutcome) {
		self.emit(outcome);
		self.completed = true;
	}

	fn emit(&self, outcome: QuotaEventOutcome) {
		self.tx.emit_quota_event(&QuotaEvent {
			safe: QuotaEventSafe {
				kind: QuotaEventKind::Rebuild,
				outcome,
				duration: Some(self.started.elapsed()),
			},
			ctx: QuotaEventCtx {
				operation_id: Some(self.operation_id.clone()),
				namespace: Some(self.namespace.clone()),
				database: Some(self.database.clone()),
				actor: Some(self.actor.clone()),
			},
		});
	}
}

impl Drop for RebuildEventGuard {
	fn drop(&mut self) {
		if !self.completed {
			tracing::warn!(
				target: "surrealdb::core::quota",
				error_code = "quota_rebuild_failed",
				operation_id = %self.operation_id,
				"native quota rebuild failed"
			);
			self.emit(QuotaEventOutcome::Error);
		}
	}
}

#[expect(clippy::too_many_arguments)]
fn rebuild_result(
	operation_id: &str,
	database: &str,
	changed: bool,
	generation: Option<u64>,
	before: &crate::catalog::QuotaUsageMeta,
	after: &crate::catalog::QuotaUsageMeta,
	tables: u64,
	fields: u64,
	records: u64,
	started: Instant,
) -> Value {
	let generation = generation.map_or(Value::None, Value::from);
	let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
	Value::Object(Object::from(map! {
		"after" => rebuild_state(generation.clone(), after),
		"before" => rebuild_state(generation, before),
		"changed" => changed.into(),
		"database" => database.into(),
		"duration_ms" => duration_ms.into(),
		"format_version" => 1u64.into(),
		"operation" => "rebuild_quota".into(),
		"operation_id" => operation_id.into(),
		"scanned" => Value::Object(Object::from(map! {
			"field" => fields.into(),
			"record" => records.into(),
			"table" => tables.into(),
		})),
	}))
}

fn rebuild_state(generation: Value, meta: &crate::catalog::QuotaUsageMeta) -> Value {
	let state = match meta.state {
		QuotaUsageState::Uninitialized => "uninitialized",
		QuotaUsageState::Rebuilding => "rebuilding",
		QuotaUsageState::Ready => "ready",
		QuotaUsageState::Corrupt => "corrupt",
	};
	Value::Object(Object::from(map! {
		"active_epoch" => meta.active_epoch.map_or(Value::None, Value::from),
		"generation" => generation,
		"ledger_state" => state.into(),
	}))
}

pub(crate) struct QuotaOperation {
	pub(crate) id: String,
	pub(crate) action: &'static str,
	pub(crate) database: String,
}

impl QuotaOperation {
	pub(crate) fn new(action: &'static str, database: String) -> Self {
		Self {
			id: Uuid::now_v7().to_string(),
			action,
			database,
		}
	}

	pub(crate) fn latest_change(&self, actor: String, generation: u64) -> QuotaPolicyChange {
		QuotaPolicyChange {
			operation_id: self.id.clone(),
			action: self.action.to_owned(),
			actor,
			generation,
			changed_at: Datetime::now(),
		}
	}

	pub(crate) fn audit_event(
		&self,
		kind: QuotaEventKind,
		namespace: &str,
		actor: String,
		outcome: QuotaEventOutcome,
	) -> QuotaEvent {
		QuotaEvent {
			safe: QuotaEventSafe {
				kind,
				outcome,
				duration: None,
			},
			ctx: QuotaEventCtx {
				operation_id: Some(self.id.clone()),
				namespace: Some(namespace.to_owned()),
				database: Some(self.database.clone()),
				actor: Some(actor),
			},
		}
	}

	pub(crate) fn result(
		&self,
		changed: bool,
		before_generation: Option<u64>,
		after_generation: Option<u64>,
		meta: &QuotaUsageMeta,
	) -> Value {
		Value::Object(Object::from(map! {
			"after" => operation_state(after_generation, meta),
			"before" => operation_state(before_generation, meta),
			"changed" => changed.into(),
			"database" => self.database.as_str().into(),
			"format_version" => 1u64.into(),
			"operation" => format!("{}_quota", self.action).into(),
			"operation_id" => self.id.as_str().into(),
		}))
	}
}

fn operation_state(generation: Option<u64>, meta: &QuotaUsageMeta) -> Value {
	let state = match meta.state {
		QuotaUsageState::Uninitialized => "uninitialized",
		QuotaUsageState::Rebuilding => "rebuilding",
		QuotaUsageState::Ready => "ready",
		QuotaUsageState::Corrupt => "corrupt",
	};
	Value::Object(Object::from(map! {
		"active_epoch" => meta.active_epoch.map_or(Value::None, Value::from),
		"generation" => generation.map_or(Value::None, Value::from),
		"ledger_state" => state.into(),
	}))
}
