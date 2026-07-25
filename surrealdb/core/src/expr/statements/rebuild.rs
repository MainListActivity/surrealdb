use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};
use uuid::Uuid;
use web_time::Instant;

use crate::catalog::QuotaUsageState;
use crate::catalog::providers::DatabaseProvider;
use crate::catalog::providers::TableProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::Base;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::statements::define::run_indexing;
use crate::iam::{Action, ResourceKind};
use crate::key::database::qg::Qg;
use crate::kvs::Transaction;
use crate::kvs::{LockType, TransactionType};
use crate::observe::{
	QuotaEvent, QuotaEventCtx, QuotaEventKind, QuotaEventOutcome, QuotaEventSafe,
};
use crate::val::{Object, TableName, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum RebuildStatement {
	Index(RebuildIndexStatement),
	Quota(RebuildQuotaStatement),
}

impl RebuildStatement {
	/// Process this type returning a computed simple Value
	#[instrument(level = "trace", name = "RebuildStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		match self {
			Self::Index(s) => s.compute(ctx, opt).await,
			Self::Quota(s) => s.compute(stk, ctx, opt, doc).await,
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RebuildQuotaStatement {
	pub database: crate::expr::Expr,
	pub if_needed: bool,
}

impl RebuildQuotaStatement {
	async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		let started = Instant::now();
		ctx.is_allowed(opt, Action::Edit, ResourceKind::Quota, Base::Ns)?;
		if opt.import {
			return Err(Error::QuotaImportNotAllowed.into());
		}
		let namespace = opt.ns()?;
		let database =
			expr_to_ident(stk, ctx, opt, doc, &self.database, "quota database name").await?;
		let db = ctx.tx().get_db_by_name(namespace, &database, None).await?.ok_or_else(|| {
			Error::DbNotFound {
				name: database.clone(),
			}
		})?;
		let factory = ctx.try_get_transaction_factory()?;
		let sequences = ctx.try_get_sequences()?.clone();
		let operation_id = Uuid::now_v7().to_string();
		let event = RebuildEventGuard::new(
			ctx.tx(),
			operation_id.clone(),
			namespace.to_owned(),
			database.clone(),
			opt.auth.id().to_owned(),
			started,
		);

		// Persist the maintenance fence before any scan. A crash or disconnect
		// after this commit leaves the database read-only in Rebuilding.
		let fence = factory
			.transaction(TransactionType::Write, LockType::Optimistic, sequences.clone())
			.await?;
		let old_meta = fence.quota_usage(db.namespace_id, db.database_id).meta().await?;
		let generation = fence.get(&Qg::new(db.namespace_id, db.database_id), None).await?;
		if self.if_needed && old_meta.state == QuotaUsageState::Ready {
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
		fence.quota_usage(db.namespace_id, db.database_id).begin_rebuild().await?;
		fence.commit().await?;

		// Build the trusted snapshot while the committed fence rejects all
		// catalog and record mutations for this database.
		let scan_tx = factory
			.transaction(TransactionType::Read, LockType::Optimistic, sequences.clone())
			.await?;
		let scan = scan_tx.scan_quota_usage(db.namespace_id, db.database_id).await?;
		scan_tx.cancel().await?;

		// Stage, validate, and switch the epoch atomically.
		let activate = factory
			.transaction(TransactionType::Write, LockType::Optimistic, sequences.clone())
			.await?;
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
			let cleanup = factory
				.transaction(TransactionType::Write, LockType::Optimistic, sequences)
				.await?;
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

impl ToSql for RebuildQuotaStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::rebuild::RebuildQuotaStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}

impl ToSql for RebuildStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::rebuild::RebuildStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct RebuildIndexStatement {
	pub name: Strand,
	pub table: TableName,
	pub if_exists: bool,
	pub concurrently: bool,
}

impl RebuildIndexStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &FrozenContext, opt: &Options) -> Result<Value> {
		// Allowed to run?
		ctx.is_allowed(opt, Action::Edit, ResourceKind::Index, Base::Db)?;
		// Get the index definition
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let res = ctx.tx().get_tb_index(ns, db, &self.table, self.name.as_str(), None).await?;
		let ix = match res {
			Some(x) => x,
			None => {
				if self.if_exists {
					return Ok(Value::None);
				} else {
					return Err(Error::IxNotFound {
						name: self.name.to_string(),
					}
					.into());
				}
			}
		};
		let tb = ctx.tx().expect_tb(ns, db, &self.table).await?;

		// Rebuild the index
		run_indexing(ctx, opt, tb.table_id, ix, !self.concurrently).await?;
		// Ok all good
		Ok(Value::None)
	}
}

impl ToSql for RebuildIndexStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::rebuild::RebuildIndexStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
