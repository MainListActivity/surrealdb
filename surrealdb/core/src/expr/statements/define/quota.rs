use anyhow::{Result, bail};
use reblessive::tree::Stk;

use super::DefineKind;
use crate::catalog::QuotaPolicyDefinition;
use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::statements::quota::QuotaOperation;
use crate::expr::{Base, Expr, Value};
use crate::iam::{Action, ResourceKind};
use crate::key::database::qg::Qg;
use crate::key::database::ql::Ql;
use crate::key::database::qt::Qt;
use crate::observe::{QuotaEventKind, QuotaEventOutcome};
use crate::sql::statements::quota::QuotaRule;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct DefineQuotaStatement {
	pub kind: DefineKind,
	pub database: Expr,
	pub expected_generation: Option<u64>,
	pub rules: Vec<QuotaRule>,
}

impl DefineQuotaStatement {
	#[instrument(level = "trace", name = "DefineQuotaStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		ctx.is_allowed(opt, Action::Edit, ResourceKind::Quota, Base::Ns)?;
		if opt.import {
			bail!(Error::QuotaImportNotAllowed);
		}
		let namespace = opt.ns()?;
		let database =
			expr_to_ident(stk, ctx, opt, doc, &self.database, "quota database name").await?;
		let txn = ctx.tx();
		let db = txn.get_db_by_name(namespace, &database, None).await?.ok_or_else(|| {
			Error::DbNotFound {
				name: database.clone(),
			}
		})?;
		let operation = QuotaOperation::new("define", database.clone());
		txn.flush_quota_usage().await?;
		let meta =
			txn.quota_usage(db.namespace_id, db.database_id).ensure_writable_for_update().await?;
		let quota_key = Qt::new(db.namespace_id, db.database_id);
		let generation_key = Qg::new(db.namespace_id, db.database_id);
		let stored_generation = txn.get(&generation_key, None).await?;
		let current = txn.get_db_quota(db.namespace_id, db.database_id, None).await?;
		let policy = match current.as_deref() {
			Some(current) => match self.kind {
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
					let Some(expected) = self.expected_generation else {
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
						self.rules.clone(),
					)?;
					if current.rules == policy.rules {
						return Ok(operation.result(
							false,
							Some(current.generation),
							Some(current.generation),
							&meta,
						));
					}
					policy.generation = current.generation.checked_add(1).ok_or_else(|| {
						Error::QuotaPolicyInvalid {
							reason: "quota policy generation overflow".to_owned(),
						}
					})?;
					policy
				}
			},
			None => {
				if let Some(expected) = self.expected_generation {
					bail!(Error::QuotaGenerationMismatch {
						database,
						expected,
						actual: 0,
					});
				}
				let generation =
					stored_generation.unwrap_or(0).checked_add(1).ok_or_else(|| {
						Error::QuotaPolicyInvalid {
							reason: "quota policy generation overflow".to_owned(),
						}
					})?;
				QuotaPolicyDefinition::new(database.clone().into(), generation, self.rules.clone())?
			}
		};

		txn.putc(&quota_key, &policy, current.as_deref()).await?;
		txn.putc(&generation_key, &policy.generation, stored_generation.as_ref()).await?;
		let tables = txn.all_tb(db.namespace_id, db.database_id, None).await?;
		txn.quota_usage(db.namespace_id, db.database_id)
			.initialize_policy_table_buckets(&policy, &tables)
			.await?;
		let change = operation.latest_change(opt.auth.id().to_string(), policy.generation);
		txn.set(&Ql::new(db.namespace_id, db.database_id), &change).await?;
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
}
