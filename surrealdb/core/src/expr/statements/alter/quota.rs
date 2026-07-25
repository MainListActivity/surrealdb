use std::collections::{BTreeMap, HashSet};

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::catalog::{QuotaPolicyDefinition, QuotaRuleDefinition};
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
use crate::sql::statements::quota::AlterQuotaClause;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct AlterQuotaStatement {
	pub database: Expr,
	pub if_exists: bool,
	pub expected_generation: u64,
	pub clauses: Vec<AlterQuotaClause>,
}

impl AlterQuotaStatement {
	#[instrument(level = "trace", name = "AlterQuotaStatement::compute", skip_all)]
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
		let operation = QuotaOperation::new("alter", database.clone());
		txn.flush_quota_usage().await?;
		let meta =
			txn.quota_usage(db.namespace_id, db.database_id).ensure_writable_for_update().await?;
		let generation_key = Qg::new(db.namespace_id, db.database_id);
		let stored_generation = txn.get(&generation_key, None).await?;
		let Some(current) = txn.get_db_quota(db.namespace_id, db.database_id, None).await? else {
			if self.if_exists {
				return Ok(operation.result(false, stored_generation, stored_generation, &meta));
			}
			bail!(Error::QuotaNotFound {
				database,
			});
		};
		if current.generation != self.expected_generation {
			bail!(Error::QuotaGenerationMismatch {
				database,
				expected: self.expected_generation,
				actual: current.generation,
			});
		}

		let mut rules = current
			.rules
			.iter()
			.cloned()
			.map(|rule| (rule.id.clone(), rule))
			.collect::<BTreeMap<_, _>>();
		let mut operated = HashSet::with_capacity(self.clauses.len());
		for clause in &self.clauses {
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
		let mut policy =
			QuotaPolicyDefinition::new(database.into(), current.generation, next_rules)?;
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
		txn.putc(&Qt::new(db.namespace_id, db.database_id), &policy, Some(current.as_ref()))
			.await?;
		txn.putc(&generation_key, &policy.generation, stored_generation.as_ref()).await?;
		let tables = txn.all_tb(db.namespace_id, db.database_id, None).await?;
		txn.quota_usage(db.namespace_id, db.database_id)
			.initialize_policy_table_buckets(&policy, &tables)
			.await?;
		let change = operation.latest_change(opt.auth.id().to_string(), policy.generation);
		txn.set(&Ql::new(db.namespace_id, db.database_id), &change).await?;
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
}

impl ToSql for AlterQuotaStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let statement: crate::sql::statements::alter::AlterQuotaStatement = self.clone().into();
		statement.fmt_sql(f, fmt);
	}
}
