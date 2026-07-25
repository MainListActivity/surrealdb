use std::collections::{BTreeMap, HashSet};

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{QuotaPolicyDefinition, QuotaRuleDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, Value};
use crate::iam::{Action, ResourceKind};
use crate::key::database::qg::Qg;
use crate::key::database::qt::Qt;
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
		let Some(current) = txn.get_db_quota(db.namespace_id, db.database_id, None).await? else {
			if self.if_exists {
				return Ok(Value::None);
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
			return Ok(Value::None);
		}
		policy.generation =
			current.generation.checked_add(1).ok_or_else(|| Error::QuotaPolicyInvalid {
				reason: "quota policy generation overflow".to_owned(),
			})?;
		txn.set(&Qt::new(db.namespace_id, db.database_id), &policy).await?;
		txn.set(&Qg::new(db.namespace_id, db.database_id), &policy.generation).await?;
		txn.clear_cache();
		Ok(Value::None)
	}
}

impl ToSql for AlterQuotaStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let statement: crate::sql::statements::alter::AlterQuotaStatement = self.clone().into();
		statement.fmt_sql(f, fmt);
	}
}
