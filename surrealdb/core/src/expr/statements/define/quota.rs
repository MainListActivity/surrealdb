use anyhow::{Result, bail};
use reblessive::tree::Stk;

use super::DefineKind;
use crate::catalog::QuotaPolicyDefinition;
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, Value};
use crate::iam::{Action, ResourceKind};
use crate::key::database::qg::Qg;
use crate::key::database::qt::Qt;
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
		let quota_key = Qt::new(db.namespace_id, db.database_id);
		let generation_key = Qg::new(db.namespace_id, db.database_id);
		let current = txn.get_db_quota(db.namespace_id, db.database_id, None).await?;
		let policy = match current.as_deref() {
			Some(current) => match self.kind {
				DefineKind::Default => {
					bail!(Error::QuotaAlreadyExists {
						database,
					});
				}
				DefineKind::IfNotExists => return Ok(Value::None),
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
						return Ok(Value::None);
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
					txn.get(&generation_key, None).await?.unwrap_or(0).checked_add(1).ok_or_else(
						|| Error::QuotaPolicyInvalid {
							reason: "quota policy generation overflow".to_owned(),
						},
					)?;
				QuotaPolicyDefinition::new(database.clone().into(), generation, self.rules.clone())?
			}
		};

		txn.set(&quota_key, &policy).await?;
		txn.set(&generation_key, &policy.generation).await?;
		txn.clear_cache();
		Ok(Value::None)
	}
}
