use anyhow::{Result, bail};
use reblessive::tree::Stk;

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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RemoveQuotaStatement {
	pub database: Expr,
	pub if_exists: bool,
	pub expected_generation: u64,
}

impl RemoveQuotaStatement {
	#[instrument(level = "trace", name = "RemoveQuotaStatement::compute", skip_all)]
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
		txn.flush_quota_usage().await?;
		txn.quota_usage(db.namespace_id, db.database_id).ensure_writable_for_update().await?;
		let generation_key = Qg::new(db.namespace_id, db.database_id);
		let stored_generation = txn.get(&generation_key, None).await?;
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
		let tombstone_generation =
			current.generation.checked_add(1).ok_or_else(|| Error::QuotaPolicyInvalid {
				reason: "quota policy generation overflow".to_owned(),
			})?;
		txn.putc(&generation_key, &tombstone_generation, stored_generation.as_ref()).await?;
		txn.delc(&Qt::new(db.namespace_id, db.database_id), Some(current.as_ref())).await?;
		txn.clear_cache();
		Ok(Value::None)
	}
}
