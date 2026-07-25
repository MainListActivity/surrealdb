//! Native quota INFO operator.

use std::sync::Arc;

use futures::stream;
use surrealdb_types::ToSql;

use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{
	AccessMode, CardinalityHint, ExecOperator, FlowResult, OperatorMetrics, ValueBatch,
	ValueBatchStream,
};
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

/// Returns the canonical policy text or stable structured policy/ledger/usage DTO.
#[derive(Debug)]
pub struct QuotaInfoPlan {
	database: Arc<dyn PhysicalExpr>,
	structured: bool,
	metrics: Arc<OperatorMetrics>,
}

impl QuotaInfoPlan {
	pub(crate) fn new(database: Arc<dyn PhysicalExpr>, structured: bool) -> Self {
		Self {
			database,
			structured,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

impl ExecOperator for QuotaInfoPlan {
	fn name(&self) -> &'static str {
		"InfoQuota"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![
			("database".to_owned(), self.database.to_sql()),
			("structured".to_owned(), self.structured.to_string()),
		]
	}

	fn required_context(&self) -> ContextLevel {
		self.database.required_context().max(ContextLevel::Namespace)
	}

	fn access_mode(&self) -> AccessMode {
		self.database.access_mode()
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(self.metrics.as_ref())
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		vec![("database", &self.database)]
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let database = Arc::clone(&self.database);
		let structured = self.structured;
		let ctx = ctx.clone();
		Ok(Box::pin(stream::once(async move {
			let value = execute_quota_info(&ctx, database.as_ref(), structured).await?;
			Ok(ValueBatch {
				values: vec![value],
			})
		})))
	}

	fn is_scalar(&self) -> bool {
		true
	}
}

async fn execute_quota_info(
	ctx: &ExecutionContext,
	database_expr: &dyn PhysicalExpr,
	structured: bool,
) -> crate::expr::FlowResult<Value> {
	let options = ctx
		.options()
		.ok_or_else(|| anyhow::anyhow!("Options not available in execution context"))?;
	let eval_ctx = EvalContext::from_exec_ctx(ctx);
	let database = database_expr
		.evaluate(eval_ctx)
		.await?
		.coerce_to::<String>()
		.map_err(|error| anyhow::anyhow!("{error}"))?;
	let base = if options.db().is_ok_and(|selected| selected == database) {
		Base::Db
	} else {
		Base::Ns
	};
	ctx.is_allowed(Action::View, ResourceKind::Quota, base)?;

	let namespace = options.ns()?;
	let txn = ctx.txn();
	let db = txn.get_db_by_name(namespace, &database, None).await?.ok_or_else(|| {
		crate::err::Error::DbNotFound {
			name: database.clone(),
		}
	})?;
	let policy = txn.get_db_quota(db.namespace_id, db.database_id, None).await?;
	if structured {
		let tables = txn.all_tb(db.namespace_id, db.database_id, None).await?;
		Ok(txn
			.quota_usage(db.namespace_id, db.database_id)
			.info_structure(&database, policy.as_deref(), &tables)
			.await?)
	} else {
		Ok(policy.as_deref().map(|policy| Value::from(policy.to_sql())).unwrap_or(Value::None))
	}
}
