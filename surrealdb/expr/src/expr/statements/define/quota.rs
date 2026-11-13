use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::expr::statements::quota::QuotaRule;
use crate::expr::{CoverStmts, Expr};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineQuotaStatement {
	pub kind: DefineKind,
	pub database: Expr,
	pub expected_generation: Option<u64>,
	pub rules: Vec<QuotaRule>,
}

impl ToSql for DefineQuotaStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "DEFINE QUOTA");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, fmt, " IF NOT EXISTS"),
		}
		write_sql!(f, fmt, " ON DATABASE {}", CoverStmts(&self.database));
		if let Some(generation) = self.expected_generation {
			write_sql!(f, fmt, " EXPECT GENERATION {}", generation);
		}
		for rule in &self.rules {
			write_sql!(f, fmt, " {}", rule);
		}
	}
}
