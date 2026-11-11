use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::CoverStmts;
use crate::expr::Expr;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RemoveQuotaStatement {
	pub database: Expr,
	pub if_exists: bool,
	pub expected_generation: u64,
}

impl ToSql for RemoveQuotaStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "REMOVE QUOTA");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(
			f,
			fmt,
			" ON DATABASE {} EXPECT GENERATION {}",
			CoverStmts(&self.database),
			self.expected_generation
		);
	}
}
