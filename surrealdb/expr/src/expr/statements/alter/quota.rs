use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::statements::quota::AlterQuotaClause;
use crate::expr::{CoverStmts, Expr};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AlterQuotaStatement {
	pub database: Expr,
	pub if_exists: bool,
	pub expected_generation: u64,
	pub clauses: Vec<AlterQuotaClause>,
}

impl ToSql for AlterQuotaStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER QUOTA");
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
		for clause in &self.clauses {
			write_sql!(f, fmt, " {}", clause);
		}
	}
}
