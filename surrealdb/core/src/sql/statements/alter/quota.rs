use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::CoverStmts;
use crate::sql::Expr;
use crate::sql::statements::quota::AlterQuotaClause;

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

impl From<AlterQuotaStatement> for crate::expr::statements::alter::AlterQuotaStatement {
	fn from(value: AlterQuotaStatement) -> Self {
		Self {
			database: value.database.into(),
			if_exists: value.if_exists,
			expected_generation: value.expected_generation,
			clauses: value.clauses,
		}
	}
}

impl From<crate::expr::statements::alter::AlterQuotaStatement> for AlterQuotaStatement {
	fn from(value: crate::expr::statements::alter::AlterQuotaStatement) -> Self {
		Self {
			database: value.database.into(),
			if_exists: value.if_exists,
			expected_generation: value.expected_generation,
			clauses: value.clauses,
		}
	}
}
