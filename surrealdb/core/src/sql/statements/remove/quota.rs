use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::CoverStmts;
use crate::sql::Expr;

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct RemoveQuotaStatement {
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

impl From<RemoveQuotaStatement> for crate::expr::statements::remove::RemoveQuotaStatement {
	fn from(value: RemoveQuotaStatement) -> Self {
		Self {
			database: value.database.into(),
			if_exists: value.if_exists,
			expected_generation: value.expected_generation,
		}
	}
}

impl From<crate::expr::statements::remove::RemoveQuotaStatement> for RemoveQuotaStatement {
	fn from(value: crate::expr::statements::remove::RemoveQuotaStatement) -> Self {
		Self {
			database: value.database.into(),
			if_exists: value.if_exists,
			expected_generation: value.expected_generation,
		}
	}
}
