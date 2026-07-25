use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::{CoverStmts, EscapeKwFreeIdent, EscapeKwIdent};
use crate::sql::Expr;
use crate::val::TableName;

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum RebuildStatement {
	Index(RebuildIndexStatement),
	Quota(RebuildQuotaStatement),
}

impl ToSql for RebuildStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Index(v) => v.fmt_sql(f, fmt),
			Self::Quota(v) => v.fmt_sql(f, fmt),
		}
	}
}

impl From<RebuildStatement> for crate::expr::statements::rebuild::RebuildStatement {
	fn from(v: RebuildStatement) -> Self {
		match v {
			RebuildStatement::Index(v) => Self::Index(v.into()),
			RebuildStatement::Quota(v) => Self::Quota(v.into()),
		}
	}
}

impl From<crate::expr::statements::rebuild::RebuildStatement> for RebuildStatement {
	fn from(v: crate::expr::statements::rebuild::RebuildStatement) -> Self {
		match v {
			crate::expr::statements::rebuild::RebuildStatement::Index(v) => Self::Index(v.into()),
			crate::expr::statements::rebuild::RebuildStatement::Quota(v) => Self::Quota(v.into()),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RebuildQuotaStatement {
	pub database: Expr,
	pub if_needed: bool,
}

impl ToSql for RebuildQuotaStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("REBUILD QUOTA");
		if self.if_needed {
			f.push_str(" IF NEEDED");
		}
		write_sql!(f, fmt, " ON DATABASE {}", CoverStmts(&self.database));
	}
}

impl From<RebuildQuotaStatement> for crate::expr::statements::rebuild::RebuildQuotaStatement {
	fn from(value: RebuildQuotaStatement) -> Self {
		Self {
			database: value.database.into(),
			if_needed: value.if_needed,
		}
	}
}

impl From<crate::expr::statements::rebuild::RebuildQuotaStatement> for RebuildQuotaStatement {
	fn from(value: crate::expr::statements::rebuild::RebuildQuotaStatement) -> Self {
		Self {
			database: value.database.into(),
			if_needed: value.if_needed,
		}
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RebuildIndexStatement {
	pub name: Strand,
	pub what: TableName,
	pub if_exists: bool,
	pub concurrently: bool,
}

impl ToSql for RebuildIndexStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "REBUILD INDEX");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(
			f,
			fmt,
			" {} ON {}",
			EscapeKwIdent(self.name.as_str(), &["IF"]),
			EscapeKwFreeIdent(self.what.as_str())
		);
		if self.concurrently {
			write_sql!(f, fmt, " CONCURRENTLY");
		}
	}
}

impl From<RebuildIndexStatement> for crate::expr::statements::rebuild::RebuildIndexStatement {
	fn from(v: RebuildIndexStatement) -> Self {
		Self {
			name: v.name,
			table: v.what,
			if_exists: v.if_exists,
			concurrently: v.concurrently,
		}
	}
}

impl From<crate::expr::statements::rebuild::RebuildIndexStatement> for RebuildIndexStatement {
	fn from(v: crate::expr::statements::rebuild::RebuildIndexStatement) -> Self {
		Self {
			name: v.name,
			what: v.table,
			if_exists: v.if_exists,
			concurrently: v.concurrently,
		}
	}
}
