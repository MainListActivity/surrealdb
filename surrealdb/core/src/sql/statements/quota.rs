use surrealdb_strand::Strand;
use surrealdb_types::{Regex, SqlFormat, ToSql, write_sql};

use crate::fmt::EscapeIdent;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum QuotaResource {
	Table,
	Field,
	Record,
}

impl ToSql for QuotaResource {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(match self {
			Self::Table => "TABLE",
			Self::Field => "FIELD",
			Self::Record => "RECORD",
		});
	}
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum QuotaSelector {
	Exact(Strand),
	Regex(Regex),
}

impl ToSql for QuotaSelector {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Exact(table) => write_sql!(f, fmt, "EXACT {}", EscapeIdent(table.as_str())),
			Self::Regex(regex) => write_sql!(f, fmt, "REGEX {}", regex),
		}
	}
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum QuotaLimit {
	Finite(u64),
	Unlimited,
}

impl ToSql for QuotaLimit {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		match self {
			Self::Finite(limit) => f.push_str(&limit.to_string()),
			Self::Unlimited => f.push_str("UNLIMITED"),
		}
	}
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct QuotaRule {
	pub id: Strand,
	pub resource: QuotaResource,
	pub selector: QuotaSelector,
	pub limit: QuotaLimit,
}

impl ToSql for QuotaRule {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(
			f,
			fmt,
			"RULE {} FOR {} MATCH {} LIMIT {}",
			EscapeIdent(self.id.as_str()),
			self.resource,
			self.selector,
			self.limit
		);
	}
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum AlterQuotaClause {
	Set(QuotaRule),
	Drop {
		id: Strand,
		if_exists: bool,
	},
}

impl ToSql for AlterQuotaClause {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Set(rule) => write_sql!(f, fmt, "SET {}", rule),
			Self::Drop {
				id,
				if_exists,
			} => {
				write_sql!(f, fmt, "DROP RULE");
				if *if_exists {
					write_sql!(f, fmt, " IF EXISTS");
				}
				write_sql!(f, fmt, " {}", EscapeIdent(id.as_str()));
			}
		}
	}
}
