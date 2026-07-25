use std::collections::HashSet;

use anyhow::{Result, bail};
use revision::revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::err::Error;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql;
use crate::sql::statements::define::{DefineKind, DefineQuotaStatement};
use crate::sql::statements::quota::{
	QuotaLimit as SqlQuotaLimit, QuotaResource as SqlQuotaResource, QuotaRule as SqlQuotaRule,
	QuotaSelector as SqlQuotaSelector,
};
use crate::val::{Datetime, Regex};

/// Current format revision of native quota policies.
pub(crate) const QUOTA_POLICY_FORMAT_REVISION: u16 = 1;

/// A resource counted by a quota rule.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum QuotaResource {
	/// Number of physical tables matching the selector.
	Table,
	/// Number of fields on each physical table matching the selector.
	Field,
	/// Number of records in each physical table matching the selector.
	Record,
}

/// A table selector used by a quota rule.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum QuotaSelector {
	/// Match one exact physical table name.
	Exact(Strand),
	/// Match physical table names using a compiled regular expression.
	Regex(Regex),
}

/// A finite or explicitly unlimited quota.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum QuotaLimit {
	/// A finite non-negative limit.
	Finite(u64),
	/// An explicit unlimited rule.
	Unlimited,
}

/// One stable, typed quota rule.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct QuotaRuleDefinition {
	/// Stable identifier, unique within the policy.
	pub id: Strand,
	/// Counted resource.
	pub resource: QuotaResource,
	/// Physical-table selector.
	pub selector: QuotaSelector,
	/// Applied limit.
	pub limit: QuotaLimit,
}

/// Complete database-scoped quota policy snapshot.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct QuotaPolicyDefinition {
	/// Catalog payload format revision.
	pub format_revision: u16,
	/// Optimistic-concurrency generation.
	pub generation: u64,
	/// Target database name, retained for canonical SurrealQL output.
	pub database: Strand,
	/// Canonically sorted rules.
	pub rules: Vec<QuotaRuleDefinition>,
}

impl_kv_value_revisioned!(QuotaPolicyDefinition);

/// Durable pointer to the latest committed quota-policy change.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct QuotaPolicyChange {
	/// Correlation identifier returned by the mutating operation.
	pub operation_id: String,
	/// Stable action label (`define`, `alter`, or `remove`).
	pub action: String,
	/// Authenticated actor identifier.
	pub actor: String,
	/// Commit-time policy generation, including remove tombstones.
	pub generation: u64,
	/// Time at which the change was staged in the committing transaction.
	pub changed_at: Datetime,
}

impl_kv_value_revisioned!(QuotaPolicyChange);

impl QuotaPolicyDefinition {
	/// Construct and validate a canonical policy snapshot.
	pub(crate) fn new(database: Strand, generation: u64, rules: Vec<SqlQuotaRule>) -> Result<Self> {
		let rules =
			rules.into_iter().map(QuotaRuleDefinition::try_from).collect::<Result<Vec<_>>>()?;
		let mut policy = Self {
			format_revision: QUOTA_POLICY_FORMAT_REVISION,
			generation,
			database,
			rules,
		};
		policy.normalize_and_validate()?;
		Ok(policy)
	}

	/// Sort rules and reject ambiguous identities or exact selectors.
	pub(crate) fn normalize_and_validate(&mut self) -> Result<()> {
		if self.format_revision != QUOTA_POLICY_FORMAT_REVISION {
			bail!(Error::QuotaPolicyInvalid {
				reason: format!(
					"unsupported quota policy format revision {}",
					self.format_revision
				),
			});
		}
		if self.generation == 0 {
			bail!(Error::QuotaPolicyInvalid {
				reason: "quota policy generation must be greater than zero".to_owned(),
			});
		}
		self.rules.sort_by(|left, right| left.id.cmp(&right.id));
		let mut ids = HashSet::with_capacity(self.rules.len());
		let mut exact = HashSet::with_capacity(self.rules.len());
		for rule in &self.rules {
			if !ids.insert(rule.id.clone()) {
				bail!(Error::QuotaPolicyInvalid {
					reason: format!("duplicate quota rule id '{}'", rule.id),
				});
			}
			if let QuotaSelector::Exact(table) = &rule.selector
				&& !exact.insert((rule.resource, table.clone()))
			{
				bail!(Error::QuotaPolicyInvalid {
					reason: format!(
						"duplicate exact {:?} quota rule for table '{}'",
						rule.resource, table
					),
				});
			}
		}
		Ok(())
	}

	fn to_sql_definition(&self) -> DefineQuotaStatement {
		DefineQuotaStatement {
			kind: DefineKind::Default,
			database: sql::Expr::Idiom(sql::Idiom::field(self.database.clone())),
			expected_generation: None,
			rules: self.rules.iter().cloned().map(Into::into).collect(),
		}
	}
}

impl ToSql for &QuotaPolicyDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt);
	}
}

impl TryFrom<SqlQuotaRule> for QuotaRuleDefinition {
	type Error = anyhow::Error;

	fn try_from(value: SqlQuotaRule) -> Result<Self> {
		let selector = match value.selector {
			SqlQuotaSelector::Exact(table) => QuotaSelector::Exact(table),
			SqlQuotaSelector::Regex(regex) => {
				let regex =
					regex.regex().as_str().parse().map_err(|error| Error::QuotaPolicyInvalid {
						reason: format!("invalid quota table selector regex: {error}"),
					})?;
				QuotaSelector::Regex(regex)
			}
		};
		Ok(Self {
			id: value.id,
			resource: value.resource.into(),
			selector,
			limit: value.limit.into(),
		})
	}
}

impl From<QuotaRuleDefinition> for SqlQuotaRule {
	fn from(value: QuotaRuleDefinition) -> Self {
		Self {
			id: value.id,
			resource: value.resource.into(),
			selector: value.selector.into(),
			limit: value.limit.into(),
		}
	}
}

impl From<SqlQuotaResource> for QuotaResource {
	fn from(value: SqlQuotaResource) -> Self {
		match value {
			SqlQuotaResource::Table => Self::Table,
			SqlQuotaResource::Field => Self::Field,
			SqlQuotaResource::Record => Self::Record,
		}
	}
}

impl From<QuotaResource> for SqlQuotaResource {
	fn from(value: QuotaResource) -> Self {
		match value {
			QuotaResource::Table => Self::Table,
			QuotaResource::Field => Self::Field,
			QuotaResource::Record => Self::Record,
		}
	}
}

impl From<QuotaSelector> for SqlQuotaSelector {
	fn from(value: QuotaSelector) -> Self {
		match value {
			QuotaSelector::Exact(table) => Self::Exact(table),
			QuotaSelector::Regex(regex) => Self::Regex(regex.into()),
		}
	}
}

impl From<SqlQuotaLimit> for QuotaLimit {
	fn from(value: SqlQuotaLimit) -> Self {
		match value {
			SqlQuotaLimit::Finite(limit) => Self::Finite(limit),
			SqlQuotaLimit::Unlimited => Self::Unlimited,
		}
	}
}

impl From<QuotaLimit> for SqlQuotaLimit {
	fn from(value: QuotaLimit) -> Self {
		match value {
			QuotaLimit::Finite(limit) => Self::Finite(limit),
			QuotaLimit::Unlimited => Self::Unlimited,
		}
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_types::ToSql;

	use super::*;

	fn exact_rule(id: &str, resource: SqlQuotaResource, table: &str, limit: u64) -> SqlQuotaRule {
		SqlQuotaRule {
			id: id.into(),
			resource,
			selector: SqlQuotaSelector::Exact(table.into()),
			limit: SqlQuotaLimit::Finite(limit),
		}
	}

	#[test]
	fn policy_is_canonical_and_revision_round_trips() {
		let policy = QuotaPolicyDefinition::new(
			"app".into(),
			3,
			vec![
				exact_rule("records", SqlQuotaResource::Record, "user", 100),
				exact_rule("fields", SqlQuotaResource::Field, "user", 20),
			],
		)
		.unwrap();
		assert_eq!(policy.rules[0].id.as_str(), "fields");
		assert_eq!(
			(&policy).to_sql(),
			"DEFINE QUOTA ON DATABASE app RULE fields FOR FIELD MATCH EXACT user LIMIT 20 \
			 RULE records FOR RECORD MATCH EXACT user LIMIT 100"
		);

		let encoded = revision::to_vec(&policy).unwrap();
		assert_eq!(
			encoded,
			[
				1, 1, 3, 3, 97, 112, 112, 2, 1, 6, 102, 105, 101, 108, 100, 115, 1, 1, 1, 0, 4,
				117, 115, 101, 114, 1, 0, 20, 1, 7, 114, 101, 99, 111, 114, 100, 115, 1, 2, 1, 0,
				4, 117, 115, 101, 114, 1, 0, 100,
			]
		);
		let decoded: QuotaPolicyDefinition = revision::from_slice(&encoded).unwrap();
		assert_eq!(decoded, policy);
	}

	#[test]
	fn duplicate_ids_and_exact_selectors_are_rejected() {
		let duplicate_id = QuotaPolicyDefinition::new(
			"app".into(),
			1,
			vec![
				exact_rule("same", SqlQuotaResource::Field, "user", 10),
				exact_rule("same", SqlQuotaResource::Record, "user", 20),
			],
		)
		.unwrap_err();
		assert!(matches!(duplicate_id.downcast_ref(), Some(Error::QuotaPolicyInvalid { .. })));

		let duplicate_exact = QuotaPolicyDefinition::new(
			"app".into(),
			1,
			vec![
				exact_rule("first", SqlQuotaResource::Record, "user", 10),
				exact_rule("second", SqlQuotaResource::Record, "user", 20),
			],
		)
		.unwrap_err();
		assert!(matches!(duplicate_exact.downcast_ref(), Some(Error::QuotaPolicyInvalid { .. })));

		let invalid_generation =
			QuotaPolicyDefinition::new("app".into(), 0, Vec::new()).unwrap_err();
		assert!(matches!(
			invalid_generation.downcast_ref(),
			Some(Error::QuotaPolicyInvalid { .. })
		));
	}
}
