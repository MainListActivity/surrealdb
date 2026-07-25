use anyhow::{Result, bail};
use revision::revisioned;

use crate::err::Error;
use crate::kvs::impl_kv_value_revisioned;

/// Current format revision of native quota usage metadata.
pub(crate) const QUOTA_USAGE_FORMAT_REVISION: u16 = 1;

/// Persistent lifecycle state for a database quota usage ledger.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum QuotaUsageState {
	/// The database must be scanned before writes can resume.
	Uninitialized,
	/// A replacement epoch is being built behind a maintenance fence.
	Rebuilding,
	/// The active epoch is valid and writes may proceed.
	Ready,
	/// The ledger failed validation and must be rebuilt.
	Corrupt,
}

/// Protected database-scoped metadata for quota usage epochs.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct QuotaUsageMeta {
	/// Catalog payload format revision.
	pub(crate) format_revision: u16,
	/// Current maintenance state.
	pub(crate) state: QuotaUsageState,
	/// Validated epoch used by normal reads and writes.
	pub(crate) active_epoch: Option<u64>,
	/// Epoch currently receiving rebuild output.
	pub(crate) staged_epoch: Option<u64>,
	/// Monotonic epoch high-water mark.
	pub(crate) epoch_high_water: u64,
}

impl_kv_value_revisioned!(QuotaUsageMeta);

impl QuotaUsageMeta {
	/// Initial state for a newly-created database.
	pub(crate) fn ready_empty() -> Self {
		Self {
			format_revision: QUOTA_USAGE_FORMAT_REVISION,
			state: QuotaUsageState::Ready,
			active_epoch: Some(1),
			staged_epoch: None,
			epoch_high_water: 1,
		}
	}

	/// Compatibility state for an existing database without a trusted ledger.
	pub(crate) fn uninitialized() -> Self {
		Self {
			format_revision: QUOTA_USAGE_FORMAT_REVISION,
			state: QuotaUsageState::Uninitialized,
			active_epoch: None,
			staged_epoch: None,
			epoch_high_water: 0,
		}
	}

	/// Validate the persisted state machine invariants.
	pub(crate) fn validate(&self) -> Result<()> {
		if self.format_revision != QUOTA_USAGE_FORMAT_REVISION {
			bail!(Error::QuotaUsageInvalid {
				reason: format!("unsupported quota usage format revision {}", self.format_revision),
			});
		}
		if self.active_epoch == Some(0) || self.staged_epoch == Some(0) {
			bail!(Error::QuotaUsageInvalid {
				reason: "quota usage epochs must be greater than zero".to_owned(),
			});
		}
		if self.active_epoch.is_some_and(|epoch| epoch > self.epoch_high_water)
			|| self.staged_epoch.is_some_and(|epoch| epoch > self.epoch_high_water)
		{
			bail!(Error::QuotaUsageInvalid {
				reason: "quota usage epoch exceeds the high-water mark".to_owned(),
			});
		}
		match self.state {
			QuotaUsageState::Uninitialized => {
				if self.active_epoch.is_some() || self.staged_epoch.is_some() {
					bail!(Error::QuotaUsageInvalid {
						reason: "uninitialized quota usage cannot reference an epoch".to_owned(),
					});
				}
			}
			QuotaUsageState::Rebuilding => {
				if self.staged_epoch.is_none() || self.active_epoch == self.staged_epoch {
					bail!(Error::QuotaUsageInvalid {
						reason: "rebuilding quota usage requires a distinct staged epoch"
							.to_owned(),
					});
				}
			}
			QuotaUsageState::Ready => {
				if self.active_epoch.is_none() || self.staged_epoch.is_some() {
					bail!(Error::QuotaUsageInvalid {
						reason: "ready quota usage requires only an active epoch".to_owned(),
					});
				}
			}
			QuotaUsageState::Corrupt => {
				if self.staged_epoch.is_some() {
					bail!(Error::QuotaUsageInvalid {
						reason: "corrupt quota usage cannot reference a staged epoch".to_owned(),
					});
				}
			}
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVValue;

	#[test]
	fn ready_usage_revision_fixture_is_frozen() {
		assert_eq!(
			QuotaUsageMeta::ready_empty().kv_encode_value().unwrap(),
			vec![1, 1, 1, 2, 1, 1, 0, 1]
		);
	}
}
