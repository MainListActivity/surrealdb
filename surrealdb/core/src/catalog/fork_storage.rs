use anyhow::{Result, bail};
use revision::revisioned;

use crate::catalog::{QUOTA_POLICY_FORMAT_REVISION, QUOTA_USAGE_FORMAT_REVISION};
use crate::err::Error;
use crate::kvs::impl_kv_value_revisioned;
use crate::kvs::version::MajorVersion;

/// Stable identifier written into datastores that require the native-quota fork.
pub(crate) const NATIVE_QUOTA_FORK_ID: &str = "mainlistactivity/surrealdb-native-quota";
/// First fork release line that can read the current protected storage format.
pub(crate) const MINIMUM_COMPATIBLE_FORK_RELEASE: &str = "3.3.0-native-quota.1";
/// Release identity of this binary for storage compatibility comparisons.
pub(crate) const CURRENT_FORK_RELEASE: &str = "3.3.0-native-quota.1";
/// Current structured storage marker format.
pub(crate) const FORK_STORAGE_FORMAT_REVISION: u16 = 1;

/// Explicit migration lifecycle stored in the global fork marker.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum ForkMigrationState {
	/// Every protected format is complete and normal startup may continue.
	Clean,
	/// An explicit offline migration started but has not completed.
	InProgress,
}

/// Structured compatibility marker paired with the fork-required storage version.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ForkStorageFormat {
	/// Marker payload format revision.
	pub(crate) format_revision: u16,
	/// Stable fork identity.
	pub(crate) fork_id: String,
	/// Upstream storage major on which the fork format is based.
	pub(crate) upstream_storage_major: u16,
	/// Native quota policy catalog format.
	pub(crate) quota_policy_format_revision: u16,
	/// Native quota usage catalog format.
	pub(crate) quota_usage_format_revision: u16,
	/// Oldest fork release line allowed to open this format.
	pub(crate) minimum_compatible_fork_release: String,
	/// Explicit migration state.
	pub(crate) migration_state: ForkMigrationState,
}

impl_kv_value_revisioned!(ForkStorageFormat);

impl ForkStorageFormat {
	/// Marker emitted by this binary for a new datastore.
	pub(crate) fn current() -> Self {
		Self {
			format_revision: FORK_STORAGE_FORMAT_REVISION,
			fork_id: NATIVE_QUOTA_FORK_ID.to_owned(),
			upstream_storage_major: MajorVersion::UPSTREAM_LATEST,
			quota_policy_format_revision: QUOTA_POLICY_FORMAT_REVISION,
			quota_usage_format_revision: QUOTA_USAGE_FORMAT_REVISION,
			minimum_compatible_fork_release: MINIMUM_COMPATIBLE_FORK_RELEASE.to_owned(),
			migration_state: ForkMigrationState::Clean,
		}
	}

	/// Fail closed on unknown identities, formats, or incomplete migration.
	pub(crate) fn validate(&self) -> Result<()> {
		if self.fork_id != NATIVE_QUOTA_FORK_ID {
			bail!(Error::ForkStorageFormatIncompatible {
				reason: format!("unknown datastore fork id '{}'", self.fork_id),
			});
		}
		if self.upstream_storage_major != MajorVersion::UPSTREAM_LATEST {
			bail!(Error::ForkStorageFormatIncompatible {
				reason: format!(
					"unsupported upstream storage major {}",
					self.upstream_storage_major
				),
			});
		}
		let minimum =
			semver::Version::parse(&self.minimum_compatible_fork_release).map_err(|error| {
				Error::ForkStorageFormatIncompatible {
					reason: format!(
						"invalid minimum compatible fork release '{}': {error}",
						self.minimum_compatible_fork_release
					),
				}
			})?;
		let current = semver::Version::parse(CURRENT_FORK_RELEASE).map_err(|error| {
			Error::ForkStorageFormatIncompatible {
				reason: format!("invalid current fork release '{CURRENT_FORK_RELEASE}': {error}"),
			}
		})?;
		if current < minimum {
			bail!(Error::ForkStorageFormatIncompatible {
				reason: format!(
					"datastore requires fork release {} but this binary is {}",
					self.minimum_compatible_fork_release, CURRENT_FORK_RELEASE
				),
			});
		}
		if self.format_revision > FORK_STORAGE_FORMAT_REVISION {
			bail!(Error::ForkStorageFormatIncompatible {
				reason: format!(
					"unsupported newer fork storage marker revision {}",
					self.format_revision
				),
			});
		}
		if self.quota_policy_format_revision > QUOTA_POLICY_FORMAT_REVISION {
			bail!(Error::ForkStorageFormatIncompatible {
				reason: format!(
					"unsupported newer quota policy format revision {}",
					self.quota_policy_format_revision
				),
			});
		}
		if self.quota_usage_format_revision > QUOTA_USAGE_FORMAT_REVISION {
			bail!(Error::ForkStorageFormatIncompatible {
				reason: format!(
					"unsupported newer quota usage format revision {}",
					self.quota_usage_format_revision
				),
			});
		}
		if self.format_revision < FORK_STORAGE_FORMAT_REVISION
			|| self.quota_policy_format_revision < QUOTA_POLICY_FORMAT_REVISION
			|| self.quota_usage_format_revision < QUOTA_USAGE_FORMAT_REVISION
			|| self.migration_state != ForkMigrationState::Clean
		{
			bail!(Error::ForkStorageMigrationRequired {
				actual: MajorVersion::LATEST,
			});
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVValue;

	#[test]
	fn current_marker_revision_fixture_is_frozen() {
		assert_eq!(
			ForkStorageFormat::current().kv_encode_value().unwrap(),
			vec![
				1, 1, 39, 109, 97, 105, 110, 108, 105, 115, 116, 97, 99, 116, 105, 118, 105, 116,
				121, 47, 115, 117, 114, 114, 101, 97, 108, 100, 98, 45, 110, 97, 116, 105, 118,
				101, 45, 113, 117, 111, 116, 97, 3, 1, 1, 20, 51, 46, 51, 46, 48, 45, 110, 97, 116,
				105, 118, 101, 45, 113, 117, 111, 116, 97, 46, 49, 1, 0,
			]
		);
	}
}
