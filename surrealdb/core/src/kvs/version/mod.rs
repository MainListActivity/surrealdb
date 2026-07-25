use anyhow::Result;

use crate::err::Error;
use crate::kvs::KVValue;

#[derive(Copy, Debug, Clone, PartialEq)]
pub struct MajorVersion(u16);

impl From<u16> for MajorVersion {
	fn from(version: u16) -> Self {
		MajorVersion(version)
	}
}

impl From<Option<u16>> for MajorVersion {
	fn from(v: Option<u16>) -> Self {
		v.unwrap_or(0).into()
	}
}

impl From<MajorVersion> for u16 {
	fn from(v: MajorVersion) -> Self {
		v.0
	}
}

impl KVValue for MajorVersion {
	type KeyContext = ();

	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		Ok(self.0.to_be_bytes().to_vec())
	}

	#[inline]
	fn kv_decode_value(v: &[u8], _: ()) -> Result<Self> {
		let bin = v.try_into().map_err(|_| Error::InvalidStorageVersion)?;
		let val = u16::from_be_bytes(bin).into();
		Ok(val)
	}
}

impl MajorVersion {
	/// High bit that makes vanilla and pre-marker forks reject this datastore.
	pub const FORK_REQUIRED_FLAG: u16 = 0x8000;
	/// Latest upstream storage major understood by this fork.
	pub const UPSTREAM_LATEST: u16 = 3;
	/// The latest fork-required version.
	pub const LATEST: u16 = Self::FORK_REQUIRED_FLAG | Self::UPSTREAM_LATEST;
	/// The latest version
	pub fn latest() -> Self {
		Self(Self::LATEST)
	}
	/// SurrealDB version 1
	pub fn v1() -> Self {
		Self(1)
	}
	/// SurrealDB version 2
	pub fn v2() -> Self {
		Self(2)
	}
	/// Latest vanilla upstream storage version, accepted only by the explicit migrator.
	pub fn upstream_latest() -> Self {
		Self(Self::UPSTREAM_LATEST)
	}
	/// Whether opening this version requires a marker-aware native-quota fork.
	pub fn requires_quota_fork(self) -> bool {
		self.0 & Self::FORK_REQUIRED_FLAG != 0
	}
	/// Check if we are running the latest version
	pub fn is_latest(self) -> bool {
		self.0 == Self::LATEST
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_version_encode_decode() {
		let version = MajorVersion::v2();
		let encoded = version.kv_encode_value().unwrap();
		assert_eq!(encoded, vec![0, 2]);
		let decoded = MajorVersion::kv_decode_value(&encoded, ()).unwrap();
		assert_eq!(decoded, version);
	}
}
