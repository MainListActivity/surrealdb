//! Structured global marker for the fork-required native-quota storage format.

use storekey::{BorrowDecode, Encode};

use crate::catalog::ForkStorageFormat;
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct StorageFormat {
	__: u8,
	_a: u8,
	_b: u8,
}

impl_kv_key_storekey!(StorageFormat => ForkStorageFormat);

impl Categorise for StorageFormat {
	fn categorise(&self) -> Category {
		Category::ForkStorageFormat
	}
}

impl StorageFormat {
	pub(crate) fn new() -> Self {
		Self {
			__: b'!',
			_a: b'v',
			_b: b'f',
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key_format_is_frozen() {
		assert_eq!(StorageFormat::new().encode_key().unwrap(), b"!vf");
	}
}
