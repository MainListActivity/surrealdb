//! Prefix for every quota usage counter in one epoch.

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct QuotaEpochRoot {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
	pub epoch: u64,
}

impl_kv_key_storekey!(QuotaEpochRoot => Vec<u8>);

impl Categorise for QuotaEpochRoot {
	fn categorise(&self) -> Category {
		Category::DatabaseQuotaEpochRoot
	}
}

impl QuotaEpochRoot {
	pub(crate) fn new(ns: NamespaceId, db: DatabaseId, epoch: u64) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'q',
			_e: b'u',
			epoch,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key_format_is_frozen() {
		let encoded = QuotaEpochRoot::new(NamespaceId(1), DatabaseId(2), 3).encode_key().unwrap();
		assert_eq!(
			encoded,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!qu\x00\x00\x00\x00\x00\x00\x00\x03"
		);
	}
}
