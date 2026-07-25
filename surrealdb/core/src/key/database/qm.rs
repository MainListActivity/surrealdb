//! Stores protected quota usage metadata for a database.

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId, QuotaUsageMeta};
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct Qm {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
}

impl_kv_key_storekey!(Qm => QuotaUsageMeta);

impl Categorise for Qm {
	fn categorise(&self) -> Category {
		Category::DatabaseQuotaUsageMeta
	}
}

impl Qm {
	pub(crate) fn new(ns: NamespaceId, db: DatabaseId) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'q',
			_e: b'm',
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key_format_is_frozen() {
		let encoded = Qm::new(NamespaceId(1), DatabaseId(2)).encode_key().unwrap();
		assert_eq!(encoded, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!qm");
	}
}
