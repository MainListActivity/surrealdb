//! Stores the monotonic quota-policy generation high-water mark for a database.

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct Qg {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
}

impl_kv_key_storekey!(Qg => u64);

impl Categorise for Qg {
	fn categorise(&self) -> Category {
		Category::DatabaseQuotaGeneration
	}
}

impl Qg {
	pub(crate) fn new(ns: NamespaceId, db: DatabaseId) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'q',
			_e: b'g',
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let val = Qg::new(NamespaceId(1), DatabaseId(2));
		let enc = Qg::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!qg");
	}
}
