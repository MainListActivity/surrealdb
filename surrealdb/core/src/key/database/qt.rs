//! Stores the singleton native quota policy for a database.

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId, QuotaPolicyDefinition};
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct Qt {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
}

impl_kv_key_storekey!(Qt => QuotaPolicyDefinition);

impl Categorise for Qt {
	fn categorise(&self) -> Category {
		Category::DatabaseQuota
	}
}

impl Qt {
	pub(crate) fn new(ns: NamespaceId, db: DatabaseId) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'q',
			_e: b't',
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let val = Qt::new(NamespaceId(1), DatabaseId(2));
		let enc = Qt::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!qt");
	}
}
