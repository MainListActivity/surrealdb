//! Stores the number of records in one physical table.

use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;
use crate::val::TableName;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct QuotaRecordUsage<'a> {
	root: u8,
	namespace_marker: u8,
	pub ns: NamespaceId,
	database_marker: u8,
	pub db: DatabaseId,
	quota_marker: u8,
	quota_q: u8,
	quota_u: u8,
	pub epoch: u64,
	resource_marker: u8,
	resource_r: u8,
	resource_c: u8,
	pub table: Cow<'a, TableName>,
}

impl_kv_key_storekey!(QuotaRecordUsage<'_> => u64);

impl Categorise for QuotaRecordUsage<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseQuotaRecordUsage
	}
}

impl<'a> QuotaRecordUsage<'a> {
	pub(crate) fn new(ns: NamespaceId, db: DatabaseId, epoch: u64, table: &'a TableName) -> Self {
		Self {
			root: b'/',
			namespace_marker: b'*',
			ns,
			database_marker: b'*',
			db,
			quota_marker: b'!',
			quota_q: b'q',
			quota_u: b'u',
			epoch,
			resource_marker: b'!',
			resource_r: b'r',
			resource_c: b'c',
			table: Cow::Borrowed(table),
		}
	}

	pub(crate) fn decode_key(key: &'a [u8]) -> anyhow::Result<Self> {
		let decoded: Self = storekey::decode_borrow(key)?;
		anyhow::ensure!(
			decoded.root == b'/'
				&& decoded.namespace_marker == b'*'
				&& decoded.database_marker == b'*'
				&& decoded.quota_marker == b'!'
				&& decoded.quota_q == b'q'
				&& decoded.quota_u == b'u'
				&& decoded.resource_marker == b'!'
				&& decoded.resource_r == b'r'
				&& decoded.resource_c == b'c',
			"not a quota record usage key"
		);
		Ok(decoded)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key_format_is_frozen() {
		let table = TableName::from("user");
		let encoded =
			QuotaRecordUsage::new(NamespaceId(1), DatabaseId(2), 3, &table).encode_key().unwrap();
		assert_eq!(
			encoded,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!qu\x00\x00\x00\x00\x00\x00\x00\x03!rcuser\0"
		);
		assert_eq!(QuotaRecordUsage::decode_key(&encoded).unwrap().table.as_ref(), &table);
	}
}
