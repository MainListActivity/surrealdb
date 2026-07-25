//! Stores the table count charged to one policy generation and rule.

use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct QuotaTableBucket<'a> {
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
	resource_t: u8,
	resource_b: u8,
	pub generation: u64,
	pub rule: Cow<'a, str>,
}

impl_kv_key_storekey!(QuotaTableBucket<'_> => u64);

impl Categorise for QuotaTableBucket<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseQuotaTableBucket
	}
}

impl<'a> QuotaTableBucket<'a> {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		epoch: u64,
		generation: u64,
		rule: &'a str,
	) -> Self {
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
			resource_t: b't',
			resource_b: b'b',
			generation,
			rule: Cow::Borrowed(rule),
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
				&& decoded.resource_t == b't'
				&& decoded.resource_b == b'b',
			"not a quota table bucket key"
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
		let encoded = QuotaTableBucket::new(NamespaceId(1), DatabaseId(2), 3, 7, "ent-tables")
			.encode_key()
			.unwrap();
		assert_eq!(
			encoded,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!qu\x00\x00\x00\x00\x00\x00\x00\x03!tb\x00\x00\x00\x00\x00\x00\x00\x07ent-tables\0"
		);
		let decoded = QuotaTableBucket::decode_key(&encoded).unwrap();
		assert_eq!(decoded.generation, 7);
		assert_eq!(decoded.rule, "ent-tables");
	}
}
