use std::ops::Range;

use surrealdb_kvs::api::{
	BoxFut, GetMultiResult, KeySpan, KeyValSpan, KeyVisitor, KeysBatch, KeysResult, ScanChunkStats,
	ScanCursorKeys, ScanCursorVals, ScanResult, ValVisitor, ValsBatch,
};
use surrealdb_kvs::{
	Direction, Error, Key, KeyRange, Metrics, Result, Transactable, TransactionBuilder,
	TransactionType, Val,
};
use surrealdb_kvs_any::{BackendProvider, ConnectContext};
use tokio::sync::Mutex;

use crate::RocksDbConfig;
use crate::kvs::{self, api as legacy};

/// A real RocksDB backend; this provider never opens a store as SurrealKV.
pub struct RocksDbProvider;

impl BackendProvider for RocksDbProvider {
	fn schemes(&self) -> &[&'static str] {
		&["rocksdb"]
	}
	fn connect<'a>(
		&'a self,
		ctx: ConnectContext<'a>,
	) -> BoxFut<'a, Result<Box<dyn TransactionBuilder>>> {
		Box::pin(async move {
			let config = ctx.config.load();
			Ok(Box::new(Datastore::new(ctx.path, config).await?) as Box<dyn TransactionBuilder>)
		})
	}
}

pub struct Datastore {
	inner: kvs::rocksdb::Datastore,
}

impl Datastore {
	pub async fn new(path: &str, config: RocksDbConfig) -> Result<Self> {
		Ok(Self {
			inner: kvs::rocksdb::Datastore::new(path, config).await?,
		})
	}
}

impl TransactionBuilder for Datastore {
	fn name(&self) -> &'static str {
		"rocksdb"
	}
	fn new_transaction(
		&self,
		write: TransactionType,
	) -> BoxFut<'_, Result<(Box<dyn Transactable>, bool)>> {
		Box::pin(async move {
			let inner = self.inner.transaction(write == TransactionType::Write, false).await?;
			Ok((
				Box::new(Transaction {
					inner,
					savepoints: Mutex::new(surrealdb_kvs::SavepointStack::default()),
				}) as Box<dyn Transactable>,
				true,
			))
		})
	}
	fn shutdown(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move { self.inner.shutdown().await.map_err(Into::into) })
	}
	fn register_metrics(&self) -> Option<Metrics> {
		Some(self.inner.register_metrics())
	}
	fn collect_u64_metric(&self, metric: &str) -> Option<u64> {
		self.inner.collect_u64_metric(metric)
	}
}

struct Transaction {
	inner: Box<dyn legacy::Transactable>,
	// Native RocksDB bindings have rollback-to-savepoint but no pop-savepoint.
	// Each logical frame counts its own snapshot and released descendant snapshots.
	// Rolling it back pops all of them, preserving outer rollback semantics.
	savepoints: Mutex<surrealdb_kvs::SavepointStack>,
}

fn owned_range(range: KeyRange<'_>) -> Range<Vec<u8>> {
	range.start.into_vec()..range.end.into_vec()
}

impl From<kvs::Error> for Error {
	fn from(error: kvs::Error) -> Self {
		match error {
			kvs::Error::Datastore(message) => Self::Datastore(message),
			kvs::Error::ConnectionFailed(message) => Self::ConnectionFailed(message),
			kvs::Error::Transaction(message) => Self::Transaction(message),
			kvs::Error::TransactionRangeTooLarge(message) => {
				Self::TransactionRangeTooLarge(message)
			}
			kvs::Error::TransactionConflict(message) => Self::TransactionConflict(message),
			kvs::Error::TimestampInvalid(message) => Self::TimestampInvalid(message),
			kvs::Error::Internal(message) => Self::Internal(message),
			kvs::Error::CommitOutcomeUnknown(message) => Self::CommitOutcomeUnknown(message),
			kvs::Error::ReadAndDeleteOnly => Self::ReadAndDeleteOnly,
			kvs::Error::TransactionTooLarge => Self::TransactionTooLarge,
			kvs::Error::TransactionKeyTooLarge => Self::TransactionKeyTooLarge,
			kvs::Error::TransactionFinished => Self::TransactionFinished,
			kvs::Error::TransactionReadonly => Self::TransactionReadonly,
			kvs::Error::TransactionConditionNotMet => Self::TransactionConditionNotMet,
			kvs::Error::TransactionKeyAlreadyExists => Self::TransactionKeyAlreadyExists,
			kvs::Error::UnsupportedVersionedQueries => Self::UnsupportedVersionedQueries,
			kvs::Error::CompactionNotSupported => Self::CompactionNotSupported,
			kvs::Error::NoSavepoint => Self::NoSavepoint,
		}
	}
}

impl Transactable for Transaction {
	fn kind(&self) -> &'static str {
		"rocksdb"
	}
	fn closed(&self) -> bool {
		self.inner.closed()
	}
	fn writeable(&self) -> bool {
		self.inner.writeable()
	}
	// RocksDB owns the exclusive directory lock and sets a snapshot on every transaction.
	fn sole_writer(&self) -> bool {
		true
	}
	fn parallel_count(&self) -> bool {
		true
	}
	// Plain reads do not register conflicts. Only locked reads and writes do.
	fn validates_reads(&self) -> bool {
		false
	}
	fn shared_locked_reads(&self) -> bool {
		true
	}
	fn blind_write_conflicts(&self) -> bool {
		true
	}
	fn cancel(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move { self.inner.cancel().await.map_err(Into::into) })
	}
	fn commit(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move { self.inner.commit().await.map_err(Into::into) })
	}
	fn exists<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, Result<bool>> {
		Box::pin(
			async move { self.inner.exists(key.into_vec(), version).await.map_err(Into::into) },
		)
	}
	fn get<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, Result<Option<Val>>> {
		Box::pin(async move { self.inner.get(key.into_vec(), version).await.map_err(Into::into) })
	}
	fn getu<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, Result<Option<Val>>> {
		Box::pin(async move { self.inner.getu(key.into_vec()).await.map_err(Into::into) })
	}
	fn getm<'a>(
		&'a self,
		keys: &'a [Key<'a>],
		version: Option<u64>,
	) -> BoxFut<'a, Result<GetMultiResult>> {
		Box::pin(async move {
			let old = self
				.inner
				.getm(keys.iter().map(|key| key.as_slice().to_vec()).collect(), version)
				.await?;
			Ok(GetMultiResult {
				values: old.values,
				records: old.records,
				value_bytes: old.value_bytes,
			})
		})
	}
	fn set<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, Result<()>> {
		Box::pin(async move { self.inner.set(key.into_vec(), val).await.map_err(Into::into) })
	}
	fn put<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, Result<()>> {
		Box::pin(async move { self.inner.put(key.into_vec(), val).await.map_err(Into::into) })
	}
	fn putc<'a>(&'a self, key: Key<'a>, val: Val, chk: Option<Val>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move { self.inner.putc(key.into_vec(), val, chk).await.map_err(Into::into) })
	}
	fn del<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move { self.inner.del(key.into_vec()).await.map_err(Into::into) })
	}
	fn delc<'a>(&'a self, key: Key<'a>, chk: Option<&'a [u8]>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			self.inner.delc(key.into_vec(), chk.map(<[u8]>::to_vec)).await.map_err(Into::into)
		})
	}
	fn keys<'a>(
		&'a self,
		range: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<KeysResult>> {
		Box::pin(async move {
			let old = self.inner.keys(owned_range(range), limit, skip, version).await?;
			Ok(KeysResult {
				keys: old.keys,
				key_bytes: old.key_bytes,
			})
		})
	}
	fn keysr<'a>(
		&'a self,
		range: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<KeysResult>> {
		Box::pin(async move {
			let old = self.inner.keysr(owned_range(range), limit, skip, version).await?;
			Ok(KeysResult {
				keys: old.keys,
				key_bytes: old.key_bytes,
			})
		})
	}
	fn scan<'a>(
		&'a self,
		range: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<ScanResult>> {
		Box::pin(async move {
			let old = self.inner.scan(owned_range(range), limit, skip, version).await?;
			Ok(ScanResult {
				values: old.values,
				key_bytes: old.key_bytes,
				value_bytes: old.value_bytes,
			})
		})
	}
	fn scanr<'a>(
		&'a self,
		range: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<ScanResult>> {
		Box::pin(async move {
			let old = self.inner.scanr(owned_range(range), limit, skip, version).await?;
			Ok(ScanResult {
				values: old.values,
				key_bytes: old.key_bytes,
				value_bytes: old.value_bytes,
			})
		})
	}
	fn count<'a>(&'a self, range: KeyRange<'a>, version: Option<u64>) -> BoxFut<'a, Result<usize>> {
		Box::pin(
			async move { self.inner.count(owned_range(range), version).await.map_err(Into::into) },
		)
	}
	fn open_keys_cursor<'a>(
		&'a self,
		range: KeyRange<'a>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn ScanCursorKeys + 'a>>> {
		Box::pin(async move {
			let inner = self.inner.open_keys_cursor(owned_range(range), dir, skip, version).await?;
			Ok(Box::new(KeysCursor {
				inner,
				bytes: Vec::new(),
				spans: Vec::new(),
			}) as Box<dyn ScanCursorKeys>)
		})
	}
	fn open_vals_cursor<'a>(
		&'a self,
		range: KeyRange<'a>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn ScanCursorVals + 'a>>> {
		Box::pin(async move {
			let inner = self.inner.open_vals_cursor(owned_range(range), dir, skip, version).await?;
			Ok(Box::new(ValsCursor {
				inner,
				keys: Vec::new(),
				values: Vec::new(),
				spans: Vec::new(),
			}) as Box<dyn ScanCursorVals>)
		})
	}
	fn new_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			let mut frames = self.savepoints.lock().await;
			self.inner.new_save_point().await?;
			frames.open();
			Ok(())
		})
	}
	fn release_last_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			let mut frames = self.savepoints.lock().await;
			frames.release();
			Ok(())
		})
	}
	fn rollback_to_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			let mut frames = self.savepoints.lock().await;
			let count = frames.take()?;
			for _ in 0..count {
				if let Err(error) = self.inner.rollback_to_save_point().await {
					// A partially unwound native stack cannot safely be reused.
					let _ = self.inner.cancel().await;
					return Err(error.into());
				}
			}
			Ok(())
		})
	}
	fn compact<'a>(&'a self, range: Option<KeyRange<'a>>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			self.inner.compact(range.map(owned_range)).await.map_err(|error| {
				match error.downcast::<kvs::Error>() {
					Ok(error) => error.into(),
					Err(error) => Error::Transaction(error.to_string()),
				}
			})
		})
	}
}

struct KeysCursor<'a> {
	inner: Box<dyn legacy::ScanCursorKeys + 'a>,
	bytes: Vec<u8>,
	spans: Vec<KeySpan>,
}
impl ScanCursorKeys for KeysCursor<'_> {
	fn next_batch<'s>(&'s mut self, limit: u32) -> BoxFut<'s, Result<KeysBatch<'s>>> {
		Box::pin(async move {
			let old = self.inner.next_batch(limit).await?;
			self.bytes.clear();
			self.spans.clear();
			for key in old.iter() {
				self.spans.push(KeySpan {
					offset: self.bytes.len(),
					len: key.len(),
				});
				self.bytes.extend_from_slice(key);
			}
			Ok(KeysBatch::from_parts(&self.bytes, &self.spans, old.key_bytes))
		})
	}
	fn for_each<'s>(
		&'s mut self,
		limit: u32,
		f: &'s mut dyn KeyVisitor,
	) -> BoxFut<'s, Result<ScanChunkStats>> {
		Box::pin(async move {
			let mut visitor_error = None;
			let result = {
				let mut bridge = |key: &[u8]| {
					f(key).map_err(|error| {
						visitor_error = Some(error);
						kvs::Error::Internal("v4 cursor visitor failed".into())
					})
				};
				self.inner.for_each(limit, &mut bridge).await
			};
			if let Some(error) = visitor_error {
				return Err(error);
			}
			result.map(stats).map_err(Into::into)
		})
	}
}
struct ValsCursor<'a> {
	inner: Box<dyn legacy::ScanCursorVals + 'a>,
	keys: Vec<u8>,
	values: Vec<u8>,
	spans: Vec<KeyValSpan>,
}
impl ScanCursorVals for ValsCursor<'_> {
	fn next_batch<'s>(&'s mut self, limit: u32) -> BoxFut<'s, Result<ValsBatch<'s>>> {
		Box::pin(async move {
			let old = self.inner.next_batch(limit).await?;
			self.keys.clear();
			self.values.clear();
			self.spans.clear();
			for (key, value) in old.iter() {
				self.spans.push(KeyValSpan {
					key_offset: self.keys.len(),
					key_len: key.len(),
					val_offset: self.values.len(),
					val_len: value.len(),
				});
				self.keys.extend_from_slice(key);
				self.values.extend_from_slice(value);
			}
			Ok(ValsBatch::from_parts(
				&self.keys,
				&self.values,
				&self.spans,
				old.key_bytes,
				old.value_bytes,
			))
		})
	}
	fn for_each<'s>(
		&'s mut self,
		limit: u32,
		f: &'s mut dyn ValVisitor,
	) -> BoxFut<'s, Result<ScanChunkStats>> {
		Box::pin(async move {
			let mut visitor_error = None;
			let result = {
				let mut bridge = |key: &[u8], value: &[u8]| {
					f(key, value).map_err(|error| {
						visitor_error = Some(error);
						kvs::Error::Internal("v4 cursor visitor failed".into())
					})
				};
				self.inner.for_each(limit, &mut bridge).await
			};
			if let Some(error) = visitor_error {
				return Err(error);
			}
			result.map(stats).map_err(Into::into)
		})
	}
}
fn stats(old: legacy::ScanChunkStats) -> ScanChunkStats {
	ScanChunkStats {
		rows: old.rows,
		key_bytes: old.key_bytes,
		value_bytes: old.value_bytes,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn owned_ranges_preserve_binary_bounds() {
		let start = [0, 255, 1];
		let end = [1, 0];
		assert_eq!(
			owned_range((start.as_slice()..end.as_slice()).into()),
			start.to_vec()..end.to_vec()
		);
	}

	#[test]
	fn uncertain_commit_is_not_downgraded_to_retryable_conflict() {
		assert!(matches!(
			Error::from(kvs::Error::CommitOutcomeUnknown("after commit".into())),
			Error::CommitOutcomeUnknown(_)
		));
		assert!(matches!(
			Error::from(kvs::Error::TransactionConflict("busy".into())),
			Error::TransactionConflict(_)
		));
	}

	#[test]
	fn provider_claims_only_rocksdb() {
		assert_eq!(RocksDbProvider.schemes(), &["rocksdb"]);
	}
}
