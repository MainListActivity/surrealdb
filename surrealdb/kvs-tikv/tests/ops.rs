//! Exercises the TiKV-specific operational surface the backend publishes to the engine: the
//! `Maintenance` and `DestroyRange` capabilities and the in-flight transaction count.
//!
//! These need a live cluster, so they are ignored by default; run them with
//! a dedicated disposable cluster and SURREAL_TIKV_TEST_ALLOW_DESTRUCTIVE=1.
//! Never point these maintenance tests at production.
#![expect(clippy::unwrap_used, reason = "a failed setup step should fail the test")]

use std::any::TypeId;
use std::time::Duration;

use surrealdb_cnf::ConfigMap;
use surrealdb_kvs::{
	DestroyRangeHandle, MaintenanceHandle, Transactable, TransactionBuilder, TransactionType,
};
use surrealdb_kvs_any::Backends;
use surrealdb_kvs_tikv::TikvProvider;
use tokio_util::sync::CancellationToken;

async fn builder(config: ConfigMap) -> Box<dyn TransactionBuilder> {
	let endpoint = std::env::var("SURREAL_TIKV_TEST_ENDPOINT")
		.expect("SURREAL_TIKV_TEST_ENDPOINT must name a PD endpoint");
	assert_eq!(
		std::env::var("SURREAL_TIKV_TEST_ALLOW_DESTRUCTIVE").as_deref(),
		Ok("1"),
		"Refusing TiKV maintenance tests without SURREAL_TIKV_TEST_ALLOW_DESTRUCTIVE=1. \
		 Use only a dedicated disposable cluster; never production."
	);
	let mut backends = Backends::community();
	backends.register(TikvProvider);
	backends
		.new_transaction_builder(&format!("tikv:{endpoint}"), CancellationToken::new(), config)
		.await
		.unwrap()
}

async fn begin(builder: &dyn TransactionBuilder, ty: TransactionType) -> Box<dyn Transactable> {
	builder.new_transaction(ty).await.unwrap().0
}

#[tokio::test]
#[ignore = "needs a live TiKV cluster"]
async fn maintenance_runs_against_the_cluster() {
	let builder = builder(ConfigMap::empty()).await;
	let ext =
		builder.extension(TypeId::of::<MaintenanceHandle>()).expect("maintenance is published");
	let handle = ext.downcast::<MaintenanceHandle>().expect("a MaintenanceHandle");
	handle.0.collect_garbage(Duration::from_secs(600)).await.unwrap();
	handle.0.cleanup_locks(Duration::from_secs(600)).await.unwrap();
}

#[tokio::test]
#[ignore = "needs a live TiKV cluster"]
async fn in_flight_transactions_are_counted_until_they_finish() {
	let builder = builder(ConfigMap::empty()).await;
	let ext = builder.extension(TypeId::of::<MaintenanceHandle>()).unwrap();
	let handle = ext.downcast::<MaintenanceHandle>().ok().unwrap();
	let before = handle.0.in_flight_transactions().expect("TiKV tracks transactions");
	let txn = begin(builder.as_ref(), TransactionType::Read).await;
	assert_eq!(handle.0.in_flight_transactions(), Some(before + 1));
	txn.cancel().await.unwrap();
	assert_eq!(handle.0.in_flight_transactions(), Some(before));
}

#[tokio::test]
#[ignore = "needs a live TiKV cluster"]
async fn destroy_range_is_published_while_garbage_collection_runs() {
	let builder = builder(ConfigMap::empty()).await;
	assert!(builder.extension(TypeId::of::<DestroyRangeHandle>()).is_some());
}

#[tokio::test]
#[ignore = "needs a live TiKV cluster"]
async fn destroy_range_is_withheld_when_garbage_collection_is_off() {
	let config = ConfigMap::empty().with_key_value("tikv_gc_enabled", "false");
	let builder = builder(config).await;
	assert!(builder.extension(TypeId::of::<DestroyRangeHandle>()).is_none());
}

#[tokio::test]
#[ignore = "needs a live TiKV cluster"]
async fn maintenance_is_a_no_op_when_garbage_collection_is_off() {
	let config = ConfigMap::empty().with_key_value("tikv_gc_enabled", "false");
	let builder = builder(config).await;
	let ext = builder.extension(TypeId::of::<MaintenanceHandle>()).unwrap();
	let handle = ext.downcast::<MaintenanceHandle>().ok().unwrap();
	handle.0.collect_garbage(Duration::from_secs(600)).await.unwrap();
	handle.0.cleanup_locks(Duration::from_secs(600)).await.unwrap();
}
