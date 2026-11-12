//! Tests for core behaviour layered on top of the KV backends: the
//! transaction metrics wrappers, the transaction cache, and the deferred
//! data reclaim task. The raw KV-store behaviour tests live in the
//! `surrealdb-kvs-any` crate.
//!
//! A module gated on `kv-mem` asserts behaviour every backend shares, so the
//! in-memory store is enough to cover it. An ungated module carries per-backend
//! tests, each gated on its own `kv-*` feature.
#![cfg(any(
	feature = "kv-mem",
	feature = "kv-surrealkv",
	feature = "kv-rocksdb",
	feature = "kv-tikv"
))]

#[cfg(feature = "kv-surrealkv")]
mod blocking_rebuild_index_test;
#[cfg(feature = "kv-mem")]
mod caller_owned_transaction_test;
#[cfg(feature = "kv-mem")]
mod changefeed_gc_test;
#[cfg(feature = "kv-mem")]
mod compatibility_test;
#[cfg(any(feature = "kv-mem", feature = "kv-surrealkv"))]
mod doc_id_claim_test;
#[cfg(feature = "kv-mem")]
mod export_id_components_test;
#[cfg(feature = "kv-mem")]
mod export_implied_element_fields_test;
#[cfg(feature = "kv-mem")]
mod export_legacy_extension_test;
#[cfg(all(feature = "kv-mem", feature = "postgres"))]
mod export_user_types_test;
#[cfg(feature = "kv-mem")]
mod export_views_test;
#[cfg(all(feature = "kv-mem", feature = "surrealism"))]
mod extension_bindings_test;
#[cfg(all(feature = "search", feature = "kv-mem"))]
mod fulltext_nodes_test;
#[cfg(feature = "kv-mem")]
mod graph_degree_test;
#[cfg(feature = "kv-mem")]
mod graph_fold_test;
#[cfg(feature = "kv-mem")]
mod graph_migration_test;
#[cfg(feature = "kv-mem")]
mod graph_ordered_limit_test;
#[cfg(feature = "kv-mem")]
mod graph_semijoin_test;
#[cfg(any(feature = "kv-mem", feature = "kv-surrealkv"))]
mod held_creates_bench;
#[cfg(feature = "kv-mem")]
mod index_build_sync_test;
#[cfg(feature = "kv-mem")]
mod index_format_level_test;
#[cfg(feature = "kv-mem")]
mod index_key_kernel_test;
#[cfg(feature = "kv-mem")]
mod inline_cache_test;
#[cfg(feature = "kv-mem")]
mod inline_props_test;
#[cfg(feature = "kv-mem")]
mod interrupted_write_test;
#[cfg(feature = "kv-mem")]
mod isolation_level_test;
#[cfg(feature = "kv-mem")]
mod keyspace_test;
#[cfg(any(feature = "kv-mem", feature = "kv-surrealkv"))]
mod latency;
#[cfg(feature = "kv-mem")]
mod layout_oracle_test;
#[cfg(feature = "kv-mem")]
mod lightweight_test;
#[cfg(feature = "kv-mem")]
mod own_write_catalog_test;
#[cfg(feature = "kv-mem")]
mod read_only_transaction_test;
mod reclaim_backends_test;
#[cfg(feature = "kv-mem")]
mod reclaim_test;
#[cfg(any(feature = "kv-mem", feature = "kv-surrealkv"))]
mod roundtrips_test;
#[cfg(feature = "kv-mem")]
mod rpc_session_test;
#[cfg(feature = "kv-surrealkv")]
mod select_for_update_test;
#[cfg(all(feature = "search", feature = "kv-mem"))]
mod shared_store_materialise_test;
#[cfg(feature = "kv-mem")]
mod stale_read_test;
#[cfg(feature = "kv-mem")]
mod table_migration_resilience_test;
#[cfg(feature = "kv-mem")]
mod table_migration_test;
#[cfg(feature = "kv-mem")]
mod tx_cache_test;
mod tx_cache_versioned_test;
#[cfg(feature = "kv-mem")]
mod tx_engine_test;
mod tx_metrics;
#[cfg(feature = "kv-mem")]
mod version_test;
#[cfg(feature = "kv-mem")]
mod warm_write_reads_test;
#[cfg(feature = "kv-mem")]
mod write_guard_test;

#[cfg(any(feature = "kv-mem", feature = "kv-rocksdb", feature = "kv-tikv"))]
use std::future::Future;

#[cfg(any(feature = "kv-mem", feature = "kv-rocksdb", feature = "kv-tikv"))]
use uuid::Uuid;

#[cfg(any(feature = "kv-mem", feature = "kv-rocksdb", feature = "kv-tikv"))]
use super::Datastore;
#[cfg(any(feature = "kv-mem", feature = "kv-rocksdb", feature = "kv-tikv"))]
macro_rules! include_tests { ($new_ds:ident => $($name:ident),* $(,)?) => { $(super::$name::define_tests!($new_ds);)* }; }
#[cfg(any(feature = "kv-mem", feature = "kv-rocksdb", feature = "kv-tikv"))]
mod quota_backend_contract;
#[cfg(feature = "kv-mem")]
mod quota_info_test;
#[cfg(feature = "kv-mem")]
mod quota_rebuild_test;
#[cfg(feature = "kv-mem")]
mod quota_record_test;
#[cfg(feature = "kv-rocksdb")]
mod quota_rocksdb_certification;
#[cfg(feature = "kv-mem")]
mod quota_table_field_test;
#[cfg(feature = "kv-mem")]
mod quota_test;
#[cfg(feature = "kv-mem")]
mod quota_usage_test;
#[cfg(feature = "kv-mem")]
mod storage_format_test;
#[cfg(feature = "kv-mem")]
mod storage_migration_test;
#[derive(Clone, Debug)]
#[cfg(any(feature = "kv-mem", feature = "kv-rocksdb", feature = "kv-tikv"))]
pub(crate) enum Kvs {
	#[cfg(feature = "kv-mem")]
	Mem,
	#[cfg(feature = "kv-rocksdb")]
	Rocksdb,
	#[cfg(feature = "kv-tikv")]
	Tikv,
}

#[cfg(any(feature = "kv-mem", feature = "kv-rocksdb", feature = "kv-tikv"))]
trait CreateDs {
	async fn create_ds(&self, id: Uuid) -> (std::sync::Arc<Datastore>, Kvs);
}

#[cfg(any(feature = "kv-mem", feature = "kv-rocksdb", feature = "kv-tikv"))]
impl<F, Fut> CreateDs for F
where
	F: Fn(Uuid) -> Fut,
	Fut: Future<Output = (std::sync::Arc<Datastore>, Kvs)>,
{
	async fn create_ds(&self, id: Uuid) -> (std::sync::Arc<Datastore>, Kvs) {
		(self)(id).await
	}
}

#[cfg(feature = "kv-mem")]
mod mem {
	use uuid::Uuid;

	use super::Kvs;
	use crate::kvs::Datastore;

	async fn new_ds(id: Uuid) -> (std::sync::Arc<Datastore>, Kvs) {
		// Use a memory datastore instance
		let path = "memory";
		// Setup the in-memory datastore
		let ds = Datastore::builder()
			.without_maintenance_tasks()
			.with_id(id)
			.build_with_path(path)
			.await
			.unwrap();
		// Return the datastore
		(ds, Kvs::Mem)
	}

	include_tests!(new_ds =>
		quota_backend_contract,
	);
}

#[cfg(feature = "kv-rocksdb")]
mod rocksdb {
	use temp_dir::TempDir;
	use uuid::Uuid;

	use super::Kvs;
	use crate::kvs::Datastore;

	async fn new_ds(id: Uuid) -> (std::sync::Arc<Datastore>, Kvs) {
		// Setup the temporary data storage path
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		let path = format!("rocksdb:{path}");
		// Setup the RocksDB datastore
		let ds = Datastore::builder()
			.without_maintenance_tasks()
			.with_id(id)
			.build_with_path(&path)
			.await
			.unwrap();
		// Return the datastore
		(ds, Kvs::Rocksdb)
	}

	include_tests!(new_ds =>
		quota_backend_contract,
	);
}

#[cfg(feature = "kv-tikv")]
mod tikv {
	use uuid::Uuid;

	use super::Kvs;
	use crate::kvs::{Datastore, TransactionType};

	async fn new_ds(id: Uuid) -> (std::sync::Arc<Datastore>, Kvs) {
		// Setup the cluster connection string
		let path = "tikv:127.0.0.1:2379";
		// Setup the TiKV datastore
		let ds = Datastore::builder()
			.without_maintenance_tasks()
			.with_id(id)
			.build_with_path(path)
			.await
			.unwrap();
		// Clear any previous test entries
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		tx.delr(crate::key::RawRange::of_bytes(&[0u8], &[0xffu8])).await.unwrap();
		tx.commit().await.unwrap();
		// Return the datastore
		(ds, Kvs::Tikv)
	}

	include_tests!(new_ds =>
		quota_backend_contract,
	);
}

/// Preserve caller-owned transaction coverage through the 4.0 request API.
trait QuotaTransactionTestExt {
	async fn execute_with_transaction(
		&self,
		sql: &str,
		session: &crate::dbs::Session,
		vars: Option<crate::types::PublicVariables>,
		transaction: std::sync::Arc<crate::kvs::Transaction>,
	) -> Result<Vec<crate::dbs::QueryResult>, surrealdb_types::Error>;
}
impl QuotaTransactionTestExt for Datastore {
	async fn execute_with_transaction(
		&self,
		sql: &str,
		session: &crate::dbs::Session,
		vars: Option<crate::types::PublicVariables>,
		transaction: std::sync::Arc<crate::kvs::Transaction>,
	) -> Result<Vec<crate::dbs::QueryResult>, surrealdb_types::Error> {
		self.run(
			crate::kvs::QueryRequest::new(sql, session)
				.with_variables(vars)
				.with_transaction(transaction),
		)
		.await
	}
}

#[cfg(feature = "kv-mem")]
mod quota_write_paths_test;
