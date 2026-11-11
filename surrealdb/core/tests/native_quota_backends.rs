//! Focused backend certification using the existing contract assertions.
//! TiKV requires explicit destructive opt-in and accepts only loopback PD.
#![recursion_limit = "256"]
use std::future::Future;
use std::sync::Arc;
use surrealdb_catalog as catalog;
use surrealdb_core::dbs;
use uuid::Uuid;
mod kvs {
	pub use surrealdb_core::kvs::*;
	pub use surrealdb_datastore::{quota, testing};
}
use kvs::Datastore;
#[path = "../src/kvs/tests/quota_backend_contract.rs"]
mod quota_backend_contract;
#[cfg(feature = "kv-rocksdb")]
#[path = "../src/kvs/tests/quota_rocksdb_certification.rs"]
mod quota_rocksdb_certification;
trait CreateDs {
	async fn create_ds(&self, id: Uuid) -> (Arc<Datastore>, ());
}

#[cfg(feature = "kv-mem")]
mod mem {
	use super::*;
	async fn new_ds(id: Uuid) -> (Arc<Datastore>, ()) {
		let ds = Datastore::builder()
			.without_maintenance_tasks()
			.with_id(id)
			.build_with_path("memory")
			.await
			.unwrap();
		(ds, ())
	}
	quota_backend_contract::define_tests!(new_ds);
}
impl<F, Fut> CreateDs for F
where
	F: Fn(Uuid) -> Fut,
	Fut: Future<Output = (Arc<Datastore>, ())>,
{
	async fn create_ds(&self, id: Uuid) -> (Arc<Datastore>, ()) {
		self(id).await
	}
}
trait QuotaTransactionTestExt {
	async fn execute_with_transaction(
		&self,
		sql: &str,
		session: &dbs::Session,
		vars: Option<surrealdb_types::Variables>,
		transaction: std::sync::Arc<kvs::Transaction>,
	) -> Result<Vec<surrealdb_rpc::query::QueryResult>, surrealdb_types::Error>;
}
impl QuotaTransactionTestExt for kvs::Datastore {
	async fn execute_with_transaction(
		&self,
		sql: &str,
		session: &dbs::Session,
		vars: Option<surrealdb_types::Variables>,
		transaction: std::sync::Arc<kvs::Transaction>,
	) -> Result<Vec<surrealdb_rpc::query::QueryResult>, surrealdb_types::Error> {
		self.run(
			kvs::QueryRequest::new(sql, session).with_variables(vars).with_transaction(transaction),
		)
		.await
	}
}

#[cfg(feature = "kv-rocksdb")]
mod rocksdb {
	use super::*;
	#[tokio::test]
	async fn provider_opens_genuine_rocksdb() {
		let directory = temp_dir::TempDir::new().unwrap();
		let path = format!("rocksdb:{}", directory.path().display());
		let ds =
			Datastore::builder().without_maintenance_tasks().build_with_path(&path).await.unwrap();
		let tx = ds.transaction(surrealdb_kvs::TransactionType::Read).await.unwrap();
		assert_eq!(tx.kind(), "rocksdb");
		tx.cancel().await.unwrap();
		assert!(directory.path().join("CURRENT").is_file(), "native RocksDB manifest pointer");
		ds.shutdown().await.unwrap();
	}
	async fn new_ds(id: Uuid) -> (Arc<Datastore>, ()) {
		// Each UUID names a new directory under the disposable test root.
		let root = std::env::temp_dir().join("surrealdb-backend-certification");
		std::fs::create_dir_all(&root).unwrap();
		let path = format!("rocksdb:{}", root.join(id.to_string()).display());
		let ds = Datastore::builder()
			.without_maintenance_tasks()
			.with_id(id)
			.build_with_path(&path)
			.await
			.unwrap();
		(ds, ())
	}
	quota_backend_contract::define_tests!(new_ds);
}
#[cfg(feature = "kv-tikv")]
mod tikv {
	use super::*;
	use surrealdb_kvs::TransactionType;
	async fn new_ds(id: Uuid) -> (Arc<Datastore>, ()) {
		assert_eq!(
			std::env::var("SURREAL_TIKV_TEST_ALLOW_DESTRUCTIVE").as_deref(),
			Ok("1"),
			"TiKV backend contracts require a disposable cluster and explicit opt-in"
		);
		assert_eq!(
			std::env::var("SURREAL_TIKV_TEST_ENDPOINT").as_deref(),
			Ok("127.0.0.1:2379"),
			"this focused runner accepts only its disposable loopback PD"
		);
		let mut backends = surrealdb_kvs_any::Backends::community();
		backends.register(surrealdb_kvs_tikv::TikvProvider);
		let builder = backends
			.new_transaction_builder(
				"tikv:127.0.0.1:2379",
				tokio_util::sync::CancellationToken::new(),
				surrealdb_cnf::ConfigMap::empty(),
			)
			.await
			.unwrap();
		let tx = builder.new_transaction(TransactionType::Write).await.unwrap().0;
		tx.delr((vec![0u8]..vec![0xffu8]).into()).await.unwrap();
		tx.commit().await.unwrap();
		drop(tx);
		builder.shutdown().await.unwrap();
		let ds = Datastore::builder()
			.without_maintenance_tasks()
			.with_id(id)
			.build_with_path("tikv:127.0.0.1:2379")
			.await
			.unwrap();
		(ds, ())
	}
	quota_backend_contract::define_tests!(new_ds);
}
