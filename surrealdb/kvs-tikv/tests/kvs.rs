//! Runs the shared key-value store behaviour suite against a live TiKV cluster.
//!
//! The cluster is reached through the PD endpoint in `SURREAL_TIKV_TEST_ENDPOINT`, for example
//! `127.0.0.1:2379`. Without it there is no cluster to test, so the run reports that and passes.
//! The backend is constructed through the same `tikv:` provider the server uses, so the
//! connection-string parsing is covered on every run. All tests share one cluster, so they run
//! serially and each begins by wiping the ENTIRE keyspace. Use ONLY a dedicated
//! disposable cluster. SURREAL_TIKV_TEST_ALLOW_DESTRUCTIVE=1 is also required.
#![expect(clippy::unwrap_used, reason = "a failed setup step should fail the test run")]

use std::process::ExitCode;

use surrealdb_cnf::ConfigMap;
use surrealdb_kvs::TransactionType;
use surrealdb_kvs_any::Backends;
use surrealdb_kvs_test::{TestBackend, TestDs};
use surrealdb_kvs_tikv::TikvProvider;
use tokio_util::sync::CancellationToken;

async fn tikv_ds(endpoint: String) -> TestDs {
	let mut backends = Backends::community();
	backends.register(TikvProvider);
	let builder = backends
		.new_transaction_builder(
			&format!("tikv:{endpoint}"),
			CancellationToken::new(),
			ConfigMap::empty(),
		)
		.await
		.unwrap();
	let ds = TestDs::from_builder(builder);
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	tx.delr((vec![0u8]..vec![0xffu8]).into()).await.unwrap();
	tx.commit().await.unwrap();
	ds
}

fn main() -> ExitCode {
	let Ok(endpoint) = std::env::var("SURREAL_TIKV_TEST_ENDPOINT") else {
		eprintln!("SURREAL_TIKV_TEST_ENDPOINT is not set; skipping the TiKV suite");
		return ExitCode::SUCCESS;
	};
	if std::env::var("SURREAL_TIKV_TEST_ALLOW_DESTRUCTIVE").as_deref() != Ok("1") {
		eprintln!(
			"Refusing destructive TiKV tests: the suite wipes the ENTIRE keyspace. \
			 Use only a dedicated disposable cluster and set \
			 SURREAL_TIKV_TEST_ALLOW_DESTRUCTIVE=1 explicitly. Never use production."
		);
		return ExitCode::FAILURE;
	}
	surrealdb_kvs_test::run(vec![
		TestBackend::new("tikv", move || tikv_ds(endpoint.clone())).serial(),
	])
}
