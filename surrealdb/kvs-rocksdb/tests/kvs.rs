//! Shared KVS contracts against a newly allocated, disposable RocksDB per test.
use std::process::ExitCode;

use surrealdb_kvs_rocksdb::{Datastore, RocksDbConfig};
use surrealdb_kvs_test::{TestBackend, TestDs};
use temp_dir::TempDir;

async fn rocksdb_ds() -> TestDs {
	let directory = TempDir::new().expect("allocate disposable RocksDB directory");
	let path = directory.path().to_str().expect("temporary path is UTF-8");
	let datastore = Datastore::new(path, RocksDbConfig::default()).await.expect("open RocksDB");
	TestDs::from_builder_with_guard(Box::new(datastore), directory)
}

fn main() -> ExitCode {
	surrealdb_kvs_test::run(vec![TestBackend::new("rocksdb", rocksdb_ds).serial()])
}
