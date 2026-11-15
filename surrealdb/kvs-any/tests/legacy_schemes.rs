//! The `indxdb` scheme of an earlier storage engine opens the same path with
//! SurrealKV. The `rocksdb` scheme is not claimed in this fork's community
//! backends: RocksDB lives in the separately built `surrealdb-kvs-rocksdb`
//! crate, so `rocksdb:` paths do not resolve to SurrealKV here.
#![cfg(feature = "kv-surrealkv")]
#![allow(clippy::unwrap_used)]

use surrealdb_cnf::ConfigMap;
use surrealdb_kvs::TransactionType::*;
use temp_dir::TempDir;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn legacy_schemes_open_the_path_with_surrealkv() {
	for scheme in ["indxdb"] {
		let backends = surrealdb_kvs_any::Backends::community();
		let dir = TempDir::new().unwrap();
		let path = format!("{scheme}:{}", dir.path().to_string_lossy());
		assert!(backends.path_valid(&path).is_ok(), "{scheme} is not a valid path");
		let builder = backends
			.new_transaction_builder(&path, CancellationToken::new(), ConfigMap::empty())
			.await
			.unwrap();
		assert_eq!(builder.name(), "surrealkv", "{scheme} opened another engine");

		let (tx, _) = builder.new_transaction(Write).await.unwrap();
		tx.set(b"key".to_vec().into(), b"value".to_vec()).await.unwrap();
		tx.commit().await.unwrap();
		let (tx, _) = builder.new_transaction(Read).await.unwrap();
		assert_eq!(tx.get(b"key".to_vec().into(), None).await.unwrap(), Some(b"value".to_vec()));
		tx.cancel().await.unwrap();
		builder.shutdown().await.unwrap();
	}
}
