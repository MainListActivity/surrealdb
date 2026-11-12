//! Two-phase recovery probe for the dedicated loopback test cluster.
//! Run `seed NONCE`, restart/kill disposable TiKV nodes, then `verify NONCE`.
use surrealdb_cnf::ConfigMap;
use surrealdb_kvs::TransactionType;
use surrealdb_kvs_any::{BackendProvider, ConnectContext};
use surrealdb_kvs_tikv::TikvProvider;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
	assert_eq!(std::env::var("SURREAL_TIKV_TEST_ALLOW_DESTRUCTIVE").as_deref(), Ok("1"));
	assert_eq!(std::env::var("SURREAL_TIKV_TEST_ENDPOINT").as_deref(), Ok("127.0.0.1:2379"));
	let mut args = std::env::args().skip(1);
	let mode = args.next().expect("seed|verify");
	let nonce = args.next().expect("unique test nonce").into_bytes();
	assert!(args.next().is_none());
	let builder = TikvProvider
		.connect(ConnectContext {
			scheme: "tikv",
			path: "127.0.0.1:2379",
			canceller: CancellationToken::new(),
			config: ConfigMap::empty(),
		})
		.await
		.expect("connect disposable PD");
	let key = b"/surrealdb-upgrade-disposable-recovery".to_vec();
	let tx = builder.new_transaction(TransactionType::Write).await.unwrap().0;
	match mode.as_str() {
		"seed" => tx.set(key.into(), nonce).await.unwrap(),
		"verify" => {
			assert_eq!(tx.get(key.clone().into(), None).await.unwrap(), Some(nonce));
			tx.del(key.into()).await.unwrap();
		}
		_ => panic!("expected seed|verify"),
	}
	tx.commit().await.unwrap();
	drop(tx);
	builder.shutdown().await.unwrap();
	println!("disposable recovery {mode}: PASS");
}
