//! Extended native-quota contracts shared verbatim with the unit suite.
//! Uses public dependency APIs without widening core's internal visibility.
#![cfg(feature = "kv-mem")]

use surrealdb_catalog as catalog;
use surrealdb_core::{dbs, kvs};
use surrealdb_datastore::key;
use surrealdb_iam as iam;
use surrealdb_observe as observe;

#[path = "../src/kvs/tests/quota_info_test.rs"]
mod info;
#[path = "../src/kvs/tests/quota_test.rs"]
mod policy;
#[path = "../src/kvs/tests/quota_rebuild_test.rs"]
mod rebuild;
#[path = "../src/kvs/tests/quota_record_test.rs"]
mod record;
#[path = "../src/kvs/tests/quota_usage_test.rs"]
mod usage;

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
