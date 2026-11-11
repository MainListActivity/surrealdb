//! One-link runner for the focused native-quota contracts. The individual
//! targets remain available for compile diagnostics; this runner avoids
//! linking the large engine separately for each contract on small builders.
#![cfg(feature = "kv-mem")]

use surrealdb_catalog as catalog;
use surrealdb_core::{dbs, kvs};
use surrealdb_datastore::key;
use surrealdb_expr::val;

#[path = "native_quota_frozen_migration.rs"]
mod frozen_migration;
#[path = "../src/kvs/tests/storage_migration_test.rs"]
mod migration;
#[path = "../src/kvs/tests/quota_write_paths_test.rs"]
mod write_paths;

#[path = "../src/kvs/tests/quota_table_field_test.rs"]
mod table_field;

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
