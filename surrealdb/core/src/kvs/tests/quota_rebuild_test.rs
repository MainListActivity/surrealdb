use surrealdb_types::ToSql;

use crate::catalog::providers::DatabaseProvider;
use crate::dbs::Session;
use crate::iam::{Level, Role};
use crate::kvs::{Datastore, LockType, TransactionType};
use crate::val::Value;

async fn query_value(ds: &Datastore, sql: &str, session: &Session) -> Value {
	ds.execute(sql, session, None).await.unwrap().remove(0).result.unwrap().into()
}

async fn statement_error(ds: &Datastore, sql: &str, session: &Session) -> String {
	match ds.execute(sql, session, None).await {
		Ok(mut responses) => responses.remove(0).result.unwrap_err().to_string(),
		Err(error) => error.to_string(),
	}
}

#[tokio::test]
async fn rebuild_quota_backfills_legacy_usage_and_reopens_writes() {
	let ds = Datastore::new("memory").await.unwrap();
	let root = Session::owner();
	let namespace_owner = Session::for_level(Level::Namespace("tenant".into()), Role::Owner);
	let database_owner =
		Session::for_level(Level::Database("tenant".into(), "app".into()), Role::Owner);
	ds.execute("DEFINE NAMESPACE tenant", &root, None).await.unwrap();
	ds.execute("DEFINE DATABASE app", &namespace_owner, None).await.unwrap();
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE ent_fields FOR FIELD MATCH REGEX /^ent_/ LIMIT 5 \
		 RULE ent_records FOR RECORD MATCH REGEX /^ent_/ LIMIT 5 \
		 RULE ent_tables FOR TABLE MATCH REGEX /^ent_/ LIMIT 5",
		&namespace_owner,
		None,
	)
	.await
	.unwrap();
	ds.execute(
		"DEFINE TABLE ent_user SCHEMAFULL; \
		 DEFINE FIELD name ON ent_user TYPE string; \
		 CREATE ent_user:one SET name = 'A'; \
		 CREATE ent_user:two SET name = 'B'",
		&database_owner,
		None,
	)
	.await
	.unwrap();

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let db = tx.get_db_by_name("tenant", "app", None).await.unwrap().unwrap();
	tx.quota_usage(db.namespace_id, db.database_id)
		.begin_external_write_maintenance()
		.await
		.unwrap();
	tx.commit().await.unwrap();

	let result = query_value(&ds, "REBUILD QUOTA ON DATABASE app", &namespace_owner).await;
	let Value::Object(result) = result else {
		panic!("expected rebuild result object");
	};
	assert_eq!(result.get("format_version").unwrap().to_sql(), "1");
	assert_eq!(result.get("operation").unwrap().to_sql(), "'rebuild_quota'");
	assert_eq!(result.get("changed").unwrap().to_sql(), "true");
	assert_eq!(result.get("database").unwrap().to_sql(), "'app'");
	assert!(result.get("operation_id").is_some());
	let Value::Object(scanned) = result.get("scanned").unwrap() else {
		panic!("expected scan counts");
	};
	assert_eq!(scanned.get("table").unwrap().to_sql(), "1");
	assert_eq!(scanned.get("field").unwrap().to_sql(), "1");
	assert_eq!(scanned.get("record").unwrap().to_sql(), "2");

	let info = query_value(&ds, "INFO FOR QUOTA ON DATABASE app STRUCTURE", &namespace_owner).await;
	let info = info.to_sql();
	assert!(info.contains("state: 'ready'"), "{info}");
	assert!(info.contains("used: 2"), "{info}");

	query_value(&ds, "CREATE ent_user:three SET name = 'C'", &database_owner).await;
}

#[tokio::test]
async fn rebuild_if_needed_is_a_ready_noop_and_normal_rebuild_recovers_a_crash() {
	let ds = Datastore::new("memory").await.unwrap();
	let root = Session::owner();
	let namespace_owner = Session::for_level(Level::Namespace("tenant".into()), Role::Owner);
	let database_owner =
		Session::for_level(Level::Database("tenant".into(), "app".into()), Role::Owner);
	ds.execute("DEFINE NAMESPACE tenant", &root, None).await.unwrap();
	ds.execute("DEFINE DATABASE app", &namespace_owner, None).await.unwrap();

	let denied = statement_error(&ds, "REBUILD QUOTA ON DATABASE app", &database_owner).await;
	assert!(denied.contains("permissions"), "{denied}");

	let noop = query_value(&ds, "REBUILD QUOTA IF NEEDED ON DATABASE app", &namespace_owner).await;
	let Value::Object(noop) = noop else {
		panic!("expected rebuild result object");
	};
	assert_eq!(noop.get("changed").unwrap().to_sql(), "false");
	assert!(noop.get("before").unwrap().to_sql().contains("active_epoch: 1"));
	assert!(noop.get("after").unwrap().to_sql().contains("active_epoch: 1"));

	let rebuilt = query_value(&ds, "REBUILD QUOTA ON DATABASE app", &namespace_owner).await;
	let rebuilt = rebuilt.to_sql();
	assert!(rebuilt.contains("active_epoch: 2"), "{rebuilt}");

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let db = tx.get_db_by_name("tenant", "app", None).await.unwrap().unwrap();
	assert_eq!(tx.quota_usage(db.namespace_id, db.database_id).begin_rebuild().await.unwrap(), 3);
	tx.commit().await.unwrap();

	let recovered = query_value(&ds, "REBUILD QUOTA ON DATABASE app", &namespace_owner).await;
	let recovered = recovered.to_sql();
	assert!(recovered.contains("ledger_state: 'rebuilding'"), "{recovered}");
	assert!(recovered.contains("active_epoch: 4"), "{recovered}");
	assert!(recovered.contains("ledger_state: 'ready'"), "{recovered}");
}
