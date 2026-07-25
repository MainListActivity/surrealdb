use std::sync::Arc;

use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::dbs::Session;
use crate::kvs::{Datastore, LockType, TransactionType};

async fn setup() -> Datastore {
	let ds = Datastore::new("memory").await.unwrap();
	ds.execute("DEFINE NAMESPACE tenant", &Session::owner(), None).await.unwrap();
	ds.execute("DEFINE DATABASE app", &Session::owner().with_ns("tenant"), None).await.unwrap();
	ds
}

async fn statement_error(ds: &Datastore, sql: &str, session: &Session) -> String {
	match ds.execute(sql, session, None).await {
		Ok(mut responses) => responses.remove(responses.len() - 1).result.unwrap_err().to_string(),
		Err(error) => error.to_string(),
	}
}

async fn table_bucket_count(ds: &Datastore, generation: u64, rule: &str) -> u64 {
	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let db = tx.get_db_by_name("tenant", "app", None).await.unwrap().unwrap();
	let count = tx
		.quota_usage(db.namespace_id, db.database_id)
		.table_bucket_count(generation, rule)
		.await
		.unwrap();
	tx.cancel().await.unwrap();
	count
}

#[tokio::test]
async fn table_exact_and_regex_rules_consume_every_matching_bucket() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE exact_user FOR TABLE MATCH EXACT ent_user LIMIT 1 \
		 RULE ent_tables FOR TABLE MATCH REGEX /^ent_/ LIMIT 2",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();

	ds.execute("DEFINE TABLE ent_user", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	ds.execute("DEFINE TABLE ent_order", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	ds.execute("DEFINE TABLE internal", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();

	let error = statement_error(&ds, "DEFINE TABLE ent_third", &database_owner).await;
	assert!(
		error.to_ascii_lowercase().contains("quota") && error.contains("ent_tables"),
		"{error}"
	);
	assert_eq!(table_bucket_count(&ds, 1, "exact_user").await, 1);
	assert_eq!(table_bucket_count(&ds, 1, "ent_tables").await, 2);
}

async fn field_count(ds: &Datastore, table: &str) -> u64 {
	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let db = tx.get_db_by_name("tenant", "app", None).await.unwrap().unwrap();
	let count =
		tx.quota_usage(db.namespace_id, db.database_id).field_count(&table.into()).await.unwrap();
	tx.cancel().await.unwrap();
	count
}

async fn record_count(ds: &Datastore, table: &str) -> u64 {
	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let db = tx.get_db_by_name("tenant", "app", None).await.unwrap().unwrap();
	let count =
		tx.quota_usage(db.namespace_id, db.database_id).record_count(&table.into()).await.unwrap();
	tx.cancel().await.unwrap();
	count
}

async fn seed_record_count(ds: &Datastore, table: &str, count: u64) {
	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let db = tx.get_db_by_name("tenant", "app", None).await.unwrap().unwrap();
	tx.quota_usage(db.namespace_id, db.database_id)
		.increment_record_count(&table.into(), count)
		.await
		.unwrap();
	tx.commit().await.unwrap();
}

#[tokio::test]
async fn field_exact_overrides_regex_and_regex_uses_smallest_finite_limit() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute("DEFINE TABLE ent_user; DEFINE TABLE ent_order", &database_owner, None)
		.await
		.unwrap();
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE ent_fields FOR FIELD MATCH REGEX /^ent_/ LIMIT 2 \
		 RULE order_fields FOR FIELD MATCH REGEX /order$/ LIMIT 1 \
		 RULE exact_user_fields FOR FIELD MATCH EXACT ent_user LIMIT UNLIMITED",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();

	ds.execute(
		"DEFINE FIELD name ON ent_user TYPE string; \
		 DEFINE FIELD profile.name ON ent_user TYPE string",
		&database_owner,
		None,
	)
	.await
	.unwrap()
	.into_iter()
	.for_each(|response| {
		response.result.unwrap();
	});
	ds.execute("DEFINE FIELD first ON ent_order TYPE string", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();

	let error =
		statement_error(&ds, "DEFINE FIELD second ON ent_order TYPE string", &database_owner).await;
	assert!(error.contains("order_fields"), "{error}");
	assert_eq!(field_count(&ds, "ent_user").await, 2);
	assert_eq!(field_count(&ds, "ent_order").await, 1);
}

#[tokio::test]
async fn assigning_policy_initializes_table_buckets_from_existing_catalog() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute("DEFINE TABLE ent_user; DEFINE TABLE ent_order", &database_owner, None)
		.await
		.unwrap();

	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE ent_tables FOR TABLE MATCH REGEX /^ent_/ LIMIT 2",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();

	assert_eq!(table_bucket_count(&ds, 1, "ent_tables").await, 2);
	let error = statement_error(&ds, "DEFINE TABLE ent_third", &database_owner).await;
	assert!(error.contains("ent_tables"), "{error}");
}

#[tokio::test]
async fn field_usage_is_continuously_counted_before_policy_assignment() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute(
		"DEFINE TABLE ent_user; DEFINE FIELD existing ON ent_user TYPE string",
		&database_owner,
		None,
	)
	.await
	.unwrap()
	.into_iter()
	.for_each(|response| {
		response.result.unwrap();
	});
	assert_eq!(field_count(&ds, "ent_user").await, 1);

	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE user_fields FOR FIELD MATCH EXACT ent_user LIMIT 1",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	let error =
		statement_error(&ds, "DEFINE FIELD extra ON ent_user TYPE string", &database_owner).await;
	assert!(error.contains("user_fields"), "{error}");
	assert_eq!(field_count(&ds, "ent_user").await, 1);
}

#[tokio::test]
async fn table_replacement_uses_final_net_delta_and_resets_per_table_usage() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE user_table FOR TABLE MATCH EXACT ent_user LIMIT 1 \
		 RULE user_fields FOR FIELD MATCH EXACT ent_user LIMIT 1",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	ds.execute(
		"DEFINE TABLE ent_user; DEFINE FIELD old ON ent_user TYPE string",
		&database_owner,
		None,
	)
	.await
	.unwrap()
	.into_iter()
	.for_each(|response| {
		response.result.unwrap();
	});
	seed_record_count(&ds, "ent_user", 5).await;

	let responses = ds
		.execute(
			"BEGIN; \
			 REMOVE FIELD old ON ent_user; \
			 REMOVE TABLE ent_user; \
			 DEFINE TABLE ent_user; \
			 DEFINE FIELD replacement ON ent_user TYPE string; \
			 COMMIT",
			&database_owner,
			None,
		)
		.await
		.unwrap();
	for response in responses {
		response.result.unwrap();
	}

	assert_eq!(table_bucket_count(&ds, 1, "user_table").await, 1);
	assert_eq!(field_count(&ds, "ent_user").await, 1);
	assert_eq!(record_count(&ds, "ent_user").await, 0);
}

#[tokio::test]
async fn quota_intents_follow_transaction_savepoint_rollback() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE tables FOR TABLE MATCH REGEX /.*/ LIMIT 1",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();

	let tx = Arc::new(ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap());
	tx.new_save_point().await.unwrap();
	ds.execute_with_transaction("DEFINE TABLE rolled_back", &database_owner, None, Arc::clone(&tx))
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	tx.rollback_to_save_point().await.unwrap();
	tx.commit().await.unwrap();

	assert_eq!(table_bucket_count(&ds, 1, "tables").await, 0);
	ds.execute("DEFINE TABLE committed", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	assert_eq!(table_bucket_count(&ds, 1, "tables").await, 1);
}

#[tokio::test]
async fn implicit_relation_and_schemaless_fields_are_not_billable() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute("DEFINE TABLE node", &database_owner, None).await.unwrap().remove(0).result.unwrap();
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE relation_fields FOR FIELD MATCH EXACT edge LIMIT 0 \
		 RULE free_fields FOR FIELD MATCH EXACT free LIMIT 0",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();

	ds.execute(
		"DEFINE TABLE edge TYPE RELATION FROM node TO node; \
		 DEFINE TABLE free SCHEMALESS; \
		 CREATE free SET dynamic = 'not a schema field'",
		&database_owner,
		None,
	)
	.await
	.unwrap()
	.into_iter()
	.for_each(|response| {
		response.result.unwrap();
	});
	assert_eq!(field_count(&ds, "edge").await, 0);
	assert_eq!(field_count(&ds, "free").await, 0);

	let error =
		statement_error(&ds, "DEFINE FIELD explicit ON free TYPE string", &database_owner).await;
	assert!(error.contains("free_fields"), "{error}");
}

#[tokio::test]
async fn policy_generation_switch_seeds_current_usage_and_allows_only_non_worsening_changes() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute("DEFINE TABLE ent_user; DEFINE TABLE ent_order", &database_owner, None)
		.await
		.unwrap();
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE ent_tables FOR TABLE MATCH REGEX /^ent_/ LIMIT 3",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	ds.execute(
		"ALTER QUOTA ON DATABASE app EXPECT GENERATION 1 \
		 SET RULE ent_tables FOR TABLE MATCH REGEX /^ent_/ LIMIT 1",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();

	assert_eq!(table_bucket_count(&ds, 2, "ent_tables").await, 2);
	let error = statement_error(&ds, "DEFINE TABLE ent_third", &database_owner).await;
	assert!(error.contains("ent_tables"), "{error}");
	ds.execute("REMOVE TABLE ent_order", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	assert_eq!(table_bucket_count(&ds, 2, "ent_tables").await, 1);
}

#[tokio::test]
async fn overwrite_noop_relation_view_remove_and_expunge_follow_catalog_existence() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE bill_tables FOR TABLE MATCH REGEX /^bill_/ LIMIT 3 \
		 RULE base_fields FOR FIELD MATCH EXACT bill_base LIMIT 1",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	let responses = ds
		.execute(
			"DEFINE TABLE bill_base; \
			 DEFINE FIELD name ON bill_base TYPE string; \
			 DEFINE TABLE OVERWRITE bill_base; \
			 DEFINE FIELD OVERWRITE name ON bill_base TYPE string; \
			 REMOVE TABLE IF EXISTS absent; \
			 REMOVE FIELD IF EXISTS absent ON bill_base; \
			 DEFINE TABLE bill_edge TYPE RELATION FROM bill_base TO bill_base; \
			 DEFINE TABLE bill_view AS SELECT * FROM bill_base",
			&database_owner,
			None,
		)
		.await
		.unwrap();
	for response in responses {
		response.result.unwrap();
	}
	assert_eq!(table_bucket_count(&ds, 1, "bill_tables").await, 3);
	assert_eq!(field_count(&ds, "bill_base").await, 1);
	assert_eq!(field_count(&ds, "bill_edge").await, 0);

	ds.execute("REMOVE TABLE AND EXPUNGE bill_view", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	assert_eq!(table_bucket_count(&ds, 1, "bill_tables").await, 2);
}

#[tokio::test]
async fn in_flight_catalog_write_conflicts_with_policy_generation_switch() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE ent_tables FOR TABLE MATCH REGEX /^ent_/ LIMIT 10",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();

	let writer =
		Arc::new(ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap());
	ds.execute_with_transaction(
		"DEFINE TABLE ent_late",
		&database_owner,
		None,
		Arc::clone(&writer),
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();

	ds.execute(
		"ALTER QUOTA ON DATABASE app EXPECT GENERATION 1 \
		 SET RULE ent_tables FOR TABLE MATCH REGEX /^ent_/ LIMIT 0",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	assert!(writer.commit().await.is_err());

	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	assert!(tx.get_tb_by_name("tenant", "app", &"ent_late".into(), None).await.unwrap().is_none());
	tx.cancel().await.unwrap();
	assert_eq!(table_bucket_count(&ds, 2, "ent_tables").await, 0);
}
