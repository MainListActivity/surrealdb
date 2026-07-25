use std::sync::Arc;

use crate::catalog::providers::DatabaseProvider;
use crate::dbs::Session;
use crate::iam::{Auth, Level, Role};
use crate::key::database::qg::Qg;
use crate::key::database::qt::Qt;
use crate::kvs::{Datastore, LockType, TransactionType};

async fn setup() -> Datastore {
	let ds = Datastore::new("memory").await.unwrap();
	let root = Session::owner();
	let ns = Session::owner().with_ns("tenant");
	ds.execute("DEFINE NAMESPACE tenant", &root, None).await.unwrap();
	ds.execute("DEFINE DATABASE app", &ns, None).await.unwrap();
	ds
}

async fn generation(ds: &Datastore) -> Option<u64> {
	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let db = tx.get_db_by_name("tenant", "app", None).await.unwrap().unwrap();
	let generation =
		tx.get_db_quota(db.namespace_id, db.database_id, None).await.unwrap().map(|x| x.generation);
	tx.cancel().await.unwrap();
	generation
}

async fn statement_error(ds: &Datastore, sql: &str, session: &Session) -> String {
	match ds.execute(sql, session, None).await {
		Ok(mut responses) => responses.remove(responses.len() - 1).result.unwrap_err().to_string(),
		Err(error) => error.to_string(),
	}
}

async fn import_error(ds: &Datastore, sql: &str, session: &Session) -> String {
	let mut responses = ds.import(sql, session).await.unwrap();
	responses.remove(responses.len() - 1).result.unwrap_err().to_string()
}

#[tokio::test]
async fn quota_generation_and_noop_semantics() {
	let ds = setup().await;
	let owner = Session::owner().with_ns("tenant");

	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE records FOR RECORD MATCH REGEX /^ent/ LIMIT 100 \
		 RULE fields FOR FIELD MATCH EXACT user LIMIT 20",
		&owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	assert_eq!(generation(&ds).await, Some(1));

	ds.execute(
		"DEFINE QUOTA OVERWRITE ON DATABASE app EXPECT GENERATION 1 \
		 RULE fields FOR FIELD MATCH EXACT user LIMIT 20 \
		 RULE records FOR RECORD MATCH REGEX /^ent/ LIMIT 100",
		&owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	assert_eq!(generation(&ds).await, Some(1), "rule reordering must be a no-op");

	ds.execute(
		"ALTER QUOTA ON DATABASE app EXPECT GENERATION 1 \
		 SET RULE records FOR RECORD MATCH REGEX /^ent/ LIMIT 100 \
		 DROP RULE IF EXISTS missing",
		&owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	assert_eq!(generation(&ds).await, Some(1), "semantic no-op must retain generation");

	ds.execute(
		"ALTER QUOTA ON DATABASE app EXPECT GENERATION 1 \
		 SET RULE records FOR RECORD MATCH REGEX /^ent/ LIMIT 101",
		&owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	assert_eq!(generation(&ds).await, Some(2));

	let error = statement_error(
		&ds,
		"ALTER QUOTA ON DATABASE app EXPECT GENERATION 1 \
		 SET RULE records FOR RECORD MATCH REGEX /^ent/ LIMIT 102",
		&owner,
	)
	.await;
	assert!(error.contains("generation 2, expected 1"), "{error}");
}

#[tokio::test]
async fn quota_generation_survives_remove_and_recreate() {
	let ds = setup().await;
	let owner = Session::owner().with_ns("tenant");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE records FOR RECORD MATCH EXACT user LIMIT 10",
		&owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	ds.execute("REMOVE QUOTA ON DATABASE app EXPECT GENERATION 1", &owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	assert_eq!(generation(&ds).await, None);

	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE records FOR RECORD MATCH EXACT user LIMIT 10",
		&owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	assert_eq!(generation(&ds).await, Some(3));
	let error = statement_error(
		&ds,
		"ALTER QUOTA ON DATABASE app EXPECT GENERATION 1 \
		 SET RULE records FOR RECORD MATCH EXACT user LIMIT 11",
		&owner,
	)
	.await;
	assert!(error.contains("generation 3, expected 1"), "{error}");
}

#[tokio::test]
async fn semantic_noop_at_max_generation_does_not_overflow() {
	let ds = setup().await;
	let owner = Session::owner().with_ns("tenant");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE records FOR RECORD MATCH EXACT user LIMIT 10",
		&owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let db = tx.get_db_by_name("tenant", "app", None).await.unwrap().unwrap();
	let mut policy = tx
		.get_db_quota(db.namespace_id, db.database_id, None)
		.await
		.unwrap()
		.unwrap()
		.as_ref()
		.clone();
	policy.generation = u64::MAX;
	tx.set(&Qt::new(db.namespace_id, db.database_id), &policy).await.unwrap();
	tx.set(&Qg::new(db.namespace_id, db.database_id), &u64::MAX).await.unwrap();
	tx.commit().await.unwrap();

	ds.execute(
		"DEFINE QUOTA OVERWRITE ON DATABASE app EXPECT GENERATION 18446744073709551615 \
		 RULE records FOR RECORD MATCH EXACT user LIMIT 10",
		&owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	assert_eq!(generation(&ds).await, Some(u64::MAX));

	ds.execute(
		"ALTER QUOTA ON DATABASE app EXPECT GENERATION 18446744073709551615 \
		 SET RULE records FOR RECORD MATCH EXACT user LIMIT 10",
		&owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	assert_eq!(generation(&ds).await, Some(u64::MAX));
}

#[tokio::test]
async fn quota_mutation_requires_parent_owner() {
	let sql = "DEFINE QUOTA ON DATABASE app RULE records FOR RECORD MATCH EXACT user LIMIT 10";

	for denied in [
		Session::for_level(Level::Root, Role::Editor).with_ns("tenant"),
		Session::for_level(Level::Namespace("tenant".into()), Role::Editor),
		Session::for_level(Level::Database("tenant".into(), "app".into()), Role::Owner),
	] {
		let ds = setup().await;
		let error = statement_error(&ds, sql, &denied).await;
		assert!(!error.is_empty());
		assert_eq!(generation(&ds).await, None);
	}

	let ds = setup().await;
	let mut record = Session::default().with_ns("tenant").with_db("app");
	record.au = Arc::new(Auth::for_record("user:one".to_owned(), "tenant", "app", "participant"));
	let error = statement_error(&ds, sql, &record).await;
	assert!(!error.is_empty());

	let ds = setup().await;
	let cross_namespace =
		Session::for_level(Level::Namespace("other".into()), Role::Owner).with_ns("tenant");
	let error = statement_error(&ds, sql, &cross_namespace).await;
	assert!(!error.is_empty());

	for allowed in [
		Session::owner().with_ns("tenant"),
		Session::for_level(Level::Namespace("tenant".into()), Role::Owner),
	] {
		let ds = setup().await;
		ds.execute(sql, &allowed, None).await.unwrap().remove(0).result.unwrap();
		assert_eq!(generation(&ds).await, Some(1));
	}
}

#[tokio::test]
async fn database_owner_import_cannot_define_quota() {
	let ds = setup().await;
	let database_owner =
		Session::for_level(Level::Database("tenant".into(), "app".into()), Role::Owner);
	let error = import_error(
		&ds,
		"OPTION IMPORT; DEFINE QUOTA ON DATABASE app \
			 RULE records FOR RECORD MATCH EXACT user LIMIT 10",
		&database_owner,
	)
	.await;
	assert!(!error.is_empty());
	assert_eq!(generation(&ds).await, None);
}

#[tokio::test]
async fn parent_owner_import_cannot_mutate_quota() {
	let ds = setup().await;
	let owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE records FOR RECORD MATCH EXACT user LIMIT 10",
		&owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();

	for sql in [
		"OPTION IMPORT; DEFINE QUOTA OVERWRITE ON DATABASE app EXPECT GENERATION 1 \
		 RULE records FOR RECORD MATCH EXACT user LIMIT 11",
		"OPTION IMPORT; ALTER QUOTA ON DATABASE app EXPECT GENERATION 1 \
		 SET RULE records FOR RECORD MATCH EXACT user LIMIT 11",
		"OPTION IMPORT; REMOVE QUOTA ON DATABASE app EXPECT GENERATION 1",
	] {
		let error = import_error(&ds, sql, &owner).await;
		assert!(
			error.contains("Quota policy statements are not allowed during database import"),
			"{error}"
		);
		assert_eq!(generation(&ds).await, Some(1));
	}
}

#[tokio::test]
async fn ordinary_database_export_excludes_quota_policy() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE records FOR RECORD MATCH EXACT user LIMIT 10",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();

	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	let (sender, receiver) = async_channel::bounded(1);
	let export = ds.export(&database_owner, sender).await.unwrap();
	let collect = async move {
		let mut bytes = Vec::new();
		while let Ok(chunk) = receiver.recv().await {
			bytes.extend(chunk);
		}
		bytes
	};
	let (export_result, bytes) = tokio::join!(export, collect);
	export_result.unwrap();
	let sql = String::from_utf8(bytes).unwrap();
	assert!(!sql.contains("QUOTA"), "{sql}");
	assert!(!sql.contains("operation_id"), "{sql}");
	assert!(!sql.contains("ledger_state"), "{sql}");
	assert!(!sql.contains("active_epoch"), "{sql}");
}
