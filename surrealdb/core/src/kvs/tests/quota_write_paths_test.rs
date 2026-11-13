//! Record quota admission is independent of the record encoding and storage write API.

use std::borrow::Cow;
use std::sync::Arc;

use surrealdb_datastore::WriteBatch;
use surrealdb_expr::expr::Dir;
use surrealdb_kvs::TransactionType;
use surrealdb_kvs::key::KVKey;

use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::catalog::{Record, StoredRecord};
use crate::dbs::Session;
use crate::key::schema::RecordKey;
use crate::kvs::Datastore;
use crate::val::{RecordId, RecordIdKey, TableName, Value};

async fn run(ds: &Datastore, session: &Session, sql: &str) {
	for response in ds.execute(sql, session, None).await.expect(sql) {
		response.result.expect(sql);
	}
}

async fn setup(packed: bool) -> (Arc<Datastore>, Session) {
	let ds = Datastore::builder()
		.build_with_path("memory")
		.await
		.expect("quota fixture operation must succeed");
	let session = Session::owner().with_ns("tenant").with_db("app");
	run(&ds, &Session::owner(), "DEFINE NAMESPACE tenant; USE NS tenant; DEFINE DATABASE app")
		.await;
	run(
		&ds,
		&session,
		if packed {
			"DEFINE TABLE item SCHEMAFULL; DEFINE FIELD value ON item TYPE option<int>"
		} else {
			"DEFINE TABLE item SCHEMALESS"
		},
	)
	.await;
	run(
		&ds,
		&Session::owner().with_ns("tenant"),
		"DEFINE QUOTA ON DATABASE app RULE records FOR RECORD MATCH EXACT item LIMIT 2",
	)
	.await;
	(ds, session)
}

async fn assert_count(ds: &Datastore, session: &Session, expected: u64) {
	let tx =
		ds.transaction(TransactionType::Read).await.expect("quota fixture operation must succeed");
	let db = tx
		.get_db_by_name("tenant", "app", None)
		.await
		.expect("quota fixture operation must succeed")
		.expect("quota fixture operation must succeed");
	assert_eq!(
		tx.quota_usage(db.namespace_id, db.database_id)
			.record_count(&"item".into())
			.await
			.expect("quota fixture operation must succeed"),
		expected
	);
	tx.cancel().await.expect("quota fixture operation must succeed");
	let mut responses = ds
		.execute("SELECT * FROM item", session, None)
		.await
		.expect("quota fixture operation must succeed");
	let surrealdb_types::Value::Array(rows) =
		responses.remove(0).result.expect("quota fixture operation must succeed")
	else {
		panic!("expected record array");
	};
	assert_eq!(rows.len() as u64, expected, "stored records and quota usage must agree");
}

async fn assert_encoding(ds: &Datastore, packed: bool) {
	let tx =
		ds.transaction(TransactionType::Read).await.expect("quota fixture operation must succeed");
	let db = tx
		.get_db_by_name("tenant", "app", None)
		.await
		.expect("quota fixture operation must succeed")
		.expect("quota fixture operation must succeed");
	let table = TableName::from("item");
	let id = RecordIdKey::Number(1);
	let key =
		RecordKey::new(db.namespace_id, db.database_id, Cow::Borrowed(&table), Cow::Borrowed(&id));
	let bytes = tx
		.get_key_raw(&key, None)
		.await
		.expect("quota fixture operation must succeed")
		.expect("record exists");
	assert_eq!(
		StoredRecord::is_packed_bytes(&bytes),
		packed,
		"test must exercise its requested encoding"
	);
	tx.cancel().await.expect("quota fixture operation must succeed");
}

async fn sql_write_paths(packed: bool) {
	let (ds, session) = setup(packed).await;
	// RETURN NONE permits the buffered write path; RETURN AFTER exercises returned rows.
	run(
		&ds,
		&session,
		"INSERT INTO item [{ id: item:1, value: 1 }, { id: item:2, value: 2 }] RETURN NONE",
	)
	.await;
	assert_encoding(&ds, packed).await;
	assert_count(&ds, &session, 2).await;
	for sql in [
		"UPDATE item SET value += 1 RETURN NONE",
		"UPSERT item:1 SET value = 9 RETURN AFTER",
		"INSERT INTO item { id: item:2, value: 10 } ON DUPLICATE KEY UPDATE value = $input.value RETURN NONE",
		"DELETE item:999 RETURN NONE",
		"BEGIN; DELETE item:1; CREATE item:1 SET value = 11; COMMIT",
		"BEGIN; CREATE item:3 SET value = 3; DELETE item:3; COMMIT",
	] {
		run(&ds, &session, sql).await;
		assert_count(&ds, &session, 2).await;
	}
	for sql in [
		"CREATE item:3 SET value = 3 RETURN NONE",
		"UPSERT item:3 SET value = 3 RETURN AFTER",
		"INSERT INTO item [{ id: item:3, value: 3 }, { id: item:4, value: 4 }] RETURN NONE",
	] {
		let mut result =
			ds.execute(sql, &session, None).await.expect("quota fixture operation must succeed");
		let error = result.remove(0).result.expect_err(sql);
		assert_eq!(error.kind_str(), "Quota", "{sql}: {error}");
		assert_count(&ds, &session, 2).await;
	}
	run(&ds, &session, "DELETE item RETURN NONE").await;
	assert_count(&ds, &session, 0).await;
	run(&ds, &session, "CREATE item:1 SET value = 1; CREATE item:2 SET value = 2").await;
	assert_count(&ds, &session, 2).await;
}

#[tokio::test]
async fn document_writes_charge_once_and_reject_overflow_atomically() {
	sql_write_paths(false).await;
}

#[tokio::test]
async fn packed_writes_charge_once_and_reject_overflow_atomically() {
	sql_write_paths(true).await;
}

#[derive(Clone, Copy, Debug)]
enum WritePath {
	ProviderPut,
	ProviderSet,
	RawPut,
	HeldPut,
	RawSet,
	Batch,
}

async fn write_one(ds: &Datastore, path: WritePath, id: i64) -> anyhow::Result<()> {
	let tx = ds.transaction(TransactionType::Write).await?;
	let db =
		tx.get_db_by_name("tenant", "app", None).await?.expect("write path fixture must exist");
	let (ns, db) = (db.namespace_id, db.database_id);
	let tb = TableName::from("item");
	let id = RecordIdKey::Number(id);
	let rid = RecordId {
		table: tb.clone(),
		key: id.clone(),
	};
	// A packed record must embed its exact key identity; a bare empty object
	// deliberately falls back to document encoding in the v4 record writer.
	let mut data = crate::val::Object::default();
	data.0.insert("id".into(), Value::RecordId(rid.clone()));
	data.0.insert("value".into(), Value::from(1_i64));
	let record = Arc::new(Record::new(Value::Object(data)));
	let key = RecordKey::new(ns, db, Cow::Borrowed(&tb), Cow::Borrowed(&id)).encode_key()?;
	let bytes = tx.record_writer(ns, db, &tb).await?.encode_whole(&record, Some(&rid))?;
	match path {
		WritePath::ProviderPut => tx.put_record(ns, db, &tb, &id, record).await?,
		WritePath::ProviderSet => tx.set_record(ns, db, &tb, &id, record).await?,
		WritePath::RawPut => tx.put(key, bytes).await?,
		WritePath::HeldPut => tx.put_held(key, bytes).await?,
		WritePath::RawSet => tx.set(key, bytes).await?,
		WritePath::Batch => {
			let created = tx.get(key.clone(), None).await?.is_none();
			let mut batch = WriteBatch::default();
			batch.set_record(tb, id, key, bytes, created);
			tx.write_batch(ns, db, batch).await?;
		}
	}
	tx.commit().await
}

#[tokio::test]
async fn provider_raw_held_and_batch_writes_share_one_quota_admission() {
	for packed in [false, true] {
		for path in [
			WritePath::ProviderPut,
			WritePath::ProviderSet,
			WritePath::RawPut,
			WritePath::HeldPut,
			WritePath::RawSet,
			WritePath::Batch,
		] {
			let (ds, session) = setup(packed).await;
			// Materialize the current v4 layout before calling low-level writers.
			// Direct writers encode against an existing layout; unlike SQL they
			// do not mint one. Remove the bootstrap row before counting admission.
			run(&ds, &session, "CREATE item:999 SET value = 0; DELETE item:999").await;
			assert_count(&ds, &session, 0).await;
			for id in [1, 2] {
				write_one(&ds, path, id)
					.await
					.unwrap_or_else(|e| panic!("{path:?}, packed={packed}: {e}"));
				assert_count(&ds, &session, id as u64).await;
			}
			assert_encoding(&ds, packed).await;
			for update in [WritePath::ProviderSet, WritePath::RawSet, WritePath::Batch] {
				write_one(&ds, update, 1).await.unwrap();
				assert_count(&ds, &session, 2).await;
			}
			let error =
				write_one(&ds, path, 3).await.expect_err("all write paths must reject overflow");
			assert!(
				matches!(
					error.downcast_ref::<crate::catalog::Error>(),
					Some(crate::catalog::Error::QuotaExceeded(_))
				),
				"{path:?}, packed={packed}: {error}"
			);
			assert_count(&ds, &session, 2).await;
		}
	}
}

#[tokio::test]
async fn index_build_succeeds_with_lightweight_relation_in_database() {
	let ds = Datastore::builder().build_with_path("memory").await.unwrap();
	let owner = Session::owner().with_ns("tenant").with_db("app");
	run(&ds, &Session::owner(), "DEFINE NAMESPACE tenant; USE NS tenant; DEFINE DATABASE app")
		.await;
	run(
		&ds,
		&owner,
		"DEFINE TABLE person SCHEMALESS; \
		 DEFINE TABLE follows TYPE RELATION IN person OUT person LIGHTWEIGHT; \
		 DEFINE TABLE item SCHEMAFULL; \
		 DEFINE FIELD value ON item TYPE int VALUE $value + 1; \
		 DEFINE INDEX item_value ON item FIELDS value UNIQUE",
	)
	.await;
	run(
		&ds,
		&Session::owner().with_ns("tenant"),
		"DEFINE QUOTA ON DATABASE app RULE edges FOR RECORD MATCH EXACT follows LIMIT 10",
	)
	.await;
	run(
		&ds,
		&owner,
		"DEFINE TABLE item2 SCHEMAFULL; \
		 DEFINE FIELD value ON item2 TYPE int; \
		 DEFINE INDEX item2_value ON item2 FIELDS value",
	)
	.await;
}

#[tokio::test]
async fn commit_conflict_retains_kind_without_a_quota_policy() {
	for policy in [false, true] {
		let ds = Datastore::builder().build_with_path("memory").await.unwrap();
		let owner = Session::owner().with_ns("tenant").with_db("app");
		run(&ds, &Session::owner(), "DEFINE NAMESPACE tenant; USE NS tenant; DEFINE DATABASE app")
			.await;
		run(&ds, &owner, "DEFINE TABLE item").await;
		if policy {
			run(
				&ds,
				&Session::owner().with_ns("tenant"),
				"DEFINE QUOTA ON DATABASE app RULE records FOR RECORD MATCH EXACT item LIMIT 10",
			)
			.await;
		}
		let first = Arc::new(ds.transaction(TransactionType::Write).await.unwrap());
		let second = Arc::new(ds.transaction(TransactionType::Write).await.unwrap());
		for tx in [&first, &second] {
			for response in ds
				.run(
					crate::kvs::QueryRequest::new("CREATE item:one SET value = 1", &owner)
						.with_transaction(Arc::clone(tx)),
				)
				.await
				.unwrap()
			{
				response.result.unwrap();
			}
		}
		first.commit().await.unwrap();
		let error = second.commit().await.expect_err("the stale transaction must not commit");
		if policy {
			assert!(error.to_string().contains("Quota admission conflicted"), "{error}");
		} else {
			assert!(surrealdb_datastore::is_retryable_transaction_conflict(&error), "{error}");
		}
	}
}

/// Lightweight edges have no RecordKey. Until logical-edge accounting exists,
/// a matching policy must reject the whole graph write instead of certifying
/// zero usage, and rebuild preflight must not strand a healthy ledger.
#[tokio::test]
async fn lightweight_relations_fail_closed_without_partial_edges_or_rebuild_fences() {
	for selector in ["EXACT follows", "REGEX /^follows$/"] {
		let ds = Datastore::builder().build_with_path("memory").await.unwrap();
		let owner = Session::owner().with_ns("tenant").with_db("app");
		let namespace_owner = Session::owner().with_ns("tenant");
		run(&ds, &Session::owner(), "DEFINE NAMESPACE tenant; USE NS tenant; DEFINE DATABASE app")
			.await;
		run(
			&ds,
			&owner,
			"DEFINE TABLE person; \
             DEFINE TABLE follows TYPE RELATION IN person OUT person LIGHTWEIGHT; \
             CREATE person:one; CREATE person:two",
		)
		.await;
		run(
			&ds,
			&namespace_owner,
			&format!(
				"DEFINE QUOTA ON DATABASE app RULE edges FOR RECORD MATCH {selector} LIMIT 10"
			),
		)
		.await;

		let tx = ds.transaction(TransactionType::Read).await.unwrap();
		let db = tx.get_db_by_name("tenant", "app", None).await.unwrap().unwrap();
		let (ns, db) = (db.namespace_id, db.database_id);
		let before_meta = tx.quota_usage(ns, db).meta().await.unwrap();
		assert_eq!(before_meta.state, crate::catalog::QuotaUsageState::Ready);
		assert_eq!(before_meta.staged_epoch, None);
		assert_eq!(tx.quota_usage(ns, db).record_count(&"person".into()).await.unwrap(), 2);
		assert_eq!(tx.quota_usage(ns, db).record_count(&"follows".into()).await.unwrap(), 0);
		tx.cancel().await.unwrap();

		// Raw graph writes must remain fenced even when unrelated index keys
		// no longer take the lightweight-relation quota guard.
		let person = RecordId::new("person".into(), "one".to_owned());
		let other = RecordId::new("person".into(), "two".to_owned());
		let edge = RecordId::new("follows".into(), "edge".to_owned());
		let graph = crate::key::schema::GraphKey {
			ns,
			db,
			tb: Cow::Borrowed(&person.table),
			id: Cow::Borrowed(&person.key),
			dir: Dir::Out,
			foreign_table: Cow::Borrowed(&edge.table),
			foreign_key: Cow::Borrowed(&edge.key),
		};
		let pointer = crate::key::schema::GraphPointerKey {
			ns,
			db,
			tb: Cow::Borrowed(&person.table),
			id: Cow::Borrowed(&person.key),
			dir: Dir::Out,
			foreign_table: Cow::Borrowed(&edge.table),
			foreign_key: Cow::Borrowed(&edge.key),
			target_table: Cow::Borrowed(&other.table),
			target_key: Cow::Borrowed(&other.key),
		};
		let adjacency = crate::key::schema::AdjacencyKey {
			ns,
			db,
			tb: Cow::Borrowed(&person.table),
			id: Cow::Borrowed(&person.key),
			dir: Dir::Out,
			edge_table: Cow::Borrowed(&edge.table),
			chunk_start: Cow::Borrowed(&edge.key),
		};
		for key in [
			graph.encode_key().unwrap(),
			pointer.encode_key().unwrap(),
			adjacency.encode_key().unwrap(),
		] {
			let tx = ds.transaction(TransactionType::Write).await.unwrap();
			let error = tx.set(key, Vec::<u8>::new()).await.unwrap_err();
			assert!(error.to_string().contains("lightweight"), "{selector}: {error}");
			tx.cancel().await.unwrap();
		}

		let graph_query = "SELECT * FROM follows; \
            SELECT VALUE ->follows FROM person:one; \
            SELECT VALUE <-follows FROM person:two";
		let before_graph: Vec<_> = ds
			.execute(graph_query, &owner, None)
			.await
			.unwrap()
			.into_iter()
			.map(|response| response.result.unwrap())
			.collect();
		assert!(matches!(&before_graph[0], surrealdb_types::Value::Array(rows) if rows.is_empty()));

		for sql in ["RELATE person:one->follows->person:two", "REBUILD QUOTA ON DATABASE app"] {
			let session = if sql.starts_with("REBUILD") {
				&namespace_owner
			} else {
				&owner
			};
			let responses = ds.execute(sql, session, None).await.unwrap();
			assert_eq!(responses.len(), 1);
			let error = responses.into_iter().next().unwrap().result.expect_err(sql);
			assert_eq!(error.kind_str(), "Quota", "{selector}: {sql}: {error}");
			assert!(error.to_string().contains("lightweight"), "{selector}: {sql}: {error}");
			let after_graph: Vec<_> = ds
				.execute(graph_query, &owner, None)
				.await
				.unwrap()
				.into_iter()
				.map(|response| response.result.unwrap())
				.collect();
			assert_eq!(
				after_graph, before_graph,
				"{sql} must not leave either adjacency direction or an edge record"
			);
			let tx = ds.transaction(TransactionType::Read).await.unwrap();
			assert_eq!(
				tx.quota_usage(ns, db).meta().await.unwrap(),
				before_meta,
				"{sql} must not change the ready ledger or install a rebuild fence"
			);
			assert_eq!(tx.quota_usage(ns, db).record_count(&"person".into()).await.unwrap(), 2);
			assert_eq!(tx.quota_usage(ns, db).record_count(&"follows".into()).await.unwrap(), 0);
			tx.cancel().await.unwrap();
		}

		run(&ds, &namespace_owner, "REMOVE QUOTA ON DATABASE app EXPECT GENERATION 1").await;
		run(&ds, &owner, "RELATE person:one->follows->person:two").await;
		let after_removal: Vec<_> = ds
			.execute(graph_query, &owner, None)
			.await
			.unwrap()
			.into_iter()
			.map(|response| response.result.unwrap())
			.collect();
		assert!(
			matches!(&after_removal[0], surrealdb_types::Value::Array(rows) if rows.len() == 1),
			"removing the matching policy must unblock a supported lightweight relation"
		);
		for direction in &after_removal[1..] {
			let surrealdb_types::Value::Array(rows) = direction else {
				panic!("expected graph rows");
			};
			assert_eq!(rows.len(), 1);
			assert!(
				matches!(&rows[0], surrealdb_types::Value::Array(edges) if edges.len() == 1),
				"successful RELATE must install both adjacency directions"
			);
		}
		let tx = ds.transaction(TransactionType::Read).await.unwrap();
		assert_eq!(
			tx.quota_usage(ns, db).meta().await.unwrap().state,
			crate::catalog::QuotaUsageState::Ready
		);
		tx.cancel().await.unwrap();
	}
}
