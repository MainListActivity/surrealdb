use std::sync::Arc;

use tokio::sync::Barrier;

use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::dbs::Session;
use crate::err::Error;
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

async fn record_count(ds: &Datastore, table: &str) -> u64 {
	let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	let db = tx.get_db_by_name("tenant", "app", None).await.unwrap().unwrap();
	let count =
		tx.quota_usage(db.namespace_id, db.database_id).record_count(&table.into()).await.unwrap();
	tx.cancel().await.unwrap();
	count
}

async fn physical_record_count(ds: &Datastore, table: &str, session: &Session) -> i64 {
	let sql = format!("SELECT count() FROM {table} GROUP ALL");
	let mut responses = ds.execute(&sql, session, None).await.unwrap();
	let surrealdb_types::Value::Array(rows) = responses.remove(0).result.unwrap() else {
		panic!("count query did not return an array");
	};
	let Some(surrealdb_types::Value::Object(row)) = rows.first() else {
		panic!("count query did not return an object row");
	};
	let Some(surrealdb_types::Value::Number(count)) = row.get("count") else {
		panic!("count query did not return a numeric count");
	};
	count.to_int().unwrap()
}

#[tokio::test]
async fn create_consumes_record_quota_and_rejects_the_next_record_atomically() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE user_records FOR RECORD MATCH EXACT ent_user LIMIT 1",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();

	ds.execute("CREATE ent_user:one", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	let mut responses = ds.execute("CREATE ent_user:two", &database_owner, None).await.unwrap();
	let error = responses.remove(0).result.unwrap_err();

	assert_eq!(error.kind_str(), "Quota");
	let quota = error.quota_details().expect("quota error details must survive commit");
	assert_eq!(quota.code(), "quota_exceeded");
	assert!(!quota.retryable());
	assert!(error.message().contains("user_records"), "{error}");
	assert_eq!(record_count(&ds, "ent_user").await, 1);
	assert_eq!(physical_record_count(&ds, "ent_user", &database_owner).await, 1);
}

#[tokio::test]
async fn explicit_transaction_commit_preserves_quota_wire_error() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE user_records FOR RECORD MATCH EXACT ent_user LIMIT 1",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	ds.execute("CREATE ent_user:one", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();

	let responses = ds
		.execute(
			"BEGIN; CREATE ent_user:two; COMMIT",
			&database_owner,
			None,
		)
		.await
		.unwrap();
	let error = responses.last().expect("COMMIT response").result.as_ref().unwrap_err();
	assert_eq!(error.kind_str(), "Quota");
	assert_eq!(error.quota_details().expect("quota details").code(), "quota_exceeded");
	assert_eq!(physical_record_count(&ds, "ent_user", &database_owner).await, 1);
}

#[tokio::test]
async fn typed_mutations_count_only_final_record_existence_transitions() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE user_records FOR RECORD MATCH EXACT ent_user LIMIT 3",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();

	for sql in [
		"INSERT INTO ent_user [\
			{ id: ent_user:one, value: 1 }, \
			{ id: ent_user:two, value: 2 }]",
		"INSERT INTO ent_user { id: ent_user:one, value: 10 } \
		 ON DUPLICATE KEY UPDATE value = $input.value",
		"UPSERT ent_user:two SET value = 20",
		"UPSERT ent_user:three SET value = 3",
		"UPDATE ent_user:three SET value = 30",
		"DELETE ent_user:missing",
	] {
		ds.execute(sql, &database_owner, None).await.unwrap().remove(0).result.unwrap();
	}
	assert_eq!(record_count(&ds, "ent_user").await, 3);

	let responses = ds
		.execute(
			"BEGIN; CREATE ent_user:temporary; DELETE ent_user:temporary; COMMIT",
			&database_owner,
			None,
		)
		.await
		.unwrap();
	for response in responses {
		response.result.unwrap();
	}
	assert_eq!(record_count(&ds, "ent_user").await, 3);

	let error = statement_error(
		&ds,
		"INSERT INTO ent_user [\
			{ id: ent_user:four, value: 4 }, \
			{ id: ent_user:five, value: 5 }]",
		&database_owner,
	)
	.await;
	assert!(error.contains("user_records"), "{error}");
	assert_eq!(record_count(&ds, "ent_user").await, 3);
	assert_eq!(physical_record_count(&ds, "ent_user", &database_owner).await, 3);

	ds.execute("DELETE ent_user:one", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	ds.execute("CREATE ent_user:four", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	assert_eq!(record_count(&ds, "ent_user").await, 3);
}

#[tokio::test]
async fn relate_counts_only_edge_records_and_cascade_delete_releases_them() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE edge_records FOR RECORD MATCH EXACT ent_edge LIMIT 1",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	ds.execute("CREATE ent_node:a; CREATE ent_node:b; CREATE ent_node:c", &database_owner, None)
		.await
		.unwrap()
		.into_iter()
		.for_each(|response| {
			response.result.unwrap();
		});

	ds.execute(
		"RELATE ent_node:a -> ent_edge:one -> ent_node:b SET weight = 1",
		&database_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	ds.execute(
		"RELATE OR UPDATE ent_node:a -> ent_edge:one -> ent_node:b SET weight = 2",
		&database_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	assert_eq!(record_count(&ds, "ent_edge").await, 1);

	let error =
		statement_error(&ds, "RELATE ent_node:a -> ent_edge:two -> ent_node:c", &database_owner)
			.await;
	assert!(error.contains("edge_records"), "{error}");
	assert_eq!(record_count(&ds, "ent_edge").await, 1);

	ds.execute("DELETE ent_node:a", &database_owner, None).await.unwrap().remove(0).result.unwrap();
	assert_eq!(record_count(&ds, "ent_edge").await, 0);
	assert_eq!(physical_record_count(&ds, "ent_edge", &database_owner).await, 0);

	ds.execute("RELATE ent_node:b -> ent_edge:two -> ent_node:c", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	assert_eq!(record_count(&ds, "ent_edge").await, 1);
}

#[tokio::test]
async fn materialized_view_initialization_and_maintenance_use_record_quota() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute("CREATE ent_source:one SET score = 1", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE view_records FOR RECORD MATCH EXACT ent_view LIMIT 1",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();

	ds.execute("DEFINE TABLE ent_view AS SELECT score FROM ent_source", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	assert_eq!(record_count(&ds, "ent_view").await, 1);

	let error = statement_error(&ds, "CREATE ent_source:two SET score = 2", &database_owner).await;
	assert!(error.contains("view_records"), "{error}");
	assert_eq!(record_count(&ds, "ent_view").await, 1);
	assert_eq!(physical_record_count(&ds, "ent_view", &database_owner).await, 1);
	assert_eq!(physical_record_count(&ds, "ent_source", &database_owner).await, 1);
}

#[tokio::test]
async fn aggregated_view_deletion_releases_record_quota() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute("CREATE ent_source:one SET category = 'a', score = 1", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE summary_records FOR RECORD MATCH EXACT ent_summary LIMIT 1",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	ds.execute(
		"DEFINE TABLE ent_summary AS \
		 SELECT count() AS total, category FROM ent_source GROUP BY category",
		&database_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	assert_eq!(record_count(&ds, "ent_summary").await, 1);

	ds.execute("DELETE ent_source:one", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	assert_eq!(record_count(&ds, "ent_summary").await, 0);
	assert_eq!(physical_record_count(&ds, "ent_summary", &database_owner).await, 0);

	ds.execute("CREATE ent_source:two SET category = 'b', score = 2", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	assert_eq!(record_count(&ds, "ent_summary").await, 1);
}

#[tokio::test]
async fn record_usage_is_continuous_and_exact_overrides_the_strictest_regex() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute(
		"CREATE ent_user:one; CREATE ent_user:two; \
		 CREATE ent_order:one; CREATE ent_order:two",
		&database_owner,
		None,
	)
	.await
	.unwrap()
	.into_iter()
	.for_each(|response| {
		response.result.unwrap();
	});
	assert_eq!(record_count(&ds, "ent_user").await, 2);
	assert_eq!(record_count(&ds, "ent_order").await, 2);

	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE ent_records FOR RECORD MATCH REGEX /^ent_/ LIMIT 3 \
		 RULE order_records FOR RECORD MATCH REGEX /order$/ LIMIT 2 \
		 RULE user_records FOR RECORD MATCH EXACT ent_user LIMIT UNLIMITED",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	ds.execute("CREATE ent_user:three; CREATE ent_user:four", &database_owner, None)
		.await
		.unwrap()
		.into_iter()
		.for_each(|response| {
			response.result.unwrap();
		});
	assert_eq!(record_count(&ds, "ent_user").await, 4);

	let error = statement_error(&ds, "CREATE ent_order:three", &database_owner).await;
	assert!(error.contains("order_records"), "{error}");
	ds.execute("UPDATE ent_order:one SET status = 'updated'", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	ds.execute("DELETE ent_order:one; CREATE ent_order:three", &database_owner, None)
		.await
		.unwrap()
		.into_iter()
		.for_each(|response| {
			response.result.unwrap();
		});
	assert_eq!(record_count(&ds, "ent_order").await, 2);
}

#[tokio::test]
async fn record_quota_intents_follow_savepoint_rollback_and_cancel() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE user_records FOR RECORD MATCH EXACT ent_user LIMIT 1",
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

	let tx = Arc::new(ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap());
	tx.new_save_point().await.unwrap();
	ds.execute_with_transaction(
		"CREATE ent_user:rolled_back",
		&database_owner,
		None,
		Arc::clone(&tx),
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	tx.rollback_to_save_point().await.unwrap();
	tx.commit().await.unwrap();
	assert_eq!(record_count(&ds, "ent_user").await, 0);

	let cancelled =
		Arc::new(ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap());
	ds.execute_with_transaction(
		"CREATE ent_user:cancelled",
		&database_owner,
		None,
		Arc::clone(&cancelled),
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	cancelled.cancel().await.unwrap();
	assert_eq!(record_count(&ds, "ent_user").await, 0);
	assert_eq!(physical_record_count(&ds, "ent_user", &database_owner).await, 0);

	ds.execute("CREATE ent_user:committed", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	assert_eq!(record_count(&ds, "ent_user").await, 1);
}

#[tokio::test]
async fn commit_aggregates_record_violations_across_tables() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE a_records FOR RECORD MATCH EXACT ent_a LIMIT 0 \
		 RULE b_records FOR RECORD MATCH EXACT ent_b LIMIT 0",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();

	let tx = Arc::new(ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap());
	for sql in ["CREATE ent_a:one", "CREATE ent_b:one"] {
		ds.execute_with_transaction(sql, &database_owner, None, Arc::clone(&tx))
			.await
			.unwrap()
			.remove(0)
			.result
			.unwrap();
	}
	let error = tx.commit().await.unwrap_err();
	let Error::QuotaExceeded(details) = error.downcast_ref::<Error>().unwrap() else {
		panic!("expected quota exceeded error, got {error}");
	};
	assert_eq!(details.violations.len(), 2);
	assert!(!details.truncated);
	assert_eq!(
		details.violations.iter().map(|violation| violation.rule.as_str()).collect::<Vec<_>>(),
		["a_records", "b_records"]
	);
	assert_eq!(record_count(&ds, "ent_a").await, 0);
	assert_eq!(record_count(&ds, "ent_b").await, 0);
}

#[tokio::test]
async fn aggregated_quota_violations_are_deterministic_and_capped() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	let mut policy = "DEFINE QUOTA ON DATABASE app".to_owned();
	let mut mutations = String::new();
	for index in 0..65 {
		policy
			.push_str(&format!(" RULE r_{index:02} FOR RECORD MATCH EXACT ent_{index:02} LIMIT 0"));
		mutations.push_str(&format!("CREATE ent_{index:02}:one;"));
	}
	ds.execute(&policy, &namespace_owner, None).await.unwrap().remove(0).result.unwrap();

	let tx = Arc::new(ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap());
	for response in ds
		.execute_with_transaction(&mutations, &database_owner, None, Arc::clone(&tx))
		.await
		.unwrap()
	{
		response.result.unwrap();
	}
	let error = tx.commit().await.unwrap_err();
	let Error::QuotaExceeded(details) = error.downcast_ref::<Error>().unwrap() else {
		panic!("expected quota exceeded error, got {error}");
	};
	assert_eq!(details.violations.len(), 64);
	assert!(details.truncated);
	assert_eq!(details.violations.first().unwrap().rule, "r_00");
	assert_eq!(details.violations.last().unwrap().rule, "r_63");
}

#[tokio::test]
async fn over_limit_records_allow_only_non_worsening_final_usage() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute("CREATE ent_user:one; CREATE ent_user:two", &database_owner, None)
		.await
		.unwrap()
		.into_iter()
		.for_each(|response| {
			response.result.unwrap();
		});
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE user_records FOR RECORD MATCH EXACT ent_user LIMIT 3",
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
		 SET RULE user_records FOR RECORD MATCH EXACT ent_user LIMIT 1",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();

	ds.execute("UPDATE ent_user:one SET status = 'allowed'", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	let responses = ds
		.execute("BEGIN; DELETE ent_user:one; CREATE ent_user:three; COMMIT", &database_owner, None)
		.await
		.unwrap();
	for response in responses {
		response.result.unwrap();
	}
	assert_eq!(record_count(&ds, "ent_user").await, 2);

	let error = statement_error(&ds, "CREATE ent_user:four", &database_owner).await;
	assert!(error.contains("user_records"), "{error}");
	ds.execute("DELETE ent_user:two", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	assert_eq!(record_count(&ds, "ent_user").await, 1);
	let error = statement_error(&ds, "CREATE ent_user:five", &database_owner).await;
	assert!(error.contains("user_records"), "{error}");
	assert_eq!(record_count(&ds, "ent_user").await, 1);
}

#[tokio::test]
async fn table_replacement_resets_record_usage_before_counting_recreated_rows() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute("CREATE ent_user:one; CREATE ent_user:two", &database_owner, None)
		.await
		.unwrap()
		.into_iter()
		.for_each(|response| {
			response.result.unwrap();
		});
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE user_records FOR RECORD MATCH EXACT ent_user LIMIT 1",
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
			"BEGIN; \
			 CREATE ent_user:temporary; \
			 REMOVE TABLE ent_user; \
			 DEFINE TABLE ent_user; \
			 CREATE ent_user:replacement; \
			 COMMIT",
			&database_owner,
			None,
		)
		.await
		.unwrap();
	for response in responses {
		response.result.unwrap();
	}
	assert_eq!(record_count(&ds, "ent_user").await, 1);
	assert_eq!(physical_record_count(&ds, "ent_user", &database_owner).await, 1);
}

#[tokio::test]
async fn in_flight_record_write_conflicts_with_policy_generation_switch() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE user_records FOR RECORD MATCH EXACT ent_user LIMIT 10",
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

	let writer =
		Arc::new(ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap());
	ds.execute_with_transaction("CREATE ent_user:late", &database_owner, None, Arc::clone(&writer))
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	ds.execute(
		"ALTER QUOTA ON DATABASE app EXPECT GENERATION 1 \
		 SET RULE user_records FOR RECORD MATCH EXACT ent_user LIMIT 0",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();

	assert!(writer.commit().await.is_err());
	assert_eq!(record_count(&ds, "ent_user").await, 0);
	assert_eq!(physical_record_count(&ds, "ent_user", &database_owner).await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_clients_fill_exactly_the_remaining_record_slots() {
	let ds = Arc::new(setup().await);
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE user_records FOR RECORD MATCH EXACT ent_user LIMIT 5",
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

	let barrier = Arc::new(Barrier::new(13));
	let mut clients = Vec::new();
	for index in 0..12 {
		let ds = Arc::clone(&ds);
		let barrier = Arc::clone(&barrier);
		let session = database_owner.clone();
		clients.push(tokio::spawn(async move {
			barrier.wait().await;
			let sql = format!("CREATE ent_user:client_{index}");
			for _ in 0..256 {
				let result = match ds.execute(&sql, &session, None).await {
					Ok(mut responses) => responses.remove(0).result.map(|_| ()),
					Err(error) => Err(error),
				};
				match result {
					Ok(()) => return true,
					Err(error) => {
						let message = error.to_string();
						if message.contains("user_records") {
							return false;
						}
						assert!(
							message.contains("Transaction conflict")
								|| message.contains("failed transaction")
								|| message.contains("Quota admission conflicted"),
							"unexpected concurrent create error: {message}"
						);
						tokio::task::yield_now().await;
					}
				}
			}
			panic!("concurrent record create did not converge");
		}));
	}
	barrier.wait().await;
	let mut successes = 0;
	for client in clients {
		successes += usize::from(client.await.unwrap());
	}

	assert_eq!(successes, 5);
	assert_eq!(record_count(&ds, "ent_user").await, 5);
	assert_eq!(physical_record_count(&ds, "ent_user", &database_owner).await, 5);
}

#[tokio::test]
async fn semantic_import_uses_the_same_record_quota_and_statement_boundaries() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE user_records FOR RECORD MATCH EXACT ent_user LIMIT 1",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();

	let responses = ds
		.import(
			"OPTION IMPORT; \
			 INSERT INTO ent_user { id: ent_user:one, score: 1 }; \
			 INSERT INTO ent_user { id: ent_user:two, score: 2 };",
			&database_owner,
		)
		.await
		.unwrap();
	let errors =
		responses.into_iter().filter_map(|response| response.result.err()).collect::<Vec<_>>();
	assert_eq!(errors.len(), 1);
	assert!(errors[0].to_string().contains("user_records"), "{}", errors[0]);
	assert_eq!(record_count(&ds, "ent_user").await, 1);
	assert_eq!(physical_record_count(&ds, "ent_user", &database_owner).await, 1);
}

#[tokio::test]
async fn implicit_table_and_record_violations_roll_back_together() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE ent_tables FOR TABLE MATCH REGEX /^ent_/ LIMIT 0 \
		 RULE forbidden_records FOR RECORD MATCH EXACT ent_forbidden LIMIT 0",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();

	let tx = Arc::new(ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap());
	ds.execute_with_transaction("CREATE ent_forbidden:one", &database_owner, None, Arc::clone(&tx))
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	let error = tx.commit().await.unwrap_err();
	let Error::QuotaExceeded(details) = error.downcast_ref::<Error>().unwrap() else {
		panic!("expected quota exceeded error, got {error}");
	};
	assert_eq!(
		details.violations.iter().map(|violation| violation.resource.as_str()).collect::<Vec<_>>(),
		["table", "record"]
	);

	let read = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
	assert!(
		read.get_tb_by_name("tenant", "app", &"ent_forbidden".into(), None)
			.await
			.unwrap()
			.is_none()
	);
	read.cancel().await.unwrap();
	assert_eq!(record_count(&ds, "ent_forbidden").await, 0);
}

#[tokio::test]
async fn record_ranges_release_and_consume_their_actual_cardinality() {
	let ds = setup().await;
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner = Session::owner().with_ns("tenant").with_db("app");
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE user_records FOR RECORD MATCH EXACT ent_user LIMIT 5",
		&namespace_owner,
		None,
	)
	.await
	.unwrap()
	.remove(0)
	.result
	.unwrap();
	ds.execute("CREATE |ent_user:1..=5|", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	assert_eq!(record_count(&ds, "ent_user").await, 5);

	ds.execute("DELETE |ent_user:2..=4|", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	assert_eq!(record_count(&ds, "ent_user").await, 2);

	ds.execute("CREATE |ent_user:6..=8|", &database_owner, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	assert_eq!(record_count(&ds, "ent_user").await, 5);
	assert_eq!(physical_record_count(&ds, "ent_user", &database_owner).await, 5);
}
