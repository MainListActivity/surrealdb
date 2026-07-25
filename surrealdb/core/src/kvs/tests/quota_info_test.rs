use std::sync::{Arc, Mutex};

use surrealdb_types::ToSql;

use crate::catalog::providers::DatabaseProvider;
use crate::dbs::{NewPlannerStrategy, Session};
use crate::iam::{Level, Role};
use crate::kvs::{Datastore, LockType, TransactionType};
use crate::observe::{ExecutionObserver, QuotaEvent, QuotaEventKind, QuotaEventOutcome};
use crate::val::Value;

#[derive(Default)]
struct CapturingQuotaObserver {
	events: Mutex<Vec<QuotaEvent>>,
}

impl ExecutionObserver for CapturingQuotaObserver {
	fn on_quota_event(&self, event: &QuotaEvent) {
		self.events.lock().unwrap().push(event.clone());
	}
}

async fn query_value(ds: &Datastore, sql: &str, session: &Session) -> Value {
	ds.execute(sql, session, None).await.unwrap().remove(0).result.unwrap().into()
}

#[tokio::test]
async fn info_for_quota_returns_canonical_text_and_stable_ready_structure() {
	let ds = Datastore::new("memory").await.unwrap();
	let root = Session::owner();
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner =
		Session::for_level(Level::Database("tenant".into(), "app".into()), Role::Owner);
	ds.execute("DEFINE NAMESPACE tenant", &root, None).await.unwrap();
	ds.execute("DEFINE DATABASE app", &namespace_owner, None).await.unwrap();
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE ent_fields FOR FIELD MATCH REGEX /^ent_/ LIMIT 3 \
		 RULE ent_records FOR RECORD MATCH REGEX /^ent_/ LIMIT 2 \
		 RULE ent_tables FOR TABLE MATCH REGEX /^ent_/ LIMIT 2 \
		 RULE exact_records FOR RECORD MATCH EXACT ent_user LIMIT UNLIMITED",
		&namespace_owner,
		None,
	)
	.await
	.unwrap();
	ds.execute(
		"DEFINE TABLE ent_user SCHEMAFULL; \
		 DEFINE FIELD name ON ent_user TYPE string; \
		 DEFINE TABLE misc SCHEMALESS; \
		 CREATE ent_user:one SET name = 'A'",
		&database_owner,
		None,
	)
	.await
	.unwrap();

	let text = query_value(&ds, "INFO FOR QUOTA ON DATABASE app", &namespace_owner).await;
	assert_eq!(
		text,
		Value::from(
			"DEFINE QUOTA ON DATABASE app RULE ent_fields FOR FIELD MATCH REGEX /^ent_/ \
			 LIMIT 3 RULE ent_records FOR RECORD MATCH REGEX /^ent_/ LIMIT 2 RULE ent_tables \
			 FOR TABLE MATCH REGEX /^ent_/ LIMIT 2 RULE exact_records FOR RECORD MATCH EXACT \
			 ent_user LIMIT UNLIMITED"
		)
	);

	let structure =
		query_value(&ds, "INFO FOR QUOTA ON DATABASE app STRUCTURE", &namespace_owner).await;
	let Value::Object(root) = structure else {
		panic!("expected quota structure object");
	};
	assert_eq!(root.get("format_version").unwrap().to_sql(), "1");
	assert_eq!(root.get("database").unwrap().to_sql(), "'app'");
	assert!(matches!(root.get("observed_at"), Some(Value::Datetime(_))));

	let Value::Object(policy) = root.get("policy").unwrap() else {
		panic!("expected policy object");
	};
	assert_eq!(policy.get("generation").unwrap().to_sql(), "1");
	assert_eq!(policy.get("rules").unwrap().to_sql().matches("rule_id").count(), 4);

	let Value::Object(ledger) = root.get("ledger").unwrap() else {
		panic!("expected ledger object");
	};
	assert_eq!(ledger.get("state").unwrap().to_sql(), "'ready'");
	assert_eq!(ledger.get("active_epoch").unwrap().to_sql(), "1");
	assert_eq!(ledger.get("usage_trusted").unwrap().to_sql(), "true");

	let usage = root.get("usage").unwrap().to_sql();
	assert!(usage.contains("rule_id: 'ent_tables'"), "{usage}");
	assert!(usage.contains("used: 1"), "{usage}");
	assert!(usage.contains("table: 'ent_user'"), "{usage}");
	assert!(usage.contains("table: 'misc'"), "{usage}");
	assert!(usage.contains("limit_origin: 'explicit_unlimited'"), "{usage}");
	assert!(usage.contains("record: ['misc']"), "{usage}");
	assert!(usage.contains("field: ['misc']"), "{usage}");
	assert!(usage.contains("table: ['misc']"), "{usage}");

	let database_info = query_value(&ds, "INFO FOR DATABASE STRUCTURE", &database_owner).await;
	let Value::Object(database_info) = database_info else {
		panic!("expected database info object");
	};
	let Value::Object(quota) = database_info.get("quota").unwrap() else {
		panic!("expected lightweight quota summary");
	};
	assert_eq!(quota.get("defined").unwrap().to_sql(), "true");
	assert_eq!(quota.get("generation").unwrap().to_sql(), "1");
	assert!(!quota.contains_key("usage"));
}

#[tokio::test]
async fn database_owner_can_read_quota_but_untrusted_usage_is_hidden() {
	let ds = Datastore::new("memory").await.unwrap();
	let root = Session::owner();
	let namespace_owner = Session::owner().with_ns("tenant");
	let database_owner =
		Session::for_level(Level::Database("tenant".into(), "app".into()), Role::Owner);
	ds.execute("DEFINE NAMESPACE tenant", &root, None).await.unwrap();
	ds.execute("DEFINE DATABASE app", &namespace_owner, None).await.unwrap();

	let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
	let db = tx.get_db_by_name("tenant", "app", None).await.unwrap().unwrap();
	tx.quota_usage(db.namespace_id, db.database_id)
		.begin_external_write_maintenance()
		.await
		.unwrap();
	tx.commit().await.unwrap();

	let structure =
		query_value(&ds, "INFO FOR QUOTA ON DATABASE app STRUCTURE", &database_owner).await;
	let Value::Object(root) = structure else {
		panic!("expected quota structure object");
	};
	let Value::Object(ledger) = root.get("ledger").unwrap() else {
		panic!("expected ledger object");
	};
	assert_eq!(ledger.get("state").unwrap().to_sql(), "'uninitialized'");
	assert_eq!(ledger.get("usage_trusted").unwrap().to_sql(), "false");
	assert_eq!(root.get("usage"), Some(&Value::None));
}

#[tokio::test]
async fn streaming_executor_returns_the_same_quota_structure_contract() {
	let ds = Datastore::new("memory").await.unwrap();
	let root = Session::owner();
	let namespace_owner = Session::owner().with_ns("tenant");
	ds.execute("DEFINE NAMESPACE tenant", &root, None).await.unwrap();
	ds.execute("DEFINE DATABASE app", &namespace_owner, None).await.unwrap();
	ds.execute(
		"DEFINE QUOTA ON DATABASE app \
		 RULE records FOR RECORD MATCH REGEX /^ent_/ LIMIT 10",
		&namespace_owner,
		None,
	)
	.await
	.unwrap();

	let mut streaming_owner = namespace_owner;
	streaming_owner.new_planner_strategy = NewPlannerStrategy::AllReadOnlyStatements;
	let structure =
		query_value(&ds, "INFO FOR QUOTA ON DATABASE app STRUCTURE", &streaming_owner).await;
	let Value::Object(root) = structure else {
		panic!("expected quota structure object");
	};
	assert_eq!(root.get("format_version").unwrap().to_sql(), "1");
	assert_eq!(root.get("database").unwrap().to_sql(), "'app'");
}

#[tokio::test]
async fn quota_ddl_returns_operation_results_and_preserves_latest_change_after_remove() {
	let observer = Arc::new(CapturingQuotaObserver::default());
	let configured = Arc::clone(&observer);
	let configured: Arc<dyn ExecutionObserver> = configured;
	let ds =
		Datastore::builder().with_observer(configured).build_with_path("memory").await.unwrap();
	let root = Session::owner();
	let namespace_owner = Session::owner().with_ns("tenant");
	ds.execute("DEFINE NAMESPACE tenant", &root, None).await.unwrap();
	ds.execute("DEFINE DATABASE app", &namespace_owner, None).await.unwrap();

	let defined = query_value(
		&ds,
		"DEFINE QUOTA ON DATABASE app \
		 RULE records FOR RECORD MATCH REGEX /^ent_/ LIMIT 2",
		&namespace_owner,
	)
	.await;
	let Value::Object(defined) = defined else {
		panic!("expected define operation result");
	};
	assert_eq!(defined.get("operation").unwrap().to_sql(), "'define_quota'");
	assert_eq!(defined.get("changed").unwrap().to_sql(), "true");
	assert!(defined.get("before").unwrap().to_sql().contains("generation: NONE"));
	assert!(defined.get("after").unwrap().to_sql().contains("generation: 1"));

	let noop = query_value(
		&ds,
		"DEFINE QUOTA OVERWRITE ON DATABASE app EXPECT GENERATION 1 \
		 RULE records FOR RECORD MATCH REGEX /^ent_/ LIMIT 2",
		&namespace_owner,
	)
	.await;
	let noop = noop.to_sql();
	assert!(noop.contains("changed: false"), "{noop}");

	let altered = query_value(
		&ds,
		"ALTER QUOTA ON DATABASE app EXPECT GENERATION 1 \
		 SET RULE records FOR RECORD MATCH REGEX /^ent_/ LIMIT 3",
		&namespace_owner,
	)
	.await;
	let altered = altered.to_sql();
	assert!(altered.contains("generation: 2"), "{altered}");

	let removed =
		query_value(&ds, "REMOVE QUOTA ON DATABASE app EXPECT GENERATION 2", &namespace_owner)
			.await;
	let Value::Object(removed) = removed else {
		panic!("expected remove operation result");
	};
	let remove_operation_id = removed.get("operation_id").unwrap().clone();
	assert_eq!(removed.get("operation").unwrap().to_sql(), "'remove_quota'");
	assert_eq!(removed.get("changed").unwrap().to_sql(), "true");
	assert!(removed.get("after").unwrap().to_sql().contains("generation: 3"));

	let info = query_value(&ds, "INFO FOR QUOTA ON DATABASE app STRUCTURE", &namespace_owner).await;
	let Value::Object(info) = info else {
		panic!("expected quota structure");
	};
	assert_eq!(info.get("policy"), Some(&Value::None));
	let Value::Object(latest) = info.get("latest_change").unwrap() else {
		panic!("expected latest policy change pointer");
	};
	assert_eq!(latest.get("action").unwrap().to_sql(), "'remove'");
	assert_eq!(latest.get("generation").unwrap().to_sql(), "3");
	assert_eq!(latest.get("operation_id"), Some(&remove_operation_id));
	assert!(matches!(latest.get("changed_at"), Some(Value::Datetime(_))));

	let events = observer.events.lock().unwrap();
	assert_eq!(events.len(), 3, "semantic no-op must not emit a policy-change audit event");
	assert_eq!(
		events.iter().map(|event| event.safe.kind).collect::<Vec<_>>(),
		vec![QuotaEventKind::Define, QuotaEventKind::Alter, QuotaEventKind::Remove]
	);
	assert!(events.iter().all(|event| event.safe.outcome == QuotaEventOutcome::Changed));
	assert!(events.iter().all(|event| event.ctx.operation_id.is_some()));
}
