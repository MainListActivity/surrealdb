//! Shared stable DTOs for native quota control-plane operations.

use uuid::Uuid;

use crate::catalog::{QuotaPolicyChange, QuotaUsageMeta, QuotaUsageState};
use crate::observe::{
	QuotaEvent, QuotaEventCtx, QuotaEventKind, QuotaEventOutcome, QuotaEventSafe,
};
use crate::val::{Datetime, Object, Value};

pub(crate) struct QuotaOperation {
	pub(crate) id: String,
	pub(crate) action: &'static str,
	pub(crate) database: String,
}

impl QuotaOperation {
	pub(crate) fn new(action: &'static str, database: String) -> Self {
		Self {
			id: Uuid::now_v7().to_string(),
			action,
			database,
		}
	}

	pub(crate) fn latest_change(&self, actor: String, generation: u64) -> QuotaPolicyChange {
		QuotaPolicyChange {
			operation_id: self.id.clone(),
			action: self.action.to_owned(),
			actor,
			generation,
			changed_at: Datetime::now(),
		}
	}

	pub(crate) fn audit_event(
		&self,
		kind: QuotaEventKind,
		namespace: &str,
		actor: String,
		outcome: QuotaEventOutcome,
	) -> QuotaEvent {
		QuotaEvent {
			safe: QuotaEventSafe {
				kind,
				outcome,
				duration: None,
			},
			ctx: QuotaEventCtx {
				operation_id: Some(self.id.clone()),
				namespace: Some(namespace.to_owned()),
				database: Some(self.database.clone()),
				actor: Some(actor),
			},
		}
	}

	pub(crate) fn result(
		&self,
		changed: bool,
		before_generation: Option<u64>,
		after_generation: Option<u64>,
		meta: &QuotaUsageMeta,
	) -> Value {
		Value::Object(Object::from(map! {
			"after" => operation_state(after_generation, meta),
			"before" => operation_state(before_generation, meta),
			"changed" => changed.into(),
			"database" => self.database.as_str().into(),
			"format_version" => 1u64.into(),
			"operation" => format!("{}_quota", self.action).into(),
			"operation_id" => self.id.as_str().into(),
		}))
	}
}

fn operation_state(generation: Option<u64>, meta: &QuotaUsageMeta) -> Value {
	let state = match meta.state {
		QuotaUsageState::Uninitialized => "uninitialized",
		QuotaUsageState::Rebuilding => "rebuilding",
		QuotaUsageState::Ready => "ready",
		QuotaUsageState::Corrupt => "corrupt",
	};
	Value::Object(Object::from(map! {
		"active_epoch" => meta.active_epoch.map_or(Value::None, Value::from),
		"generation" => generation.map_or(Value::None, Value::from),
		"ledger_state" => state.into(),
	}))
}
