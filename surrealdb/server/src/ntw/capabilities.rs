//! Stable, unauthenticated native quota capability endpoint.

use axum::routing::get;
use axum::{Extension, Json, Router};
use surrealdb_core::dbs::capabilities::RouteTarget;

use super::AppState;
use crate::capability::CapabilityDocument;
use crate::ntw::error::Error as NetError;

pub fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/capabilities", get(handler))
}

async fn handler(
	Extension(state): Extension<AppState>,
) -> Result<Json<CapabilityDocument>, NetError> {
	if !state.datastore.allows_http_route(&RouteTarget::Capabilities) {
		warn!(
			"Capabilities denied HTTP route request attempt, target: '{}'",
			&RouteTarget::Capabilities
		);
		return Err(NetError::ForbiddenRoute(RouteTarget::Capabilities.to_string()));
	}
	let storage = state.datastore.native_quota_storage_status().await.map_err(|error| {
		error!(error = %error, "Unable to inspect native quota storage capability");
		NetError::InvalidStorage
	})?;
	let document = CapabilityDocument::current(storage).map_err(|error| {
		error!(error = %error, "Native quota capability manifest is invalid");
		NetError::InvalidStorage
	})?;
	Ok(Json(document))
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;
	use std::sync::atomic::AtomicBool;

	use surrealdb_core::kvs::{Datastore, NativeQuotaStorageState};

	use super::*;
	use crate::ntw::Readiness;
	use crate::ntw::client_ip::ClientIp;

	#[tokio::test]
	async fn endpoint_document_reports_runtime_storage_without_tenant_data() {
		let datastore = Arc::new(Datastore::new("memory").await.unwrap());
		datastore.check_version().await.unwrap();
		let state = AppState {
			client_ip: ClientIp::None,
			datastore,
			metrics_observer: None,
			readiness: Readiness {
				ready: Arc::new(AtomicBool::new(true)),
				max_heartbeat_age: None,
			},
		};
		let document = handler(Extension(state)).await.unwrap().0;
		assert_eq!(document.format_version, 1);
		assert_eq!(document.quota.name, "native-quota-v1");
		assert_eq!(document.storage.state, NativeQuotaStorageState::Ready);
		assert_eq!(document.backend.name, "memory");
		assert!(document.backend.hard_quota_certified);
	}
}
