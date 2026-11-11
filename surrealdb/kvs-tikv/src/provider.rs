//! The [`BackendProvider`] that claims the `tikv:` scheme.

use surrealdb_cnf::ConfigMap;
use surrealdb_kvs::TransactionBuilder;
use surrealdb_kvs::api::BoxFut;
use surrealdb_kvs::err::Result;
use surrealdb_kvs_any::{BackendProvider, ConnectContext};
use tracing::info;

use crate::{Datastore, unsupported_router_watermark};

const TARGET: &str = "surrealdb::core::kvs::ds";

/// Refuse an explicitly selected Router before connecting or opening storage.
/// Direct users of `Datastore` are also protected by `safe_timestamp` itself.
fn validate_live_query_engine(config: &ConfigMap) -> Result<()> {
	let mut engine = String::new();
	config.parse_key("live_query_engine", &mut engine);
	if engine.eq_ignore_ascii_case("router") {
		return Err(unsupported_router_watermark());
	}
	Ok(())
}

/// Provider for the TiKV storage engine.
///
/// Register it on top of the community set to make `tikv://<pd endpoints>`
/// constructible and valid as a datastore path:
///
/// ```ignore
/// let mut backends = Backends::community();
/// backends.register(TikvProvider);
/// ```
pub struct TikvProvider;

impl BackendProvider for TikvProvider {
	fn schemes(&self) -> &[&'static str] {
		&["tikv"]
	}

	fn connect<'a>(
		&'a self,
		ctx: ConnectContext<'a>,
	) -> BoxFut<'a, Result<Box<dyn TransactionBuilder>>> {
		Box::pin(async move {
			validate_live_query_engine(&ctx.config)?;
			// Parse TiKV-specific configuration from env vars
			// (SURREAL_TIKV_*) and query parameters.
			let tikv_config = ctx.config.load();
			let v = Datastore::new(ctx.path, tikv_config).await?;
			info!(target: TARGET, "Started tikv kvs store");
			Ok(Box::new(v) as Box<dyn TransactionBuilder>)
		})
	}
}

#[cfg(test)]
mod tests {
	#![allow(clippy::unwrap_used)]
	use surrealdb_cnf::ConfigMap;
	use surrealdb_kvs::err::Error;
	use surrealdb_kvs_any::{BackendProvider, ConnectContext};
	use tokio_util::sync::CancellationToken;

	use super::{TikvProvider, validate_live_query_engine};

	#[tokio::test]
	async fn router_is_refused_before_any_connection() {
		for engine in ["router", "ROUTER", "RoUtEr"] {
			let error = TikvProvider
				.connect(ConnectContext {
					scheme: "tikv",
					// No real PD endpoint: the configuration must fail before a
					// client is created, so this test never needs a cluster.
					path: "",
					canceller: CancellationToken::new(),
					config: ConfigMap::empty().with_key_value("live_query_engine", engine),
				})
				.await
				.err()
				.expect("Router needs a closed event watermark");
			assert!(matches!(error, Error::Datastore(_)));
			assert!(!error.is_retryable());
			assert!(error.to_string().contains("PD timestamp is not a closed"));
		}
	}

	#[test]
	fn the_guard_does_not_change_other_engine_selections() {
		validate_live_query_engine(&ConfigMap::empty()).unwrap();
		for engine in ["inline", "INLINE", "external", "EXTERNAL"] {
			validate_live_query_engine(
				&ConfigMap::empty().with_key_value("live_query_engine", engine),
			)
			.unwrap();
		}
	}
}
