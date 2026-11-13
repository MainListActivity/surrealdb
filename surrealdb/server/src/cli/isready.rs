use anyhow::Result;
use clap::Args;
use serde_json::json;
use surrealdb::engine::any::connect;

use crate::cli::abstraction::DatabaseConnectionArguments;
use crate::cli::json;

#[derive(Args, Debug)]
pub struct IsReadyCommandArguments {
	#[command(flatten)]
	conn: DatabaseConnectionArguments,
	#[arg(
		long,
		value_delimiter = ',',
		help = "Require one or more server capabilities, such as native-quota-v1"
	)]
	require: Vec<String>,
	#[command(flatten)]
	json: json::JsonArg,
}

pub async fn init(
	IsReadyCommandArguments {
		conn: DatabaseConnectionArguments {
			endpoint,
		},
		require,
		json: json::JsonArg {
			json,
		},
	}: IsReadyCommandArguments,
) -> Result<()> {
	if !require.is_empty() {
		super::capability_client::require_remote_ready(&endpoint, &require).await?;
		if json {
			return json::print(&json!({ "ready": true, "endpoint": endpoint }));
		}
		println!("OK");
		return Ok(());
	}
	// Connect to the database engine
	connect(endpoint.as_str()).await.map_err(|error| {
		if json {
			json::network(error)
		} else {
			error.into()
		}
	})?;
	if json {
		return json::print(&json!({ "ready": true, "endpoint": endpoint }));
	}
	println!("OK");
	Ok(())
}
