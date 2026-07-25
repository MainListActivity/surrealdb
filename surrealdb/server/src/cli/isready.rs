use anyhow::Result;
use clap::Args;
use surrealdb::engine::any::connect;

use crate::cli::abstraction::DatabaseConnectionArguments;

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
}

pub async fn init(
	IsReadyCommandArguments {
		conn: DatabaseConnectionArguments {
			endpoint,
		},
		require,
	}: IsReadyCommandArguments,
) -> Result<()> {
	if !require.is_empty() {
		super::capability_client::require_remote_ready(&endpoint, &require).await?;
		println!("OK");
		return Ok(());
	}
	// Connect to the database engine
	connect(endpoint).await?;
	// Log output ok
	println!("OK");
	// All ok
	Ok(())
}
