//! Explicit native quota datastore format status, preflight, and migration.

use anyhow::{Result, bail};
use clap::{Args, Subcommand};
use serde::Serialize;
use surrealdb_core::buc::BucketStoreProvider;
use surrealdb_core::kvs::{
	Datastore, NativeQuotaMigrationOptions, NativeQuotaStorageState, NativeQuotaStorageStatus,
	TransactionBuilderFactory,
};

#[derive(Debug, Subcommand)]
pub(crate) enum DatastoreCommand {
	/// Read the storage version and protected marker without changing them.
	Status(DatastorePathArguments),
	/// Validate the snapshot, offline, binary, and datastore prerequisites.
	Preflight(DatastoreMigrationArguments),
	/// Run or resume the explicit native quota format migration.
	Migrate(DatastoreMigrationArguments),
}

#[derive(Args, Debug)]
pub(crate) struct DatastorePathArguments {
	#[arg(help = "Local datastore path to inspect")]
	#[arg(env = "SURREAL_PATH", index = 1)]
	path: String,
}

#[derive(Args, Debug)]
pub(crate) struct DatastoreMigrationArguments {
	#[arg(help = "Local datastore path to migrate")]
	#[arg(env = "SURREAL_PATH", index = 1)]
	path: String,
	#[arg(long, help = "Operator reference to a verified, recoverable pre-migration snapshot")]
	snapshot: String,
	#[arg(long, help = "Confirm that every server and writer using this datastore is stopped")]
	confirm_offline: bool,
}

#[derive(Debug, Serialize)]
struct DatastorePreflight {
	format_version: u16,
	compatible: bool,
	snapshot_reference: String,
	offline_confirmed: bool,
	status: NativeQuotaStorageStatus,
}

pub(crate) async fn init<C>(composer: C, command: DatastoreCommand) -> Result<()>
where
	C: TransactionBuilderFactory + BucketStoreProvider + 'static,
{
	match command {
		DatastoreCommand::Status(args) => {
			C::path_valid(&args.path)?;
			let datastore =
				Datastore::builder().build_with_factory_path(&args.path, composer).await?;
			let status = datastore.native_quota_storage_status().await?;
			println!("{}", serde_json::to_string_pretty(&status)?);
			datastore.shutdown().await?;
		}
		DatastoreCommand::Preflight(args) => {
			C::path_valid(&args.path)?;
			let datastore =
				Datastore::builder().build_with_factory_path(&args.path, composer).await?;
			let status = datastore.native_quota_storage_status().await?;
			validate_migration_prerequisites(&args, &status)?;
			let preflight = DatastorePreflight {
				format_version: 1,
				compatible: true,
				snapshot_reference: args.snapshot,
				offline_confirmed: args.confirm_offline,
				status,
			};
			println!("{}", serde_json::to_string_pretty(&preflight)?);
			datastore.shutdown().await?;
		}
		DatastoreCommand::Migrate(args) => {
			C::path_valid(&args.path)?;
			let datastore =
				Datastore::builder().build_with_factory_path(&args.path, composer).await?;
			let status = datastore.native_quota_storage_status().await?;
			validate_migration_prerequisites(&args, &status)?;
			let report = datastore
				.migrate_native_quota_datastore(NativeQuotaMigrationOptions {
					snapshot_reference: args.snapshot,
					offline: args.confirm_offline,
				})
				.await?;
			println!("{}", serde_json::to_string_pretty(&report)?);
			datastore.shutdown().await?;
		}
	}
	Ok(())
}

fn validate_migration_prerequisites(
	args: &DatastoreMigrationArguments,
	status: &NativeQuotaStorageStatus,
) -> Result<()> {
	if args.snapshot.trim().is_empty() {
		bail!("--snapshot must reference a verified, recoverable snapshot");
	}
	if !args.confirm_offline {
		bail!("--confirm-offline is required before native quota datastore migration");
	}
	if matches!(
		status.state,
		NativeQuotaStorageState::Empty | NativeQuotaStorageState::LegacyUnversioned
	) {
		bail!("datastore state {:?} is not directly migratable", status.state);
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	fn status(state: NativeQuotaStorageState) -> NativeQuotaStorageStatus {
		NativeQuotaStorageStatus {
			format_version: 1,
			backend: "memory".to_owned(),
			storage_version: Some(3),
			state,
			ready: state == NativeQuotaStorageState::Ready,
			migration_required: state != NativeQuotaStorageState::Ready,
			marker: None,
		}
	}

	fn args(snapshot: &str, confirm_offline: bool) -> DatastoreMigrationArguments {
		DatastoreMigrationArguments {
			path: "memory".to_owned(),
			snapshot: snapshot.to_owned(),
			confirm_offline,
		}
	}

	#[test]
	fn preflight_requires_snapshot_offline_and_migratable_state() {
		assert!(
			validate_migration_prerequisites(
				&args("", true),
				&status(NativeQuotaStorageState::MigrationRequired)
			)
			.is_err()
		);
		assert!(
			validate_migration_prerequisites(
				&args("snapshot:test", false),
				&status(NativeQuotaStorageState::MigrationRequired)
			)
			.is_err()
		);
		assert!(
			validate_migration_prerequisites(
				&args("snapshot:test", true),
				&status(NativeQuotaStorageState::LegacyUnversioned)
			)
			.is_err()
		);
		validate_migration_prerequisites(
			&args("snapshot:test", true),
			&status(NativeQuotaStorageState::MigrationRequired),
		)
		.unwrap();
		validate_migration_prerequisites(
			&args("snapshot:test", true),
			&status(NativeQuotaStorageState::Migrating),
		)
		.unwrap();
	}
}
