//! Explicit native quota datastore format status, preflight, and migration.

use anyhow::{Result, bail};
use clap::{Args, Subcommand};
use serde::Serialize;
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
	#[arg(
		long,
		default_value_t = false,
		help = "Acknowledge that the snapshot was verified as recoverable; this flag does not verify it"
	)]
	snapshot_verified: bool,
	#[arg(long, help = "Confirm that every server and writer using this datastore is stopped")]
	confirm_offline: bool,
}

#[derive(Debug, Serialize)]
struct DatastorePreflight {
	format_version: u16,
	compatible: bool,
	snapshot_reference: String,
	/// Operator acknowledgement only, not independently verified backup evidence.
	snapshot_verified: bool,
	offline_confirmed: bool,
	status: NativeQuotaStorageStatus,
}

pub(crate) async fn init<C>(composer: C, command: DatastoreCommand) -> Result<()>
where
	C: TransactionBuilderFactory + 'static,
{
	match command {
		DatastoreCommand::Status(args) => {
			let datastore = open_offline_datastore(&args.path, composer).await?;
			let status = datastore.native_quota_storage_status().await?;
			println!("{}", serde_json::to_string_pretty(&status)?);
			datastore.shutdown().await?;
		}
		DatastoreCommand::Preflight(args) => {
			let datastore = open_offline_datastore(&args.path, composer).await?;
			let status = datastore.native_quota_storage_status().await?;
			validate_migration_prerequisites(&args, &status)?;
			let preflight = DatastorePreflight {
				format_version: 1,
				compatible: true,
				snapshot_reference: args.snapshot,
				snapshot_verified: args.snapshot_verified,
				offline_confirmed: args.confirm_offline,
				status,
			};
			println!("{}", serde_json::to_string_pretty(&preflight)?);
			datastore.shutdown().await?;
		}
		DatastoreCommand::Migrate(args) => {
			let datastore = open_offline_datastore(&args.path, composer).await?;
			let status = datastore.native_quota_storage_status().await?;
			validate_migration_prerequisites(&args, &status)?;
			let report = datastore
				.migrate_native_quota_datastore(NativeQuotaMigrationOptions {
					snapshot_reference: args.snapshot,
					snapshot_verified: args.snapshot_verified,
					offline: args.confirm_offline,
				})
				.await?;
			println!("{}", serde_json::to_string_pretty(&report)?);
			datastore.shutdown().await?;
		}
	}
	Ok(())
}

/// Inspect or explicitly migrate without ordinary startup transactions or
/// background maintenance. Startup would reject an old fork marker before the
/// migrator can inspect it, and may write metadata during a status-only command.
async fn open_offline_datastore<C>(path: &str, composer: C) -> Result<std::sync::Arc<Datastore>>
where
	C: TransactionBuilderFactory + 'static,
{
	composer.path_valid(path)?;
	Datastore::builder()
		.without_startup_transactions()
		.build_with_factory_path(path, composer)
		.await
}

fn validate_migration_prerequisites(
	args: &DatastoreMigrationArguments,
	status: &NativeQuotaStorageStatus,
) -> Result<()> {
	if args.snapshot.trim().is_empty() {
		bail!("--snapshot must reference a verified, recoverable snapshot");
	}
	if !args.snapshot_verified {
		bail!(
			"--snapshot-verified is required to acknowledge a verified, recoverable snapshot; the flag does not verify the backup"
		);
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

	fn args(
		snapshot: &str,
		confirm_offline: bool,
		snapshot_verified: bool,
	) -> DatastoreMigrationArguments {
		DatastoreMigrationArguments {
			path: "memory".to_owned(),
			snapshot: snapshot.to_owned(),
			confirm_offline,
			snapshot_verified,
		}
	}

	#[test]
	fn preflight_requires_verified_snapshot_offline_and_migratable_state() {
		assert!(
			validate_migration_prerequisites(
				&args("", true, true),
				&status(NativeQuotaStorageState::MigrationRequired)
			)
			.is_err()
		);
		assert!(
			validate_migration_prerequisites(
				&args("snapshot:test", false, true),
				&status(NativeQuotaStorageState::MigrationRequired)
			)
			.is_err()
		);
		assert!(
			validate_migration_prerequisites(
				&args("snapshot:test", true, true),
				&status(NativeQuotaStorageState::LegacyUnversioned)
			)
			.is_err()
		);
		validate_migration_prerequisites(
			&args("snapshot:test", true, true),
			&status(NativeQuotaStorageState::MigrationRequired),
		)
		.unwrap();
		validate_migration_prerequisites(
			&args("snapshot:test", true, true),
			&status(NativeQuotaStorageState::Migrating),
		)
		.unwrap();
	}

	#[test]
	fn preflight_rejects_unacknowledged_snapshot() {
		let error = validate_migration_prerequisites(
			&args("snapshot:test", true, false),
			&status(NativeQuotaStorageState::MigrationRequired),
		)
		.unwrap_err();
		assert!(error.to_string().contains("--snapshot-verified"));
	}

	#[test]
	fn snapshot_acknowledgement_is_explicit_and_defaults_to_false() {
		use clap::Parser;

		#[derive(Parser)]
		struct TestCli {
			#[command(flatten)]
			args: DatastoreMigrationArguments,
		}

		let missing = TestCli::try_parse_from([
			"test",
			"memory",
			"--snapshot",
			"snapshot:test",
			"--confirm-offline",
		])
		.unwrap();
		assert!(!missing.args.snapshot_verified);

		let acknowledged = TestCli::try_parse_from([
			"test",
			"memory",
			"--snapshot",
			"snapshot:test",
			"--confirm-offline",
			"--snapshot-verified",
		])
		.unwrap();
		assert!(acknowledged.args.snapshot_verified);
	}

	#[cfg(feature = "storage-mem")]
	#[tokio::test]
	async fn offline_open_and_status_do_not_initialize_empty_storage() -> Result<()> {
		let datastore =
			open_offline_datastore("memory", surrealdb_core::CommunityComposer()).await?;
		// This is the exact constructor shared by status, preflight and migrate.
		// Repeated inspection must not stamp a version, fork marker or startup metadata.
		for _ in 0..2 {
			let status = datastore.native_quota_storage_status().await?;
			assert_eq!(status.state, NativeQuotaStorageState::Empty);
			assert_eq!(status.storage_version, None);
			assert!(status.marker.is_none());
			assert!(!status.ready);
		}
		datastore.shutdown().await?;
		Ok(())
	}
}
