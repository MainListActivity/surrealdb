//! Stable native quota capability and compatibility contracts.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use surrealdb_core::kvs::{
	NATIVE_QUOTA_CAPABILITY, NATIVE_QUOTA_ERROR_FORMAT_VERSION, NATIVE_QUOTA_INFO_FORMAT_VERSION,
	NATIVE_QUOTA_STORAGE_STATUS_FORMAT_VERSION, NativeQuotaStorageState, NativeQuotaStorageStatus,
	native_quota_fork_contract, native_quota_format_contract,
	native_quota_storage_version_contract,
};

use crate::cnf::PKG_VERSION;

/// Current public capability document major.
pub const CAPABILITY_FORMAT_VERSION: u16 = 1;

const EMBEDDED_MANIFEST: &str = include_str!("../../../compatibility/native-quota-v1.json");

/// Stable quota protocol contracts.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContractManifest {
	/// Native quota control/enforcement contract major.
	pub quota_major: u16,
	/// `INFO FOR QUOTA ... STRUCTURE` format major.
	pub info_format_version: u16,
	/// Structured quota error format major.
	pub error_format_version: u16,
	/// Numeric SurrealDB wire error code.
	pub error_wire_code: i64,
}

/// Protected catalog and storage format revisions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FormatManifest {
	/// Structured global storage marker revision.
	pub storage_marker: u16,
	/// Upstream storage major protected by the fork-required high bit.
	pub upstream_storage_major: u16,
	/// Fork-required storage major rejected by vanilla and older binaries.
	pub fork_storage_major: u16,
	/// Quota policy catalog format revision.
	pub quota_catalog: u16,
	/// Quota usage ledger format revision.
	pub quota_usage: u16,
}

/// CLI release matching rules.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CliManifest {
	/// CLI release paired with this manifest.
	pub release: String,
	/// Whether destructive operations require an exact release match.
	pub requires_exact_release_for_destructive_operations: bool,
}

/// SDK/protocol combinations certified by downstream contract tests.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SdkManifest {
	/// Exact surrealdb-js releases certified by this manifest.
	pub surrealdb_js: Vec<String>,
	/// Protocol paths covered by the release contract.
	pub protocols: Vec<String>,
}

/// Minimum capability and CLI needed to migrate into this format.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MigrationManifest {
	/// Required engine capability.
	pub capability: String,
	/// Oldest matching CLI able to perform the migration.
	pub minimum_cli_release: String,
}

/// Immutable release artifacts and cross-repository promotion rules.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReleaseSupplyChainManifest {
	/// Candidate release manifest format major.
	pub candidate_manifest_format: u16,
	/// Stable candidate image repository owned by the fork.
	pub image_repository: String,
	/// Isolated image repository for non-promotable nightly builds.
	pub nightly_image_repository: String,
	/// Protected stable release branch for this release.
	pub stable_branch: String,
	/// Production deployment reference policy.
	pub production_reference: String,
	/// Required immutable OCI tag identities.
	pub immutable_tag_kinds: Vec<String>,
	/// Ordered promotion environments.
	pub promotion_environments: Vec<String>,
	/// Evidence required before production promotion.
	pub required_attestations: Vec<String>,
	/// Downstream repository authorized to accept a candidate.
	pub downstream_repository: String,
	/// Downstream workflow whose keyless identity signs acceptance.
	pub downstream_acceptance_workflow: String,
}

/// Mixed-version and rollback safety contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MixedVersionManifest {
	/// Cluster compatibility policy.
	pub policy: String,
	/// Fork releases allowed in one cluster during a rollout.
	pub compatible_fork_releases: Vec<String>,
	/// Whether an upstream vanilla binary may open this datastore.
	pub vanilla_binary_allowed: bool,
	/// Whether an older fork binary may open this datastore.
	pub older_fork_binary_allowed: bool,
	/// Whether a migrated data format can be downgraded.
	pub data_format_downgrade_supported: bool,
	/// Whether process rollback must retain a format-compatible release.
	pub process_rollback_requires_same_release_format: bool,
}

/// Maintenance window for the previously deployed production line.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SupportManifest {
	/// Minimum support window after a new production line is promoted.
	pub previous_production_line_days: u16,
}

/// One backend's hard-quota certification.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackendContract {
	/// Stable backend identifier with no endpoint or path.
	pub name: String,
	/// Whether transactional hard-quota semantics passed the release contract.
	pub hard_quota_certified: bool,
	/// Whether this backend is approved for production by this release.
	pub production: bool,
	/// Whether durable close/reopen and interrupted rebuild recovery passed.
	pub persistent_restart_certified: bool,
	/// Immutable contract suite revision, absent for uncertified backends.
	pub certification_revision: Option<String>,
	/// Named backend-neutral contracts executed for this backend.
	pub contract_suite: Vec<String>,
	/// Network fault coverage or the reason it does not apply.
	pub network_fault_model: String,
}

/// Fixed benchmark dataset.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PerformanceDataset {
	/// Operations excluded from samples while caches warm.
	pub warmup_operations: u32,
	/// Measured operations per workload.
	pub sample_operations: u32,
	/// Logical records in the batch workload.
	pub batch_size: u32,
}

/// Maximum accepted regression percentages against a matching baseline.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PerformanceThresholds {
	/// Maximum throughput reduction.
	pub maximum_throughput_regression: u16,
	/// Maximum p95 latency increase.
	pub p95_latency_regression: u16,
	/// Maximum p99 latency increase.
	pub p99_latency_regression: u16,
	/// Maximum KV writes-per-resource increase.
	pub kv_write_amplification_regression: u16,
	/// Maximum persisted bytes-per-resource increase.
	pub storage_growth_regression: u16,
}

/// Release benchmark identity, dataset, command, and explicit gates.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PerformanceManifest {
	/// Benchmark schema and workload revision.
	pub benchmark_revision: String,
	/// Fixed workload sizes.
	pub dataset: PerformanceDataset,
	/// Expected baseline artifact file name.
	pub baseline_artifact: String,
	/// Command that creates a baseline artifact on the pinned runner.
	pub baseline_command: String,
	/// Command that reproduces a candidate artifact without comparison.
	pub repeatable_command: String,
	/// Command that compares a candidate with the release baseline.
	pub candidate_command: String,
	/// Release regression gates.
	pub thresholds_percent: PerformanceThresholds,
}

/// Machine-readable release compatibility manifest embedded in the server and CLI.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompatibilityManifest {
	/// Manifest document major.
	pub format_version: u16,
	/// Immutable compatibility manifest revision.
	pub manifest_revision: String,
	/// Stable private fork identity.
	pub fork_id: String,
	/// Fork release line.
	pub fork_release: String,
	/// Capability names implemented by this release.
	pub capabilities: Vec<String>,
	/// Public query/error contracts.
	pub contracts: ContractManifest,
	/// Protected storage/catalog formats.
	pub formats: FormatManifest,
	/// Matching CLI contract.
	pub cli: CliManifest,
	/// Certified SDK/protocol combinations.
	pub sdk: SdkManifest,
	/// Datastore migration compatibility.
	pub migration: MigrationManifest,
	/// Candidate build, signing, and promotion contract.
	pub release_supply_chain: ReleaseSupplyChainManifest,
	/// Mixed-version and rollback contract.
	pub mixed_version: MixedVersionManifest,
	/// Previous production line maintenance contract.
	pub support: SupportManifest,
	/// Backend certification allowlist.
	pub backends: Vec<BackendContract>,
	/// Reproducible benchmark and regression-gate contract.
	pub performance: PerformanceManifest,
	/// Static metadata copied into OCI image labels by release automation.
	pub oci_labels: BTreeMap<String, String>,
}

impl CompatibilityManifest {
	/// Parse and validate the manifest embedded at build time.
	pub fn embedded() -> Result<Self> {
		let manifest: Self = serde_json::from_str(EMBEDDED_MANIFEST)?;
		manifest.validate()?;
		Ok(manifest)
	}

	/// Validate stable majors, fork identity, and local format constants.
	pub fn validate(&self) -> Result<()> {
		let (fork_id, release, _) = native_quota_fork_contract();
		let (storage_marker, upstream_storage_major, quota_catalog, quota_usage) =
			native_quota_format_contract();
		if self.format_version != CAPABILITY_FORMAT_VERSION {
			bail!("unsupported native quota compatibility manifest major {}", self.format_version);
		}
		if self.fork_id != fork_id || self.fork_release != release {
			bail!("native quota compatibility manifest fork identity does not match this binary");
		}
		if self.manifest_revision.contains("-dev") {
			bail!("native quota release compatibility manifest revision must be immutable");
		}
		if !self.capabilities.iter().any(|item| item == NATIVE_QUOTA_CAPABILITY) {
			bail!("native quota compatibility manifest is missing {NATIVE_QUOTA_CAPABILITY}");
		}
		if self.contracts.quota_major != 1
			|| self.contracts.info_format_version != NATIVE_QUOTA_INFO_FORMAT_VERSION
			|| self.contracts.error_format_version != NATIVE_QUOTA_ERROR_FORMAT_VERSION
			|| self.contracts.error_wire_code != -32010
		{
			bail!("native quota compatibility manifest contract versions do not match this binary");
		}
		if self.formats.storage_marker != storage_marker
			|| self.formats.upstream_storage_major != upstream_storage_major
			|| self.formats.fork_storage_major != native_quota_storage_version_contract()
			|| self.formats.quota_catalog != quota_catalog
			|| self.formats.quota_usage != quota_usage
		{
			bail!("native quota compatibility manifest storage formats do not match this binary");
		}
		if self.cli.release != release {
			bail!("native quota compatibility manifest CLI release does not match this binary");
		}
		if self.migration.capability != NATIVE_QUOTA_CAPABILITY
			|| self.migration.minimum_cli_release != release
		{
			bail!("native quota migration manifest does not match this binary");
		}
		let supply_chain = &self.release_supply_chain;
		if supply_chain.candidate_manifest_format != 1
			|| supply_chain.image_repository != "ghcr.io/mainlistactivity/surrealdb-native-quota"
			|| supply_chain.nightly_image_repository
				!= "ghcr.io/mainlistactivity/surrealdb-native-quota-nightly"
			|| supply_chain.image_repository == supply_chain.nightly_image_repository
			|| supply_chain.image_repository.contains("surrealdb/surrealdb")
			|| supply_chain.nightly_image_repository.contains("surrealdb/surrealdb")
			|| supply_chain.stable_branch != "releases/sck-3.3"
			|| supply_chain.production_reference != "digest-only"
			|| supply_chain.promotion_environments != ["canary", "staging", "production"]
			|| supply_chain.downstream_repository != "MainListActivity/surreal_ck"
			|| supply_chain.downstream_acceptance_workflow
				!= ".github/workflows/native-quota-release-acceptance.yml"
		{
			bail!("native quota release supply-chain contract is invalid");
		}
		for tag in ["release", "git-sha"] {
			if !supply_chain.immutable_tag_kinds.iter().any(|item| item == tag) {
				bail!("native quota release supply chain is missing immutable '{tag}' tag");
			}
		}
		for evidence in [
			"signature",
			"spdx-sbom",
			"slsa-provenance",
			"vulnerability-report",
			"surreal-ck-acceptance",
		] {
			if !supply_chain.required_attestations.iter().any(|item| item == evidence) {
				bail!("native quota release supply chain is missing '{evidence}' evidence");
			}
		}
		if self.mixed_version.policy != "exact-release-only"
			|| !self.mixed_version.compatible_fork_releases.iter().any(|item| item == release)
			|| self.mixed_version.vanilla_binary_allowed
			|| self.mixed_version.older_fork_binary_allowed
			|| self.mixed_version.data_format_downgrade_supported
			|| !self.mixed_version.process_rollback_requires_same_release_format
		{
			bail!("native quota mixed-version and rollback contract is invalid");
		}
		if self.support.previous_production_line_days < 90 {
			bail!("native quota previous production line support must be at least 90 days");
		}
		if !self.sdk.protocols.iter().any(|protocol| protocol == "http")
			|| !self.sdk.protocols.iter().any(|protocol| protocol == "ws")
		{
			bail!("native quota SDK manifest must cover HTTP and WebSocket protocols");
		}
		if self.sdk.surrealdb_js.is_empty()
			|| self.sdk.surrealdb_js.iter().any(|release| semver::Version::parse(release).is_err())
		{
			bail!("native quota SDK manifest must pin at least one exact surrealdb-js release");
		}
		let current_release = semver::Version::parse(&self.fork_release)?;
		let minimum_release = semver::Version::parse(&self.migration.minimum_cli_release)?;
		if minimum_release > current_release {
			bail!("native quota migration CLI range starts after the fork release");
		}
		let mut backend_names = BTreeSet::new();
		if self.backends.iter().any(|backend| !backend_names.insert(&backend.name)) {
			bail!("native quota backend allowlist contains duplicate names");
		}
		let required_suite = [
			"transaction-contention",
			"conflict-retry",
			"multi-node",
			"atomic-fault-injection",
			"commit-outcome-unknown",
			"policy-generation-race",
			"rebuild-epoch",
		];
		for backend in &self.backends {
			if backend.production
				&& (!backend.hard_quota_certified
					|| !backend.persistent_restart_certified
					|| backend.name == "memory"
					|| backend.certification_revision.as_deref()
						!= Some("native-quota-contract-v1")
					|| backend.network_fault_model == "pending"
					|| required_suite.iter().any(|required| {
						!backend.contract_suite.iter().any(|item| item == required)
					})) {
				bail!(
					"production native quota backend '{}' lacks complete certification evidence",
					backend.name
				);
			}
		}
		if !self.backends.iter().any(|backend| backend.production) {
			bail!("native quota stable release requires a production-certified persistent backend");
		}
		if self.performance.dataset.warmup_operations == 0
			|| self.performance.dataset.sample_operations == 0
			|| self.performance.dataset.batch_size == 0
			|| self.performance.benchmark_revision.is_empty()
			|| self.performance.baseline_artifact.is_empty()
			|| self.performance.baseline_command.is_empty()
			|| self.performance.repeatable_command.is_empty()
			|| self.performance.candidate_command.is_empty()
		{
			bail!("native quota performance manifest is incomplete");
		}
		Ok(())
	}

	fn backend(&self, name: &str) -> BackendContract {
		self.backends.iter().find(|item| item.name == name).cloned().unwrap_or_else(|| {
			BackendContract {
				name: name.to_owned(),
				hard_quota_certified: false,
				production: false,
				persistent_restart_certified: false,
				certification_revision: None,
				contract_suite: Vec::new(),
				network_fault_model: "unknown".to_owned(),
			}
		})
	}
}

/// Runtime build identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BuildCapability {
	/// Existing upstream-compatible engine version.
	pub engine_version: String,
	/// Build-time source revision, or `unknown` for local developer builds.
	pub git_sha: String,
}

/// Runtime fork identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ForkCapability {
	/// Stable fork identifier.
	pub id: String,
	/// Fork release line.
	pub release: String,
}

/// Native quota feature contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct QuotaCapability {
	/// Required capability name.
	pub name: String,
	/// Contract major.
	pub contract_major: u16,
	/// Resources enforced by the first release.
	pub resources: Vec<String>,
}

/// Stable INFO response contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InfoCapability {
	/// `INFO FOR QUOTA ... STRUCTURE` document major.
	pub format_version: u16,
}

/// Stable structured quota error contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ErrorCapability {
	/// Structured error payload major.
	pub format_version: u16,
	/// Numeric SurrealDB wire error code.
	pub wire_code: i64,
}

/// Stable quota policy catalog contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CatalogCapability {
	/// Protected policy catalog format revision.
	pub format_revision: u16,
}

/// Stable quota usage ledger contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct UsageCapability {
	/// Protected usage ledger format revision.
	pub format_revision: u16,
}

/// Stable, unauthenticated `GET /capabilities` response.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CapabilityDocument {
	/// Capability document major.
	pub format_version: u16,
	/// Compatibility manifest revision used to build this binary.
	pub manifest_revision: String,
	/// Fork identity.
	pub fork: ForkCapability,
	/// Build identity.
	pub build: BuildCapability,
	/// Native quota feature contract.
	pub quota: QuotaCapability,
	/// INFO DTO contract.
	pub info: InfoCapability,
	/// Structured error contract.
	pub error: ErrorCapability,
	/// Runtime protected datastore status.
	pub storage: NativeQuotaStorageStatus,
	/// Quota catalog contract.
	pub catalog: CatalogCapability,
	/// Quota usage contract.
	pub usage: UsageCapability,
	/// Current backend certification.
	pub backend: BackendContract,
	/// Matching CLI release.
	pub cli: CliManifest,
}

impl CapabilityDocument {
	/// Construct the runtime document from the embedded manifest and redacted storage status.
	pub fn current(storage: NativeQuotaStorageStatus) -> Result<Self> {
		let manifest = CompatibilityManifest::embedded()?;
		let backend = manifest.backend(&storage.backend);
		let document = Self {
			format_version: CAPABILITY_FORMAT_VERSION,
			manifest_revision: manifest.manifest_revision,
			fork: ForkCapability {
				id: manifest.fork_id,
				release: manifest.fork_release,
			},
			build: BuildCapability {
				engine_version: PKG_VERSION.clone(),
				git_sha: option_env!("SURREAL_BUILD_GIT_SHA").unwrap_or("unknown").to_owned(),
			},
			quota: QuotaCapability {
				name: NATIVE_QUOTA_CAPABILITY.to_owned(),
				contract_major: manifest.contracts.quota_major,
				resources: vec!["table".to_owned(), "field".to_owned(), "record".to_owned()],
			},
			info: InfoCapability {
				format_version: manifest.contracts.info_format_version,
			},
			error: ErrorCapability {
				format_version: manifest.contracts.error_format_version,
				wire_code: manifest.contracts.error_wire_code,
			},
			storage,
			catalog: CatalogCapability {
				format_revision: manifest.formats.quota_catalog,
			},
			usage: UsageCapability {
				format_revision: manifest.formats.quota_usage,
			},
			backend,
			cli: manifest.cli,
		};
		document.validate_contract()?;
		Ok(document)
	}

	/// Fail closed unless all requested capabilities and runtime contracts are ready.
	pub fn require(&self, required: &[String]) -> Result<()> {
		self.validate_contract()?;
		for capability in required {
			if capability != NATIVE_QUOTA_CAPABILITY || self.quota.name != *capability {
				bail!("required capability '{capability}' is unavailable");
			}
		}
		if required.iter().any(|item| item == NATIVE_QUOTA_CAPABILITY)
			&& (!self.storage.ready || !self.backend.hard_quota_certified)
		{
			bail!("native quota runtime contract is not ready");
		}
		Ok(())
	}

	fn validate_contract(&self) -> Result<()> {
		let manifest = CompatibilityManifest::embedded()?;
		if self.format_version != CAPABILITY_FORMAT_VERSION {
			bail!("unsupported capability document major {}", self.format_version);
		}
		let (fork_id, release, _) = native_quota_fork_contract();
		if self.fork.id != fork_id || self.fork.release != release {
			bail!("capability document fork identity does not match this binary");
		}
		if self.manifest_revision != manifest.manifest_revision
			|| self.quota.name != NATIVE_QUOTA_CAPABILITY
			|| self.quota.contract_major != manifest.contracts.quota_major
			|| self.info.format_version != manifest.contracts.info_format_version
			|| self.error.format_version != manifest.contracts.error_format_version
			|| self.error.wire_code != manifest.contracts.error_wire_code
			|| self.catalog.format_revision != manifest.formats.quota_catalog
			|| self.usage.format_revision != manifest.formats.quota_usage
			|| self.storage.format_version != NATIVE_QUOTA_STORAGE_STATUS_FORMAT_VERSION
			|| self.cli != manifest.cli
		{
			bail!("capability document contract versions do not match this binary");
		}
		for resource in ["table", "field", "record"] {
			if !self.quota.resources.iter().any(|item| item == resource) {
				bail!("capability document is missing quota resource '{resource}'");
			}
		}
		let expected_backend = manifest.backend(&self.storage.backend);
		if self.backend != expected_backend {
			bail!("capability document backend certification does not match the allowlist");
		}
		let (expected_ready, expected_migration_required) = match self.storage.state {
			NativeQuotaStorageState::Empty => (false, false),
			NativeQuotaStorageState::LegacyUnversioned
			| NativeQuotaStorageState::MigrationRequired
			| NativeQuotaStorageState::Migrating => (false, true),
			NativeQuotaStorageState::Ready => (true, false),
		};
		if self.storage.ready != expected_ready
			|| self.storage.migration_required != expected_migration_required
		{
			bail!("capability document storage lifecycle flags are inconsistent");
		}
		match self.storage.state {
			NativeQuotaStorageState::Empty | NativeQuotaStorageState::LegacyUnversioned => {
				if self.storage.storage_version.is_some() || self.storage.marker.is_some() {
					bail!("unversioned native quota datastore reported protected format metadata");
				}
			}
			NativeQuotaStorageState::Migrating => {
				self.validate_protected_storage(&manifest, "in_progress")?;
			}
			NativeQuotaStorageState::Ready => {
				self.validate_protected_storage(&manifest, "clean")?;
			}
			NativeQuotaStorageState::MigrationRequired => {}
		}
		Ok(())
	}

	fn validate_protected_storage(
		&self,
		manifest: &CompatibilityManifest,
		migration_state: &str,
	) -> Result<()> {
		let marker = self.storage.marker.as_ref().ok_or_else(|| {
			anyhow::anyhow!("native quota datastore is missing its protected format marker")
		})?;
		let (_, _, minimum_release) = native_quota_fork_contract();
		if self.storage.storage_version != Some(manifest.formats.fork_storage_major)
			|| marker.format_revision != manifest.formats.storage_marker
			|| marker.fork_id != manifest.fork_id
			|| marker.upstream_storage_major != manifest.formats.upstream_storage_major
			|| marker.quota_policy_format_revision != manifest.formats.quota_catalog
			|| marker.quota_usage_format_revision != manifest.formats.quota_usage
			|| marker.minimum_compatible_fork_release != minimum_release
			|| marker.migration_state != migration_state
		{
			bail!("protected native quota datastore format does not match this binary");
		}
		Ok(())
	}

	/// Require the exact CLI/fork release before a destructive management operation.
	pub fn require_matching_cli(&self) -> Result<()> {
		self.require(&[NATIVE_QUOTA_CAPABILITY.to_owned()])?;
		let (_, release, _) = native_quota_fork_contract();
		if self.cli.requires_exact_release_for_destructive_operations
			&& (self.cli.release != release || self.fork.release != release)
		{
			bail!("server and CLI native quota releases do not match");
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use serde_json::{Value, json};
	use surrealdb_core::kvs::{
		NativeQuotaStorageMarker, NativeQuotaStorageState, NativeQuotaStorageStatus,
		native_quota_storage_version_contract,
	};

	use super::*;

	fn ready_storage(backend: &str) -> NativeQuotaStorageStatus {
		let (fork_id, _, minimum_release) = native_quota_fork_contract();
		let (storage_marker, upstream_storage_major, quota_catalog, quota_usage) =
			native_quota_format_contract();
		NativeQuotaStorageStatus {
			format_version: 1,
			backend: backend.to_owned(),
			storage_version: Some(native_quota_storage_version_contract()),
			state: NativeQuotaStorageState::Ready,
			ready: true,
			migration_required: false,
			marker: Some(NativeQuotaStorageMarker {
				format_revision: storage_marker,
				fork_id: fork_id.to_owned(),
				upstream_storage_major,
				quota_policy_format_revision: quota_catalog,
				quota_usage_format_revision: quota_usage,
				minimum_compatible_fork_release: minimum_release.to_owned(),
				migration_state: "clean".to_owned(),
			}),
		}
	}

	#[test]
	fn embedded_manifest_matches_binary_contract_and_oci_metadata() {
		let manifest = CompatibilityManifest::embedded().unwrap();
		assert_eq!(manifest.format_version, 1);
		assert_eq!(
			manifest.oci_labels.get("io.mainlistactivity.surrealdb.quota-contract"),
			Some(&NATIVE_QUOTA_CAPABILITY.to_owned())
		);
		assert_eq!(
			manifest.oci_labels.get("io.mainlistactivity.surrealdb.manifest-revision"),
			Some(&manifest.manifest_revision)
		);
	}

	#[test]
	fn unknown_fields_are_ignored_but_unknown_major_fails_closed() {
		let mut value: Value = serde_json::from_str(EMBEDDED_MANIFEST).unwrap();
		value.as_object_mut().unwrap().insert(
			"future_field".to_owned(),
			json!({
				"safe": true
			}),
		);
		let parsed: CompatibilityManifest = serde_json::from_value(value.clone()).unwrap();
		parsed.validate().unwrap();

		value["format_version"] = json!(2);
		let parsed: CompatibilityManifest = serde_json::from_value(value).unwrap();
		assert!(parsed.validate().unwrap_err().to_string().contains("major"));
	}

	#[test]
	fn readiness_requires_storage_and_backend_certification() {
		let memory = CapabilityDocument::current(ready_storage("memory")).unwrap();
		memory.require(&[NATIVE_QUOTA_CAPABILITY.to_owned()]).unwrap();

		let rocks = CapabilityDocument::current(ready_storage("rocksdb")).unwrap();
		rocks.require(&[NATIVE_QUOTA_CAPABILITY.to_owned()]).unwrap();
		assert!(rocks.backend.production);
		assert!(rocks.backend.persistent_restart_certified);

		let mut migrating = memory;
		migrating.storage.ready = false;
		migrating.storage.migration_required = true;
		migrating.storage.state = NativeQuotaStorageState::Migrating;
		migrating.storage.marker.as_mut().unwrap().migration_state = "in_progress".to_owned();
		assert!(migrating.require(&[NATIVE_QUOTA_CAPABILITY.to_owned()]).is_err());
	}

	#[test]
	fn production_backend_and_performance_evidence_fail_closed() {
		let mut manifest = CompatibilityManifest::embedded().unwrap();
		let rocks = manifest.backends.iter_mut().find(|backend| backend.name == "rocksdb").unwrap();
		rocks.persistent_restart_certified = false;
		assert!(manifest.validate().unwrap_err().to_string().contains("certification evidence"));

		let mut manifest = CompatibilityManifest::embedded().unwrap();
		for backend in &mut manifest.backends {
			backend.production = false;
		}
		assert!(manifest.validate().unwrap_err().to_string().contains("production-certified"));

		let mut manifest = CompatibilityManifest::embedded().unwrap();
		manifest.performance.dataset.sample_operations = 0;
		assert!(manifest.validate().unwrap_err().to_string().contains("performance manifest"));
	}

	#[test]
	fn release_supply_chain_and_rollback_contract_fail_closed() {
		let mut manifest = CompatibilityManifest::embedded().unwrap();
		manifest.release_supply_chain.production_reference = "tag".to_owned();
		assert!(manifest.validate().unwrap_err().to_string().contains("supply-chain"));

		let mut manifest = CompatibilityManifest::embedded().unwrap();
		manifest.mixed_version.data_format_downgrade_supported = true;
		assert!(manifest.validate().unwrap_err().to_string().contains("mixed-version"));

		let mut manifest = CompatibilityManifest::embedded().unwrap();
		manifest.support.previous_production_line_days = 89;
		assert!(manifest.validate().unwrap_err().to_string().contains("90 days"));
	}

	#[test]
	fn capability_contract_and_exact_cli_release_fail_closed() {
		let mut manifest = CompatibilityManifest::embedded().unwrap();
		manifest.capabilities.clear();
		assert!(manifest.validate().unwrap_err().to_string().contains(NATIVE_QUOTA_CAPABILITY));

		let mut document = CapabilityDocument::current(ready_storage("memory")).unwrap();
		document.info.format_version += 1;
		assert!(document.require(&[NATIVE_QUOTA_CAPABILITY.to_owned()]).is_err());

		let mut document = CapabilityDocument::current(ready_storage("memory")).unwrap();
		document.backend.hard_quota_certified = false;
		assert!(document.require(&[NATIVE_QUOTA_CAPABILITY.to_owned()]).is_err());

		let mut document = CapabilityDocument::current(ready_storage("memory")).unwrap();
		document.cli.release = "3.3.0-native-quota.0".to_owned();
		assert!(document.require_matching_cli().is_err());
	}

	#[test]
	fn public_document_contains_no_tenant_or_secret_material() {
		let document = CapabilityDocument::current(ready_storage("memory")).unwrap();
		let value = serde_json::to_value(&document).unwrap();
		let forbidden = [
			"password",
			"credential",
			"credentials",
			"policy",
			"policies",
			"rules",
			"database",
			"databases",
			"namespace",
			"namespaces",
			"endpoint",
			"path",
		];
		fn assert_redacted(value: &Value, forbidden: &[&str]) {
			match value {
				Value::Object(object) => {
					for (key, value) in object {
						assert!(
							!forbidden.contains(&key.as_str()),
							"capability leaked forbidden field {key}"
						);
						assert_redacted(value, forbidden);
					}
				}
				Value::Array(values) => {
					for value in values {
						assert_redacted(value, forbidden);
					}
				}
				_ => {}
			}
		}
		assert_redacted(&value, &forbidden);
	}
}
