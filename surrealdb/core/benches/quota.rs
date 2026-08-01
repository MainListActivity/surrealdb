//! Reproducible RocksDB native-quota release benchmark.

#![allow(clippy::unwrap_used)]
#![recursion_limit = "256"]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use surrealdb_core::CommunityComposer;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::observe::{ExecutionObserver, Outcome, TransactionEvent};
use temp_dir::TempDir;

const MANIFEST: &str = include_str!("../../../compatibility/native-quota-v1.json");

#[derive(Clone, Debug, Deserialize)]
struct Manifest {
	manifest_revision: String,
	performance: PerformanceManifest,
}

#[derive(Clone, Debug, Deserialize)]
struct PerformanceManifest {
	benchmark_revision: String,
	dataset: Dataset,
	thresholds_percent: Thresholds,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct Dataset {
	warmup_operations: u32,
	sample_operations: u32,
	batch_size: u32,
	measurement_repetitions: u16,
}

#[derive(Clone, Copy, Debug, Deserialize)]
struct Thresholds {
	maximum_throughput_regression: u16,
	p95_latency_regression: u16,
	p99_latency_regression: u16,
	kv_write_amplification_regression: u16,
	storage_growth_regression: u16,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BenchmarkReport {
	format_version: u16,
	benchmark_revision: String,
	manifest_revision: String,
	backend: String,
	os: String,
	arch: String,
	dataset: Dataset,
	workloads: Vec<WorkloadReport>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct WorkloadReport {
	name: String,
	operations: u32,
	logical_resources: u64,
	elapsed_ms: f64,
	throughput_resources_per_second: f64,
	p95_latency_us: f64,
	p99_latency_us: f64,
	kv_keys_written: u64,
	kv_bytes_written: u64,
	kv_writes_per_resource: f64,
	kv_bytes_per_resource: f64,
	storage_growth_bytes: u64,
	storage_bytes_per_resource: f64,
}

#[derive(Default)]
struct MetricsObserver {
	keys_written: AtomicU64,
	bytes_written: AtomicU64,
}

impl MetricsObserver {
	fn reset(&self) {
		self.keys_written.store(0, Ordering::Relaxed);
		self.bytes_written.store(0, Ordering::Relaxed);
	}

	fn snapshot(&self) -> (u64, u64) {
		(self.keys_written.load(Ordering::Relaxed), self.bytes_written.load(Ordering::Relaxed))
	}
}

impl ExecutionObserver for MetricsObserver {
	fn on_transaction_complete(&self, event: &TransactionEvent) {
		if event.safe.write && matches!(event.safe.outcome, Outcome::Success) {
			self.keys_written.fetch_add(event.safe.metrics.keys_written, Ordering::Relaxed);
			self.bytes_written.fetch_add(event.safe.metrics.total_bytes_written, Ordering::Relaxed);
		}
	}
}

#[derive(Clone, Copy)]
enum Workload {
	ContinuousMetering,
	FiniteExactPolicy,
	RegexMultiMatch,
	Batch,
}

impl Workload {
	fn name(self) -> &'static str {
		match self {
			Self::ContinuousMetering => "continuous-metering-no-policy",
			Self::FiniteExactPolicy => "finite-exact-policy",
			Self::RegexMultiMatch => "regex-multi-match",
			Self::Batch => "batch-write",
		}
	}
}

async fn execute(ds: &Datastore, sql: &str, session: &Session) {
	let responses = ds.execute(sql, session, None).await.unwrap();
	for response in responses {
		response.result.unwrap();
	}
}

async fn setup(
	workload: Workload,
	observer: Arc<MetricsObserver>,
) -> (Datastore, TempDir, Session) {
	let directory = TempDir::new().unwrap();
	let path = format!("rocksdb:{}", directory.path().to_string_lossy());
	let ds = Datastore::builder()
		.with_observer(observer)
		.build_with_factory_path(&path, CommunityComposer())
		.await
		.unwrap();
	let root = Session::owner();
	let namespace_owner = Session::owner().with_ns("quota_bench");
	let database_owner = Session::owner().with_ns("quota_bench").with_db("bench");
	execute(&ds, "DEFINE NAMESPACE quota_bench", &root).await;
	execute(&ds, "DEFINE DATABASE bench", &namespace_owner).await;
	match workload {
		Workload::ContinuousMetering => {}
		Workload::FiniteExactPolicy | Workload::Batch => {
			execute(
				&ds,
				"DEFINE QUOTA ON DATABASE bench \
				 RULE bench_records FOR RECORD MATCH EXACT ent_bench LIMIT 1000000",
				&namespace_owner,
			)
			.await;
		}
		Workload::RegexMultiMatch => {
			execute(
				&ds,
				"DEFINE QUOTA ON DATABASE bench \
				 RULE ent_records FOR RECORD MATCH REGEX /^ent_/ LIMIT 1000000 \
				 RULE bench_records FOR RECORD MATCH REGEX /_bench$/ LIMIT 1000000",
				&namespace_owner,
			)
			.await;
		}
	}
	execute(&ds, "DEFINE TABLE ent_bench", &database_owner).await;
	(ds, directory, database_owner)
}

fn operation_sql(workload: Workload, operation: u32, batch_size: u32) -> (String, u64) {
	if matches!(workload, Workload::Batch) {
		let values = (0..batch_size)
			.map(|offset| {
				format!(
					"{{ id: ent_bench:batch_{operation}_{offset}, payload: 'quota-benchmark' }}"
				)
			})
			.collect::<Vec<_>>()
			.join(",");
		(format!("INSERT INTO ent_bench [{values}]"), u64::from(batch_size))
	} else {
		(
			format!(
				"INSERT INTO ent_bench {{ id: ent_bench:record_{operation}, payload: 'quota-benchmark' }}"
			),
			1,
		)
	}
}

fn percentile(samples: &[Duration], percentile: f64) -> f64 {
	let rank = ((samples.len() - 1) as f64 * percentile).ceil() as usize;
	samples[rank].as_secs_f64() * 1_000_000.0
}

fn directory_size(path: &Path) -> u64 {
	let mut total = 0;
	let mut pending = vec![path.to_path_buf()];
	while let Some(path) = pending.pop() {
		for entry in fs::read_dir(path).unwrap() {
			let entry = entry.unwrap();
			let metadata = entry.metadata().unwrap();
			if metadata.is_dir() {
				pending.push(entry.path());
			} else {
				total += metadata.len();
			}
		}
	}
	total
}

async fn run_workload(workload: Workload, dataset: Dataset) -> WorkloadReport {
	let observer = Arc::new(MetricsObserver::default());
	let (ds, directory, session) = setup(workload, Arc::clone(&observer)).await;
	for operation in 0..dataset.warmup_operations {
		let (sql, _) = operation_sql(workload, operation, dataset.batch_size);
		execute(&ds, &sql, &session).await;
	}

	observer.reset();
	let storage_before = directory_size(directory.path());
	let start = Instant::now();
	let mut samples = Vec::with_capacity(dataset.sample_operations as usize);
	let mut logical_resources = 0;
	for operation in
		dataset.warmup_operations..dataset.warmup_operations + dataset.sample_operations
	{
		let (sql, resources) = operation_sql(workload, operation, dataset.batch_size);
		let operation_start = Instant::now();
		execute(&ds, &sql, &session).await;
		samples.push(operation_start.elapsed());
		logical_resources += resources;
	}
	let elapsed = start.elapsed();
	let (kv_keys_written, kv_bytes_written) = observer.snapshot();
	ds.shutdown().await.unwrap();
	drop(ds);
	let storage_after = directory_size(directory.path());
	samples.sort_unstable();

	let resources = logical_resources as f64;
	let storage_growth_bytes = storage_after.saturating_sub(storage_before);
	WorkloadReport {
		name: workload.name().to_owned(),
		operations: dataset.sample_operations,
		logical_resources,
		elapsed_ms: elapsed.as_secs_f64() * 1_000.0,
		throughput_resources_per_second: resources / elapsed.as_secs_f64(),
		p95_latency_us: percentile(&samples, 0.95),
		p99_latency_us: percentile(&samples, 0.99),
		kv_keys_written,
		kv_bytes_written,
		kv_writes_per_resource: kv_keys_written as f64 / resources,
		kv_bytes_per_resource: kv_bytes_written as f64 / resources,
		storage_growth_bytes,
		storage_bytes_per_resource: storage_growth_bytes as f64 / resources,
	}
}

fn maximum(baseline: f64, percent: u16) -> f64 {
	baseline * (1.0 + f64::from(percent) / 100.0)
}

fn minimum(baseline: f64, percent: u16) -> f64 {
	baseline * (1.0 - f64::from(percent) / 100.0)
}

fn median_f64(reports: &[WorkloadReport], value: impl Fn(&WorkloadReport) -> f64) -> f64 {
	let mut values = reports.iter().map(value).collect::<Vec<_>>();
	values.sort_by(f64::total_cmp);
	values[values.len() / 2]
}

fn median_u64(reports: &[WorkloadReport], value: impl Fn(&WorkloadReport) -> u64) -> u64 {
	let mut values = reports.iter().map(value).collect::<Vec<_>>();
	values.sort_unstable();
	values[values.len() / 2]
}

fn aggregate_measurements(reports: &[WorkloadReport]) -> WorkloadReport {
	let mut report = reports[0].clone();
	report.elapsed_ms = median_f64(reports, |report| report.elapsed_ms);
	report.throughput_resources_per_second =
		report.logical_resources as f64 / (report.elapsed_ms / 1_000.0);
	report.p95_latency_us = median_f64(reports, |report| report.p95_latency_us);
	report.p99_latency_us = median_f64(reports, |report| report.p99_latency_us);
	report.storage_growth_bytes = median_u64(reports, |report| report.storage_growth_bytes);
	report.storage_bytes_per_resource =
		report.storage_growth_bytes as f64 / report.logical_resources as f64;
	report
}

fn compare(candidate: &BenchmarkReport, baseline: &BenchmarkReport, thresholds: Thresholds) {
	assert_eq!(candidate.format_version, baseline.format_version, "benchmark format mismatch");
	assert_eq!(
		candidate.benchmark_revision, baseline.benchmark_revision,
		"benchmark revision mismatch"
	);
	assert_eq!(
		candidate.manifest_revision, baseline.manifest_revision,
		"compatibility manifest revision mismatch"
	);
	assert_eq!(candidate.backend, baseline.backend, "benchmark backend mismatch");
	assert_eq!(candidate.os, baseline.os, "benchmark operating system mismatch");
	assert_eq!(candidate.arch, baseline.arch, "benchmark architecture mismatch");
	assert_eq!(
		candidate.dataset.warmup_operations, baseline.dataset.warmup_operations,
		"benchmark warmup dataset mismatch"
	);
	assert_eq!(
		candidate.dataset.sample_operations, baseline.dataset.sample_operations,
		"benchmark sample dataset mismatch"
	);
	assert_eq!(
		candidate.dataset.batch_size, baseline.dataset.batch_size,
		"benchmark batch dataset mismatch"
	);
	assert_eq!(
		candidate.dataset.measurement_repetitions, baseline.dataset.measurement_repetitions,
		"benchmark repetition count mismatch"
	);
	assert_eq!(
		candidate.workloads.len(),
		baseline.workloads.len(),
		"benchmark workload count mismatch"
	);
	let candidate_control = candidate
		.workloads
		.iter()
		.find(|workload| workload.name == Workload::ContinuousMetering.name())
		.expect("candidate is missing the no-policy control workload");
	let baseline_control = baseline
		.workloads
		.iter()
		.find(|workload| workload.name == Workload::ContinuousMetering.name())
		.expect("baseline is missing the no-policy control workload");
	for candidate_workload in &candidate.workloads {
		let baseline_workload = baseline
			.workloads
			.iter()
			.find(|workload| workload.name == candidate_workload.name)
			.unwrap_or_else(|| panic!("baseline is missing workload {}", candidate_workload.name));
		let candidate_relative_throughput = candidate_workload.throughput_resources_per_second
			/ candidate_control.throughput_resources_per_second;
		let baseline_relative_throughput = baseline_workload.throughput_resources_per_second
			/ baseline_control.throughput_resources_per_second;
		assert!(
			candidate_relative_throughput
				>= minimum(baseline_relative_throughput, thresholds.maximum_throughput_regression,),
			"{} normalized throughput regressed beyond {}%",
			candidate_workload.name,
			thresholds.maximum_throughput_regression
		);
		let candidate_relative_p95 =
			candidate_workload.p95_latency_us / candidate_control.p95_latency_us;
		let baseline_relative_p95 =
			baseline_workload.p95_latency_us / baseline_control.p95_latency_us;
		assert!(
			candidate_relative_p95
				<= maximum(baseline_relative_p95.max(1.0), thresholds.p95_latency_regression),
			"{} normalized p95 latency regressed beyond {}%",
			candidate_workload.name,
			thresholds.p95_latency_regression
		);
		let candidate_relative_p99 =
			candidate_workload.p99_latency_us / candidate_control.p99_latency_us;
		let baseline_relative_p99 =
			baseline_workload.p99_latency_us / baseline_control.p99_latency_us;
		assert!(
			candidate_relative_p99
				<= maximum(baseline_relative_p99.max(1.0), thresholds.p99_latency_regression),
			"{} normalized p99 latency regressed beyond {}%",
			candidate_workload.name,
			thresholds.p99_latency_regression
		);
		assert!(
			candidate_workload.kv_writes_per_resource
				<= maximum(
					baseline_workload.kv_writes_per_resource,
					thresholds.kv_write_amplification_regression,
				),
			"{} KV write amplification regressed beyond {}%",
			candidate_workload.name,
			thresholds.kv_write_amplification_regression
		);
		assert!(
			candidate_workload.storage_bytes_per_resource
				<= maximum(
					baseline_workload.storage_bytes_per_resource,
					thresholds.storage_growth_regression,
				),
			"{} storage growth regressed beyond {}%",
			candidate_workload.name,
			thresholds.storage_growth_regression
		);
	}
}

fn arguments() -> (Option<PathBuf>, Option<PathBuf>) {
	let mut output = None;
	let mut baseline = None;
	let mut arguments = std::env::args().skip(1);
	while let Some(argument) = arguments.next() {
		match argument.as_str() {
			"--bench" => {}
			"--output" => output = Some(arguments.next().expect("--output requires a path").into()),
			"--baseline" => {
				baseline = Some(arguments.next().expect("--baseline requires a path").into());
			}
			other => panic!("unknown native quota benchmark argument: {other}"),
		}
	}
	(output, baseline)
}

#[tokio::main]
async fn main() {
	let manifest: Manifest = serde_json::from_str(MANIFEST).unwrap();
	let (output, baseline) = arguments();
	let mut workloads = Vec::new();
	for workload in [
		Workload::ContinuousMetering,
		Workload::FiniteExactPolicy,
		Workload::RegexMultiMatch,
		Workload::Batch,
	] {
		assert!(
			manifest.performance.dataset.measurement_repetitions > 0,
			"benchmark measurement repetitions must be positive"
		);
		let mut measurements =
			Vec::with_capacity(usize::from(manifest.performance.dataset.measurement_repetitions));
		for _ in 0..manifest.performance.dataset.measurement_repetitions {
			measurements.push(run_workload(workload, manifest.performance.dataset).await);
		}
		workloads.push(aggregate_measurements(&measurements));
	}
	let report = BenchmarkReport {
		format_version: 1,
		benchmark_revision: manifest.performance.benchmark_revision.clone(),
		manifest_revision: manifest.manifest_revision,
		backend: "rocksdb".to_owned(),
		os: std::env::consts::OS.to_owned(),
		arch: std::env::consts::ARCH.to_owned(),
		dataset: manifest.performance.dataset,
		workloads,
	};
	let json = serde_json::to_string_pretty(&report).unwrap();
	println!("{json}");
	if let Some(path) = output {
		if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
			fs::create_dir_all(parent).unwrap();
		}
		fs::write(path, format!("{json}\n")).unwrap();
	}
	if let Some(path) = baseline {
		let baseline = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
		compare(&report, &baseline, manifest.performance.thresholds_percent);
	}
}
