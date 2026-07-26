use std::env;
use std::process::Command;

const BUILD_GIT_SHA: &str = "SURREAL_BUILD_GIT_SHA";

fn main() {
	println!("cargo:rerun-if-env-changed={BUILD_GIT_SHA}");
	if let Some(git_sha) = build_git_sha() {
		println!("cargo:rustc-env={BUILD_GIT_SHA}={git_sha}");
	}
	if cfg!(target_family = "wasm") {
		println!("cargo:rustc-cfg=wasm");
		println!("cargo::rustc-check-cfg=cfg(wasm)");
	}
	if cfg!(any(
		feature = "storage-mem",
		feature = "storage-tikv",
		feature = "storage-rocksdb",
		feature = "storage-surrealkv",
	)) {
		println!("cargo:rustc-cfg=storage");
		println!("cargo::rustc-check-cfg=cfg(storage)");
	}
}

fn build_git_sha() -> Option<String> {
	let sha =
		env::var(BUILD_GIT_SHA).ok().filter(|value| !value.trim().is_empty()).or_else(|| {
			Command::new("git")
				.args(["rev-parse", "HEAD"])
				.output()
				.ok()
				.filter(|output| output.status.success())
				.and_then(|output| String::from_utf8(output.stdout).ok())
		})?;
	let sha = sha.trim();
	if sha.len() != 40 || !sha.bytes().all(|byte| byte.is_ascii_hexdigit()) {
		panic!("invalid {BUILD_GIT_SHA} `{sha}`: expected the full 40-character git SHA");
	}
	Some(sha.to_ascii_lowercase())
}
