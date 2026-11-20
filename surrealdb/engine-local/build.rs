fn main() {
	println!("cargo:rerun-if-changed=build.rs");
	println!("cargo::rustc-check-cfg=cfg(storage)");
	if cfg!(any(
		feature = "kv-mem",
		feature = "kv-surrealkv",
		feature = "kv-tikv",
		feature = "kv-rocksdb"
	)) {
		println!("cargo:rustc-cfg=storage");
	}
}
