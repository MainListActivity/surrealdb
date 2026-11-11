//! Private owned-key compatibility layer for the extracted RocksDB engine.
pub(crate) mod api;
pub(crate) mod batch;
pub(crate) mod cursor;
pub(crate) mod direction;
pub(crate) mod err;
pub(crate) mod rocksdb;
pub(crate) mod util;
pub(crate) use direction::Direction;
pub(crate) use err::{Error, Result};
pub(crate) use surrealdb_kvs::consts::*;
pub(crate) use surrealdb_kvs::timestamp::*;
pub(crate) type Key = Vec<u8>;
pub(crate) type Val = Vec<u8>;
pub(crate) mod timestamp {
	pub(crate) use surrealdb_kvs::timestamp::*;
}
pub(crate) mod config {
	pub(crate) use surrealdb_cnf::parse_duration;
	pub(crate) use surrealdb_kvs::config::SyncMode;
}
pub(crate) mod ds {
	pub(crate) use surrealdb_kvs::{Metric, Metrics};
}
