//! RocksDB adapter for the v4 storage API, retaining the fork's engine internals.
//!
//! The private compatibility layer keeps owned-key and iterator lifetimes intact.
//! Only this adapter is exposed to the datastore. This source port is not certified.
#![allow(clippy::absolute_paths)]

#[macro_use]
extern crate tracing;

mod adapter;
mod key;
mod kvs;

pub use adapter::{Datastore, RocksDbProvider};
pub use kvs::rocksdb::RocksDbConfig;
