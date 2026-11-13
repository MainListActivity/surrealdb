//! Synthetic-only public migration contracts, shared with the unit suite.
//! A focused target avoids compiling every upstream unit test.
#![cfg(feature = "kv-mem")]

use surrealdb_catalog as catalog;
use surrealdb_core::{dbs, kvs};
use surrealdb_datastore::key;

#[path = "../src/kvs/tests/storage_migration_test.rs"]
mod storage_migration;
