//! A focused integration entry point for the same quota write-path contract
//! exercised by core's unit suite. This avoids compiling every unrelated
//! upstream unit test when validating packed/raw/held/batched admission.
#![cfg(feature = "kv-mem")]

use surrealdb_catalog as catalog;
use surrealdb_core::{dbs, kvs};
use surrealdb_datastore::key;
use surrealdb_expr::val;

#[path = "../src/kvs/tests/quota_write_paths_test.rs"]
mod quota_write_paths;
