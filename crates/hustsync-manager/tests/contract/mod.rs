//! Contract-test harness for hustsync-manager.
//!
//! Centralises router construction and assertion helpers so that every
//! contract-test file (trivial_gets, workers, cmd, tls, …) can `use
//! super::contract::*` and focus only on the HTTP assertions it owns.
//!
//! Design rationale:
//! - A single `spawn_manager()` prevents copy-paste router construction across
//!   N test files.  Any change to the Manager API only needs to be updated here.
//! - `assert_json_eq_masked` replaces volatile field values (timestamps,
//!   request IDs) with a deterministic placeholder before comparison, so those
//!   fields never become a flake source.
//! - Fixture files are loaded from the committed tree so that the expected JSON
//!   is versioned alongside the tests that assert it.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    dead_code
)]

use axum::Router;
use hustsync_config_parser::{ManagerConfig, ManagerFileConfig, ManagerServerConfig};
use hustsync_manager::Manager;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Router construction
// ---------------------------------------------------------------------------

/// Build an in-process `Router` backed by a fresh redb in a tempdir.
///
/// Callers drive the router via `tower::ServiceExt::oneshot` and never need
/// to bind a real socket, which keeps tests fast and port-collision-free.
/// The `TempDir` must be kept alive for the duration of the test; drop it
/// at the end of the test body.
pub fn spawn_manager() -> (Router, TempDir) {
    let dir = TempDir::new().expect("create tempdir");
    let db_path = dir.path().join("test.db");

    let config = Arc::new(ManagerConfig {
        debug: false,
        server: ManagerServerConfig {
            addr: "127.0.0.1".to_string(),
            port: 0,
            ssl_cert: String::new(),
            ssl_key: String::new(),
        },
        files: ManagerFileConfig {
            status_file: String::new(),
            db_type: "redb".to_string(),
            db_file: db_path.to_string_lossy().into_owned(),
            ca_cert: String::new(),
        },
    });

    let manager = Manager::new(config).expect("spawn_manager: Manager::new failed");
    let router = Arc::new(manager).make_router();

    (router, dir)
}

/// Placeholder for the TLS-capable spawn used by T9.
///
/// Binding an actual socket with a self-signed cert is deferred to T9.
/// This stub is declared here so downstream slices can `use` the symbol
/// without compilation errors; calling it panics until T9 fills it in.
#[allow(dead_code)]
pub fn spawn_manager_tls() -> ! {
    panic!("spawn_manager_tls is not yet implemented — see T9 in the M2 plan")
}

// ---------------------------------------------------------------------------
// Fixture loader
// ---------------------------------------------------------------------------

/// Load a JSON fixture relative to `tests/fixtures/http/`.
///
/// `rel` is a path like `"ping/response.json"`.  The function walks up from
/// the Cargo manifest dir so it works regardless of the working directory
/// the test runner chooses.
pub fn load_fixture(rel: &str) -> serde_json::Value {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .expect("CARGO_MANIFEST_DIR must be set by cargo test");
    let full = Path::new(&manifest_dir)
        .join("tests")
        .join("fixtures")
        .join("http")
        .join(rel);
    let bytes = std::fs::read(&full)
        .unwrap_or_else(|e| panic!("load_fixture: cannot read {}: {}", full.display(), e));
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|e| panic!("load_fixture: invalid JSON in {}: {}", full.display(), e))
}

// ---------------------------------------------------------------------------
// Masked JSON equality
// ---------------------------------------------------------------------------

/// Assert that `got` equals `want` after masking volatile fields.
///
/// For each field name in `masks` (e.g. `"last_online"`, `"last_register"`,
/// `"request_id"`), the helper finds every occurrence of `"<field>": <value>`
/// in the serialised JSON and replaces the value with the string `"__masked__"`
/// before comparing.  This makes timestamps and random IDs non-flaky without
/// hiding structural differences.
///
/// Both `got` and `want` are normalised through the same mask so that fixtures
/// can contain any placeholder for the masked fields.
pub fn assert_json_eq_masked(
    got: &serde_json::Value,
    want: &serde_json::Value,
    masks: &[&str],
) {
    let got_masked = mask_value(got, masks);
    let want_masked = mask_value(want, masks);

    if got_masked != want_masked {
        panic!(
            "JSON mismatch after masking {:?}.\n  got  = {}\n  want = {}",
            masks,
            serde_json::to_string_pretty(&got_masked).unwrap(),
            serde_json::to_string_pretty(&want_masked).unwrap(),
        );
    }
}

/// Recursively walk a `serde_json::Value` and replace the *values* of any
/// object key whose name appears in `masks` with `"__masked__"`.
fn mask_value(v: &serde_json::Value, masks: &[&str]) -> serde_json::Value {
    match v {
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, val) in map {
                if masks.contains(&k.as_str()) {
                    out.insert(k.clone(), serde_json::Value::String("__masked__".into()));
                } else {
                    out.insert(k.clone(), mask_value(val, masks));
                }
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(|e| mask_value(e, masks)).collect())
        }
        other => other.clone(),
    }
}

// ---------------------------------------------------------------------------
// HTTP helpers shared across test files
// ---------------------------------------------------------------------------

/// Convenience wrapper: read the full body of an axum response into a byte vector.
pub async fn body_bytes(resp: axum::response::Response) -> Vec<u8> {
    let b = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("body_bytes: failed to read body");
    b.to_vec()
}

/// Read the response body and parse it as a `serde_json::Value`.
pub async fn body_json(resp: axum::response::Response) -> serde_json::Value {
    let b = body_bytes(resp).await;
    serde_json::from_slice(&b).expect("body_json: response is not valid JSON")
}
