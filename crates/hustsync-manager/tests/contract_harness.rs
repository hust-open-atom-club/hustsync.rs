//! Smoke test for the contract-test harness.
//!
//! Verifies that `spawn_manager()` returns a working in-process router and
//! that the harness helpers compile and behave correctly. Downstream slices
//! (, , , ) build on these primitives without re-implementing them.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod contract;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// spawn_manager smoke test
// ---------------------------------------------------------------------------

/// Confirm that the harness produces a router that can handle basic HTTP.
///
/// `GET /ping` is the simplest endpoint and exercises the full middleware
/// stack without requiring any database state. 200 OK here means the Manager
/// initialised, the redb tempfile was created, and routing is wired up.
#[tokio::test]
async fn smoke_spawn_manager_ping_200() {
    let (app, _dir) = contract::spawn_manager();

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/ping")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "GET /ping must return 200 from a freshly spawned manager"
    );

    let json = contract::body_json(response).await;
    // The response must be a JSON object — shape compliance is asserted in .
    assert!(
        json.is_object(),
        "GET /ping must return a JSON object, got: {json}"
    );
}

// ---------------------------------------------------------------------------
// assert_json_eq_masked
// ---------------------------------------------------------------------------

/// Fields listed in the mask must not affect equality even when they differ.
#[test]
fn mask_suppresses_volatile_fields() {
    let got = serde_json::json!({
    "id": "worker-1",
    "last_online": "2026-04-15T12:00:00Z",
    "last_register": "2026-04-15T11:00:00Z"
    });
    let want = serde_json::json!({
    "id": "worker-1",
    "last_online": "1970-01-01T00:00:00Z",
    "last_register": "1970-01-01T00:00:00Z"
    });

    // Must not panic — the differing timestamps are masked out.
    contract::assert_json_eq_masked(&got, &want, &["last_online", "last_register"]);
}

/// Structural differences (wrong field value that is NOT masked) must still
/// cause a panic so real contract failures are caught.
#[test]
#[should_panic(expected = "JSON mismatch")]
fn mask_does_not_hide_structural_differences() {
    let got = serde_json::json!({ "id": "worker-A" });
    let want = serde_json::json!({ "id": "worker-B" });

    // "id" is not masked, so the mismatch must surface.
    contract::assert_json_eq_masked(&got, &want, &["last_online"]);
}

/// Masks inside arrays work recursively.
#[test]
fn mask_works_inside_arrays() {
    let got = serde_json::json!([
    { "name": "debian", "last_update": "2026-04-15T10:00:00Z" },
    { "name": "ubuntu", "last_update": "2026-04-15T10:01:00Z" }
    ]);
    let want = serde_json::json!([
    { "name": "debian", "last_update": "PLACEHOLDER" },
    { "name": "ubuntu", "last_update": "PLACEHOLDER" }
    ]);

    contract::assert_json_eq_masked(&got, &want, &["last_update"]);
}

// ---------------------------------------------------------------------------
// load_fixture (placeholder directory exists)
// ---------------------------------------------------------------------------

/// The fixtures/http directory must exist; this test will fail if it was
/// accidentally deleted, giving a clear signal before fixture-dependent +
/// tests hit confusing "file not found" panics.
#[test]
fn fixtures_http_directory_exists() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be set");
    let fixtures_path = std::path::Path::new(&manifest_dir)
        .join("tests")
        .join("fixtures")
        .join("http");
    assert!(
        fixtures_path.is_dir(),
        "tests/fixtures/http must exist as a directory (path: {})",
        fixtures_path.display()
    );
}
