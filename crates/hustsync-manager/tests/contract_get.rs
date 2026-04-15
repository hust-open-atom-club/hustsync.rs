//! Trivial GET contract tests for hustsync-manager.
//!
//! Each test drives the in-process router via `tower::ServiceExt::oneshot`
//! and compares the response body against a committed fixture. No real
//! socket is bound; no external process is started.
//!
//! Fixture path convention: `tests/fixtures/http/<endpoint>/response.json`.
//!
//! Empty-list intentional divergence from Go:
//! Go serialises a nil slice as JSON `null`; Rust serialises an empty Vec as
//! `[]`. The `GET /jobs` and `GET /workers` fixtures contain `[]`, which is
//! the Rust-defined contract. Clients should treat both as "no items".

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod contract;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// GET /ping
// ---------------------------------------------------------------------------

/// GET /ping returns 200 with `{"message": "pong"}`.
///
/// The key is `"message"` (Go's `_infoKey` constant), not `"pong"`.
/// See.
#[tokio::test]
async fn get_ping_returns_pong() {
    let (app, _dir) = contract::spawn_manager();

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/ping")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    let got = contract::body_json(resp).await;
    let want = contract::load_fixture("ping/response.json");
    contract::assert_json_eq_masked(&got, &want, &[]);
}

// ---------------------------------------------------------------------------
// GET /jobs
// ---------------------------------------------------------------------------

/// GET /jobs on a fresh manager (no mirrors registered) returns 200 + `[]`.
///
/// Go would return `null` for a nil slice; Rust returns `[]` for an empty Vec.
/// This divergence is intentional — Go emits `null`, Rust emits `[]`.
#[tokio::test]
async fn get_jobs_empty_returns_empty_array() {
    let (app, _dir) = contract::spawn_manager();

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/jobs")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    let got = contract::body_json(resp).await;
    let want = contract::load_fixture("jobs/response.json");
    contract::assert_json_eq_masked(&got, &want, &[]);
}

// ---------------------------------------------------------------------------
// DELETE /jobs/disabled
// ---------------------------------------------------------------------------

/// DELETE /jobs/disabled on a fresh manager returns 200 + `{"message": "flushed"}`.
///
/// There are no disabled jobs to flush, but the handler must still succeed and
/// return the canonical Go-compatible shape ().
#[tokio::test]
async fn delete_jobs_disabled_returns_flushed() {
    let (app, _dir) = contract::spawn_manager();

    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/jobs/disabled")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    let got = contract::body_json(resp).await;
    let want = contract::load_fixture("jobs_disabled/response.json");
    contract::assert_json_eq_masked(&got, &want, &[]);
}

// ---------------------------------------------------------------------------
// GET /workers
// ---------------------------------------------------------------------------

/// GET /workers on a fresh manager (no workers registered) returns 200 + `[]`.
///
/// Go would return `null` for a nil slice; Rust returns `[]` for an empty Vec.
/// This divergence is intentional — Go emits `null`, Rust emits `[]`.
#[tokio::test]
async fn get_workers_empty_returns_empty_array() {
    let (app, _dir) = contract::spawn_manager();

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/workers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    let got = contract::body_json(resp).await;
    let want = contract::load_fixture("workers/response.json");
    contract::assert_json_eq_masked(&got, &want, &[]);
}
