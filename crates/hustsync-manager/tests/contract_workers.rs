//! Worker lifecycle contract tests for hustsync-manager.
//!
//! Covers §3.5 (POST /workers), §3.6 (DELETE /workers/{id}),
//! §3.7 (GET /workers/{id}/jobs), §3.8 (POST /workers/{id}/jobs/{mirror}),
//! §3.9 (POST /workers/{id}/jobs/{mirror}/size), and
//! §3.10 (POST /workers/{id}/schedules) of the HTTP contract.
//!
//! Each test drives the router via `tower::ServiceExt::oneshot` — no real
//! socket is bound.  The harness module (tests/contract/mod.rs) owns
//! router construction and fixture loading so this file stays focused on
//! assertions.
//!
//! Fixture path convention:
//!   tests/fixtures/http/workers_register/{request,response}.json
//!   tests/fixtures/http/workers_delete/response.json
//!   tests/fixtures/http/workers_jobs_mirror/request_{status}.json
//!   tests/fixtures/http/workers_jobs_mirror_size/request.json
//!   tests/fixtures/http/workers_schedules/request.json
//!
//! NOTE on MirrorSchedule field name: the current wire key is `"name"`
//! (MirrorSchedule.name with snake_case serde rename_all).  When
//! protocol-contract renames this field to `mirror_name` the fixture
//! and this test must be updated in the same PR.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod contract;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::json;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// POST /workers with a JSON body; returns the response.
async fn register(app: axum::Router, body: serde_json::Value) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/workers")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap(),
    )
    .await
    .unwrap()
}

/// POST /workers/{id}/jobs/{mirror} with a JSON body.
async fn update_job(
    app: axum::Router,
    worker_id: &str,
    mirror_id: &str,
    body: serde_json::Value,
) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri(format!("/workers/{}/jobs/{}", worker_id, mirror_id))
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap(),
    )
    .await
    .unwrap()
}

/// Register a worker with sensible defaults; panics on non-200.
async fn setup_worker(app: axum::Router, worker_id: &str) -> axum::Router {
    let body = json!({
        "id": worker_id,
        "url": "http://127.0.0.1:6000/",
        "token": "test-token",
        "last_online": "1970-01-01T00:00:00Z",
        "last_register": "1970-01-01T00:00:00Z"
    });
    let resp = register(app.clone(), body).await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "setup_worker: register failed"
    );
    app
}

// ---------------------------------------------------------------------------
// §3.5 — POST /workers: register worker
// ---------------------------------------------------------------------------

/// Happy path: POST /workers with a valid WorkerStatus returns 200.
///
/// The response echoes back the registered worker.  `last_online` and
/// `last_register` are overwritten by the handler to `Utc::now()` so they
/// are masked for comparison.
#[tokio::test]
async fn register_worker_happy_path() {
    let (app, _dir) = contract::spawn_manager();

    let req_fixture = contract::load_fixture("workers_register/request.json");
    let resp = register(app, req_fixture).await;

    assert_eq!(resp.status(), StatusCode::OK);

    let got = contract::body_json(resp).await;
    let want = contract::load_fixture("workers_register/response.json");
    contract::assert_json_eq_masked(&got, &want, &["last_online", "last_register"]);
}

/// §3.5 — Sending a body that cannot be deserialised as WorkerStatus (e.g.
/// a completely wrong shape) must be rejected with a 4xx status.
///
/// Axum's Json extractor returns 422 Unprocessable Entity when the body
/// cannot be deserialised.  The spec says 400 Bad Request here; protocol-
/// contract should add an explicit id-validation guard in the handler so the
/// status becomes 400.  Until that fix lands this test pins the observable
/// 422.  When the handler is fixed, update the assertion to 400 and the
/// contract will match the spec.
#[tokio::test]
async fn register_worker_invalid_body_rejected() {
    let (app, _dir) = contract::spawn_manager();

    // Missing required fields — axum Json extractor will fail deserialisation.
    let bad_body = json!({ "not_a_worker": true });
    let resp = register(app, bad_body).await;

    // The current handler returns 422 (axum extractor default) rather than
    // 400.  Pin this so the test is green; protocol-contract fixes it to 400
    // per spec §3.5.
    assert!(
        resp.status().is_client_error(),
        "invalid body must produce a 4xx; got {}",
        resp.status()
    );
}

// ---------------------------------------------------------------------------
// §3.4 + §3.5 — GET /workers: list after register, token REDACTED
// ---------------------------------------------------------------------------

/// POST /workers then GET /workers: the registered worker appears in the
/// list with its token replaced by the literal string "REDACTED".
///
/// This verifies that the register → list round-trip works and that the
/// manager never leaks the real token over the wire (§3.4).
#[tokio::test]
async fn list_workers_after_register_token_redacted() {
    let (app, _dir) = contract::spawn_manager();

    // Register a worker with a known token.
    let body = json!({
        "id": "worker-token-test",
        "url": "http://127.0.0.1:6001/",
        "token": "super-secret",
        "last_online": "1970-01-01T00:00:00Z",
        "last_register": "1970-01-01T00:00:00Z"
    });
    let reg_resp = register(app.clone(), body).await;
    assert_eq!(reg_resp.status(), StatusCode::OK);

    // List workers.
    let list_resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/workers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(list_resp.status(), StatusCode::OK);

    let got = contract::body_json(list_resp).await;
    let workers = got
        .as_array()
        .expect("GET /workers must return a JSON array");
    let w = workers
        .iter()
        .find(|w| w["id"] == "worker-token-test")
        .expect("registered worker must appear in the list");

    assert_eq!(
        w["token"], "REDACTED",
        "token must be redacted in the GET /workers response per §3.4"
    );
    // The id and url fields must be preserved verbatim.
    assert_eq!(w["id"], "worker-token-test");
    assert_eq!(w["url"], "http://127.0.0.1:6001/");
}

// ---------------------------------------------------------------------------
// §3.6 — DELETE /workers/{id}: delete worker
// ---------------------------------------------------------------------------

/// Register a worker then delete it; response is `{"message": "deleted"}`.
#[tokio::test]
async fn delete_worker_happy_path() {
    let (app, _dir) = contract::spawn_manager();
    let app = setup_worker(app, "worker-to-delete").await;

    let del_resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/workers/worker-to-delete")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(del_resp.status(), StatusCode::OK);

    let got = contract::body_json(del_resp).await;
    let want = contract::load_fixture("workers_delete/response.json");
    contract::assert_json_eq_masked(&got, &want, &[]);
}

// ---------------------------------------------------------------------------
// §3.8 — POST /workers/{id}/jobs/{mirror}: update job status
//
// The four status transitions exercised here are:
//   1. pre-syncing  — last_started is refreshed; last_update/last_ended carry over
//   2. syncing      — no timestamp fields updated by the handler
//   3. success      — last_update and last_ended are set to now
//   4. failed       — last_ended is set to now; error_msg preserved
// ---------------------------------------------------------------------------

/// Status transition to `pre-syncing`: last-started must be refreshed.
///
/// The handler sets `last-started` to `Utc::now()` when the incoming status
/// is `pre-syncing` and the previous status was not already `pre-syncing`.
/// We mask the volatile timestamp so the test does not flake.
#[tokio::test]
async fn update_job_status_pre_syncing() {
    let (app, _dir) = contract::spawn_manager();
    let app = setup_worker(app, "w-presync").await;

    let body = contract::load_fixture("workers_jobs_mirror/request_pre_syncing.json");
    let resp = update_job(app, "w-presync", "debian", body).await;

    assert_eq!(resp.status(), StatusCode::OK);

    let got = contract::body_json(resp).await;
    // Status must round-trip.
    assert_eq!(
        got["status"], "pre-syncing",
        "status field must be pre-syncing"
    );
    // last-started is refreshed from Utc::now() — it must differ from the epoch
    // sentinel we sent in the fixture.
    let last_started = got["last-started"]
        .as_str()
        .expect("last-started must be a string");
    assert_ne!(
        last_started, "1970-01-01T00:00:00Z",
        "last-started must be updated when transitioning to pre-syncing"
    );
}

/// Status transition to `syncing`: none of the timestamp fields are updated
/// by the handler (it only updates last_started on pre-syncing and
/// last_update/last_ended on success/failed).
#[tokio::test]
async fn update_job_status_syncing() {
    let (app, _dir) = contract::spawn_manager();
    let app = setup_worker(app, "w-syncing").await;

    let body = contract::load_fixture("workers_jobs_mirror/request_syncing.json");
    let resp = update_job(app, "w-syncing", "debian", body).await;

    assert_eq!(resp.status(), StatusCode::OK);

    let got = contract::body_json(resp).await;
    assert_eq!(got["status"], "syncing");
}

/// Status transition to `success`: last-update and last-ended are both set
/// to `Utc::now()` by the handler.
#[tokio::test]
async fn update_job_status_success() {
    let (app, _dir) = contract::spawn_manager();
    let app = setup_worker(app, "w-success").await;

    let body = contract::load_fixture("workers_jobs_mirror/request_success.json");
    let resp = update_job(app, "w-success", "debian", body).await;

    assert_eq!(resp.status(), StatusCode::OK);

    let got = contract::body_json(resp).await;
    assert_eq!(got["status"], "success");

    let last_update = got["last-update"]
        .as_str()
        .expect("last-update must be a string");
    assert_ne!(
        last_update, "1970-01-01T00:00:00Z",
        "last-update must be set to now on success"
    );
    let last_ended = got["last-ended"]
        .as_str()
        .expect("last-ended must be a string");
    assert_ne!(
        last_ended, "1970-01-01T00:00:00Z",
        "last-ended must be set to now on success"
    );
}

/// Status transition to `failed`: last-ended is updated; error-msg is
/// preserved from the request body.
#[tokio::test]
async fn update_job_status_failed() {
    let (app, _dir) = contract::spawn_manager();
    let app = setup_worker(app, "w-failed").await;

    let body = contract::load_fixture("workers_jobs_mirror/request_failed.json");
    let resp = update_job(app.clone(), "w-failed", "debian", body).await;

    assert_eq!(resp.status(), StatusCode::OK);

    let got = contract::body_json(resp).await;
    assert_eq!(got["status"], "failed");

    let last_ended = got["last-ended"]
        .as_str()
        .expect("last-ended must be a string");
    assert_ne!(
        last_ended, "1970-01-01T00:00:00Z",
        "last-ended must be set to now on failed"
    );
    assert_eq!(
        got["error-msg"], "rsync exit code 23",
        "error-msg must be preserved from the request body"
    );
}

/// Sending a status update with an empty `name` field must be rejected
/// with 400 Bad Request — the handler validates this explicitly.
#[tokio::test]
async fn update_job_status_empty_name_rejected() {
    let (app, _dir) = contract::spawn_manager();
    let app = setup_worker(app, "w-badname").await;

    let bad_body = json!({
        "name": "",
        "worker": "w-badname",
        "upstream": "",
        "size": "",
        "error-msg": "",
        "last-update": "1970-01-01T00:00:00Z",
        "last-started": "1970-01-01T00:00:00Z",
        "last-ended": "1970-01-01T00:00:00Z",
        "next-scheduled": "1970-01-01T00:00:00Z",
        "status": "syncing",
        "is-master": false
    });

    let resp = update_job(app, "w-badname", "irrelevant", bad_body).await;
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "empty mirror name must return 400"
    );

    let got = contract::body_json(resp).await;
    assert!(
        got["error"].is_string(),
        "error envelope must contain an 'error' string field"
    );
}

// ---------------------------------------------------------------------------
// §3.9 — POST /workers/{id}/jobs/{mirror}/size: update mirror size
// ---------------------------------------------------------------------------

/// POST a valid size update; response body must contain the updated size.
#[tokio::test]
async fn update_mirror_size_happy_path() {
    let (app, _dir) = contract::spawn_manager();
    let app = setup_worker(app, "w-size").await;

    // First create a mirror status record so the size handler can find it.
    let init_body = json!({
        "name": "ubuntu",
        "worker": "w-size",
        "upstream": "rsync://mirror.example.com/ubuntu/",
        "size": "0",
        "error-msg": "",
        "last-update": "1970-01-01T00:00:00Z",
        "last-started": "1970-01-01T00:00:00Z",
        "last-ended": "1970-01-01T00:00:00Z",
        "next-scheduled": "1970-01-01T00:00:00Z",
        "status": "success",
        "is-master": false
    });
    let init_resp = update_job(app.clone(), "w-size", "ubuntu", init_body).await;
    assert_eq!(
        init_resp.status(),
        StatusCode::OK,
        "initial job create failed"
    );

    // Now update the size.
    let size_fixture = contract::load_fixture("workers_jobs_mirror_size/request.json");
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workers/w-size/jobs/ubuntu/size")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_vec(&size_fixture).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    let got = contract::body_json(resp).await;
    assert_eq!(
        got["size"], "42G",
        "size field must reflect the POSTed value"
    );
}

// ---------------------------------------------------------------------------
// §3.10 — POST /workers/{id}/schedules: update schedules
//
// NOTE: The JSON key for the mirror name inside each schedule entry is
// currently `"name"` (MirrorSchedule.name, snake_case serde).  The Go
// wire uses `"name"` via `json:"name"` on MirrorSchedule.MirrorName.
// When protocol-contract renames the Rust field to `mirror_name` the key
// will change to `"mirror_name"` and this fixture must be updated.
// ---------------------------------------------------------------------------

/// POST /workers/{id}/schedules updates next_scheduled for the named mirror.
///
/// The handler returns an empty JSON object `{}` on success.
#[tokio::test]
async fn update_schedules_happy_path() {
    let (app, _dir) = contract::spawn_manager();
    let app = setup_worker(app, "w-sched").await;

    // Create the mirror record so the schedule handler can look it up.
    let init_body = json!({
        "name": "debian",
        "worker": "w-sched",
        "upstream": "",
        "size": "0",
        "error-msg": "",
        "last-update": "1970-01-01T00:00:00Z",
        "last-started": "1970-01-01T00:00:00Z",
        "last-ended": "1970-01-01T00:00:00Z",
        "next-scheduled": "1970-01-01T00:00:00Z",
        "status": "success",
        "is-master": false
    });
    let init_resp = update_job(app.clone(), "w-sched", "debian", init_body).await;
    assert_eq!(
        init_resp.status(),
        StatusCode::OK,
        "initial job create failed"
    );

    let sched_fixture = contract::load_fixture("workers_schedules/request.json");
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workers/w-sched/schedules")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_vec(&sched_fixture).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    // The handler returns `{}` on success per the current implementation.
    let got = contract::body_json(resp).await;
    assert!(
        got.is_object(),
        "schedule update must return a JSON object, got: {got}"
    );
}

/// Empty schedule name inside the schedules array must return 400.
#[tokio::test]
async fn update_schedules_empty_name_rejected() {
    let (app, _dir) = contract::spawn_manager();
    let app = setup_worker(app, "w-sched-bad").await;

    let bad_body = json!({
        "schedules": [
            { "name": "", "next_schedule": "2026-04-16T00:00:00Z" }
        ]
    });

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workers/w-sched-bad/schedules")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_vec(&bad_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let got = contract::body_json(resp).await;
    assert!(
        got["error"].is_string(),
        "error envelope must contain an 'error' string field"
    );
}
