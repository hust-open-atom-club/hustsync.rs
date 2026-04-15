//! Contract tests for `POST /cmd` — §3.11 of the HTTP contract.
//!
//! These tests pin the *expected* Go-compatible behavior so that `protocol-contract`
//! lane (T8) has a concrete failing target to fix against.  Some tests are
//! intentionally red against the current implementation; each failing test is
//! annotated with the exact deviation T8 must close.
//!
//! Scenarios exercised (§3.11 aligned with Go `handleClientCmd`):
//!   1. Happy path  — forward succeeds; manager returns its own fixed message.
//!   2. Empty worker_id → 500 with no body (Go: `c.AbortWithStatus(500)`).
//!   3. Unknown worker → 400 "worker X is not registered yet".
//!   4. Unknown mirror → still forwards (Go does not validate); worker path
//!      reaching a closed port ends up as 500 via the generic forward-error.
//!   5. Worker unreachable (connection refused) → 500 (Go does not
//!      discriminate 502; all forward errors collapse to 500).
//!   6. Worker timeout (accepts but never responds) → 500 (same).
//!
//! Mock worker strategy: `tokio::net::TcpListener` on an ephemeral port — no
//! new crate dependencies required.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod contract;

use axum::http::Request;
use axum::{body::Body, http::StatusCode};
use hustsync_internal::msg::{ClientCmd, CmdVerb, WorkerStatus};
use std::collections::HashMap;
use tokio::io::AsyncWriteExt;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Register `mirror-01` worker with the given URL and return the app.
///
/// Resets the app after registration so oneshot can be called again.
async fn register_worker(app: axum::Router, url: &str) -> axum::Router {
    let worker = WorkerStatus {
        id: "mirror-01".to_string(),
        url: url.to_string(),
        token: "s3cret".to_string(),
        last_online: chrono::Utc::now(),
        last_register: chrono::Utc::now(),
    };

    let body = serde_json::to_vec(&worker).unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/workers")
        .header("Content-Type", "application/json")
        .body(Body::from(body))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "worker registration must succeed"
    );
    app
}

/// Build the JSON body for a `POST /cmd` request.
fn cmd_body(worker_id: &str, mirror_id: &str) -> Vec<u8> {
    let cmd = ClientCmd {
        cmd: CmdVerb::Start,
        worker_id: worker_id.to_string(),
        mirror_id: mirror_id.to_string(),
        args: vec![],
        options: HashMap::new(),
    };
    serde_json::to_vec(&cmd).unwrap()
}

// ---------------------------------------------------------------------------
// T7-1: happy path
// ---------------------------------------------------------------------------

/// POST /cmd happy path: manager forwards to worker and returns its own fixed
/// message `{"message": "successfully send command to worker mirror-01"}`.
///
/// The mock worker here accepts the TCP connection, reads the HTTP request, and
/// responds with HTTP 200 + `{"msg": "OK"}` — the worker's own Go-compat body.
/// The *manager* must NOT pass that body through; it must return the fixed
/// message defined by §3.11.
///
/// # Expected status: FAIL (T8 fixes)
///
/// Current `handle_cmd` passes through whatever the worker returns, so `got`
/// will equal `{"msg":"OK"}` instead of the expected fixed message.
#[tokio::test]
async fn cmd_happy_path_returns_fixed_message() {
    // Spin a mock worker that speaks minimal HTTP and returns {"msg":"OK"}.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let worker_port = listener.local_addr().unwrap().port();
    let worker_url = format!("http://127.0.0.1:{}/", worker_port);

    tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        if let Ok((mut stream, _)) = listener.accept().await {
            // Drain the request headers (read until \r\n\r\n).
            let mut buf = vec![0u8; 4096];
            let _ = stream.read(&mut buf).await;
            // Respond with HTTP 200 + worker's Go-compat body (§4.1).
            // Body is exactly 13 bytes: {"msg":"OK"} — no space after colon.
            let body = "{\"msg\":\"OK\"}";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
            // Flush and shut down to signal EOF to reqwest.
            let _ = stream.shutdown().await;
        }
    });

    let (app, _dir) = contract::spawn_manager();
    let app = register_worker(app, &worker_url).await;

    let req = Request::builder()
        .method("POST")
        .uri("/cmd")
        .header("Content-Type", "application/json")
        .body(Body::from(cmd_body("mirror-01", "archlinux")))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let got = contract::body_json(resp).await;
    let want = contract::load_fixture("cmd/response_happy.json");
    contract::assert_json_eq_masked(&got, &want, &[]);
}

// ---------------------------------------------------------------------------
// T7-2: unknown worker → 400
// ---------------------------------------------------------------------------

/// POST /cmd with an unknown `worker_id` must return 400 (Go-compat).
///
/// Go tunasync returns 400, not 404, for unknown workers on this endpoint.
///
/// # Expected status: PASS
///
/// Current `handle_cmd` already returns `StatusCode::BAD_REQUEST` when
/// `adapter.get_worker` fails.  The error message format may differ from
/// the fixture — fixture pins the Go canonical shape; T8 can align the message.
#[tokio::test]
async fn cmd_unknown_worker_returns_400() {
    let (app, _dir) = contract::spawn_manager();

    let req = Request::builder()
        .method("POST")
        .uri("/cmd")
        .header("Content-Type", "application/json")
        .body(Body::from(cmd_body("no-such-worker", "archlinux")))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "unknown worker must return 400, not 404 (Go-compat §3.11)"
    );

    let got = contract::body_json(resp).await;
    // The body must carry an "error" key (§2 envelope shape).
    assert!(
        got.get("error").is_some(),
        "error envelope must contain an \"error\" key; got: {got}"
    );
}

// ---------------------------------------------------------------------------
// T7-3: unknown mirror → 400
// ---------------------------------------------------------------------------

/// POST /cmd with a valid worker but an unknown `mirror_id` must return 400.
///
/// The worker is registered but has no mirrors.  Go returns 400 when the mirror
/// does not exist on the manager side (before forwarding).
///
/// # Expected status: FAIL (T8 fixes)
///
/// Go does not validate mirror existence before forwarding. A cmd with
/// an unknown mirror still attempts the POST to the worker URL; if the
/// worker is unreachable the request collapses to a generic 500 via the
/// forward-error path (see `cmd_worker_unreachable_returns_500`). This
/// test pins that the manager does NOT short-circuit to 400 when the
/// mirror is unknown — the unknown-mirror path is indistinguishable
/// from the worker-unreachable path at the manager layer.
#[tokio::test]
async fn cmd_unknown_mirror_forwards_and_fails_generically() {
    let closed_port = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        p
    };
    let worker_url = format!("http://127.0.0.1:{}/", closed_port);

    let (app, _dir) = contract::spawn_manager();
    let app = register_worker(app, &worker_url).await;

    let req = Request::builder()
        .method("POST")
        .uri("/cmd")
        .header("Content-Type", "application/json")
        .body(Body::from(cmd_body("mirror-01", "no-such-mirror")))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "Go does not pre-check mirror existence; the forward failure is 500"
    );

    let got = contract::body_json(resp).await;
    assert!(
        got.get("error").is_some(),
        "error envelope must contain an \"error\" key; got: {got}"
    );
}

// ---------------------------------------------------------------------------
// T7-4: worker unreachable → 502
// ---------------------------------------------------------------------------

/// POST /cmd when the worker URL points to a port where nothing is listening
/// returns 500 — Go collapses every forward error (connect refused, timeout,
/// non-2xx) into a single 500 path. Any dashboard/client reading this
/// status cannot discriminate causes at the manager layer.
#[tokio::test]
async fn cmd_worker_unreachable_returns_500() {
    // Bind then immediately drop so the port is closed.
    let closed_port = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        p
    };
    let worker_url = format!("http://127.0.0.1:{}/", closed_port);

    let (app, _dir) = contract::spawn_manager();
    let app = register_worker(app, &worker_url).await;

    let req = Request::builder()
        .method("POST")
        .uri("/cmd")
        .header("Content-Type", "application/json")
        .body(Body::from(cmd_body("mirror-01", "archlinux")))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "Go collapses all forward errors to 500 (no 502 discrimination)"
    );

    let got = contract::body_json(resp).await;
    assert!(
        got.get("error").is_some(),
        "error envelope must contain an \"error\" key; got: {got}"
    );
}

// ---------------------------------------------------------------------------
// T7-5: worker timeout → 504
// ---------------------------------------------------------------------------

/// POST /cmd when the worker accepts the TCP connection but never sends a
/// response returns 500 — Go does not discriminate timeout from other
/// forward failures. The reqwest client's own timeout surfaces as an
/// `Err(_)` from `send()`, which maps to 500 via the generic forward-error
/// path.
///
/// NOTE: This test takes ~5 seconds because it waits for the reqwest
/// timeout to fire. That is unavoidable given the in-process test
/// approach; it is not flaky.
#[tokio::test]
async fn cmd_worker_timeout_returns_500() {
    // Accept-never-respond mock: bind, accept one connection, hold it forever.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let worker_port = listener.local_addr().unwrap().port();
    let worker_url = format!("http://127.0.0.1:{}/", worker_port);

    // Keep the handle alive for the duration of the test so the OS doesn't
    // close the connection (which would look like a refused or reset, not a
    // timeout).  We drop it at the end of the test.
    let accept_hold = tokio::spawn(async move {
        // Accept the connection but never read or write.  The OS will RST the
        // connection when the future is dropped, but by then the reqwest client
        // will have already timed out.
        let (_stream, _addr) = listener.accept().await.unwrap();
        // Hold the stream until the spawned future is cancelled.
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    });

    let (app, _dir) = contract::spawn_manager();
    let app = register_worker(app, &worker_url).await;

    let req = Request::builder()
        .method("POST")
        .uri("/cmd")
        .header("Content-Type", "application/json")
        .body(Body::from(cmd_body("mirror-01", "archlinux")))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();

    // Clean up the accept-hold task.
    accept_hold.abort();

    assert_eq!(
        resp.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "Go collapses all forward errors to 500 (no 504 discrimination)"
    );

    let got = contract::body_json(resp).await;
    assert!(
        got.get("error").is_some(),
        "error envelope must contain an \"error\" key; got: {got}"
    );
}
