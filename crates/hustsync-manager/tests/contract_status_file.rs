//! Contract tests for manager `files.status_file`.
//!
//! The status file is an operator-facing snapshot of `GET /jobs`. It must be
//! refreshed when manager-side mirror status changes, not just parsed from
//! config and ignored.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod contract;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use hustsync_config_parser::{ManagerConfig, ManagerFileConfig, ManagerServerConfig};
use hustsync_manager::Manager;
use serde_json::{Value, json};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

fn spawn_manager_with_status_file() -> (axum::Router, TempDir, std::path::PathBuf) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let status_path = dir.path().join("status.json");

    let config = Arc::new(ManagerConfig {
        debug: false,
        server: ManagerServerConfig {
            addr: "127.0.0.1".to_string(),
            port: 0,
            ssl_cert: String::new(),
            ssl_key: String::new(),
        },
        files: ManagerFileConfig {
            status_file: status_path.to_string_lossy().into_owned(),
            db_type: "redb".to_string(),
            db_file: db_path.to_string_lossy().into_owned(),
            ca_cert: String::new(),
        },
    });

    let manager = Manager::new(config).unwrap();
    let router = Arc::new(manager).make_router();
    (router, dir, status_path)
}

async fn post_json(app: axum::Router, uri: &str, body: Value) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn status_file_is_refreshed_after_status_and_size_updates() {
    let (app, _dir, status_path) = spawn_manager_with_status_file();

    let worker = json!({
        "id": "status-worker",
        "url": "http://127.0.0.1:6000/",
        "token": "secret",
        "last_online": "2023-01-01T00:00:00Z",
        "last_register": "2023-01-01T00:00:00Z"
    });
    let resp = post_json(app.clone(), "/workers", worker).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let status = json!({
        "name": "archlinux",
        "worker": "status-worker",
        "upstream": "rsync://mirror.example/archlinux/",
        "size": "unknown",
        "error_msg": "",
        "last_update": "2023-01-01T00:00:00Z",
        "last_started": "2023-01-01T00:00:00Z",
        "last_ended": "2023-01-01T00:00:00Z",
        "next_schedule": "2023-01-01T00:00:00Z",
        "status": "success",
        "is_master": true
    });
    let resp = post_json(app.clone(), "/workers/status-worker/jobs/archlinux", status).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let snapshot: Value =
        serde_json::from_slice(&std::fs::read(&status_path).expect("status file must exist"))
            .unwrap();
    let jobs = snapshot.as_array().unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0]["name"], "archlinux");
    assert_eq!(jobs[0]["status"], "success");
    assert!(
        jobs[0].get("last_update_ts").is_some(),
        "status file must use the same web view shape as GET /jobs"
    );

    let resp = post_json(
        app,
        "/workers/status-worker/jobs/archlinux/size",
        json!({"size": "42M"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    let snapshot: Value =
        serde_json::from_slice(&std::fs::read(&status_path).expect("status file must exist"))
            .unwrap();
    assert_eq!(snapshot[0]["size"], "42M");
}
