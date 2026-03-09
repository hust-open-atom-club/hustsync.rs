use axum::{
    body::{Body, Bytes},
    http::{Request, StatusCode},
};
use hustsync_manager::{get_hustsync_manager, Manager};
use hustsync_config_parser::ManagerConfig;
use hustsync_internal::msg::{WorkerStatus, MirrorStatus};
use hustsync_internal::status::SyncStatus;
use serde_json::{json, Value};
use std::sync::Arc;
use tempfile::NamedTempFile;
use tower::ServiceExt; // for `oneshot` and `ready`
use chrono::Utc;

#[tokio::test]
async fn test_manager_api_workflow() {
    let tmp_db = NamedTempFile::new().unwrap();
    let db_path = tmp_db.path().to_str().unwrap().to_string();

    let mut config = ManagerConfig::default();
    config.files.db_type = "redb".to_string();
    config.files.db_file = db_path;
    config.debug = true;

    let manager = get_hustsync_manager(Some(Arc::new(config))).expect("Failed to get manager");
    let app = manager.engine.clone();

    // 1. Test /ping
    let response = app.clone()
        .oneshot(Request::builder().uri("/ping").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let obj: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(obj["message"], "pong");

    // 2. Register a worker
    let worker = WorkerStatus {
        id: "test-worker".to_string(),
        url: "http://127.0.0.1:6000".to_string(),
        token: "secret-token".to_string(),
        last_online: Utc::now(),
        last_register: Utc::now(),
    };

    let response = app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workers")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_vec(&worker).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // 3. List workers and check redaction
    let response = app.clone()
        .oneshot(Request::builder().uri("/workers").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let workers: Vec<WorkerStatus> = serde_json::from_slice(&body).unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].id, "test-worker");
    assert_eq!(workers[0].token, "REDACTED");

    // 4. Update a job status
    let mut mirror = MirrorStatus {
        name: "test-mirror".to_string(),
        worker: "test-worker".to_string(),
        upstream: "http://upstream.com".to_string(),
        size: "10GB".to_string(),
        error_msg: "".to_string(),
        last_update: Utc::now(),
        last_started: Utc::now(),
        last_ended: Utc::now(),
        next_scheduled: Utc::now(),
        status: SyncStatus::Syncing,
        is_master: true,
    };

    let response = app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workers/test-worker/jobs/test-mirror")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_vec(&mirror).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        panic!("Request failed with status {}. Body: {:?}", status, String::from_utf8_lossy(&body));
    }
    assert_eq!(response.status(), StatusCode::OK);

    // 5. List all jobs and check custom time format compatibility
    let response = app.clone()
        .oneshot(Request::builder().uri("/jobs").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let jobs: Value = serde_json::from_slice(&body).unwrap();
    
    assert!(jobs.is_array());
    let job = &jobs[0];
    assert_eq!(job["name"], "test-mirror");
    assert_eq!(job["status"], "syncing");
    
    // Check custom time format: "2006-01-02 15:04:05 -0700"
    let last_update_str = job["last_update"].as_str().unwrap();
    assert!(last_update_str.contains(" +0000") || last_update_str.contains(" -0000"));
    assert!(job["last_update_ts"].is_number());
}
