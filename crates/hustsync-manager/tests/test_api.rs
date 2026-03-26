use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use hustsync_config_parser::ManagerConfig;
use hustsync_manager::get_hustsync_manager;
use serde_json::json;
use std::sync::Arc;
use tower::ServiceExt;
use once_cell::sync::Lazy;

static SHARED_MANAGER: Lazy<&'static hustsync_manager::Manager> = Lazy::new(|| {
    let db_path = "/tmp/test_api_shared.db";
    let _ = std::fs::remove_file(db_path);

    let config = Arc::new(ManagerConfig {
        debug: true,
        server: hustsync_config_parser::ManagerServerConfig {
            addr: "127.0.0.1".to_string(),
            port: 0,
            ssl_cert: "".to_string(),
            ssl_key: "".to_string(),
        },
        files: hustsync_config_parser::ManagerFileConfig {
            status_file: "".to_string(),
            db_type: "redb".to_string(),
            db_file: db_path.to_string(),
            ca_cert: "".to_string(),
        },
    });

    get_hustsync_manager(config).expect("Failed to get manager")
});

#[tokio::test]
async fn test_register_worker_api() {
    let manager = *SHARED_MANAGER;
    let app = manager.engine.clone();

    let worker_json = json!({
        "id": "test-worker-api",
        "url": "http://127.0.0.1:8081",
        "token": "test-token",
        "last_online": "2023-01-01T00:00:00Z",
        "last_register": "2023-01-01T00:00:00Z"
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workers")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_vec(&worker_json).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let res_json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(res_json["id"], "test-worker-api");
    assert_ne!(res_json["last_online"], "2023-01-01T00:00:00Z");
}

#[tokio::test]
async fn test_update_job_status_logic() {
    let manager = *SHARED_MANAGER;
    let app = manager.engine.clone();

    let worker_id = "worker-update-test";
    let _ = app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri("/workers")
            .header("Content-Type", "application/json")
            .body(Body::from(json!({
                "id": worker_id,
                "url": "http://127.0.0.1:8081",
                "token": "test-token",
                "last_online": "2023-01-01T00:00:00Z",
                "last_register": "2023-01-01T00:00:00Z"
            }).to_string()))
            .unwrap(),
    ).await.unwrap();

    let mirror_id = "debian";
    let status_success = json!({
        "name": mirror_id,
        "worker": worker_id,
        "upstream": "http://deb.debian.org",
        "size": "0",
        "error-msg": "",
        "last-update": "2023-01-01T00:00:00Z",
        "last-started": "2023-01-01T00:00:00Z",
        "last-ended": "2023-01-01T00:00:00Z",
        "next-scheduled": "2023-01-01T00:00:00Z",
        "status": "success",
        "is-master": true
    });

    let res = app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri(format!("/workers/{}/jobs/{}", worker_id, mirror_id))
            .header("Content-Type", "application/json")
            .body(Body::from(status_success.to_string()))
            .unwrap(),
    ).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let status_presync = json!({
        "name": mirror_id,
        "worker": worker_id,
        "upstream": "http://deb.debian.org",
        "size": "0",
        "error-msg": "",
        "last-update": "2023-01-01T00:00:00Z",
        "last-started": "2023-01-01T00:00:00Z",
        "last-ended": "2023-01-01T00:00:00Z",
        "next-scheduled": "2023-01-01T00:00:00Z",
        "status": "pre-syncing",
        "is-master": true
    });

    let res = app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri(format!("/workers/{}/jobs/{}", worker_id, mirror_id))
            .header("Content-Type", "application/json")
            .body(Body::from(status_presync.to_string()))
            .unwrap(),
    ).await.unwrap();
    
    let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let res_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_ne!(res_json["last-started"], "2023-01-01T00:00:00Z");
}

#[tokio::test]
async fn test_list_query_apis() {
    let manager = *SHARED_MANAGER;
    let app = manager.engine.clone();

    let worker_id = "list-test-worker";
    let _ = app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri("/workers")
            .header("Content-Type", "application/json")
            .body(Body::from(json!({
                "id": worker_id,
                "url": "http://127.0.0.1:8082",
                "token": "SUPER-SECRET-TOKEN",
                "last_online": "2023-01-01T00:00:00Z",
                "last_register": "2023-01-01T00:00:00Z"
            }).to_string()))
            .unwrap(),
    ).await.unwrap();

    let _ = app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri(format!("/workers/{}/jobs/archlinux", worker_id))
            .header("Content-Type", "application/json")
            .body(Body::from(json!({
                "name": "archlinux",
                "worker": worker_id,
                "upstream": "http://mirrors.kernel.org",
                "size": "500GB",
                "error-msg": "",
                "last-update": "2023-01-01T00:00:00Z",
                "last-started": "2023-01-01T00:00:00Z",
                "last-ended": "2023-01-01T00:00:00Z",
                "next-scheduled": "2023-01-01T00:00:00Z",
                "status": "success",
                "is-master": true
            }).to_string()))
            .unwrap(),
    ).await.unwrap();

    let res = app.clone().oneshot(
        Request::builder().method("GET").uri("/workers").body(Body::empty()).unwrap()
    ).await.unwrap();
    let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let workers: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    let test_worker = workers.iter().find(|w| w["id"] == worker_id).expect("Worker not found");
    assert_eq!(test_worker["token"], "REDACTED");

    let res = app.clone().oneshot(
        Request::builder().method("GET").uri("/jobs").body(Body::empty()).unwrap()
    ).await.unwrap();
    let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let jobs: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    let test_job = jobs.iter().find(|j| j["name"] == "archlinux").expect("Job not found");

    // Verify Time Format: Text vs Timestamp
    assert!(test_job["last_update"].is_string());
    assert!(test_job["last_update_ts"].is_number());
    assert!(test_job["next_schedule"].is_string());
    assert!(test_job["next_schedule_ts"].is_number());
    
    let time_str = test_job["last_update"].as_str().unwrap();
    assert!(time_str.contains(" +0000") || time_str.contains(" -0000"));
}

#[tokio::test]
async fn test_flush_disabled_jobs() {
    let manager = *SHARED_MANAGER;
    let app = manager.engine.clone();

    let worker_id = "flush-test-worker";
    let _ = app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri(format!("/workers/{}/jobs/job-active", worker_id))
            .header("Content-Type", "application/json")
            .body(Body::from(json!({
                "name": "job-active",
                "worker": worker_id,
                "upstream": "http://upstream",
                "size": "0",
                "error-msg": "",
                "last-update": "2023-01-01T00:00:00Z",
                "last-started": "2023-01-01T00:00:00Z",
                "last-ended": "2023-01-01T00:00:00Z",
                "next-scheduled": "2023-01-01T00:00:00Z",
                "status": "success",
                "is-master": true
            }).to_string()))
            .unwrap(),
    ).await.unwrap();

    let _ = app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri(format!("/workers/{}/jobs/job-disabled", worker_id))
            .header("Content-Type", "application/json")
            .body(Body::from(json!({
                "name": "job-disabled",
                "worker": worker_id,
                "upstream": "http://upstream",
                "size": "0",
                "error-msg": "",
                "last-update": "2023-01-01T00:00:00Z",
                "last-started": "2023-01-01T00:00:00Z",
                "last-ended": "2023-01-01T00:00:00Z",
                "next-scheduled": "2023-01-01T00:00:00Z",
                "status": "disabled",
                "is-master": true
            }).to_string()))
            .unwrap(),
    ).await.unwrap();

    let res = app.clone().oneshot(
        Request::builder().method("DELETE").uri("/jobs/disabled").body(Body::empty()).unwrap()
    ).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let res = app.clone().oneshot(
        Request::builder().method("GET").uri("/jobs").body(Body::empty()).unwrap()
    ).await.unwrap();
    let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let jobs: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    
    assert!(jobs.iter().any(|j| j["name"] == "job-active"));
    assert!(!jobs.iter().any(|j| j["name"] == "job-disabled"));
}

#[tokio::test]
async fn test_handle_cmd_worker_not_found() {
    let manager = *SHARED_MANAGER;
    let app = manager.engine.clone();

    let cmd_json = json!({
        "options": {},
        "args": [],
        "mirror_id": "debian",
        "worker_id": "non-existent-worker",
        "cmd": "start"
    });

    let res = app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri("/cmd")
            .header("Content-Type", "application/json")
            .body(Body::from(cmd_json.to_string()))
            .unwrap(),
    ).await.unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let res_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(res_json["error"].as_str().unwrap().contains("not found"));
}

#[tokio::test]
async fn test_size_and_schedule_updates() {
    let manager = *SHARED_MANAGER;
    let app = manager.engine.clone();

    let worker_id = "size-sched-worker";
    let mirror_id = "ubuntu";

    // 1. Register and report
    let _ = app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri("/workers")
            .header("Content-Type", "application/json")
            .body(Body::from(json!({"id": worker_id, "url": "http://127.0.0.1", "token": ""}).to_string()))
            .unwrap(),
    ).await.unwrap();

    let _ = app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri(format!("/workers/{}/jobs/{}", worker_id, mirror_id))
            .header("Content-Type", "application/json")
            .body(Body::from(json!({
                "name": mirror_id, "worker": worker_id, "upstream": "", "size": "0",
                "error-msg": "", "last-update": "2023-01-01T00:00:00Z",
                "last-started": "2023-01-01T00:00:00Z", "last-ended": "2023-01-01T00:00:00Z",
                "next-scheduled": "2023-01-01T00:00:00Z", "status": "success", "is-master": true
            }).to_string()))
            .unwrap(),
    ).await.unwrap();

    // 2. Update Size
    let res = app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri(format!("/workers/{}/jobs/size", worker_id))
            .header("Content-Type", "application/json")
            .body(Body::from(json!({"name": mirror_id, "size": "1TB"}).to_string()))
            .unwrap(),
    ).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 3. Update Schedules
    let future_time = "2030-01-01T00:00:00Z";
    let res = app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri(format!("/workers/{}/schedules", worker_id))
            .header("Content-Type", "application/json")
            .body(Body::from(json!({
                "schedules": [
                    { "name": mirror_id, "next_schedule": future_time }
                ]
            }).to_string()))
            .unwrap(),
    ).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 4. Verify in List
    let res = app.clone().oneshot(
        Request::builder().method("GET").uri("/jobs").body(Body::empty()).unwrap()
    ).await.unwrap();
    let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let jobs: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    let job = jobs.iter().find(|j| j["name"] == mirror_id).unwrap();
    
    assert_eq!(job["size"], "1TB");
    assert!(job["next_schedule"].as_str().unwrap().contains("2030-01-01"));
}
