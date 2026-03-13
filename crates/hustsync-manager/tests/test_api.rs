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
    assert_ne!(res_json["last_online"], "2023-01-01T00:00:00Z"); // Should be updated to now
}

#[tokio::test]
async fn test_update_job_status_logic() {
    let manager = *SHARED_MANAGER;
    let app = manager.engine.clone();

    // 1. Register worker
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

    // 2. First report: Success
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

    // 3. Update to PreSyncing -> should update last_started
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
    let last_started_saved = res_json["last-started"].as_str().unwrap().to_string();

    // 4. Update to Success -> should update last_ended
    let status_success_final = json!({
        "name": mirror_id,
        "worker": worker_id,
        "upstream": "http://deb.debian.org",
        "size": "100GB",
        "error-msg": "",
        "last-update": "2023-01-01T00:00:00Z",
        "last-started": last_started_saved,
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
            .body(Body::from(status_success_final.to_string()))
            .unwrap(),
    ).await.unwrap();

    let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let res_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_ne!(res_json["last-ended"], "2023-01-01T00:00:00Z");
    assert_eq!(res_json["last-started"], last_started_saved); // Should be preserved
}

#[tokio::test]
async fn test_list_query_apis() {
    let manager = *SHARED_MANAGER;
    let app = manager.engine.clone();

    // 1. Register a worker with a sensitive token
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

    // 2. Report a job
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

    // 3. Query workers list -> check redaction
    let res = app.clone().oneshot(
        Request::builder().method("GET").uri("/workers").body(Body::empty()).unwrap()
    ).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let workers: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    
    let test_worker = workers.iter().find(|w| w["id"] == worker_id).expect("Worker not found");
    assert_eq!(test_worker["token"], "REDACTED");

    // 4. Query jobs list -> check WebMirrorStatus format
    let res = app.clone().oneshot(
        Request::builder().method("GET").uri("/jobs").body(Body::empty()).unwrap()
    ).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let jobs: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    
    let test_job = jobs.iter().find(|j| j["name"] == "archlinux").expect("Job not found");

    assert!(test_job.as_object().unwrap().contains_key("last_update"));
    assert!(test_job.as_object().unwrap().contains_key("next_schedule"));
}
