use axum::{Router, routing::get};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use hustsync_worker::metrics::metrics_handler;
use hustsync_worker::server::AppState;
use hustsync_worker::schedule::ScheduleQueue;

/// Test that the worker metrics endpoint returns Prometheus-formatted output.
#[tokio::test]
async fn worker_metrics_endpoint_returns_prometheus_format() {
    let state = Arc::new(AppState {
        jobs: Arc::new(RwLock::new(HashMap::new())),
        schedule_queue: Arc::new(tokio::sync::Mutex::new(ScheduleQueue::new())),
    });

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(state);

    let response = app
        .oneshot(Request::builder().uri("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();

    // Verify Prometheus format headers
    assert!(text.contains("# HELP hustsync_worker_mirrors_total"));
    assert!(text.contains("# TYPE hustsync_worker_mirrors_total gauge"));
    assert!(text.contains("hustsync_worker_mirrors_total"));

    assert!(text.contains("# HELP hustsync_worker_queue_length"));
    assert!(text.contains("# TYPE hustsync_worker_queue_length gauge"));
    assert!(text.contains("hustsync_worker_queue_length"));

    assert!(text.contains("# HELP hustsync_worker_concurrent_tasks"));
    assert!(text.contains("# TYPE hustsync_worker_concurrent_tasks gauge"));
    assert!(text.contains("hustsync_worker_concurrent_tasks"));

    assert!(text.contains("# HELP hustsync_worker_sync_failures_total"));
    assert!(text.contains("# TYPE hustsync_worker_sync_failures_total counter"));

    assert!(text.contains("# HELP hustsync_worker_sync_successes_total"));
    assert!(text.contains("# TYPE hustsync_worker_sync_successes_total counter"));
}
