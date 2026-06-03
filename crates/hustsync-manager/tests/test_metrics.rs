use axum::{Router, routing::get};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use hustsync_manager::metrics::metrics_handler;
use hustsync_manager::database::{make_db_adapter, DbAdapterTrait};
use hustsync_manager::server::Manager;
use hustsync_config_parser::ManagerConfig;
use std::sync::Arc;

/// Test that the metrics endpoint returns Prometheus-formatted output.
#[tokio::test]
async fn metrics_endpoint_returns_prometheus_format() {
    // Create a temporary database
    let tmp_file = tempfile::NamedTempFile::new().unwrap();
    let db_path = tmp_file.path().to_str().unwrap();
    let adapter = make_db_adapter("redb", db_path).unwrap();
    adapter.init().unwrap();

    let config = Arc::new(ManagerConfig::default());
    let manager = Arc::new(Manager {
        config,
        adapter: Some(Arc::from(adapter)),
        http_client: None,
    });

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(manager);

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
    assert!(text.contains("# HELP hustsync_manager_mirrors_total"));
    assert!(text.contains("# TYPE hustsync_manager_mirrors_total gauge"));
    assert!(text.contains("hustsync_manager_mirrors_total"));

    assert!(text.contains("# HELP hustsync_manager_workers_online"));
    assert!(text.contains("# TYPE hustsync_manager_workers_online gauge"));
    assert!(text.contains("hustsync_manager_workers_online"));

    assert!(text.contains("# HELP hustsync_manager_sync_failures_total"));
    assert!(text.contains("# TYPE hustsync_manager_sync_failures_total counter"));

    assert!(text.contains("# HELP hustsync_manager_sync_successes_total"));
    assert!(text.contains("# TYPE hustsync_manager_sync_successes_total counter"));
}
