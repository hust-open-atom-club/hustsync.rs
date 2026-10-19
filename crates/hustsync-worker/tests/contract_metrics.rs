//! `GET /metrics` and `/metrics/<group>` on the worker control server.

#![cfg(feature = "p8s")]

use std::{collections::HashMap, sync::Arc};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt as _;
use hustsync_worker::{
    metrics::Metrics,
    schedule::ScheduleQueue,
    server::{AppState, make_http_server},
};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tower::ServiceExt;

#[tokio::test]
async fn metrics_report_jobs_queue_and_concurrency() -> Result<(), Box<dyn std::error::Error>> {
    let semaphore = Arc::new(Semaphore::new(3));
    let _held = Arc::clone(&semaphore).acquire_owned().await?;
    let state = Arc::new(AppState {
        jobs: Arc::new(RwLock::new(HashMap::new())),
        schedule_queue: Arc::new(Mutex::new(ScheduleQueue::new())),
        semaphore,
        concurrent: 3,
        reload_tx: None,
        metrics: Arc::new(Metrics::default()),
    });
    for uri in ["/metrics", "/metrics/all", "/metrics/jobs"] {
        let resp = make_http_server(Arc::clone(&state))
            .oneshot(Request::get(uri).body(Body::empty())?)
            .await?;
        assert_eq!(resp.status(), StatusCode::OK, "{uri}");
        let text = String::from_utf8(resp.into_body().collect().await?.to_bytes().to_vec())?;
        for line in [
            "hustsync_worker_jobs 0",
            "hustsync_worker_jobs_running 1",
            "hustsync_worker_jobs_queued 0",
            "hustsync_worker_concurrency 3",
        ] {
            assert!(
                text.lines().any(|l| l == line),
                "{uri}: missing `{line}` in\n{text}"
            );
        }
    }
    Ok(())
}
