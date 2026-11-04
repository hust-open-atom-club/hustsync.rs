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
    reach::Reach,
    schedule::ScheduleQueue,
    server::{AppState, make_http_server},
};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tower::ServiceExt;

type Res<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn state(semaphore: Arc<Semaphore>, metrics: Metrics) -> Arc<AppState> {
    Arc::new(AppState {
        jobs: Arc::new(RwLock::new(HashMap::new())),
        schedule_queue: Arc::new(Mutex::new(ScheduleQueue::new())),
        semaphore,
        concurrent: 3,
        reload_tx: None,
        metrics: Arc::new(metrics),
    })
}

async fn get(s: &Arc<AppState>, uri: &str) -> Res<(StatusCode, String)> {
    let resp = make_http_server(Arc::clone(s))
        .oneshot(Request::get(uri).body(Body::empty())?)
        .await?;
    let status = resp.status();
    let body = resp.into_body().collect().await?.to_bytes();
    Ok((status, String::from_utf8(body.to_vec())?))
}

#[tokio::test]
async fn metrics_report_jobs_queue_and_concurrency() -> Res {
    let semaphore = Arc::new(Semaphore::new(3));
    let _held = Arc::clone(&semaphore).acquire_owned().await?;
    let s = state(semaphore, Metrics::default());
    for uri in ["/metrics", "/metrics/all", "/metrics/jobs"] {
        let (status, text) = get(&s, uri).await?;
        assert_eq!(status, StatusCode::OK, "{uri}");
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

#[tokio::test]
async fn unknown_groups_are_404() -> Res {
    let s = state(Arc::new(Semaphore::new(3)), Metrics::default());
    let (status, body) = get(&s, "/metrics/mirrors").await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        body,
        "unknown metric group `mirrors`; groups are jobs, manager, process"
    );
    Ok(())
}

#[tokio::test]
async fn cached_jobs_keep_their_values() -> Res {
    let cfg: toml::Table = toml::from_str("cache = { jobs = 3600 }")?;
    let semaphore = Arc::new(Semaphore::new(3));
    let s = state(
        Arc::clone(&semaphore),
        Metrics::new(Some(&cfg), Arc::default())?,
    );
    let (_, before) = get(&s, "/metrics/jobs").await?;
    let _held = semaphore.acquire_owned().await?;
    assert_eq!(get(&s, "/metrics/jobs").await?.1, before);
    Ok(())
}

#[tokio::test]
async fn manager_links_show_outages() -> Res {
    let reach = Arc::new(Reach::default());
    reach.ok("http://a:14242");
    reach.fail("http://b:14242", &"connection refused");
    reach.fail("http://b:14242", &"connection refused");
    let s = state(
        Arc::new(Semaphore::new(3)),
        Metrics::new(None, Arc::clone(&reach))?,
    );
    let (status, text) = get(&s, "/metrics/manager").await?;
    assert_eq!(status, StatusCode::OK);
    for line in [
        r#"hustsync_worker_manager_up{manager="http://a:14242"} 1"#,
        r#"hustsync_worker_manager_up{manager="http://b:14242"} 0"#,
        r#"hustsync_worker_manager_failures{manager="http://b:14242"} 2"#,
    ] {
        assert!(
            text.lines().any(|l| l == line),
            "missing `{line}` in\n{text}"
        );
    }
    assert!(text.contains(
        r#"hustsync_worker_manager_last_success_timestamp_seconds{manager="http://a:14242"}"#
    ));
    assert!(!text.contains(
        r#"hustsync_worker_manager_last_success_timestamp_seconds{manager="http://b:14242"}"#
    ));
    Ok(())
}
