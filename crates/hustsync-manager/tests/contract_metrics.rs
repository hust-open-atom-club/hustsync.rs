//! `GET /metrics` and `/metrics/<group>`: sync counters, mirror gauges, live worker probes and pushes.

#![cfg(feature = "p8s")]

mod contract;

use std::collections::BTreeSet;

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
    routing::get,
};
use serde_json::{Value, json};
use tower::ServiceExt;

type Res<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn post(app: &Router, uri: &str, body: Value) -> Res<StatusCode> {
    let req = Request::post(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))?;
    Ok(app.clone().oneshot(req).await?.status())
}

async fn register(app: &Router, id: &str, url: &str) -> Res {
    let w = json!({
        "id": id, "url": url, "token": "",
        "last_online": "1970-01-01T00:00:00Z", "last_register": "1970-01-01T00:00:00Z"
    });
    assert_eq!(post(app, "/workers", w).await?, StatusCode::OK);
    Ok(())
}

async fn report(app: &Router, worker: &str, mirror: &str, status: &str, size: &str) -> Res {
    let zero = "1970-01-01T00:00:00Z";
    let s = json!({
        "name": mirror, "worker": worker, "upstream": "rsync://up/m/", "size": size, "error_msg": "",
        "last_update": zero, "last_started": zero, "last_ended": zero,
        "next_schedule": "2030-01-01T00:00:00Z", "status": status, "is_master": true
    });
    let uri = format!("/workers/{worker}/jobs/{mirror}");
    assert_eq!(post(app, &uri, s).await?, StatusCode::OK);
    Ok(())
}

async fn scrape(app: &Router, uri: &str) -> Res<String> {
    let resp = app
        .clone()
        .oneshot(Request::get(uri).body(Body::empty())?)
        .await?;
    assert_eq!(resp.status(), StatusCode::OK, "{uri}");
    let ty = resp.headers()[header::CONTENT_TYPE].to_str()?;
    assert!(ty.starts_with("text/plain"), "{ty}");
    Ok(String::from_utf8(contract::body_bytes(resp).await)?)
}

fn families(text: &str) -> BTreeSet<String> {
    text.lines()
        .filter_map(|l| l.strip_prefix("# TYPE ")?.split(' ').next())
        .map(str::to_owned)
        .collect()
}

async fn seeded() -> Res<(Router, tempfile::TempDir)> {
    let (app, dir) = contract::spawn_manager();
    let live = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/", live.local_addr()?);
    tokio::spawn(axum::serve(live, Router::new().route("/", get(|| async { "ok" }))).into_future());

    register(&app, "w1", &url).await?;
    register(&app, "w2", "http://127.0.0.1:9/").await?;
    for (mirror, status, size) in [
        ("m1", "pre-syncing", ""),
        ("m1", "success", "1.5G"),
        ("m2", "failed", "unknown"),
        ("m2", "failed", "unknown"),
    ] {
        report(&app, "w1", mirror, status, size).await?;
    }
    Ok((app, dir))
}

#[tokio::test]
async fn metrics_cover_syncs_mirrors_and_workers() -> Res {
    let (app, _dir) = seeded().await?;
    let text = scrape(&app, "/metrics").await?;
    for line in [
        r#"hustsync_syncs_total{mirror="m1",result="success",worker="w1"} 1"#,
        r#"hustsync_syncs_total{mirror="m2",result="failed",worker="w1"} 2"#,
        r#"hustsync_sync_duration_seconds_count{result="success",worker="w1"} 1"#,
        r#"hustsync_mirrors{status="success"} 1"#,
        r#"hustsync_mirrors{status="failed"} 1"#,
        r#"hustsync_mirrors{status="syncing"} 0"#,
        r#"hustsync_mirror_state{mirror="m1",worker="w1"} 2"#,
        r#"hustsync_mirror_state{mirror="m2",worker="w1"} 1"#,
        r#"hustsync_mirror_info{mirror="m1",role="master",upstream="rsync://up/m/",worker="w1"} 1"#,
        r#"hustsync_mirror_size_bytes{mirror="m1",worker="w1"} 1500000000"#,
        r#"hustsync_mirror_next_sync_timestamp_seconds{mirror="m1",worker="w1"} 1893456000"#,
        "hustsync_workers 2",
        "hustsync_workers_online 1",
        r#"hustsync_worker_up{worker="w1"} 1"#,
        r#"hustsync_worker_up{worker="w2"} 0"#,
    ] {
        assert!(
            text.lines().any(|l| l == line),
            "missing `{line}` in\n{text}"
        );
    }
    for prefix in [
        r#"hustsync_mirror_sync_duration_seconds{mirror="m1",worker="w1"} "#,
        r#"hustsync_mirror_last_success_timestamp_seconds{mirror="m1",worker="w1"} "#,
        r#"hustsync_worker_last_online_timestamp_seconds{worker="w1"} "#,
        r#"hustsync_build_info{version=""#,
    ] {
        assert!(
            text.lines().any(|l| l.starts_with(prefix)),
            "missing `{prefix}` in\n{text}"
        );
    }
    assert!(!text.contains(r#"hustsync_mirror_last_success_timestamp_seconds{mirror="m2""#));
    assert!(!text.contains(r#"hustsync_mirror_size_bytes{mirror="m2""#));
    Ok(())
}

#[tokio::test]
async fn groups_split_the_whole_without_overlap() -> Res {
    let (app, _dir) = seeded().await?;
    let all = families(&scrape(&app, "/metrics").await?);
    assert_eq!(all, families(&scrape(&app, "/metrics/all").await?));
    let mut union = BTreeSet::new();
    for g in ["mirrors", "workers", "syncs", "process"] {
        let fams = families(&scrape(&app, &format!("/metrics/{g}")).await?);
        assert!(!fams.is_empty(), "{g} is empty");
        assert!(union.is_disjoint(&fams), "{g} overlaps: {fams:?}");
        union.extend(fams);
    }
    assert_eq!(union, all);
    Ok(())
}

#[cfg(feature = "influx")]
#[tokio::test]
async fn pushes_reach_the_target_as_line_protocol() -> Res {
    use std::{sync::Arc, time::Duration};

    use axum::{http::HeaderMap, routing::post as on_post};
    use hustsync_config_parser::{MetricsConfig, PushConfig};
    use tokio::sync::mpsc;

    let (tx, mut rx) = mpsc::channel::<(Option<String>, String)>(4);
    let sink = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/telegraf", sink.local_addr()?);
    let recv = Router::new().route(
        "/telegraf",
        on_post(move |headers: HeaderMap, body: String| {
            let tx = tx.clone();
            async move {
                let auth = headers
                    .get(header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned);
                let _ = tx.send((auth, body)).await;
                StatusCode::NO_CONTENT
            }
        }),
    );
    tokio::spawn(axum::serve(sink, recv).into_future());

    let cfg = MetricsConfig {
        cache: Default::default(),
        push: vec![PushConfig {
            url,
            format: Default::default(),
            interval: 1,
            groups: Some(vec!["mirrors".into(), "syncs".into()]),
            headers: [("Authorization".into(), "Token t".into())].into(),
        }],
    };
    let (mgr, _dir) = contract::manager_with(Some(cfg));
    let app = Arc::clone(&mgr).make_router();
    report(&app, "w1", "m1", "success", "").await?;
    let run = tokio::spawn(Arc::clone(&mgr).run(std::future::pending()));

    let (auth, body) = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await?
        .ok_or("push channel closed")?;
    run.abort();
    assert_eq!(auth.as_deref(), Some("Token t"));
    let lines: Vec<_> = body
        .lines()
        .filter_map(|l| l.rsplit_once(' ').map(|(head, _)| head))
        .collect();
    for want in [
        "prometheus,status=success hustsync_mirrors=1",
        "prometheus,mirror=m1,worker=w1 hustsync_mirror_state=2",
        "prometheus,mirror=m1,result=success,worker=w1 hustsync_syncs_total=1",
    ] {
        assert!(lines.contains(&want), "missing `{want}` in\n{body}");
    }
    assert!(
        !body.contains("hustsync_workers"),
        "workers were not asked for:\n{body}"
    );
    Ok(())
}
