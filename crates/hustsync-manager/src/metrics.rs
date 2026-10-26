//! Prometheus metrics: sync counters kept in memory, gauges rebuilt from the database per scrape.

use std::{error::Error, sync::Arc, time::Duration};

use axum::{
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use hustsync_internal::{
    msg::{MirrorStatus, WorkerStatus},
    status::SyncStatus,
};
use prometheus::{
    GaugeVec, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TEXT_FORMAT, TextEncoder,
    core::Collector,
};
use tokio::task::JoinSet;

use crate::Manager;

type Res<T> = Result<T, Box<dyn Error + Send + Sync>>;

const MIRROR: [&str; 2] = ["mirror", "worker"];

/// Counters that live as long as the manager.
pub struct Metrics {
    reg: Registry,
    syncs: IntCounterVec,
}

impl Metrics {
    pub fn new() -> prometheus::Result<Self> {
        let reg = Registry::new();
        let syncs = add(
            &reg,
            IntCounterVec::new(
                Opts::new("hustsync_syncs_total", "Finished sync attempts by result."),
                &["mirror", "worker", "result"],
            )?,
        )?;
        Ok(Self { reg, syncs })
    }

    /// Counts a finished sync attempt.
    pub fn record(&self, s: &MirrorStatus) {
        if matches!(s.status, SyncStatus::Success | SyncStatus::Failed) {
            self.syncs
                .with_label_values(&[s.name.as_str(), &s.worker, s.status.as_str()])
                .inc();
        }
    }
}

/// `GET /metrics` in the Prometheus text format.
pub async fn serve(State(mgr): State<Arc<Manager>>) -> Response {
    match encode(&mgr).await {
        Ok(body) => ([(header::CONTENT_TYPE, TEXT_FORMAT)], body).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn encode(mgr: &Manager) -> Res<String> {
    let mut all = mgr.metrics.reg.gather();
    all.extend(snapshot(mgr).await?.gather());
    Ok(TextEncoder::new().encode_to_string(&all)?)
}

/// Gauges describing mirrors and workers right now.
async fn snapshot(mgr: &Manager) -> Res<Registry> {
    let db = mgr.adapter.as_ref().ok_or("database not initialised")?;
    let workers = db.list_workers()?;
    let mirrors = db.list_all_mirror_status()?;
    let reg = Registry::new();

    let by_status = add(
        &reg,
        IntGaugeVec::new(
            Opts::new("hustsync_mirrors", "Mirrors by status."),
            &["status"],
        )?,
    )?;
    for s in SyncStatus::ALL {
        by_status.with_label_values(&[s.as_str()]).set(0);
    }

    let took = add(
        &reg,
        GaugeVec::new(
            Opts::new(
                "hustsync_mirror_sync_duration_seconds",
                "Duration of the last finished sync.",
            ),
            &MIRROR,
        )?,
    )?;
    let fresh = add(
        &reg,
        IntGaugeVec::new(
            Opts::new(
                "hustsync_mirror_last_success_timestamp_seconds",
                "Unix time of the last successful sync.",
            ),
            &MIRROR,
        )?,
    )?;
    for m in &mirrors {
        let labels = [m.name.as_str(), &m.worker];
        by_status.with_label_values(&[m.status.as_str()]).inc();
        if m.last_started.timestamp() > 0
            && let Ok(d) = (m.last_ended - m.last_started).to_std()
        {
            took.with_label_values(&labels).set(d.as_secs_f64());
        }
        if m.last_update.timestamp() > 0 {
            fresh
                .with_label_values(&labels)
                .set(m.last_update.timestamp());
        }
    }

    let up = probe(mgr, &workers).await;
    let worker_up = add(
        &reg,
        IntGaugeVec::new(
            Opts::new("hustsync_worker_up", "Whether the worker answers HTTP."),
            &["worker"],
        )?,
    )?;
    for (id, ok) in &up {
        worker_up
            .with_label_values(&[id.as_str()])
            .set(i64::from(*ok));
    }
    add(
        &reg,
        IntGauge::new("hustsync_workers", "Registered workers.")?,
    )?
    .set(i64::try_from(workers.len())?);
    add(
        &reg,
        IntGauge::new("hustsync_workers_online", "Workers answering HTTP.")?,
    )?
    .set(i64::try_from(up.iter().filter(|(_, ok)| *ok).count())?);

    Ok(reg)
}

/// Contacts every worker; any HTTP response counts as online.
async fn probe(mgr: &Manager, workers: &[WorkerStatus]) -> Vec<(String, bool)> {
    let Some(client) = &mgr.http_client else {
        return workers.iter().map(|w| (w.id.clone(), false)).collect();
    };
    let mut set = JoinSet::new();
    for w in workers {
        let req = client.get(&w.url).timeout(Duration::from_secs(2));
        let id = w.id.clone();
        set.spawn(async move { (id, req.send().await.is_ok()) });
    }
    set.join_all().await
}

fn add<C: Collector + Clone + 'static>(reg: &Registry, c: C) -> prometheus::Result<C> {
    reg.register(Box::new(c.clone()))?;
    Ok(c)
}
