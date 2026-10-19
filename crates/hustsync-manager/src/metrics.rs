//! Metrics in groups: sync counters live in memory, the rest is read from the database when asked for.

use std::time::Duration;

use hustsync_config_parser::MetricsConfig;
use hustsync_internal::{
    metrics::{
        self, Board, Families, Res,
        prometheus::{
            GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts,
            Registry, core::Collector, proto::MetricFamily,
        },
    },
    msg::{MirrorStatus, WorkerStatus},
    status::SyncStatus,
};
use tokio::task::JoinSet;

use crate::Manager;

const MIRROR: [&str; 2] = ["mirror", "worker"];

/// Bucket bounds of `hustsync_sync_duration_seconds`, one second to one day.
const TOOK: [f64; 16] = [
    1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1200.0, 1800.0, 3600.0, 7200.0, 14400.0,
    28800.0, 43200.0, 86400.0,
];

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Group {
    Mirrors,
    Workers,
    Syncs,
    Process,
}

impl metrics::Group for Group {
    const ALL: &'static [Self] = &[Self::Mirrors, Self::Workers, Self::Syncs, Self::Process];

    fn name(self) -> &'static str {
        match self {
            Self::Mirrors => "mirrors",
            Self::Workers => "workers",
            Self::Syncs => "syncs",
            Self::Process => "process",
        }
    }
}

/// Counters that live as long as the manager, and the group caches.
pub struct Metrics {
    board: Board<Group>,
    syncs: IntCounterVec,
    took: HistogramVec,
    #[cfg(feature = "push")]
    pub(crate) push: Vec<metrics::push::Target<Group>>,
}

impl Metrics {
    pub fn new(cfg: Option<&MetricsConfig>) -> Result<Self, String> {
        let bad = |e: metrics::prometheus::Error| e.to_string();
        #[cfg(not(feature = "push"))]
        if cfg.is_some_and(|c| !c.push.is_empty()) {
            return Err("[[metrics.push]]: built without the `influx` and `otlp` features".into());
        }
        Ok(Self {
            board: Board::new(cfg)?,
            syncs: IntCounterVec::new(
                Opts::new("hustsync_syncs_total", "Finished sync attempts by result."),
                &["mirror", "worker", "result"],
            )
            .map_err(bad)?,
            took: HistogramVec::new(
                HistogramOpts::new(
                    "hustsync_sync_duration_seconds",
                    "Durations of finished syncs.",
                )
                .buckets(TOOK.to_vec()),
                &["worker", "result"],
            )
            .map_err(bad)?,
            #[cfg(feature = "push")]
            push: metrics::push::Target::all(cfg.map_or(&[][..], |c| &c.push))?,
        })
    }

    /// Counts a finished sync attempt.
    pub fn record(&self, s: &MirrorStatus) {
        if !matches!(s.status, SyncStatus::Success | SyncStatus::Failed) {
            return;
        }
        let result = s.status.as_str();
        self.syncs
            .with_label_values(&[s.name.as_str(), &s.worker, result])
            .inc();
        if s.last_started.timestamp() > 0
            && let Ok(d) = (s.last_ended - s.last_started).to_std()
        {
            self.took
                .with_label_values(&[s.worker.as_str(), result])
                .observe(d.as_secs_f64());
        }
    }
}

/// The group, from the cache when it is fresh.
pub async fn gather(mgr: &Manager, g: Group) -> Res<Families> {
    let m = &mgr.metrics;
    m.board
        .get(g, || async move {
            match g {
                Group::Mirrors => mirrors(mgr),
                Group::Workers => workers(mgr).await,
                Group::Syncs => Ok([m.syncs.collect(), m.took.collect()].concat()),
                Group::Process => metrics::process(),
            }
        })
        .await
}

/// Pushes to each `[[metrics.push]]` target until the manager stops.
#[cfg(feature = "push")]
pub fn push(mgr: &std::sync::Arc<Manager>) -> Vec<tokio::task::JoinHandle<()>> {
    let client = mgr.http_client.clone().unwrap_or_default();
    let origin = metrics::push::Origin {
        service: "hustsync-manager",
        instance: format!("{}:{}", mgr.config.server.addr, mgr.config.server.port),
        start: std::time::SystemTime::now(),
    };
    let weak = std::sync::Arc::downgrade(mgr);
    metrics::push::spawn(mgr.metrics.push.clone(), &client, &origin, move |g| {
        let weak = std::sync::Weak::clone(&weak);
        async move {
            let mgr = weak.upgrade().ok_or("manager stopped")?;
            gather(&mgr, g).await
        }
    })
}

#[cfg(feature = "p8s")]
pub mod serve {
    //! `GET /metrics`, `/metrics/all` and `/metrics/<group>` in the p8s text format.

    use std::sync::Arc;

    use axum::{
        extract::{Path, State},
        http::{StatusCode, header},
        response::{IntoResponse, Response},
    };
    use hustsync_internal::metrics::{self, Families, Group as _, Res};

    use super::{Group, gather};
    use crate::Manager;

    pub async fn all(State(mgr): State<Arc<Manager>>) -> Response {
        reply(gather_all(&mgr).await)
    }

    pub async fn one(State(mgr): State<Arc<Manager>>, Path(name): Path<String>) -> Response {
        if name == "all" {
            return reply(gather_all(&mgr).await);
        }
        let Some(g) = Group::parse(&name) else {
            return (StatusCode::NOT_FOUND, metrics::unknown::<Group>(&name)).into_response();
        };
        reply(gather(&mgr, g).await.map(|f| vec![f]))
    }

    async fn gather_all(mgr: &Manager) -> Res<Vec<Families>> {
        let mut out = Vec::new();
        for &g in Group::ALL {
            out.push(gather(mgr, g).await?);
        }
        Ok(out)
    }

    fn reply(groups: Res<Vec<Families>>) -> Response {
        match groups.and_then(|g| metrics::text(&g)) {
            Ok(body) => ([(header::CONTENT_TYPE, metrics::TEXT)], body).into_response(),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        }
    }
}

fn mirrors(mgr: &Manager) -> Res<Vec<MetricFamily>> {
    let db = mgr.adapter.as_ref().ok_or("database not initialised")?;
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
    let state = add(
        &reg,
        IntGaugeVec::new(
            Opts::new(
                "hustsync_mirror_state",
                "Status: 0 none, 1 failed, 2 success, 3 syncing, 4 pre-syncing, 5 paused, 6 disabled.",
            ),
            &MIRROR,
        )?,
    )?;
    let info = add(
        &reg,
        IntGaugeVec::new(
            Opts::new("hustsync_mirror_info", "Upstream and role of each mirror."),
            &["mirror", "worker", "upstream", "role"],
        )?,
    )?;
    let size = gauge(&reg, "hustsync_mirror_size_bytes", "Size of the mirror.")?;
    let took = gauge(
        &reg,
        "hustsync_mirror_sync_duration_seconds",
        "Duration of the last finished sync.",
    )?;
    let fresh = gauge(
        &reg,
        "hustsync_mirror_last_success_timestamp_seconds",
        "Unix time of the last successful sync.",
    )?;
    let next = gauge(
        &reg,
        "hustsync_mirror_next_sync_timestamp_seconds",
        "Unix time of the next scheduled sync.",
    )?;
    let failures = gauge(
        &reg,
        "hustsync_mirror_failures",
        "Failed runs in a row since the manager started.",
    )?;
    for ((mirror, worker), n) in mgr.streaks.all() {
        failures
            .with_label_values(&[mirror.as_str(), &worker])
            .set(n.into());
    }

    for m in &mirrors {
        let labels = [m.name.as_str(), &m.worker];
        by_status.with_label_values(&[m.status.as_str()]).inc();
        let code = SyncStatus::ALL.iter().position(|s| *s == m.status);
        state
            .with_label_values(&labels)
            .set(i64::try_from(code.unwrap_or_default())?);
        let role = if m.is_master { "master" } else { "slave" };
        info.with_label_values(&[m.name.as_str(), &m.worker, &m.upstream, role])
            .set(1);
        if let Ok(b) = parse_size::parse_size(m.size.replace(',', "")) {
            size.with_label_values(&labels).set(b as f64);
        }
        if m.last_started.timestamp() > 0
            && let Ok(d) = (m.last_ended - m.last_started).to_std()
        {
            took.with_label_values(&labels).set(d.as_secs_f64());
        }
        for (g, t) in [(&fresh, m.last_update), (&next, m.next_scheduled)] {
            if t.timestamp() > 0 {
                g.with_label_values(&labels).set(t.timestamp() as f64);
            }
        }
    }
    Ok(reg.gather())
}

async fn workers(mgr: &Manager) -> Res<Vec<MetricFamily>> {
    let db = mgr.adapter.as_ref().ok_or("database not initialised")?;
    let workers = db.list_workers()?;
    let up = probe(mgr, &workers).await;
    let reg = Registry::new();

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
    let seen = add(
        &reg,
        GaugeVec::new(
            Opts::new(
                "hustsync_worker_last_online_timestamp_seconds",
                "Unix time the worker last reached the manager.",
            ),
            &["worker"],
        )?,
    )?;
    for w in &workers {
        if w.last_online.timestamp() > 0 {
            seen.with_label_values(&[w.id.as_str()])
                .set(w.last_online.timestamp() as f64);
        }
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
    Ok(reg.gather())
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

fn gauge(reg: &Registry, name: &str, help: &str) -> Res<GaugeVec> {
    Ok(add(reg, GaugeVec::new(Opts::new(name, help), &MIRROR)?)?)
}

fn add<C: Collector + Clone + 'static>(reg: &Registry, c: C) -> metrics::prometheus::Result<C> {
    reg.register(Box::new(c.clone()))?;
    Ok(c)
}
