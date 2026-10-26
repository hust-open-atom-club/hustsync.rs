//! The worker's metrics in groups: jobs, queue and concurrency, its links to the managers, and the process.

use std::sync::Arc;

use hustsync_config_parser::MetricsConfig;
#[cfg(feature = "metrics")]
use hustsync_internal::metrics::{self, Board, Families, Res, prometheus::proto::MetricFamily};

#[cfg(feature = "metrics")]
use crate::server::AppState;

#[cfg(feature = "metrics")]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Group {
    Jobs,
    Manager,
    Process,
}

#[cfg(feature = "metrics")]
impl metrics::Group for Group {
    const ALL: &'static [Self] = &[Self::Jobs, Self::Manager, Self::Process];

    fn name(self) -> &'static str {
        match self {
            Self::Jobs => "jobs",
            Self::Manager => "manager",
            Self::Process => "process",
        }
    }
}

/// Group caches and push targets; empty when the worker is built without metrics.
#[derive(Default)]
pub struct Metrics {
    #[cfg(feature = "metrics")]
    board: Board<Group>,
    #[cfg(feature = "push")]
    push: Vec<metrics::push::Target<Group>>,
    #[cfg(feature = "metrics")]
    reach: Arc<crate::reach::Reach>,
}

impl Metrics {
    pub fn new(
        cfg: Option<&MetricsConfig>,
        reach: Arc<crate::reach::Reach>,
    ) -> Result<Self, String> {
        #[cfg(not(feature = "metrics"))]
        let _ = reach;
        #[cfg(not(feature = "metrics"))]
        if cfg.is_some() {
            return Err("[metrics]: built without the `p8s`, `influx` and `otlp` features".into());
        }
        #[cfg(all(feature = "metrics", not(feature = "push")))]
        if cfg.is_some_and(|c| !c.push.is_empty()) {
            return Err("[[metrics.push]]: built without the `influx` and `otlp` features".into());
        }
        Ok(Self {
            #[cfg(feature = "metrics")]
            board: Board::new(cfg)?,
            #[cfg(feature = "push")]
            push: metrics::push::Target::all(cfg.map_or(&[][..], |c| &c.push))?,
            #[cfg(feature = "metrics")]
            reach,
        })
    }
}

/// The group, from the cache when it is fresh.
#[cfg(feature = "metrics")]
pub async fn gather(s: &AppState, g: Group) -> Res<Families> {
    s.metrics
        .board
        .get(g, || async move {
            match g {
                Group::Jobs => jobs(s).await,
                Group::Manager => links(&s.metrics.reach),
                Group::Process => metrics::process(),
            }
        })
        .await
}

#[cfg(feature = "metrics")]
async fn jobs(s: &AppState) -> Res<Vec<MetricFamily>> {
    use hustsync_internal::metrics::prometheus::{IntGauge, core::Collector};

    let jobs = s.jobs.read().await.len();
    let queued = s.schedule_queue.lock().await.len();
    let running = s.concurrent.saturating_sub(s.semaphore.available_permits());
    let gauges = [
        ("hustsync_worker_jobs", "Configured mirror jobs.", jobs),
        (
            "hustsync_worker_jobs_running",
            "Jobs holding a concurrency slot.",
            running,
        ),
        (
            "hustsync_worker_jobs_queued",
            "Jobs scheduled for a later run.",
            queued,
        ),
        (
            "hustsync_worker_concurrency",
            "Maximum concurrent jobs.",
            s.concurrent,
        ),
    ];
    let mut out = Vec::with_capacity(gauges.len());
    for (name, help, v) in gauges {
        let g = IntGauge::new(name, help)?;
        g.set(i64::try_from(v)?);
        out.extend(g.collect());
    }
    Ok(out)
}

#[cfg(feature = "metrics")]
fn links(reach: &crate::reach::Reach) -> Res<Vec<MetricFamily>> {
    use hustsync_internal::metrics::prometheus::{GaugeVec, IntGaugeVec, Opts, core::Collector};

    let up = IntGaugeVec::new(
        Opts::new(
            "hustsync_worker_manager_up",
            "Whether the manager took the last request.",
        ),
        &["manager"],
    )?;
    let failures = IntGaugeVec::new(
        Opts::new(
            "hustsync_worker_manager_failures",
            "Failed requests in a row.",
        ),
        &["manager"],
    )?;
    let last = GaugeVec::new(
        Opts::new(
            "hustsync_worker_manager_last_success_timestamp_seconds",
            "Unix time of the last request the manager took.",
        ),
        &["manager"],
    )?;
    for (m, l) in reach.all() {
        up.with_label_values(&[m.as_str()]).set(i64::from(l.up));
        failures
            .with_label_values(&[m.as_str()])
            .set(l.failures.into());
        if let Some(t) = l.last_ok {
            last.with_label_values(&[m.as_str()])
                .set(t.timestamp() as f64);
        }
    }
    Ok([up.collect(), failures.collect(), last.collect()].concat())
}

/// Pushes to each `[[metrics.push]]` target until the handles are aborted.
#[cfg(feature = "push")]
pub fn push(
    s: &std::sync::Arc<AppState>,
    worker: String,
    client: &reqwest::Client,
) -> Vec<tokio::task::JoinHandle<()>> {
    let origin = metrics::push::Origin {
        service: "hustsync-worker",
        instance: worker,
        start: std::time::SystemTime::now(),
    };
    let weak = std::sync::Arc::downgrade(s);
    metrics::push::spawn(s.metrics.push.clone(), client, &origin, move |g| {
        let weak = std::sync::Weak::clone(&weak);
        async move {
            let s = weak.upgrade().ok_or("worker stopped")?;
            gather(&s, g).await
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
    use crate::server::AppState;

    pub async fn all(State(s): State<Arc<AppState>>) -> Response {
        reply(gather_all(&s).await)
    }

    pub async fn one(State(s): State<Arc<AppState>>, Path(name): Path<String>) -> Response {
        if name == "all" {
            return reply(gather_all(&s).await);
        }
        let Some(g) = Group::parse(&name) else {
            return (StatusCode::NOT_FOUND, metrics::unknown::<Group>(&name)).into_response();
        };
        reply(gather(&s, g).await.map(|f| vec![f]))
    }

    async fn gather_all(s: &AppState) -> Res<Vec<Families>> {
        let mut out = Vec::new();
        for &g in Group::ALL {
            out.push(gather(s, g).await?);
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
