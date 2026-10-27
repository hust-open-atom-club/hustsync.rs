//! Metrics split into named groups, gathered on demand and cached per group.

#[cfg(feature = "influx")]
pub mod influx;
#[cfg(feature = "otlp")]
pub mod otlp;
#[cfg(feature = "push")]
pub mod push;

use std::{
    collections::HashMap,
    error::Error,
    hash::Hash,
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant},
};

use hustsync_config_parser::MetricsConfig;
pub use prometheus;
use prometheus::{IntGaugeVec, Opts, core::Collector, proto::MetricFamily};

pub type Res<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// One gathered group.
pub type Families = Arc<[MetricFamily]>;

/// The metric groups of a service, as named in `/metrics/<group>` and `[metrics]`.
pub trait Group: Copy + Eq + Hash + Send + Sync + 'static {
    const ALL: &'static [Self];

    fn name(self) -> &'static str;

    fn parse(name: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|g| g.name() == name)
    }
}

/// The groups behind configured names, or which names exist.
pub fn groups<G: Group>(names: &[String]) -> Result<Vec<G>, String> {
    names
        .iter()
        .map(|n| G::parse(n).ok_or_else(|| unknown::<G>(n)))
        .collect()
}

/// Says that `name` is not a group and lists the groups.
pub fn unknown<G: Group>(name: &str) -> String {
    let all: Vec<_> = G::ALL.iter().map(|g| g.name()).collect();
    format!(
        "unknown metric group `{name}`; groups are {}",
        all.join(", ")
    )
}

/// Per-group caches in front of the code that gathers each group.
pub struct Board<G> {
    ttl: HashMap<G, Duration>,
    memo: Mutex<HashMap<G, (Instant, Families)>>,
}

impl<G> Default for Board<G> {
    fn default() -> Self {
        Self {
            ttl: HashMap::new(),
            memo: Mutex::default(),
        }
    }
}

impl<G: Group> Board<G> {
    pub fn new(cfg: Option<&MetricsConfig>) -> Result<Self, String> {
        let ttl = cfg
            .map(|c| &c.cache)
            .into_iter()
            .flatten()
            .map(|(name, secs)| {
                G::parse(name)
                    .map(|g| (g, Duration::from_secs(*secs)))
                    .ok_or_else(|| unknown::<G>(name))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            ttl,
            memo: Mutex::default(),
        })
    }

    /// The group as cached, or gathered anew when it is not cached or has gone stale; empty families are dropped,
    /// as the text format has no way to write them.
    pub async fn get<F, Fut>(&self, g: G, gather: F) -> Res<Families>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Res<Vec<MetricFamily>>>,
    {
        let ttl = self.ttl.get(&g).copied().unwrap_or_default();
        if let Some(hit) = self.hit(g, ttl) {
            return Ok(hit);
        }
        let fams: Families = gather()
            .await?
            .into_iter()
            .filter(|f| !f.get_metric().is_empty())
            .collect();
        if !ttl.is_zero() {
            self.lock().insert(g, (Instant::now(), Arc::clone(&fams)));
        }
        Ok(fams)
    }

    fn hit(&self, g: G, ttl: Duration) -> Option<Families> {
        self.lock()
            .get(&g)
            .filter(|(at, _)| at.elapsed() < ttl)
            .map(|(_, f)| Arc::clone(f))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<G, (Instant, Families)>> {
        self.memo.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// `Content-Type` of the p8s text format.
#[cfg(feature = "p8s")]
pub const TEXT: &str = prometheus::TEXT_FORMAT;

/// The given groups in the p8s text format, in order.
#[cfg(feature = "p8s")]
pub fn text(groups: &[Families]) -> Res<String> {
    let enc = prometheus::TextEncoder::new();
    groups.iter().try_fold(String::new(), |mut out, g| {
        out.push_str(&enc.encode_to_string(g)?);
        Ok(out)
    })
}

/// `hustsync_build_info` and, on Linux, the process's CPU, memory, file and thread metrics.
pub fn process() -> Res<Vec<MetricFamily>> {
    let info = IntGaugeVec::new(
        Opts::new("hustsync_build_info", "Version of the running build."),
        &["version"],
    )?;
    info.with_label_values(&[env!("CARGO_PKG_VERSION")]).set(1);
    #[cfg_attr(not(target_os = "linux"), allow(unused_mut))]
    let mut out = info.collect();
    #[cfg(target_os = "linux")]
    {
        use prometheus::process_collector::ProcessCollector;
        static PROC: std::sync::OnceLock<ProcessCollector> = std::sync::OnceLock::new();
        out.extend(PROC.get_or_init(ProcessCollector::for_self).collect());
    }
    Ok(out)
}
