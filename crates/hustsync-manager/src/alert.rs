//! Failed runs in a row per mirror, and the `hustsync::alert` events when a streak starts and ends.

use std::{
    collections::HashMap,
    sync::{Mutex, PoisonError},
};

use hustsync_internal::{msg::MirrorStatus, status::SyncStatus};

#[derive(Default)]
struct Run {
    failures: u32,
    counted: bool,
}

/// Counts failed runs; retries within a run count once, as a run starts with `pre-syncing`.
pub struct Streaks {
    limit: u32,
    runs: Mutex<HashMap<(String, String), Run>>,
}

impl Streaks {
    pub fn new(limit: u32) -> Self {
        Self {
            limit,
            runs: Mutex::default(),
        }
    }

    /// Updates the mirror's streak; logs `mirror failing` when it reaches the limit and `mirror recovered` when
    /// a success ends a streak that did.
    pub fn record(&self, s: &MirrorStatus) {
        let (before, after) = step(
            &mut self.runs.lock().unwrap_or_else(PoisonError::into_inner),
            (s.name.clone(), s.worker.clone()),
            s.status,
        );
        if self.limit == 0 {
            return;
        }
        if before < self.limit && after >= self.limit {
            tracing::error!(
                target: "hustsync::alert",
                mirror = %s.name,
                worker = %s.worker,
                failures = after,
                reason = %s.error_msg,
                "mirror failing"
            );
        } else if before >= self.limit && after == 0 {
            tracing::info!(
                target: "hustsync::alert",
                mirror = %s.name,
                worker = %s.worker,
                failures = before,
                "mirror recovered"
            );
        }
    }

    /// Current streaks as `((mirror, worker), failures)`.
    pub fn all(&self) -> Vec<((String, String), u32)> {
        self.runs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(k, r)| (k.clone(), r.failures))
            .collect()
    }
}

/// Failures before and after `status` for the mirror at `key`.
fn step(
    runs: &mut HashMap<(String, String), Run>,
    key: (String, String),
    status: SyncStatus,
) -> (u32, u32) {
    let run = runs.entry(key).or_default();
    let before = run.failures;
    match status {
        SyncStatus::PreSyncing => run.counted = false,
        SyncStatus::Failed if !run.counted => {
            run.failures += 1;
            run.counted = true;
        }
        SyncStatus::Success => *run = Run::default(),
        _ => {}
    }
    (before, run.failures)
}
