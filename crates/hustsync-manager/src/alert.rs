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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex as StdMutex};

    use chrono::Utc;
    use hustsync_internal::msg::ZERO_TIME;
    use tracing::subscriber::with_default;
    use tracing_subscriber::{Layer, layer::SubscriberExt};

    use super::*;

    fn status(s: SyncStatus) -> MirrorStatus {
        MirrorStatus {
            name: "debian".into(),
            worker: "w1".into(),
            upstream: String::new(),
            size: String::new(),
            error_msg: "rsync exited 23".into(),
            last_update: ZERO_TIME,
            last_started: Utc::now(),
            last_ended: Utc::now(),
            next_scheduled: ZERO_TIME,
            status: s,
            is_master: true,
        }
    }

    #[derive(Clone, Default)]
    struct Seen(Arc<StdMutex<Vec<String>>>);

    impl<S: tracing::Subscriber> Layer<S> for Seen {
        fn on_event(&self, e: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
            struct Msg(String);
            impl tracing::field::Visit for Msg {
                fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                    if f.name() == "message" {
                        self.0 = format!("{v:?}");
                    }
                }
            }
            if e.metadata().target() == "hustsync::alert" {
                let mut m = Msg(String::new());
                e.record(&mut m);
                self.0
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(m.0);
            }
        }
    }

    impl Seen {
        fn events(&self) -> Vec<String> {
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }
    }

    fn run(streaks: &Streaks, ok: bool, tries: usize) {
        streaks.record(&status(SyncStatus::PreSyncing));
        streaks.record(&status(SyncStatus::Syncing));
        for _ in 0..tries {
            streaks.record(&status(SyncStatus::Failed));
        }
        if ok {
            streaks.record(&status(SyncStatus::Success));
        }
    }

    #[test]
    fn a_streak_alerts_once_and_recovers_once() {
        let seen = Seen::default();
        let streaks = Streaks::new(3);
        with_default(tracing_subscriber::registry().with(seen.clone()), || {
            run(&streaks, false, 2);
            run(&streaks, false, 2);
            assert_eq!(streaks.all()[0].1, 2);
            run(&streaks, false, 2);
            run(&streaks, false, 2);
            run(&streaks, false, 2);
            assert_eq!(streaks.all()[0].1, 5);
            run(&streaks, true, 0);
            run(&streaks, false, 1);
            run(&streaks, true, 0);
        });
        assert_eq!(seen.events(), ["mirror failing", "mirror recovered"]);
        assert_eq!(streaks.all()[0].1, 0);
    }

    #[test]
    fn zero_turns_alerts_off() {
        let seen = Seen::default();
        let streaks = Streaks::new(0);
        with_default(tracing_subscriber::registry().with(seen.clone()), || {
            for _ in 0..5 {
                run(&streaks, false, 1);
            }
            run(&streaks, true, 0);
        });
        assert!(seen.events().is_empty());
    }
}
