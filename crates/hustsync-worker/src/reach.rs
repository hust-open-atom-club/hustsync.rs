//! Whether each manager took the worker's last request, and the `hustsync::alert` events when that changes.

use std::{
    collections::HashMap,
    fmt::Display,
    sync::{Mutex, PoisonError},
};

use chrono::{DateTime, Utc};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Link {
    pub up: bool,
    pub failures: u32,
    pub last_ok: Option<DateTime<Utc>>,
}

impl Default for Link {
    fn default() -> Self {
        Self {
            up: true,
            failures: 0,
            last_ok: None,
        }
    }
}

/// One link per manager base URL; a manager not yet contacted counts as up.
#[derive(Default)]
pub struct Reach {
    links: Mutex<HashMap<String, Link>>,
}

impl Reach {
    /// Records a request the manager took; logs `manager reachable` when it ends a run of failures, whose length
    /// it returns.
    pub fn ok(&self, manager: &str) -> Option<u32> {
        let failures = self.update(manager, |l| {
            let was = (!l.up).then_some(l.failures);
            *l = Link {
                up: true,
                failures: 0,
                last_ok: Some(Utc::now()),
            };
            was
        });
        if let Some(failures) = failures {
            tracing::info!(target: "hustsync::alert", manager, failures, "manager reachable");
        }
        failures
    }

    /// Records a request that failed; logs `manager unreachable` on the first failure after a success, and says
    /// whether this was it.
    pub fn fail(&self, manager: &str, reason: &dyn Display) -> bool {
        let first = self.update(manager, |l| {
            l.failures += 1;
            std::mem::replace(&mut l.up, false)
        });
        if first {
            tracing::error!(target: "hustsync::alert", manager, reason = %reason, "manager unreachable");
        }
        first
    }

    /// Every manager contacted so far.
    pub fn all(&self) -> Vec<(String, Link)> {
        self.lock()
            .iter()
            .map(|(m, l)| (m.clone(), l.clone()))
            .collect()
    }

    fn update<T>(&self, manager: &str, f: impl FnOnce(&mut Link) -> T) -> T {
        f(self.lock().entry(manager.to_owned()).or_default())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Link>> {
        self.links.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn links_flip_once_per_outage() {
        let r = Reach::default();
        assert_eq!(r.ok("http://m:14242"), None);
        assert!(r.fail("http://m:14242", &"connection refused"));
        assert!(!r.fail("http://m:14242", &"connection refused"));
        let l = &r.all()[0].1;
        assert!(!l.up);
        assert_eq!(l.failures, 2);
        assert_eq!(r.ok("http://m:14242"), Some(2));
        assert_eq!(r.ok("http://m:14242"), None);
        let l = &r.all()[0].1;
        assert!(l.up && l.failures == 0 && l.last_ok.is_some());
    }
}
