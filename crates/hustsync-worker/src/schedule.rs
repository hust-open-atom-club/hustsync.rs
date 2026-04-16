use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use tokio::time::Instant;

use crate::job::MirrorJob;

#[derive(Clone, Debug)]
pub struct JobScheduleInfo {
    pub job_name: String,
    pub next_scheduled: chrono::DateTime<chrono::Utc>,
}

struct ScheduledJob {
    job: MirrorJob,
    sched_time: Instant,
    real_time: chrono::DateTime<chrono::Utc>,
}

// Implement Ord and PartialOrd to make BinaryHeap a min-heap based on time
impl PartialEq for ScheduledJob {
    fn eq(&self, other: &Self) -> bool {
        self.sched_time == other.sched_time && self.job.name == other.job.name
    }
}

impl Eq for ScheduledJob {}

impl PartialOrd for ScheduledJob {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledJob {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for Min-Heap (earliest time first)
        other
            .sched_time
            .cmp(&self.sched_time)
            .then_with(|| self.job.name.cmp(&other.job.name))
    }
}

pub struct ScheduleQueue {
    heap: BinaryHeap<ScheduledJob>,
    // Store exact scheduled time by job name for O(1) existence and removal check logic
    jobs_time: HashMap<String, Instant>,
}

impl Default for ScheduleQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl ScheduleQueue {
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            jobs_time: HashMap::new(),
        }
    }

    pub fn get_jobs(&self) -> Vec<JobScheduleInfo> {
        let mut jobs = Vec::new();
        // Heap iteration doesn't guarantee order, but we just need the info
        for item in self.heap.iter() {
            // Only report if it's the valid currently scheduled instance
            if self.jobs_time.get(&item.job.name) == Some(&item.sched_time) {
                jobs.push(JobScheduleInfo {
                    job_name: item.job.name.clone(),
                    next_scheduled: item.real_time,
                });
            }
        }
        jobs.sort_by_key(|info| info.next_scheduled);
        jobs
    }

    pub fn add_job(&mut self, real_time: chrono::DateTime<chrono::Utc>, job: MirrorJob) {
        // Convert UTC to tokio Instant for heap operations
        let now_utc = chrono::Utc::now();
        let diff = real_time.signed_duration_since(now_utc);

        let sched_time = if diff.num_milliseconds() <= 0 {
            Instant::now()
        } else {
            #[allow(clippy::cast_sign_loss)] // guarded by `diff > 0` above
            let millis = diff.num_milliseconds() as u64;
            Instant::now() + std::time::Duration::from_millis(millis)
        };

        if self.jobs_time.contains_key(&job.name) {
            tracing::warn!(
                "Job {} already scheduled, removing the existing one (lazy delete)",
                job.name
            );
        }

        self.jobs_time.insert(job.name.clone(), sched_time);
        tracing::debug!("Added job {} @ {}", job.name, real_time);

        self.heap.push(ScheduledJob {
            job,
            sched_time,
            real_time,
        });
    }

    pub fn pop_if_ready(&mut self) -> Option<MirrorJob> {
        let now = Instant::now();

        loop {
            match self.heap.peek_mut() {
                Some(peek) if peek.sched_time <= now => {
                    // `PeekMut::pop` consumes the guard and removes the element,
                    // replacing the second `heap.pop().unwrap()` that was here.
                    let item = std::collections::binary_heap::PeekMut::pop(peek);
                    // Check if this is a stale entry (was removed/rescheduled)
                    if self.jobs_time.get(&item.job.name) == Some(&item.sched_time) {
                        self.jobs_time.remove(&item.job.name);
                        tracing::debug!("Popped out job {} @ {}", item.job.name, item.real_time);
                        return Some(item.job);
                    }
                    // Stale entry, just drop it and continue checking
                }
                _ => break,
            }
        }
        None
    }

    pub fn remove(&mut self, name: &str) -> bool {
        // Lazy delete: just remove from the map, the heap will discard it on pop
        self.jobs_time.remove(name).is_some()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;
    use tokio::sync::mpsc;
    use tokio::time::Duration;

    fn create_dummy_job(name: &str) -> MirrorJob {
        let (tx, _) = mpsc::channel(1);
        MirrorJob {
            name: name.to_string(),
            tx,
            state: Arc::new(AtomicU32::new(0)),
            disabled: Arc::new(tokio::sync::Notify::new()),
            interval: Duration::from_secs(60),
        }
    }

    #[tokio::test]
    async fn test_schedule_order() {
        let mut queue = ScheduleQueue::new();
        let now = chrono::Utc::now();

        let job1 = create_dummy_job("job1");
        let job2 = create_dummy_job("job2");
        let job3 = create_dummy_job("job3");

        // Add out of order
        queue.add_job(now + chrono::Duration::seconds(10), job2);
        queue.add_job(now + chrono::Duration::seconds(5), job1);
        queue.add_job(now + chrono::Duration::seconds(15), job3);

        // Advance time manually is tricky with tokio::time,
        // but our add_job uses chrono for calculation.
        // For testing, let's just use immediate jobs.
        let job_now = create_dummy_job("immediate");
        queue.add_job(now - chrono::Duration::seconds(1), job_now);

        // Should pop "immediate" first
        let popped = queue.pop_if_ready().expect("Should pop immediate job");
        assert_eq!(popped.name, "immediate");

        // Others should not be ready yet
        assert!(queue.pop_if_ready().is_none());
    }

    #[tokio::test]
    async fn test_lazy_removal() {
        let mut queue = ScheduleQueue::new();
        let now = chrono::Utc::now();

        let job1 = create_dummy_job("to_be_removed");
        queue.add_job(now - chrono::Duration::seconds(10), job1);

        // Remove it
        queue.remove("to_be_removed");

        // Should NOT pop anything because it was removed from the map
        assert!(queue.pop_if_ready().is_none());
    }

    #[tokio::test]
    async fn test_reschedule_updates_time() {
        let mut queue = ScheduleQueue::new();
        let now = chrono::Utc::now();

        let job1 = create_dummy_job("job1");

        // Schedule for 10s later
        queue.add_job(now + chrono::Duration::seconds(10), job1.clone());

        // Re-schedule for now
        queue.add_job(now - chrono::Duration::seconds(1), job1);

        // Should pop now
        let popped = queue.pop_if_ready().expect("Should pop job1");
        assert_eq!(popped.name, "job1");
    }
}
