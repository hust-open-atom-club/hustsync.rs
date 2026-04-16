#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

use chrono::Utc;
use hustsync_config_parser::WorkerConfig;
use hustsync_internal::msg::WorkerStatus;
use hustsync_internal::status::SyncStatus;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

pub mod error;
pub mod hooks;
pub mod job;
pub mod provider;
pub mod schedule;
pub mod server;

pub use error::{HookError, HookErrorKind, WorkerError};

pub use job::{CtrlAction, MirrorJob};
use provider::MirrorProvider;
use schedule::ScheduleQueue;

pub struct JobMessage {
    pub status: SyncStatus,
    pub name: String,
    pub msg: String,
    pub schedule: bool,
    pub upstream: String,
    pub size: Option<String>,
    pub is_master: bool,
}

use tokio::task::JoinSet;

/// Resolve the effective list of manager API base URLs from worker manager
/// config.
///
/// If `api_base_list` is set and non-empty it is returned directly — this is
/// the HA broadcast list. Otherwise the comma-separated `api_base` string is
/// split and trimmed as a fallback, matching Go's `worker/worker.go` `apiBases`
/// helper.
fn resolve_api_bases(cfg: &hustsync_config_parser::WorkerManagerConfig) -> Vec<String> {
    if let Some(list) = &cfg.api_base_list
        && !list.is_empty()
    {
        return list.clone();
    }
    cfg.api_base
        .as_deref()
        .unwrap_or("http://localhost:12345")
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

/// Attempt to POST `msg` to `url` up to 10 times (1 s between attempts).
/// Returns `true` on the first successful response.
async fn try_register(client: &Client, url: &str, msg: &WorkerStatus) -> bool {
    let mut retries = 10u32;
    while retries > 0 {
        if let Ok(resp) = client.post(url).json(msg).send().await
            && resp.status().is_success()
        {
            tracing::info!("Successfully registered to manager: {}", url);
            return true;
        }
        retries -= 1;
        if retries > 0 {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    false
}

fn format_manager_url(base: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

/// Records the names of mirrors that differ between the running jobs map
/// and a freshly-loaded config slice. Names that appear only in the old
/// map are `removed`; names only in the new config are `added`; names in
/// both are `modified` (the actor is torn down and rebuilt so the new
/// provider takes effect).
struct MirrorDiff {
    added: Vec<String>,
    removed: Vec<String>,
    modified: Vec<String>,
}

/// Compare the set of currently-running jobs against a new config slice.
///
/// Because `MirrorJob` does not retain its originating `MirrorConfig`, a
/// deep-equal comparison is not possible without storing extra state. The
/// caller rebuilds the actor for every name that appears in both sets —
/// identical to Go's behaviour for the modify path.
fn diff_mirror_configs(
    old: &HashMap<String, MirrorJob>,
    new_cfg: &[hustsync_config_parser::MirrorConfig],
) -> MirrorDiff {
    let new_names: std::collections::HashSet<&str> = new_cfg
        .iter()
        .filter_map(|m| m.name.as_deref())
        .collect();

    let mut removed = Vec::new();
    let mut modified = Vec::new();

    for name in old.keys() {
        if new_names.contains(name.as_str()) {
            modified.push(name.clone());
        } else {
            removed.push(name.clone());
        }
    }

    let old_names: std::collections::HashSet<&str> =
        old.keys().map(|s| s.as_str()).collect();

    let added: Vec<String> = new_cfg
        .iter()
        .filter_map(|m| m.name.as_deref())
        .filter(|n| !old_names.contains(*n))
        .map(|n| n.to_owned())
        .collect();

    MirrorDiff {
        added,
        removed,
        modified,
    }
}

pub struct Worker {
    pub cfg: Arc<WorkerConfig>,
    pub jobs: Arc<RwLock<HashMap<String, MirrorJob>>>,
    pub job_handles: Mutex<JoinSet<()>>,

    pub manager_tx: mpsc::Sender<JobMessage>,
    pub manager_rx: Mutex<Option<mpsc::Receiver<JobMessage>>>,
    pub semaphore: Arc<Semaphore>,
    pub schedule_queue: Arc<Mutex<ScheduleQueue>>,
    pub exit_token: CancellationToken,

    pub http_client: Option<Client>,
}

impl Worker {
    pub fn new(mut cfg: WorkerConfig) -> Self {
        // Ignore SIGPIPE process-wide: a broken pipe from an rsync (or other)
        // child must not terminate the worker. Providers already observe
        // child exit status and surface errors through ProviderError.
        #[cfg(unix)]
        // SAFETY: SIG_IGN is async-signal-safe; Worker::new runs before any
        // other thread exists, so installing a process-wide disposition here
        // is race-free.
        unsafe {
            use nix::sys::signal::{SigHandler, Signal, signal as nix_signal};
            let _ = nix_signal(Signal::SIGPIPE, SigHandler::SigIgn);
        }

        if let Some(ref mut global) = cfg.global
            && let Some(ref mut retry) = global.retry
            && retry.retry.unwrap_or(0) == 0
        {
            retry.retry = Some(2); // defaultMaxRetry
        }

        let cfg = Arc::new(cfg);

        let concurrent = cfg.global.as_ref().and_then(|g| g.concurrent).unwrap_or(10) as usize;

        let (manager_tx, manager_rx) = mpsc::channel(32);

        let semaphore = Arc::new(Semaphore::new(concurrent));
        let exit_token = CancellationToken::new();

        let mut jobs_map = HashMap::new();
        let mut handles = JoinSet::new();

        if let Some(mirrors) = &cfg.mirrors {
            for m_cfg in mirrors {
                if let Some(name) = &m_cfg.name {
                    let provider = match Self::create_provider(name, m_cfg, &cfg) {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::error!("Failed to create provider for {}: {}", name, e);
                            continue;
                        }
                    };

                    let hooks = Self::build_hooks(m_cfg, &cfg);
                    let (job, actor) = job::JobActor::new(
                        name.clone(),
                        manager_tx.clone(),
                        Arc::clone(&semaphore),
                        provider,
                        hooks,
                    );
                    jobs_map.insert(name.clone(), job);
                    handles.spawn(actor.run());
                }
            }
        }

        let mut worker = Worker {
            cfg: Arc::clone(&cfg),
            jobs: Arc::new(RwLock::new(jobs_map)),
            job_handles: Mutex::new(handles),
            manager_tx,
            manager_rx: Mutex::new(Some(manager_rx)),
            semaphore,
            schedule_queue: Arc::new(Mutex::new(ScheduleQueue::new())),
            exit_token,
            http_client: None,
        };

        let ca = cfg
            .manager
            .as_ref()
            .and_then(|m| m.ca_cert.as_ref())
            .filter(|s| !s.is_empty());
        match hustsync_internal::util::create_http_client(ca) {
            Ok(client) => worker.http_client = Some(client),
            Err(e) => tracing::error!("Error initializing HTTP client: {}", e),
        }

        worker
    }

    async fn fetch_job_status(&self) -> Vec<hustsync_internal::msg::MirrorStatus> {
        let Some(manager_cfg) = &self.cfg.manager else {
            return vec![];
        };
        // Reads always use the first (primary) manager only.
        let api_bases = resolve_api_bases(manager_cfg);
        let root = api_bases
            .first()
            .map(|s| s.as_str())
            .unwrap_or("http://localhost:12345");
        let url = format_manager_url(root, &format!("workers/{}/jobs", self.name()));

        match hustsync_internal::util::get_json::<Vec<hustsync_internal::msg::MirrorStatus>>(
            &url,
            self.http_client.as_ref(),
        )
        .await
        {
            Ok(statuses) => statuses,
            Err(e) => {
                tracing::error!("Failed to fetch job status from manager: {}", e);
                vec![]
            }
        }
    }

    fn create_provider(
        name: &str,
        m_cfg: &hustsync_config_parser::MirrorConfig,
        g_cfg: &WorkerConfig,
    ) -> Result<Box<dyn MirrorProvider>, provider::ProviderError> {
        provider::build_provider(name, m_cfg, g_cfg)
    }

    /// Assemble the hook chain for one mirror. Order matters — `pre_*`
    /// runs in this vec order, `post_*` in reverse (LIFO).
    fn build_hooks(
        m_cfg: &hustsync_config_parser::MirrorConfig,
        _g_cfg: &WorkerConfig,
    ) -> Vec<Arc<dyn hooks::JobHook>> {
        let mut chain: Vec<Arc<dyn hooks::JobHook>> = Vec::new();
        // 1. Built-in: ensure working_dir exists before anything else.
        chain.push(Arc::new(hooks::WorkingDirHook::new()));
        // 2. Built-in: rotate and stamp the log file before the provider
        //    runs, so the new path is what the exec hook env and the
        //    provider itself see.
        chain.push(Arc::new(hooks::LogLimitHook::new()));
        // 3. User-configured exec_on_{success,failure}. `exec_on_status_extra`
        //    (mirror-level additions to global defaults) is appended after
        //    the base list, matching Go's "append extra to the end".
        let mut on_success: Vec<String> = Vec::new();
        let mut on_failure: Vec<String> = Vec::new();
        if let Some(status) = &m_cfg.exec_on_status {
            if let Some(list) = &status.exec_on_success {
                on_success.extend(list.iter().cloned());
            }
            if let Some(list) = &status.exec_on_failure {
                on_failure.extend(list.iter().cloned());
            }
        }
        if let Some(extra) = &m_cfg.exec_on_status_extra {
            if let Some(list) = &extra.exec_on_success_extra {
                on_success.extend(list.iter().cloned());
            }
            if let Some(list) = &extra.exec_on_failure_extra {
                on_failure.extend(list.iter().cloned());
            }
        }
        if !on_success.is_empty() || !on_failure.is_empty() {
            chain.push(Arc::new(hooks::ExecPostHook::new(on_success, on_failure)));
        }
        chain
    }

    pub fn name(&self) -> String {
        self.cfg
            .global
            .as_ref()
            .and_then(|g| g.name.clone())
            .unwrap_or_else(|| "default_worker".to_string())
    }

    pub fn url(&self) -> String {
        let proto = if let Some(server) = &self.cfg.server {
            if server.ssl_cert.as_deref().unwrap_or("").is_empty()
                && server.ssl_key.as_deref().unwrap_or("").is_empty()
            {
                "http"
            } else {
                "https"
            }
        } else {
            "http"
        };

        let hostname = self
            .cfg
            .server
            .as_ref()
            .and_then(|s| s.hostname.clone())
            .unwrap_or_else(|| "localhost".to_string());

        let port = self
            .cfg
            .server
            .as_ref()
            .and_then(|s| s.listen_port)
            .unwrap_or(6000);

        format!("{}://{}:{}/", proto, hostname, port)
    }

    pub async fn register_worker(&self) {
        let Some(manager_cfg) = &self.cfg.manager else {
            tracing::warn!("No manager configuration found, skipping registration.");
            return;
        };

        let Some(client) = &self.http_client else {
            return;
        };

        let msg = WorkerStatus {
            id: self.name(),
            url: self.url(),
            token: manager_cfg.token.clone().unwrap_or_default(),
            last_online: Utc::now(),
            last_register: Utc::now(),
        };

        for root in resolve_api_bases(manager_cfg) {
            let url = format_manager_url(&root, "workers");
            if try_register(client, &url, &msg).await {
                break;
            }
            tracing::warn!("Failed to register to manager after retries: {}", url);
        }
    }

    /// Bootstrap the schedule queue: apply statuses fetched from manager, schedule
    /// unknown jobs immediately, and push the initial schedule snapshot back.
    async fn bootstrap_queue(&self, initial_statuses: Vec<hustsync_internal::msg::MirrorStatus>) {
        let mut queue = self.schedule_queue.lock().await;
        let jobs = self.jobs.read().await;
        let mut unset = jobs
            .keys()
            .map(|k| (k.clone(), true))
            .collect::<HashMap<String, bool>>();

        for s in initial_statuses {
            if let Some(job) = jobs.get(&s.name) {
                unset.remove(&s.name);
                match s.status {
                    SyncStatus::Disabled => {
                        job.set_state(crate::job::STATE_DISABLED);
                    }
                    SyncStatus::Paused => {
                        job.set_state(crate::job::STATE_PAUSED);
                    }
                    _ => {
                        job.set_state(crate::job::STATE_NONE);
                        let next = s.last_update + job.interval;
                        queue.add_job(next, job.clone());
                    }
                }
            }
        }
        for (name, _) in unset {
            if let Some(job) = jobs.get(&name) {
                queue.add_job(Utc::now(), job.clone());
            }
        }

        // Push initial schedule snapshot; drop locks before the network I/O.
        if let Some(client) = &self.http_client
            && let Some(manager_cfg) = &self.cfg.manager
        {
            let s: Vec<_> = queue
                .get_jobs()
                .into_iter()
                .map(|info| hustsync_internal::msg::MirrorSchedule {
                    name: info.job_name,
                    next_schedule: info.next_scheduled,
                })
                .collect();
            drop(queue);
            drop(jobs);

            let sched_msg = hustsync_internal::msg::MirrorSchedules { schedules: s };
            let api_bases = resolve_api_bases(manager_cfg);
            let worker_name = self.name();

            for root in api_bases {
                let url =
                    format_manager_url(&root, &format!("workers/{}/schedules", worker_name));
                tokio::spawn({
                    let client = client.clone();
                    let url = url.clone();
                    let msg = sched_msg.clone();
                    async move {
                        let _ = client.post(&url).json(&msg).send().await;
                    }
                });
            }
        }
    }

    /// Spawn an HTTPS server that shuts down when the exit token fires.
    /// Returns `false` if TLS config could not be loaded.
    async fn spawn_tls_server(
        addr: &str,
        socket_addr: std::net::SocketAddr,
        cert_path: &str,
        key_path: &str,
        app: axum::Router,
        exit_token: CancellationToken,
    ) -> bool {
        let tls_config =
            match axum_server::tls_rustls::RustlsConfig::from_pem_file(cert_path, key_path).await {
                Ok(cfg) => cfg,
                Err(e) => {
                    tracing::error!("Failed to load worker TLS certificates: {}", e);
                    return false;
                }
            };
        tracing::info!("Worker (HTTPS) listening on {}", addr);
        let handle = axum_server::Handle::new();
        let handle_clone = handle.clone();
        tokio::spawn(async move {
            exit_token.cancelled().await;
            handle_clone.graceful_shutdown(Some(Duration::from_secs(10)));
        });
        tokio::spawn(async move {
            axum_server::bind_rustls(socket_addr, tls_config)
                .handle(handle)
                .serve(app.into_make_service())
                .await
                .unwrap_or_else(|e| tracing::error!("Worker HTTPS server error: {}", e));
        });
        true
    }

    /// Spawn a plain HTTP server that shuts down when the exit token fires.
    /// Returns `false` if the address could not be bound.
    async fn spawn_plain_server(
        addr: &str,
        socket_addr: std::net::SocketAddr,
        app: axum::Router,
        exit_token: CancellationToken,
    ) -> bool {
        let listener = match tokio::net::TcpListener::bind(&socket_addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!("Failed to bind worker HTTP server on {}: {}", addr, e);
                return false;
            }
        };
        tracing::info!("Worker (HTTP) listening on {}", addr);
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { exit_token.cancelled().await })
                .await
                .unwrap_or_else(|e| tracing::error!("Worker HTTP server error: {}", e));
        });
        true
    }

    /// Bind and spawn the HTTP (or HTTPS) control server, hooked to the exit token.
    /// Returns `false` if the server could not be started (address parse or TLS error).
    async fn start_http_server(&self) -> bool {
        let app_state = Arc::new(server::AppState {
            jobs: Arc::clone(&self.jobs),
            schedule_queue: Arc::clone(&self.schedule_queue),
        });
        let app = server::make_http_server(app_state);
        let listen_addr = self
            .cfg
            .server
            .as_ref()
            .and_then(|s| s.listen_addr.clone())
            .unwrap_or_else(|| "127.0.0.1".to_string());
        let listen_port = self
            .cfg
            .server
            .as_ref()
            .and_then(|s| s.listen_port)
            .unwrap_or(6000);
        let addr = format!("{}:{}", listen_addr, listen_port);
        let socket_addr: std::net::SocketAddr = match addr.parse() {
            Ok(a) => a,
            Err(e) => {
                tracing::error!("Failed to parse worker address '{}': {}", addr, e);
                return false;
            }
        };

        let tls_paths = self.cfg.server.as_ref().and_then(|s| {
            let cert = s.ssl_cert.as_deref().filter(|c| !c.is_empty())?;
            let key = s.ssl_key.as_deref().filter(|k| !k.is_empty())?;
            Some((cert.to_string(), key.to_string()))
        });

        if let Some((cert_path, key_path)) = tls_paths {
            Self::spawn_tls_server(
                &addr,
                socket_addr,
                &cert_path,
                &key_path,
                app,
                self.exit_token.clone(),
            )
            .await
        } else {
            Self::spawn_plain_server(&addr, socket_addr, app, self.exit_token.clone()).await
        }
    }

    /// Spawn the task that relays `JobMessage`s from job actors to the manager
    /// and re-schedules jobs whose `schedule` flag is set.
    async fn start_message_relay(&self) {
        let rx = {
            let mut guard = self.manager_rx.lock().await;
            guard.take()
        };
        let Some(mut rx) = rx else { return };

        let http_client = self.http_client.clone();
        let cfg = Arc::clone(&self.cfg);
        let worker_name = self.name();
        let schedule_queue = Arc::clone(&self.schedule_queue);
        let jobs_handle = Arc::clone(&self.jobs);

        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let (Some(client), Some(manager_cfg)) = (&http_client, &cfg.manager) else {
                    continue;
                };

                // Skip Failed reports for paused/disabled jobs (mirrors Go behaviour).
                {
                    let jobs = jobs_handle.read().await;
                    if let Some(job) = jobs.get(&msg.name) {
                        let state = job.state();
                        if msg.status == hustsync_internal::status::SyncStatus::Failed
                            && (state == crate::job::STATE_PAUSED
                                || state == crate::job::STATE_DISABLED)
                        {
                            tracing::info!(
                                "Job {} state is {}, skip reporting Failed status",
                                msg.name,
                                state
                            );
                            continue;
                        }
                    }
                }

                let smsg = hustsync_internal::msg::MirrorStatus {
                    name: msg.name.clone(),
                    worker: worker_name.clone(),
                    upstream: msg.upstream.clone(),
                    size: msg.size.clone().unwrap_or_else(|| "unknown".to_string()),
                    error_msg: msg.msg.clone(),
                    last_update: Utc::now(),
                    last_started: Utc::now(),
                    last_ended: Utc::now(),
                    next_scheduled: Utc::now(),
                    status: msg.status,
                    is_master: msg.is_master,
                };

                let api_bases = resolve_api_bases(manager_cfg);

                // Status push: broadcast, break on first success.
                let mut status_sent = false;
                for root in &api_bases {
                    let url = format_manager_url(
                        root,
                        &format!("workers/{}/jobs/{}", worker_name, msg.name),
                    );
                    match client.post(&url).json(&smsg).send().await {
                        Ok(resp) if resp.status().is_success() => {
                            status_sent = true;
                            break;
                        }
                        Ok(resp) => {
                            tracing::warn!(
                                "Status push to {} returned {}", url, resp.status()
                            );
                        }
                        Err(e) => {
                            tracing::warn!("Status push to {} failed: {}", url, e);
                        }
                    }
                }
                if !status_sent {
                    tracing::error!(
                        "Failed to push status for {} to all managers", msg.name
                    );
                }

                if msg.schedule {
                    let jobs = jobs_handle.read().await;
                    if let Some(job) = jobs.get(&msg.name) {
                        let next = Utc::now() + job.interval;
                        schedule_queue.lock().await.add_job(next, job.clone());
                    }
                }

                // Push updated schedule snapshot. Extract jobs first, drop lock before
                // the network calls to avoid holding the mutex across await points.
                let sched_infos = schedule_queue.lock().await.get_jobs();
                let s: Vec<_> = sched_infos
                    .into_iter()
                    .map(|info| hustsync_internal::msg::MirrorSchedule {
                        name: info.job_name,
                        next_schedule: info.next_scheduled,
                    })
                    .collect();
                let sched_msg = hustsync_internal::msg::MirrorSchedules { schedules: s };

                // Schedule push: broadcast, break on first success.
                let mut sched_sent = false;
                for root in &api_bases {
                    let url = format_manager_url(
                        root,
                        &format!("workers/{}/schedules", worker_name),
                    );
                    match client.post(&url).json(&sched_msg).send().await {
                        Ok(resp) if resp.status().is_success() => {
                            sched_sent = true;
                            break;
                        }
                        Ok(resp) => {
                            tracing::warn!(
                                "Schedule push to {} returned {}", url, resp.status()
                            );
                        }
                        Err(e) => {
                            tracing::warn!("Schedule push to {} failed: {}", url, e);
                        }
                    }
                }
                if !sched_sent {
                    tracing::error!("Failed to push schedule to all managers");
                }
            }
        });
    }

    /// Spawn the dispatch ticker that fires ready jobs every 5 seconds.
    fn start_dispatch_loop(&self) {
        let schedule_queue = Arc::clone(&self.schedule_queue);
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        let exit_token = self.exit_token.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let mut ready_jobs = Vec::new();
                        {
                            let mut queue = schedule_queue.lock().await;
                            while let Some(job) = queue.pop_if_ready() {
                                ready_jobs.push(job);
                            }
                        }
                        for job in ready_jobs {
                            let _ = job.send_ctrl(crate::job::CtrlAction::Start).await;
                        }
                    }
                    _ = exit_token.cancelled() => break,
                }
            }
        });
    }

    /// Halt all running job actors and wait for them to drain (≤ 30 s by design).
    // The complexity lint over-counts here because it accumulates closure complexity
    // from `start_message_relay`'s spawned async block into this function's score.
    #[allow(clippy::cognitive_complexity)]
    async fn shutdown(&self) {
        tracing::info!("Worker exit signal received. Halting all jobs gracefully...");
        {
            let jobs = self.jobs.read().await;
            for (name, job) in jobs.iter() {
                if let Err(e) = job.send_ctrl(crate::job::CtrlAction::Halt).await {
                    tracing::debug!("Job {} channel closed before halt: {}", name, e);
                }
            }
        }
        let mut handles = self.job_handles.lock().await;
        while let Some(res) = handles.join_next().await {
            if let Err(e) = res {
                tracing::error!("Job actor panicked or cancelled: {}", e);
            }
        }
        tracing::info!("Worker fully stopped.");
    }

    /// Disable and remove a job by name if it exists.
    async fn disable_and_remove_job(&self, name: &str) -> Option<u32> {
        let job = self.jobs.write().await.remove(name);
        if let Some(job) = job {
            let prev = job.state();
            let _ = job.send_ctrl(CtrlAction::Disable).await;
            return Some(prev);
        }
        None
    }

    /// Build provider + hooks for `m_cfg`, spawn actor, schedule it, and
    /// insert it into the jobs map. The actor is scheduled at `now` unless
    /// `initial_state` forces it into disabled/paused.
    async fn spawn_mirror_from_cfg(
        &self,
        name: &str,
        m_cfg: &hustsync_config_parser::MirrorConfig,
        initial_state: u32,
    ) {
        let provider = match Self::create_provider(name, m_cfg, &self.cfg) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("Failed to create provider for {}: {}", name, e);
                return;
            }
        };

        let hooks = Self::build_hooks(m_cfg, &self.cfg);
        let (job, actor) = job::JobActor::new(
            name.to_owned(),
            self.manager_tx.clone(),
            Arc::clone(&self.semaphore),
            provider,
            hooks,
        );

        if initial_state == crate::job::STATE_DISABLED {
            job.set_state(crate::job::STATE_DISABLED);
        } else {
            // Paused jobs re-enter the queue at now (same as Go's stateNone path).
            let resolved = if initial_state == crate::job::STATE_PAUSED {
                crate::job::STATE_PAUSED
            } else {
                crate::job::STATE_NONE
            };
            job.set_state(resolved);
            self.schedule_queue
                .lock()
                .await
                .add_job(chrono::Utc::now(), job.clone());
        }

        self.job_handles.lock().await.spawn(actor.run());
        self.jobs.write().await.insert(name.to_owned(), job);
    }

    /// Remove mirrors dropped from the config.
    async fn apply_removed_mirrors(&self, removed: &[String]) {
        for name in removed {
            self.disable_and_remove_job(name).await;
            tracing::info!("Mirror {} removed by config reload", name);
        }
    }

    /// Rebuild mirrors whose config changed, preserving previous state.
    async fn apply_modified_mirrors(
        &self,
        modified: &[String],
        new_mirrors: &[hustsync_config_parser::MirrorConfig],
    ) {
        for name in modified {
            let prev = self.disable_and_remove_job(name).await;
            let Some(m_cfg) = new_mirrors.iter().find(|m| m.name.as_deref() == Some(name)) else {
                continue;
            };
            let state = prev.unwrap_or(crate::job::STATE_NONE);
            self.spawn_mirror_from_cfg(name, m_cfg, state).await;
            tracing::info!("Mirror {} reloaded by config reload", name);
        }
    }

    /// Instantiate and schedule mirrors that appear only in the new config.
    async fn apply_added_mirrors(
        &self,
        added: &[String],
        new_mirrors: &[hustsync_config_parser::MirrorConfig],
    ) {
        for name in added {
            let Some(m_cfg) = new_mirrors.iter().find(|m| m.name.as_deref() == Some(name)) else {
                continue;
            };
            self.spawn_mirror_from_cfg(name, m_cfg, crate::job::STATE_NONE)
                .await;
            tracing::info!("Mirror {} added by config reload", name);
        }
    }

    /// Apply a freshly-loaded mirror config slice to the running worker.
    ///
    /// Mirrors absent from the new config are disabled and dropped. Mirrors
    /// present in both are torn down and rebuilt so the new provider config
    /// takes effect; the previous paused/disabled state is preserved. New
    /// mirrors are created and scheduled immediately. Matches Go
    /// `Worker.ReloadMirrorConfig`.
    pub async fn reload_mirror_config(
        &self,
        new_mirrors: Vec<hustsync_config_parser::MirrorConfig>,
    ) {
        tracing::info!("Reloading mirror configs");

        let diff = {
            let jobs = self.jobs.read().await;
            diff_mirror_configs(&jobs, &new_mirrors)
        };

        self.apply_removed_mirrors(&diff.removed).await;
        self.apply_modified_mirrors(&diff.modified, &new_mirrors)
            .await;
        self.apply_added_mirrors(&diff.added, &new_mirrors).await;
    }

    pub async fn run(&self) {
        tracing::info!("Worker started.");
        self.register_worker().await;

        let initial_statuses = self.fetch_job_status().await;
        self.bootstrap_queue(initial_statuses).await;

        if !self.start_http_server().await {
            return;
        }

        self.start_message_relay().await;
        self.start_dispatch_loop();

        self.exit_token.cancelled().await;
        self.shutdown().await;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use hustsync_config_parser::WorkerManagerConfig;

    use super::resolve_api_bases;

    fn make_cfg(api_base: Option<&str>, api_base_list: Option<Vec<&str>>) -> WorkerManagerConfig {
        WorkerManagerConfig {
            api_base: api_base.map(String::from),
            api_base_list: api_base_list
                .map(|v| v.into_iter().map(String::from).collect()),
            token: None,
            ca_cert: None,
        }
    }

    #[test]
    fn resolve_prefers_api_base_list_when_set() {
        let cfg = make_cfg(
            Some("http://old:12345"),
            Some(vec!["http://m1:14242", "http://m2:14242"]),
        );
        let bases = resolve_api_bases(&cfg);
        assert_eq!(bases, vec!["http://m1:14242", "http://m2:14242"]);
    }

    #[test]
    fn resolve_falls_back_to_api_base_csv() {
        let cfg = make_cfg(Some("http://m1:12345, http://m2:12345"), None);
        let bases = resolve_api_bases(&cfg);
        assert_eq!(bases, vec!["http://m1:12345", "http://m2:12345"]);
    }

    #[test]
    fn resolve_empty_api_base_list_falls_back_to_api_base() {
        let cfg = make_cfg(Some("http://m1:12345"), Some(vec![]));
        let bases = resolve_api_bases(&cfg);
        assert_eq!(bases, vec!["http://m1:12345"]);
    }

    #[test]
    fn resolve_defaults_when_both_absent() {
        let cfg = make_cfg(None, None);
        let bases = resolve_api_bases(&cfg);
        assert_eq!(bases, vec!["http://localhost:12345"]);
    }

    #[test]
    fn resolve_single_api_base_no_csv() {
        let cfg = make_cfg(Some("http://manager:14242"), None);
        let bases = resolve_api_bases(&cfg);
        assert_eq!(bases, vec!["http://manager:14242"]);
    }
}
