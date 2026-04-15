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
pub mod job;
pub mod provider;
pub mod schedule;
mod server;

pub use error::{HookError, WorkerError};

use hustsync_internal::util::format_path;
pub use job::MirrorJob;
use provider::{
    MirrorProvider,
    cmd_provider::{CmdProvider, CmdProviderConfig},
    rsync_provider::{RsyncProvider, RsyncProviderConfig},
};
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

                    let (job, actor) = job::JobActor::new(
                        name.clone(),
                        manager_tx.clone(),
                        Arc::clone(&semaphore),
                        provider,
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
        let api_base = manager_cfg
            .api_base
            .as_deref()
            .unwrap_or("http://localhost:12345");
        let root = api_base
            .split(',')
            .next()
            .unwrap_or("http://localhost:12345");
        let url = format!(
            "{}/workers/{}/jobs",
            root.trim_end_matches('/'),
            self.name()
        );

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
        let global = g_cfg.global.as_ref();

        let interval = m_cfg
            .retry
            .as_ref()
            .and_then(|r| r.interval)
            .or_else(|| global.and_then(|g| g.retry.as_ref().and_then(|r| r.interval)))
            .unwrap_or(120);

        let retry = m_cfg
            .retry
            .as_ref()
            .and_then(|r| r.retry)
            .or_else(|| global.and_then(|g| g.retry.as_ref().and_then(|r| r.retry)))
            .unwrap_or(2);

        let timeout = m_cfg
            .retry
            .as_ref()
            .and_then(|r| r.timeout)
            .or_else(|| global.and_then(|g| g.retry.as_ref().and_then(|r| r.timeout)))
            .unwrap_or(3600);

        let log_dir_base = m_cfg
            .log_dir
            .as_deref()
            .or_else(|| global.and_then(|g| g.log_dir.as_deref()))
            .unwrap_or("/tmp/hustsync/log/{{.Name}}");

        let log_dir = format_path(log_dir_base, name);
        let log_file = format!("{}/latest.log", log_dir.trim_end_matches('/'));

        let mirror_dir_base = m_cfg
            .mirror_dir
            .as_deref()
            .or_else(|| global.and_then(|g| g.mirror_dir.as_deref()))
            .unwrap_or("/tmp/hustsync");

        let mirror_dir = format_path(mirror_dir_base, name);

        let is_master = m_cfg.role.as_deref() != Some("slave");

        let p_type = m_cfg.provider.as_deref().unwrap_or("rsync");

        match p_type {
            "command" => {
                let cmd_cfg = CmdProviderConfig {
                    name: name.to_string(),
                    upstream_url: m_cfg.upstream.clone().unwrap_or_default(),
                    command: m_cfg.command.clone().unwrap_or_default(),
                    working_dir: mirror_dir,
                    log_dir,
                    log_file,
                    interval: Duration::from_secs(interval as u64 * 60),
                    retry,
                    timeout: Duration::from_secs(timeout as u64),
                    env: m_cfg.env.clone().unwrap_or_default(),
                    fail_on_match: m_cfg.fail_on_match.clone(),
                    size_pattern: m_cfg.size_pattern.clone(),
                    is_master,
                };
                let p = CmdProvider::new(cmd_cfg)?;
                Ok(Box::new(p))
            }
            "rsync" => {
                let rsync_cfg = RsyncProviderConfig {
                    name: name.to_string(),
                    command: m_cfg.command.clone().unwrap_or_else(|| "rsync".to_string()),
                    upstream_url: m_cfg.upstream.clone().unwrap_or_default(),
                    username: m_cfg.username.clone(),
                    password: m_cfg.password.clone(),
                    exclude_file: m_cfg.exclude_file.clone(),
                    rsync_options: m_cfg.rsync_options.clone().unwrap_or_default(),
                    global_options: global
                        .and_then(|g| g.rsync_options.clone())
                        .unwrap_or_default(),
                    rsync_override: m_cfg.rsync_override.clone(),
                    rsync_override_only: m_cfg.rsync_override_only.unwrap_or(false),
                    rsync_no_timeout: m_cfg.rsync_no_timeout.unwrap_or(false),
                    rsync_timeout: m_cfg.rsync_timeout,
                    env: m_cfg.env.clone().unwrap_or_default(),
                    working_dir: mirror_dir,
                    log_dir,
                    log_file,
                    use_ipv6: m_cfg.use_ipv6.unwrap_or(false),
                    use_ipv4: m_cfg.use_ipv4.unwrap_or(false),
                    interval: Duration::from_secs(interval as u64 * 60),
                    retry,
                    timeout: Duration::from_secs(timeout as u64),
                    is_master,
                };
                let p = RsyncProvider::new(rsync_cfg)?;
                Ok(Box::new(p))
            }
            "two-stage-rsync" => {
                // TODO: implement two-stage-rsync provider
                tracing::error!(
                    "Provider type 'two-stage-rsync' for mirror '{}' is not implemented yet!",
                    name
                );
                Err(provider::ProviderError::UnknownType(p_type.to_string()))
            }
            _ => {
                tracing::error!(
                    "Provider type '{}' for mirror '{}' is unknown!",
                    p_type,
                    name
                );
                Err(provider::ProviderError::UnknownType(p_type.to_string()))
            }
        }
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

        let api_base = manager_cfg
            .api_base
            .as_deref()
            .unwrap_or("http://localhost:12345");
        let api_bases: Vec<&str> = api_base
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        let msg = WorkerStatus {
            id: self.name(),
            url: self.url(),
            token: manager_cfg.token.clone().unwrap_or_default(),
            last_online: Utc::now(),
            last_register: Utc::now(),
        };

        let Some(client) = &self.http_client else {
            return;
        };

        for root in api_bases {
            let url = format!("{}/workers", root.trim_end_matches('/'));
            let mut retries = 10;
            while retries > 0 {
                if let Ok(resp) = client.post(&url).json(&msg).send().await
                    && resp.status().is_success()
                {
                    tracing::info!("Successfully registered to manager: {}", url);
                    break;
                }
                retries -= 1;
                if retries > 0 {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
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
            let api_base = manager_cfg
                .api_base
                .as_deref()
                .unwrap_or("http://localhost:12345");
            let worker_name = self.name();

            for root in api_base.split(',').map(|str| str.trim()) {
                let url = format!(
                    "{}/workers/{}/schedules",
                    root.trim_end_matches('/'),
                    worker_name
                );
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

                let api_base = manager_cfg
                    .api_base
                    .as_deref()
                    .unwrap_or("http://localhost:12345");
                for root in api_base.split(',').map(|s| s.trim()) {
                    let url = format!(
                        "{}/workers/{}/jobs/{}",
                        root.trim_end_matches('/'),
                        worker_name,
                        msg.name
                    );
                    let _ = client.post(&url).json(&smsg).send().await;
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
                for root in api_base.split(',').map(|s| s.trim()) {
                    let url = format!(
                        "{}/workers/{}/schedules",
                        root.trim_end_matches('/'),
                        worker_name
                    );
                    let _ = client.post(&url).json(&sched_msg).send().await;
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
