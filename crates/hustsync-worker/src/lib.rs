use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, mpsc, Semaphore, Mutex};
use tokio_util::sync::CancellationToken;
use reqwest::Client;
use hustsync_config_parser::WorkerConfig;
use hustsync_internal::status::SyncStatus;
use hustsync_internal::msg::WorkerStatus;
use chrono::Utc;

mod server;
pub mod job;
pub mod provider;
pub mod schedule;

pub use job::MirrorJob;
use provider::{MirrorProvider, cmd_provider::{CmdProvider, CmdProviderConfig}};
use hustsync_internal::util::format_path;
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
        if let Some(ref mut global) = cfg.global {
            if let Some(ref mut retry) = global.retry {
                if retry.retry.unwrap_or(0) == 0 {
                    retry.retry = Some(3); // defaultMaxRetry equivalent
                }
            }
        }

        let cfg = Arc::new(cfg);
        
        let concurrent = cfg.global.as_ref()
            .and_then(|g| g.concurrent)
            .unwrap_or(10) as usize;

        let (manager_tx, manager_rx) = mpsc::channel(32);

        let mut worker = Worker {
            cfg: Arc::clone(&cfg),
            jobs: Arc::new(RwLock::new(HashMap::new())),
            job_handles: Mutex::new(JoinSet::new()),
            manager_tx,
            manager_rx: Mutex::new(Some(manager_rx)),
            semaphore: Arc::new(Semaphore::new(concurrent)),
            schedule_queue: Arc::new(Mutex::new(ScheduleQueue::new())),
            exit_token: CancellationToken::new(),
            http_client: None,
        };

        if let Some(mirrors) = &cfg.mirrors {
            let mut jobs_map = HashMap::new();
            let mut handles = worker.job_handles.try_lock().expect("Failed to lock handles during init");
            
            for m_cfg in mirrors {
                if let Some(name) = &m_cfg.name {
                    let provider = match Self::create_provider(name, m_cfg, &worker.cfg) {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::error!("Failed to create provider for {}: {}", name, e);
                            continue;
                        }
                    };

                    let (job, actor) = job::JobActor::new(
                        name.clone(),
                        worker.manager_tx.clone(),
                        Arc::clone(&worker.semaphore),
                        provider,
                    );
                    jobs_map.insert(name.clone(), job);
                    handles.spawn(actor.run());
                }
            }
            worker.jobs = Arc::new(RwLock::new(jobs_map));
        }

        let ca = cfg.manager.as_ref().and_then(|m| m.ca_cert.as_ref()).filter(|s| !s.is_empty());
        match hustsync_internal::util::create_http_client(ca) {
            Ok(client) => worker.http_client = Some(client),
            Err(e) => tracing::error!("Error initializing HTTP client: {}", e),
        }

        worker
    }

    async fn fetch_job_status(&self) -> Vec<hustsync_internal::msg::MirrorStatus> {
        let Some(manager_cfg) = &self.cfg.manager else { return vec![]; };
        let api_base = manager_cfg.api_base.as_deref().unwrap_or("http://localhost:12345");
        let root = api_base.split(',').next().unwrap_or("http://localhost:12345");
        let url = format!("{}/workers/{}/jobs", root.trim_end_matches('/'), self.name());

        match hustsync_internal::util::get_json::<Vec<hustsync_internal::msg::MirrorStatus>>(&url, self.http_client.as_ref()).await {
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
        
        let interval = m_cfg.retry.as_ref().and_then(|r| r.interval)
            .or_else(|| global.and_then(|g| g.retry.as_ref().and_then(|r| r.interval)))
            .unwrap_or(120);

        let retry = m_cfg.retry.as_ref().and_then(|r| r.retry)
            .or_else(|| global.and_then(|g| g.retry.as_ref().and_then(|r| r.retry)))
            .unwrap_or(3);

        let timeout = m_cfg.retry.as_ref().and_then(|r| r.timeout)
            .or_else(|| global.and_then(|g| g.retry.as_ref().and_then(|r| r.timeout)))
            .unwrap_or(3600);

        let log_dir_base = m_cfg.log_dir.as_deref()
            .or_else(|| global.and_then(|g| g.log_dir.as_deref()))
            .unwrap_or("/tmp/tunasync/log/{{.Name}}");
        
        let log_dir = format_path(log_dir_base, name);
        let log_file = format!("{}/latest.log", log_dir.trim_end_matches('/'));

        let mirror_dir_base = m_cfg.mirror_dir.as_deref()
            .or_else(|| global.and_then(|g| g.mirror_dir.as_deref()))
            .unwrap_or("/tmp/tunasync");
        
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
                // TODO: implement rsync provider
                tracing::error!("Provider type 'rsync' for mirror '{}' is not implemented yet!", name);
                Err(provider::ProviderError::UnknownType(p_type.to_string()))
            }
            "two-stage-rsync" => {
                // TODO: implement two-stage-rsync provider
                tracing::error!("Provider type 'two-stage-rsync' for mirror '{}' is not implemented yet!", name);
                Err(provider::ProviderError::UnknownType(p_type.to_string()))
            }
            _ => {
                tracing::error!("Provider type '{}' for mirror '{}' is unknown!", p_type, name);
                Err(provider::ProviderError::UnknownType(p_type.to_string()))
            }
        }
    }

    pub fn name(&self) -> String {
        self.cfg.global.as_ref()
            .and_then(|g| g.name.clone())
            .unwrap_or_else(|| "default_worker".to_string())
    }

    pub fn url(&self) -> String {
        let proto = if let Some(server) = &self.cfg.server {
            if server.ssl_cert.as_deref().unwrap_or("").is_empty() 
               && server.ssl_key.as_deref().unwrap_or("").is_empty() {
                "http"
            } else {
                "https"
            }
        } else {
            "http"
        };

        let hostname = self.cfg.server.as_ref()
            .and_then(|s| s.hostname.clone())
            .unwrap_or_else(|| "localhost".to_string());
            
        let port = self.cfg.server.as_ref()
            .and_then(|s| s.listen_port)
            .unwrap_or(6000);

        format!("{}://{}:{}/", proto, hostname, port)
    }

    pub async fn register_worker(&self) {
        let Some(manager_cfg) = &self.cfg.manager else {
            tracing::warn!("No manager configuration found, skipping registration.");
            return;
        };

        let api_base = manager_cfg.api_base.as_deref().unwrap_or("http://localhost:12345");
        let api_bases: Vec<&str> = api_base.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();

        let msg = WorkerStatus {
            id: self.name(),
            url: self.url(),
            token: manager_cfg.token.clone().unwrap_or_default(),
            last_online: Utc::now(),
            last_register: Utc::now(),
        };

        let Some(client) = &self.http_client else { return; };

        for root in api_bases {
            let url = format!("{}/workers", root.trim_end_matches('/'));
            let mut retries = 10;
            while retries > 0 {
                match client.post(&url).json(&msg).send().await {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            tracing::info!("Successfully registered to manager: {}", url);
                            break;
                        }
                    }
                    Err(_) => {}
                }
                retries -= 1;
                if retries > 0 { tokio::time::sleep(Duration::from_secs(1)).await; }
            }
        }
    }

    pub async fn run(&self) {
        tracing::info!("Worker started.");
        self.register_worker().await;
        
        // 1. Initialize Schedule Queue from Manager
        let initial_statuses = self.fetch_job_status().await;
        {
            let mut queue = self.schedule_queue.lock().await;
            let jobs = self.jobs.read().await;
            let mut unset = jobs.keys().map(|k| (k.clone(), true)).collect::<HashMap<String, bool>>();

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
                            // Schedule: LastUpdate + Interval
                            let next = s.last_update + job.interval;
                            queue.add_job(next, job.clone());
                        }
                    }
                }
            }
            // For jobs not known by manager, schedule now
            for (name, _) in unset {
                if let Some(job) = jobs.get(&name) {
                    queue.add_job(Utc::now(), job.clone());
                }
            }

            // Immediately send the initial schedule info back to manager
            if let Some(client) = &self.http_client {
                if let Some(manager_cfg) = &self.cfg.manager {
                    let mut s = vec![];
                    for info in queue.get_jobs() {
                        s.push(hustsync_internal::msg::MirrorSchedule {
                            name: info.job_name.clone(),
                            next_schedule: info.next_scheduled,
                        });
                    }
                    
                    let sched_msg = hustsync_internal::msg::MirrorSchedules { schedules: s };
                    let api_base = manager_cfg.api_base.as_deref().unwrap_or("http://localhost:12345");
                    let worker_name = self.name();
                    
                    for root in api_base.split(',').map(|str| str.trim()) {
                        let url = format!("{}/workers/{}/schedules", root.trim_end_matches('/'), worker_name);
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
        }

        // 2. Start HTTP Control Server
        let app_state = Arc::new(server::AppState { 
            jobs: Arc::clone(&self.jobs),
            schedule_queue: Arc::clone(&self.schedule_queue),
        });
        let app = server::make_http_server(app_state);
        let listen_addr = self.cfg.server.as_ref().and_then(|s| s.listen_addr.clone()).unwrap_or_else(|| "127.0.0.1".to_string());
        let listen_port = self.cfg.server.as_ref().and_then(|s| s.listen_port).unwrap_or(6000);
        let addr = format!("{}:{}", listen_addr, listen_port);
        let socket_addr: std::net::SocketAddr = addr.parse().expect("Failed to parse worker address");

        let is_tls = if let Some(server_cfg) = &self.cfg.server {
            !server_cfg.ssl_cert.as_deref().unwrap_or("").is_empty() 
            && !server_cfg.ssl_key.as_deref().unwrap_or("").is_empty()
        } else {
            false
        };

        let exit_token_server = self.exit_token.clone();
        
        if is_tls {
            let server_cfg = self.cfg.server.as_ref().unwrap();
            let cert_path = server_cfg.ssl_cert.as_deref().unwrap();
            let key_path = server_cfg.ssl_key.as_deref().unwrap();
            
            let tls_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(
                cert_path,
                key_path,
            )
            .await
            .expect("Failed to load worker TLS certificates");

            tracing::info!("Worker (HTTPS) listening on {}", addr);
            
            let handle = axum_server::Handle::new();
            let handle_clone = handle.clone();
            
            tokio::spawn(async move {
                exit_token_server.cancelled().await;
                handle_clone.graceful_shutdown(Some(Duration::from_secs(10)));
            });

            tokio::spawn(async move {
                axum_server::bind_rustls(socket_addr, tls_config)
                    .handle(handle)
                    .serve(app.into_make_service())
                    .await
                    .unwrap_or_else(|e| tracing::error!("Worker HTTPS server error: {}", e));
            });
        } else {
            let listener = tokio::net::TcpListener::bind(&socket_addr).await.expect("Failed to bind worker HTTP server");
            tracing::info!("Worker (HTTP) listening on {}", addr);
            tokio::spawn(async move {
                axum::serve(listener, app)
                    .with_graceful_shutdown(async move { exit_token_server.cancelled().await; })
                    .await
                    .unwrap_or_else(|e| tracing::error!("Worker HTTP server error: {}", e));
            });
        }

        // 3. Start Manager Message Receiver & Rescheduler
        let mut rx_guard = self.manager_rx.lock().await;
        if let Some(mut rx) = rx_guard.take() {
            let http_client = self.http_client.clone();
            let cfg = Arc::clone(&self.cfg);
            let worker_name = self.name();
            let schedule_queue = Arc::clone(&self.schedule_queue);
            let jobs_handle = Arc::clone(&self.jobs);

            tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    let (Some(client), Some(manager_cfg)) = (&http_client, &cfg.manager) else { continue };

                    // Syncing status is only meaningful when job is running. 
                    // If it's paused or disabled, a sync failure signal would be emitted
                    // which needs to be ignored. (Mirroring Go original logic)
                    {
                        let jobs = jobs_handle.read().await;
                        if let Some(job) = jobs.get(&msg.name) {
                            let state = job.state();
                            if msg.status == hustsync_internal::status::SyncStatus::Failed 
                                && (state == crate::job::STATE_PAUSED || state == crate::job::STATE_DISABLED) {
                                tracing::info!("Job {} state is {}, skip reporting Failed status", msg.name, state);
                                continue;
                            }
                        }
                    }

                    // Report to Manager
                    let smsg = hustsync_internal::msg::MirrorStatus {
                        name: msg.name.clone(),
                        worker: worker_name.clone(),
                        upstream: msg.upstream.clone(), 
                        size: msg.size.clone().unwrap_or_else(|| "unknown".to_string()),
                        error_msg: msg.msg.clone(),
                        last_update: Utc::now(), // Manager will override this, but we send it
                        last_started: Utc::now(), // Manager overrides
                        last_ended: Utc::now(), // Manager overrides
                        next_scheduled: Utc::now(), // Sent in a separate schedule update endpoint in Go, or kept here
                        status: msg.status,
                        is_master: msg.is_master, 
                    };

                    let api_base = manager_cfg.api_base.as_deref().unwrap_or("http://localhost:12345");
                    for root in api_base.split(',').map(|s| s.trim()) {
                        let url = format!("{}/workers/{}/jobs/{}", root.trim_end_matches('/'), worker_name, msg.name);
                        let _ = client.post(&url).json(&smsg).send().await;
                        
                    }

                    // Reschedule if needed
                    if msg.schedule {
                        let jobs = jobs_handle.read().await;
                        if let Some(job) = jobs.get(&msg.name) {
                            let next = Utc::now() + job.interval;
                            schedule_queue.lock().await.add_job(next, job.clone());
                        }
                    }

                    // Push updated schedule info back to manager
                    let mut s = vec![];
                    for info in schedule_queue.lock().await.get_jobs() {
                        s.push(hustsync_internal::msg::MirrorSchedule {
                            name: info.job_name.clone(),
                            next_schedule: info.next_scheduled,
                        });
                    }
                    
                    let sched_msg = hustsync_internal::msg::MirrorSchedules { schedules: s };
                    for root in api_base.split(',').map(|s| s.trim()) {
                        let url = format!("{}/workers/{}/schedules", root.trim_end_matches('/'), worker_name);
                        let _ = client.post(&url).json(&sched_msg).send().await;
                    }
                }
            });
        }

        // 4. Start Ticker (the Heartbeat of Automation)
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
                        } // Lock is dropped here

                        for job in ready_jobs {
                            let _ = job.send_ctrl(crate::job::CtrlAction::Start).await;
                        }
                    }
                    _ = exit_token.cancelled() => break,
                }
            }
        });
        
        self.exit_token.cancelled().await;
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
}
