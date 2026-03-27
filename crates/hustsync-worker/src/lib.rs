use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, mpsc, Semaphore};
use tokio_util::sync::CancellationToken;
use reqwest::Client;
use hustsync_config_parser::WorkerConfig;
use hustsync_internal::status::SyncStatus;
use hustsync_internal::msg::WorkerStatus;
use chrono::Utc;

pub struct JobMessage {
    pub status: SyncStatus,
    pub name: String,
    pub msg: String,
    pub schedule: bool,
}

// will be implemented later in job.rs
pub struct MirrorJob {
    pub name: String,

}

pub struct Worker {
    pub cfg: Arc<WorkerConfig>,
    pub jobs: Arc<RwLock<HashMap<String, MirrorJob>>>,
    
    pub manager_tx: mpsc::Sender<JobMessage>,
    pub semaphore: Arc<Semaphore>,
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

        let (manager_tx, _manager_rx) = mpsc::channel(32);

        let mut worker = Worker {
            cfg: Arc::clone(&cfg),
            jobs: Arc::new(RwLock::new(HashMap::new())),
            manager_tx,
            semaphore: Arc::new(Semaphore::new(concurrent)),
            exit_token: CancellationToken::new(),
            http_client: None,
        };

        if let Some(manager_cfg) = &cfg.manager {
            if let Some(ca_cert) = &manager_cfg.ca_cert {
                if !ca_cert.is_empty() {
                    match hustsync_internal::util::create_http_client(Some(ca_cert)) {
                        Ok(client) => {
                            worker.http_client = Some(client);
                        }
                        Err(e) => {
                            tracing::error!("Error initializing HTTP client: {}", e);
                        }
                    }
                }
            }
        }
        
        if worker.http_client.is_none() {
            if let Ok(client) = hustsync_internal::util::create_http_client(None) {
                worker.http_client = Some(client);
            }
        }

        worker
    }

    // Returns the name (ID) of the worker
    pub fn name(&self) -> String {
        self.cfg.global.as_ref()
            .and_then(|g| g.name.clone())
            .unwrap_or_else(|| "default_worker".to_string())
    }

    // Returns the URL that the worker's HTTP server will listen on
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

        let Some(client) = &self.http_client else {
            tracing::error!("HTTP client is not initialized, cannot register worker.");
            return;
        };

        for root in api_bases {
            let url = format!("{}/workers", root.trim_end_matches('/'));
            tracing::debug!("Registering on manager URL: {}", url);

            let mut retries = 10;
            while retries > 0 {
                match client.post(&url).json(&msg).send().await {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            tracing::info!("Successfully registered to manager: {}", url);
                            break;
                        } else {
                            tracing::error!("Failed to register worker. Manager returned: {}", resp.status());
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to register worker: {}", e);
                    }
                }

                retries -= 1;
                if retries > 0 {
                    tracing::warn!("Retrying... ({})", retries);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    // Run the worker
    pub async fn run(&self) {
        tracing::info!("Worker started.");
        
        self.register_worker().await;
        
        // TODO: go w.runHTTPServer()
        // TODO: w.runSchedule()
        
        // wait for exit token
        self.exit_token.cancelled().await;
        tracing::info!("Worker stopped.");
    }
}
