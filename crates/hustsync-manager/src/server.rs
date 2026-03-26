use axum::{
    Router, middleware,
    routing::{delete, get, post},
};
use axum_server::tls_rustls::RustlsConfig;
use hustsync_config_parser::ManagerConfig;
use hustsync_internal::util::create_http_client;
use once_cell::sync::OnceCell;
use reqwest::{Certificate, Client, Response};
use std::sync::Arc;
use std::{error::Error, time::Duration};
use tokio::sync::RwLock;
use tower_http::timeout::TimeoutLayer;
use tower_http::{catch_panic::CatchPanicLayer, trace::TraceLayer};

use crate::database::{DbAdapterTrait, make_db_adapter};
use crate::handlers;

pub(crate) const ERROR_KEY: &str = "error";
pub(crate) const INFO_KEY: &str = "message";

static MANAGER: OnceCell<Manager> = OnceCell::new();

pub struct Manager {
    pub config: Arc<ManagerConfig>,
    pub engine: Router,
    pub adapter: Option<Box<dyn DbAdapterTrait>>,
    // pub rwmu: RwLock<()>,
    pub http_client: Option<Client>,
}

pub fn get_hustsync_manager(
    config: Arc<ManagerConfig>,
) -> Result<&'static Manager, Box<dyn Error>> {
    if let Some(manager) = MANAGER.get() {
        return Ok(manager);
    }

    let mut manager = Manager {
        config: Arc::clone(&config),
        engine: Router::new(),
        adapter: None,
        http_client: None,
    };

    manager.engine = manager.engine.layer(CatchPanicLayer::new());
    if config.debug {
        manager.engine = manager.engine.layer(TraceLayer::new_for_http());
    }

    if !config.files.ca_cert.is_empty() {
        match create_http_client(Some(&config.files.ca_cert)) {
            Ok(client) => {
                manager.http_client = Some(client);
            }
            Err(e) => {
                tracing::error!("Failed to create HTTP client with CA certificate: {}", e);
                return Err(e);
            }
        }
    }
    if !config.files.db_file.is_empty() {
        match make_db_adapter(&config.files.db_type, &config.files.db_file) {
            Ok(adapter) => {
                manager.adapter = Some(adapter);
            }
            Err(e) => {
                tracing::error!("Failed to create database adapter: {}", e);
                return Err(Box::new(e));
            }
        };
    }

    manager.engine = manager
        .engine
        .layer(middleware::from_fn(crate::middleware::context_error_logger));

    manager.engine = manager.engine.route("/ping", get(handlers::ping_handler));
    manager.engine = manager.engine.route("/jobs", get(handlers::list_all_jobs));
    manager.engine = manager.engine.route("/jobs/disabled", delete(handlers::flush_disabled_jobs));
    manager.engine = manager.engine.route("/workers", get(handlers::list_all_workers));
    manager.engine = manager.engine.route("/workers", post(handlers::register_worker));
    manager.engine = manager.engine.route("/cmd", post(handlers::handle_cmd));

    let worker_validate_group = Router::new()
        // .route("/{id}", delete(delete_worker))
        .route("/{id}/jobs/{job}", post(handlers::update_job_of_worker))
        .route("/{id}/jobs/size", post(handlers::update_mirror_size))
        .route("/{id}/schedules", post(handlers::update_schedules_of_worker));
    // // .layer(middleware::from_fn(crate::middleware::worker_id_validator));
    manager.engine = manager.engine.nest("/workers", worker_validate_group);
    MANAGER
        .set(manager)
        .map_err(|_| "Manager cell already initialized")?;

    MANAGER
        .get()
        .ok_or_else(|| "Failed to retrieve Manager after setting".into())
}

impl Manager {
    pub async fn run(&'static self) -> Result<(), Box<dyn Error>> {
        let addr = format!("{}:{}", self.config.server.addr, self.config.server.port);
        let socket_addr: std::net::SocketAddr = addr.parse().expect("Failed to parse address");

        let app = self.engine.clone().layer(TimeoutLayer::with_status_code(
            axum::http::StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(10),
        ));

        let is_tls =
            !self.config.server.ssl_cert.is_empty() && !self.config.server.ssl_key.is_empty();

        if !is_tls {
            let listener = tokio::net::TcpListener::bind(&socket_addr)
                .await
                .expect("Failed to bind address");
            tracing::info!("Manager (HTTP) listening on {}", addr);
            axum::serve(listener, app).await.expect("Server error");
        } else {
            let tls_config = RustlsConfig::from_pem_file(
                &self.config.server.ssl_cert,
                &self.config.server.ssl_key,
            )
            .await
            .expect("Failed to load TLS certificates");

            tracing::info!("Manager (HTTPS) listening on {}", addr);
            axum_server::bind_rustls(socket_addr, tls_config)
                .serve(app.into_make_service())
                .await
                .expect("Server error");
        }
        Ok(())
    }
}
