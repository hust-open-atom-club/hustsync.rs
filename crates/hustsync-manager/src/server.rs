use axum::{
    Router, middleware,
    routing::{delete, get, post},
};
use axum_server::tls_rustls::RustlsConfig;
use hustsync_config_parser::ManagerConfig;
use hustsync_internal::util::create_http_client;
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;
use tower_http::timeout::TimeoutLayer;
use tower_http::{catch_panic::CatchPanicLayer, trace::TraceLayer};

use crate::ManagerError;
use crate::database::{DbAdapterTrait, make_db_adapter};
use crate::handlers;

pub(crate) const ERROR_KEY: &str = "error";
pub(crate) const INFO_KEY: &str = "message";

pub struct Manager {
    pub config: Arc<ManagerConfig>,
    pub adapter: Option<Arc<dyn DbAdapterTrait>>,
    pub http_client: Option<Client>,
}

impl Manager {
    pub fn new(config: Arc<ManagerConfig>) -> Result<Self, ManagerError> {
        let mut manager = Manager {
            config: Arc::clone(&config),
            adapter: None,
            http_client: None,
        };

        if !config.files.ca_cert.is_empty() {
            match create_http_client(Some(&config.files.ca_cert)) {
                Ok(client) => {
                    manager.http_client = Some(client);
                }
                Err(e) => {
                    tracing::error!("Failed to create HTTP client with CA certificate: {}", e);
                    return Err(ManagerError::Tls(e.to_string()));
                }
            }
        } else {
            match create_http_client(None) {
                Ok(client) => manager.http_client = Some(client),
                Err(e) => tracing::error!("Failed to create default HTTP client: {}", e),
            }
        }

        if !config.files.db_file.is_empty() {
            match make_db_adapter(&config.files.db_type, &config.files.db_file) {
                Ok(adapter) => {
                    if let Err(e) = adapter.init() {
                        tracing::error!("Failed to initialize database tables: {}", e);
                        return Err(ManagerError::Adapter(e));
                    }
                    manager.adapter = Some(Arc::from(adapter));
                }
                Err(e) => {
                    tracing::error!("Failed to create database adapter: {}", e);
                    return Err(ManagerError::Adapter(e));
                }
            };
        }

        Ok(manager)
    }

    pub fn make_router(self: Arc<Self>) -> Router {
        let config = Arc::clone(&self.config);

        let mut router = Router::new()
            .route("/ping", get(handlers::ping_handler))
            .route("/jobs", get(handlers::list_all_jobs))
            .route("/jobs/disabled", delete(handlers::flush_disabled_jobs))
            .route("/workers", get(handlers::list_all_workers))
            .route("/workers", post(handlers::register_worker))
            .route("/cmd", post(handlers::handle_cmd));

        let worker_validate_group = Router::new()
            .route("/{id}", delete(handlers::delete_worker))
            .route("/{id}/jobs", get(handlers::list_jobs_of_worker))
            .route("/{id}/jobs/{job}", post(handlers::update_job_of_worker))
            .route("/{id}/jobs/{job}/size", post(handlers::update_mirror_size))
            .route(
                "/{id}/schedules",
                post(handlers::update_schedules_of_worker),
            )
            .layer(middleware::from_fn_with_state(
                Arc::clone(&self),
                crate::middleware::worker_id_validator,
            ));

        router = router.nest("/workers", worker_validate_group);

        router = router
            .layer(middleware::from_fn(crate::middleware::context_error_logger))
            .layer(CatchPanicLayer::new());

        if config.debug {
            router = router.layer(TraceLayer::new_for_http());
        }

        router.with_state(self)
    }

    pub async fn run(self: Arc<Self>) -> Result<(), ManagerError> {
        let addr = format!("{}:{}", self.config.server.addr, self.config.server.port);
        let socket_addr: std::net::SocketAddr = addr
            .parse()
            .map_err(|e: std::net::AddrParseError| ManagerError::Bind(e.to_string()))?;

        let app = self.clone().make_router().layer(
            TimeoutLayer::with_status_code(
                axum::http::StatusCode::REQUEST_TIMEOUT,
                Duration::from_secs(10),
            ),
        );

        let is_tls =
            !self.config.server.ssl_cert.is_empty() && !self.config.server.ssl_key.is_empty();

        if !is_tls {
            let listener = tokio::net::TcpListener::bind(&socket_addr)
                .await
                .map_err(|e| ManagerError::Bind(e.to_string()))?;
            tracing::info!("Manager (HTTP) listening on {}", addr);
            axum::serve(listener, app)
                .await
                .map_err(|e| ManagerError::Bind(e.to_string()))?;
        } else {
            let tls_config = RustlsConfig::from_pem_file(
                &self.config.server.ssl_cert,
                &self.config.server.ssl_key,
            )
            .await
            .map_err(|e| ManagerError::Tls(e.to_string()))?;

            tracing::info!("Manager (HTTPS) listening on {}", addr);
            axum_server::bind_rustls(socket_addr, tls_config)
                .serve(app.into_make_service())
                .await
                .map_err(|e| ManagerError::Bind(e.to_string()))?;
        }
        Ok(())
    }
}
