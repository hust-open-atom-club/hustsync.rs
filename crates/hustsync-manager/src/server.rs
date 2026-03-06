use axum::{
    Router,
    routing::{delete, get, post},
};
use hustsync_config_parser::ManagerConfig;
use hustsync_internal::util::create_http_client;
use once_cell::sync::OnceCell;
use reqwest::{Certificate, Client, Response};
use std::error::Error;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::{catch_panic::CatchPanicLayer, trace::TraceLayer};

use crate::database::{DbAdapterTrait, make_db_adapter};

pub struct Manager {
    pub config: Arc<ManagerConfig>,
    pub engine: Router,
    pub adapter: Option<Box<dyn DbAdapterTrait>>,
    // pub rwmu: RwLock<()>,
    pub http_client: Option<Client>,
}

static MANAGER: OnceCell<Manager> = OnceCell::new();
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
    if config.debug.unwrap() {
        manager.engine = manager.engine.layer(TraceLayer::new_for_http());
    }

    if let Some(files) = &config.files {
        if let Some(ca) = &files.ca_cert {
            if !ca.is_empty() {
                match create_http_client(Some(ca)) {
                    Ok(client) => {
                        manager.http_client = Some(client);
                    }
                    Err(e) => {
                        tracing::error!("Failed to create HTTP client with CA certificate: {}", e);
                        return Err(e);
                    }
                }
            }
        }
        if let Some(db_file) = &files.db_file {
            if !db_file.is_empty() {
                match make_db_adapter(
                    config
                        .as_ref()
                        .files
                        .as_ref()
                        .unwrap()
                        .db_type
                        .clone()
                        .unwrap_or_default(),
                    db_file,
                ) {
                    Ok(adapter) => {
                        manager.adapter = Some(adapter);
                    }
                    Err(e) => {
                        tracing::error!("Failed to create database adapter: {}", e);
                        return Err(Box::new(e));
                    }
                };
            }
        }
    }

    todo!()
}
