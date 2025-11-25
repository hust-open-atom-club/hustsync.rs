use std::error::Error;
use tracing::{debug, error, info, trace, warn};
use tracing_subscriber::EnvFilter;

pub fn init_tracing(default_level: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(default_level));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_thread_names(true)
        .with_target(false) // we will use a fixed target ("hustsync") in helper logs
        .init();

    Ok(())
}

pub fn info_hustsync(msg: &str) {
    info!(target: "hustsync", "{}", msg);
}

pub fn debug_hustsync(msg: &str) {
    debug!(target: "hustsync", "{}", msg);
}

pub fn trace_hustsync(msg: &str) {
    trace!(target: "hustsync", "{}", msg);
}

pub fn warn_hustsync(msg: &str) {
    warn!(target: "hustsync", "{}", msg);
}

pub fn error_hustsync(msg: &str) {
    error!(target: "hustsync", "{}", msg);
}
