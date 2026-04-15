use tracing_subscriber::EnvFilter;

use crate::ManagerError;

pub fn init_tracing(default_level: &str) -> Result<(), ManagerError> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(default_level));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_thread_names(true)
        .with_target(false)
        .init();

    Ok(())
}
