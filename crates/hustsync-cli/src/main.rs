use std::{error::Error, io, path::PathBuf};

use clap::{Parser, ValueHint::FilePath};
use hustsync_internal::logger::init_logger;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(name = "hustsync-cli")]
pub struct Cli {
    /// Enable verbose output
    #[arg(short = 'v', long)]
    pub verbose: bool,

    /// Enable debug mode
    #[arg(short = 'd', long)]
    pub debug: bool,

    /// Enable systemd integration
    #[arg(long = "with-systemd")]
    pub with_systemd: bool,

    /// Path to configuration file
    #[arg(short = 'c', long, value_name = "PATH", value_hint = FilePath)]
    pub config: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    init_logger(cli.verbose, cli.debug, cli.with_systemd);

    let Some(config) = cli.config else {
        warn!("No configuration file provided; please supply -c/--config");
        return Err(Box::new(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing configuration file",
        )));
    };

    let cfg = hustsync_manager::load_config(&config).map_err(|e| {
        warn!(
            "Failed to load configuration from {}: {}",
            config.display(),
            e
        );
        e
    })?;

    // TODO set manager restful mode ?

    let manager = hustsync_manager::get_hustsync_manager(cfg).map_err(|e| {
        warn!("Failed to create hustsync manager: {}", e);
        e
    })?;

    info!("HustSync Manager started successfully.");
    // TODO start manager restful server ?

    Ok(())
}
