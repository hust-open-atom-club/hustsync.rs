#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use clap::{Args, Parser, ValueHint::FilePath};
use hustsync_config_parser::{WorkerConfig, parse_config};
use hustsync_manager::Manager;
use hustsync_worker::Worker;
use tracing::{info, warn};

/// Wait for SIGTERM or SIGINT on Unix; Ctrl-C on other platforms.
#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Failed to install SIGTERM handler: {}", e);
            return;
        }
    };
    let mut sigint = match signal(SignalKind::interrupt()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Failed to install SIGINT handler: {}", e);
            return;
        }
    };
    wait_for_unix_signal(&mut sigterm, &mut sigint).await;
}

#[cfg(unix)]
async fn wait_for_unix_signal(
    sigterm: &mut tokio::signal::unix::Signal,
    sigint: &mut tokio::signal::unix::Signal,
) {
    tokio::select! {
        _ = sigterm.recv() => tracing::info!("Received SIGTERM, shutting down..."),
        _ = sigint.recv() => tracing::info!("Received SIGINT, shutting down..."),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    if let Err(e) = tokio::signal::ctrl_c().await {
        tracing::error!("Failed to install Ctrl-C handler: {}", e);
        return;
    }
    tracing::info!("Received Ctrl-C, shutting down...");
}

#[derive(Parser, Debug)]
#[command(
    name = "hustsync-cli",
    version = env!("CARGO_PKG_VERSION"),
    about = "Command line interface for HustSync",
    arg_required_else_help = true
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand, Debug)]
enum Commands {
    /// Start the HustSync manager (aliased as 'm')
    #[command(alias = "m")]
    Manager(ManagerArgs),
    /// Start the HustSync worker (aliased as 'w')
    #[command(alias = "w")]
    Worker(WorkerArgs),
}

#[derive(Args, Debug)]
struct ManagerArgs {
    /// Load manager configurations from `FILE`
    #[arg(short, long, value_name = "config", value_hint = FilePath)]
    config: Option<PathBuf>,
    /// The manager will listen on `ADDR`
    #[arg(short, long, value_name = "addr")]
    addr: Option<String>,
    /// The manager will bind to `PORT`
    #[arg(short, long, value_name = "port")]
    port: Option<u16>,
    /// Use SSL certificate from `FILE`
    #[arg(long, value_name = "cert", value_hint = FilePath)]
    cert: Option<PathBuf>,
    /// Use SSL key from `FILE`
    #[arg(long, value_name = "key", value_hint = FilePath)]
    key: Option<PathBuf>,
    /// Use `FILE` as the database file
    #[arg(long, value_name = "db-file", value_hint = FilePath)]
    db_file: Option<PathBuf>,
    /// Use database type `TYPE`
    #[arg(long, value_name = "TYPE")]
    db_type: Option<String>,
    /// Run manager in debug mode
    #[arg(long)]
    debug: bool,
    // Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
    /// Enable systemd-compactiable logging
    #[arg(long)]
    with_systemd: bool,
    /// The pid file of the manager process
    #[arg(long, default_value = "/run/hustsync/hustsync.manager.pid")]
    pidfile: PathBuf,
}

#[derive(Args, Debug)]
struct WorkerArgs {
    /// Load worker configurations from `FILE`
    #[arg(short, long, value_name = "config", value_hint = FilePath)]
    config: Option<PathBuf>,
    /// Run worker in verbose mode
    #[arg(long)]
    verbose: bool,
    /// Run worker in debug mode
    #[arg(long)]
    debug: bool,
    /// Enable systemd-compactiable logging
    #[arg(long)]
    with_systemd: bool,
    #[arg(long, value_name = "pid-file", value_hint = FilePath)]
    pid_file: Option<PathBuf>,
}

#[allow(clippy::cognitive_complexity)]
async fn start_manager(manager_args: ManagerArgs) -> Result<()> {
    hustsync_internal::logger::init_logger(
        manager_args.verbose,
        manager_args.debug,
        manager_args.with_systemd,
    );

    let mut config = match hustsync_manager::load_config(manager_args.config.unwrap_or_default()) {
        Ok(cfg) => cfg,
        Err(err) => {
            warn!("Error loading manager config: {err}.");
            std::process::exit(1);
        }
    };

    // Override with command-line arguments
    if let Some(addr) = manager_args.addr {
        config.server.addr = addr;
    }
    if let Some(port) = manager_args.port {
        config.server.port = port;
    }
    if let Some(cert) = manager_args.cert {
        config.server.ssl_cert = cert.to_string_lossy().to_string();
    }
    if let Some(key) = manager_args.key {
        config.server.ssl_key = key.to_string_lossy().to_string();
    }
    if let Some(db_file) = manager_args.db_file {
        config.files.db_file = db_file.to_string_lossy().to_string();
    }
    if let Some(db_type) = manager_args.db_type {
        config.files.db_type = db_type;
    }
    if manager_args.debug {
        config.debug = true;
    }

    let config = Arc::new(config);

    let manager = match Manager::new(config) {
        Ok(m) => m,
        Err(err) => {
            warn!("Error initializing manager: {err}.");
            std::process::exit(1);
        }
    };
    info!("Run hustsync manager server.");

    let manager = Arc::new(manager);
    Arc::clone(&manager)
        .run(shutdown_signal())
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
        .with_context(|| "manager start")?;
    Ok(())
}

async fn start_worker(worker_args: WorkerArgs) -> Result<()> {
    hustsync_internal::logger::init_logger(
        worker_args.verbose,
        worker_args.debug,
        worker_args.with_systemd,
    );

    let config_path = worker_args
        .config
        .unwrap_or_else(|| PathBuf::from("worker.conf"));

    let config: WorkerConfig = match parse_config(&config_path) {
        Ok(cfg) => cfg,
        Err(err) => {
            warn!("Error loading worker config from {:?}: {err}.", config_path);
            std::process::exit(1);
        }
    };

    info!("Initializing HustSync Worker...");

    let worker = Worker::new(config);
    // Signal delivery is decoupled from the worker's exit_token so the
    // worker's existing shutdown path (exit_token.cancelled() → shutdown())
    // handles cleanup without any changes to worker internals.
    let exit_token = worker.exit_token.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        exit_token.cancel();
    });
    worker.run().await;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Manager(m) => start_manager(m).await.with_context(|| "manager start"),
        Commands::Worker(w) => start_worker(w).await.with_context(|| "worker start"),
    }
}
