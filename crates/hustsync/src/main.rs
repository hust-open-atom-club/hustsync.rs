use std::{error::Error, io, path::PathBuf, sync::Arc};

use clap::{Args, Parser, ValueHint::FilePath};
use hustsync_internal::logger::{self, init_logger};
use hustsync_manager::Manager;
use tracing::{info, warn};

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
    pid_file: PathBuf,
}

// TODO
fn start_manager(manager_args: ManagerArgs) -> Result<(), Box<dyn Error>> {
    hustsync_internal::logger::init_logger(
        manager_args.verbose,
        manager_args.debug,
        manager_args.with_systemd,
    );

    let config = match hustsync_manager::load_config(manager_args.config.unwrap_or_default()) {
        Ok(cfg) => cfg,
        Err(err) => {
            warn!("Error loading manager config: {err}.");
            std::process::exit(1);
        }
    };
    let config = Arc::new(config);
    //? SET WEB FRAMEWORK TO DEBUG MODE

    let manager = match hustsync_manager::get_hustsync_manager(config) {
        Ok(m) => m,
        Err(err) => {
            warn!("Error initializing manager: {err}.");
            std::process::exit(1);
        }
    };
    info!("Run hustsync manager server.");
    // TODO
    // manager.run()?;
    Ok(())
}

// TODO
fn start_worker(worker_args: WorkerArgs) -> Result<(), Box<dyn Error>> {
    hustsync_internal::logger::init_logger(
        worker_args.verbose,
        worker_args.debug,
        worker_args.with_systemd,
    );
    //? SET WEB FRAMEWORK TO DEBUG MODE

    info!("Run hustsync worker.");
    // TODO
    // let config = match
    todo!()
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Manager(m) => start_manager(m),
        Commands::Worker(w) => start_worker(w),
    }
}
