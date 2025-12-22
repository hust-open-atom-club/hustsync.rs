use std::{error::Error, io, path::PathBuf};

use clap::{Args, Parser, ValueHint::FilePath};
use hustsync_internal::logger::init_logger;
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
    /// Enable systemd-compactiable logging
    #[arg(long)]
    with_systemd: bool,
    /// The pid file of the manager process
    #[arg(long, default_value = "/run/hustsync/hustsync.manager.pid")]
    pidfile: PathBuf,
}

// TODO
#[derive(Args, Debug)]
struct WorkerArgs {}

// TODO
fn start_manager(manager_args: ManagerArgs) -> () {
    println!("Starting HustSync Manager...");
    println!("Manager Args: {:?}", manager_args);
    todo!()
}

// TODO
fn start_worker(worker_args: WorkerArgs) -> () {
    println!("Starting HustSync Worker...");
    todo!()
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Manager(m) => start_manager(m),
        Commands::Worker(w) => start_worker(w),
    }

    Ok(())
}
