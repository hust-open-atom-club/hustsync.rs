#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::process::exit;

use hustsync_internal::msg::{ClientCmd, CmdVerb, MirrorStatus, WorkerStatus};
use hustsync_internal::status::SyncStatus;
use hustsync_internal::status_web::WebMirrorStatus;

#[derive(Deserialize, Default)]
struct CtlConfig {
    manager_addr: Option<String>,
    manager_port: Option<u16>,
    ca_cert: Option<String>,
}

fn load_config(path: &str, cfg: &mut CtlConfig, strict: bool) -> Result<()> {
    match std::fs::read_to_string(path) {
        Ok(content) => match toml::from_str::<CtlConfig>(&content) {
            Ok(parsed) => {
                if let Some(addr) = parsed.manager_addr
                    && !addr.is_empty()
                {
                    cfg.manager_addr = Some(addr);
                }
                if let Some(port) = parsed.manager_port {
                    cfg.manager_port = Some(port);
                }
                if let Some(ca) = parsed.ca_cert
                    && !ca.is_empty()
                {
                    cfg.ca_cert = Some(ca);
                }
                Ok(())
            }
            Err(e) => {
                let err_msg = format!("Failed to parse config {}: {}", path, e);
                if strict {
                    Err(anyhow!("{}", err_msg))
                } else {
                    tracing::debug!("{}", err_msg);
                    Ok(())
                }
            }
        },
        Err(e) => {
            let err_msg = format!("Failed to read config {}: {}", path, e);
            if strict {
                Err(anyhow!("{}", err_msg))
            } else {
                tracing::debug!("{}", err_msg);
                Ok(())
            }
        }
    }
}

// hustsync_internal now returns typed InternalError; this bridge preserves
// the display chain for .with_context().
fn box_err(e: hustsync_internal::InternalError) -> anyhow::Error {
    anyhow!("{:#}", e)
}

#[derive(Parser)]
#[command(name = "hustsynctl")]
#[command(about = "control client for hustsync manager", version)]
struct Cli {
    /// Read configuration from FILE
    #[arg(short, long)]
    config: Option<String>,

    /// The manager server address
    #[arg(short, long)]
    manager: Option<String>,

    /// The manager server port
    #[arg(short, long)]
    port: Option<u16>,

    /// Trust root CA cert file CERT
    #[arg(long)]
    ca_cert: Option<String>,

    /// Force HTTPS connection
    #[arg(long)]
    https: bool,

    /// Enable verbosely logging
    #[arg(short, long)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// List jobs of workers
    List {
        /// List all jobs of all workers
        #[arg(short, long)]
        all: bool,

        /// Filter output based on status provided
        #[arg(short, long)]
        status: Option<String>,

        /// Pretty-print using a Tera template
        #[arg(short, long)]
        format: Option<String>,

        workers: Vec<String>,
    },
    /// Flush disabled jobs
    Flush,
    /// List workers
    Workers,
    /// Remove a worker
    RmWorker {
        /// worker-id of the worker to be removed
        #[arg(short, long)]
        worker: String,
    },
    /// Set mirror size
    SetSize {
        /// specify worker-id of the mirror job
        #[arg(short, long)]
        worker: String,

        mirror: String,
        size: String,
    },
    /// Start a job
    Start {
        /// Send the command to worker
        #[arg(short, long)]
        worker: Option<String>,
        /// Override the concurrent limit
        #[arg(short, long)]
        force: bool,
        mirror: String,
        args: Option<String>,
    },
    /// Stop a job
    Stop {
        #[arg(short, long)]
        worker: Option<String>,
        mirror: String,
        args: Option<String>,
    },
    /// Disable a job
    Disable {
        #[arg(short, long)]
        worker: Option<String>,
        mirror: String,
        args: Option<String>,
    },
    /// Restart a job
    Restart {
        #[arg(short, long)]
        worker: Option<String>,
        mirror: String,
        args: Option<String>,
    },
    /// Tell worker to reload configurations
    Reload {
        #[arg(short, long)]
        worker: String,
    },
    /// Ping a job
    Ping {
        #[arg(short, long)]
        worker: Option<String>,
        mirror: String,
        args: Option<String>,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let log_level = if cli.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(format!("hustsynctl={}", log_level))
        .init();

    let mut config = CtlConfig {
        manager_addr: Some("localhost".to_string()),
        manager_port: Some(12345),
        ca_cert: None,
    };

    let _ = load_config("/etc/hustsync/ctl.conf", &mut config, false);
    if let Ok(home) = std::env::var("HOME") {
        let _ = load_config(
            &format!("{}/.config/hustsync/ctl.conf", home),
            &mut config,
            false,
        );
    }

    if let Some(c) = &cli.config
        && let Err(e) = load_config(c, &mut config, true)
    {
        eprintln!("Error loading explicit config: {}", e);
        exit(1);
    }

    if let Some(m) = cli.manager
        && !m.is_empty()
    {
        config.manager_addr = Some(m);
    }
    if let Some(p) = cli.port {
        config.manager_port = Some(p);
    }
    if let Some(ca) = cli.ca_cert
        && !ca.is_empty()
    {
        config.ca_cert = Some(ca);
    }

    // Both fields are initialised above with hardcoded defaults; they can only
    // be None if a config file explicitly cleared them.  Bail with an
    // actionable message so the operator knows how to fix the situation.
    let addr_raw = match config.manager_addr {
        Some(ref a) if !a.is_empty() => a.clone(),
        _ => {
            eprintln!("Error: manager_addr not set; pass --manager or set manager_addr in config");
            exit(1);
        }
    };
    let manager_port = match config.manager_port {
        Some(p) => p,
        None => {
            eprintln!("Error: manager_port not set; pass --port or set manager_port in config");
            exit(1);
        }
    };

    let base_url = if addr_raw.starts_with("http://") || addr_raw.starts_with("https://") {
        addr_raw
    } else {
        let scheme = if cli.https || config.ca_cert.is_some() || manager_port == 443 {
            "https"
        } else {
            "http"
        };
        format!("{}://{}:{}", scheme, addr_raw, manager_port)
    };

    tracing::debug!("Using manager address: {}", base_url);

    let client = match hustsync_internal::util::create_http_client(config.ca_cert.as_ref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error initializing HTTP client: {}", e);
            exit(1);
        }
    };

    let result = match cli.command {
        Commands::Workers => list_workers(&base_url, &client).await,
        Commands::List {
            all,
            status,
            format,
            workers,
        } => list_jobs(&base_url, &client, all, status, format, workers).await,
        Commands::Flush => flush_disabled_jobs(&base_url, &client).await,
        Commands::RmWorker { worker } => rm_worker(&base_url, &client, &worker).await,
        Commands::SetSize {
            worker,
            mirror,
            size,
        } => set_size(&base_url, &client, &worker, &mirror, &size).await,
        Commands::Start {
            force,
            worker,
            mirror,
            args,
        } => {
            let mut opts = HashMap::new();
            if force {
                opts.insert("force".to_string(), true);
            }
            send_cmd(
                &base_url,
                &client,
                CmdVerb::Start,
                worker,
                Some(mirror),
                args,
                opts,
            )
            .await
        }
        Commands::Stop {
            worker,
            mirror,
            args,
        } => {
            send_cmd(
                &base_url,
                &client,
                CmdVerb::Stop,
                worker,
                Some(mirror),
                args,
                HashMap::new(),
            )
            .await
        }
        Commands::Disable {
            worker,
            mirror,
            args,
        } => {
            send_cmd(
                &base_url,
                &client,
                CmdVerb::Disable,
                worker,
                Some(mirror),
                args,
                HashMap::new(),
            )
            .await
        }
        Commands::Restart {
            worker,
            mirror,
            args,
        } => {
            send_cmd(
                &base_url,
                &client,
                CmdVerb::Restart,
                worker,
                Some(mirror),
                args,
                HashMap::new(),
            )
            .await
        }
        Commands::Ping {
            worker,
            mirror,
            args,
        } => {
            send_cmd(
                &base_url,
                &client,
                CmdVerb::Ping,
                worker,
                Some(mirror),
                args,
                HashMap::new(),
            )
            .await
        }
        Commands::Reload { worker } => {
            send_cmd(
                &base_url,
                &client,
                CmdVerb::Reload,
                Some(worker),
                None,
                None,
                HashMap::new(),
            )
            .await
        }
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        exit(1);
    }
}

async fn list_workers(base_url: &str, client: &Client) -> Result<()> {
    let url = format!("{}/workers", base_url);
    let workers: Vec<WorkerStatus> = hustsync_internal::util::get_json(&url, Some(client))
        .await
        .map_err(box_err)
        .with_context(|| format!("GET {}", url))?;
    let json = serde_json::to_string_pretty(&workers).context("serialising worker list")?;
    println!("{}", json);
    Ok(())
}

fn try_migrate_go_template(tpl: &str) -> String {
    if tpl.contains("{{.") {
        tracing::warn!(
            "Detected Go-style template syntax '{{{{.FieldName}}}}'. Automatically migrating to Tera syntax '{{{{field_name}}}}'. Please update your scripts."
        );
        tpl.replace("{{.", "{{")
    } else {
        tpl.to_string()
    }
}

async fn list_jobs(
    base_url: &str,
    client: &Client,
    all: bool,
    status: Option<String>,
    format_tpl: Option<String>,
    worker_ids: Vec<String>,
) -> Result<()> {
    if all {
        let url = format!("{}/jobs", base_url);
        let mut jobs: Vec<WebMirrorStatus> = hustsync_internal::util::get_json(&url, Some(client))
            .await
            .map_err(box_err)
            .with_context(|| format!("GET {}", url))?;

        if let Some(s) = status {
            let filter_statuses: Vec<String> =
                s.split(',').map(|s| format!("\"{}\"", s.trim())).collect();
            let mut expected_statuses = Vec::new();
            for st in filter_statuses {
                match serde_json::from_str::<SyncStatus>(&st) {
                    Ok(SyncStatus::Unknown) | Err(_) => {
                        bail!(
                            "Invalid status filter: {}. Supported values: success, failed, syncing, pre-syncing, paused, disabled",
                            st
                        );
                    }
                    Ok(parsed_status) => expected_statuses.push(parsed_status),
                }
            }
            jobs.retain(|job| expected_statuses.contains(&job.status));
        }

        if let Some(tpl) = format_tpl {
            let tpl = try_migrate_go_template(&tpl);
            let mut tera = tera::Tera::default();
            tera.add_raw_template("job_fmt", &tpl)
                .with_context(|| "invalid format template")?;
            for job in jobs {
                let context =
                    tera::Context::from_serialize(&job).context("building template context")?;
                println!(
                    "{}",
                    tera.render("job_fmt", &context)
                        .context("rendering template")?
                );
            }
        } else {
            let json = serde_json::to_string_pretty(&jobs).context("serialising job list")?;
            println!("{}", json);
        }
    } else {
        if worker_ids.is_empty() {
            bail!("Usage Error: jobs command need at least one arguments or \"--all\" flag.");
        }

        let mut all_jobs: Vec<MirrorStatus> = Vec::new();
        for worker_id in worker_ids {
            let url = format!("{}/workers/{}/jobs", base_url, worker_id);
            let jobs = hustsync_internal::util::get_json::<Vec<MirrorStatus>>(&url, Some(client))
                .await
                .map_err(box_err)
                .with_context(|| format!("GET {} (worker {})", url, worker_id))?;
            all_jobs.extend(jobs);
        }

        if let Some(tpl) = format_tpl {
            let tpl = try_migrate_go_template(&tpl);
            let mut tera = tera::Tera::default();
            tera.add_raw_template("job_fmt", &tpl)
                .with_context(|| "invalid format template")?;
            for job in all_jobs {
                let context =
                    tera::Context::from_serialize(&job).context("building template context")?;
                println!(
                    "{}",
                    tera.render("job_fmt", &context)
                        .context("rendering template")?
                );
            }
        } else {
            let json = serde_json::to_string_pretty(&all_jobs).context("serialising job list")?;
            println!("{}", json);
        }
    }
    Ok(())
}

async fn flush_disabled_jobs(base_url: &str, client: &Client) -> Result<()> {
    let url = format!("{}/jobs/disabled", base_url);
    let resp = client
        .delete(&url)
        .send()
        .await
        .with_context(|| format!("DELETE {}", url))?;
    if resp.status().is_success() {
        println!("Successfully flushed disabled jobs");
        Ok(())
    } else {
        let status = resp.status();
        let err_text = resp.text().await.unwrap_or_default();
        bail!("Failed with status: {} {}", status, err_text)
    }
}

async fn rm_worker(base_url: &str, client: &Client, worker_id: &str) -> Result<()> {
    let url = format!("{}/workers/{}", base_url, worker_id);
    let resp = client
        .delete(&url)
        .send()
        .await
        .with_context(|| format!("DELETE {}", url))?;
    if resp.status().is_success() {
        println!("Successfully removed the worker");
        Ok(())
    } else {
        let status = resp.status();
        let err_text = resp.text().await.unwrap_or_default();
        bail!("Failed with status: {} {}", status, err_text)
    }
}

async fn set_size(
    base_url: &str,
    client: &Client,
    worker: &str,
    mirror: &str,
    size: &str,
) -> Result<()> {
    let url = format!("{}/workers/{}/jobs/{}/size", base_url, worker, mirror);
    let msg = serde_json::json!({
        "name": mirror,
        "size": size
    });

    let resp = client
        .post(&url)
        .json(&msg)
        .send()
        .await
        .with_context(|| format!("POST {}", url))?;
    if resp.status().is_success() {
        let status: MirrorStatus = resp
            .json()
            .await
            .context("parsing set-size response body")?;
        if status.size == size {
            println!("Successfully updated mirror size to {}", size);
            Ok(())
        } else {
            bail!(
                "Mirror size error, expecting {}, manager returned {}",
                size,
                status.size
            )
        }
    } else {
        let err_text = resp.text().await.unwrap_or_default();
        bail!("Manager failed to update mirror size: {}", err_text)
    }
}

async fn send_cmd(
    base_url: &str,
    client: &Client,
    cmd: CmdVerb,
    worker_id: Option<String>,
    mirror_id: Option<String>,
    args_str: Option<String>,
    options: HashMap<String, bool>,
) -> Result<()> {
    let args_list = if let Some(a) = args_str {
        a.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        Vec::new()
    };

    let req_cmd = ClientCmd {
        cmd,
        mirror_id: mirror_id.unwrap_or_default(),
        worker_id: worker_id.unwrap_or_default(),
        args: args_list,
        options,
    };

    let url = format!("{}/cmd", base_url);
    let resp = client
        .post(&url)
        .json(&req_cmd)
        .send()
        .await
        .with_context(|| format!("POST {}", url))?;

    if resp.status().is_success() {
        println!("Successfully send the command");
        Ok(())
    } else {
        let err_text = resp.text().await.unwrap_or_default();
        bail!(
            "Failed to correctly send command: HTTP status code is not 200: {}",
            err_text
        )
    }
}
