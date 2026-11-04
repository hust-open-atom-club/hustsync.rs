//! Starts and stops provider commands.

use std::{collections::BTreeMap, sync::atomic::AtomicU32};

use tokio::process::Command;

/// How a provider's command is launched.
#[derive(Debug, Default)]
pub enum Runner {
    #[default]
    Direct,
}

/// Paths and environment of one run.
#[derive(Debug, Clone, Copy)]
pub struct Job<'a> {
    pub name: &'a str,
    pub dir: &'a str,
    pub log_dir: &'a str,
    pub log_file: &'a str,
    pub env: &'a BTreeMap<String, String>,
}

impl Runner {
    /// Builds the command running `prog args` for `job`.
    pub fn command(&self, prog: &str, args: &[String], job: Job<'_>) -> Command {
        let mut cmd = Command::new(prog);
        cmd.args(args).current_dir(job.dir).envs(job.env);
        #[cfg(unix)]
        cmd.process_group(0);
        cmd
    }

    /// Stops the running job's process group.
    #[cfg_attr(not(unix), allow(unused_variables, clippy::unused_async))]
    pub async fn stop(&self, name: &str, pgid: &AtomicU32) {
        #[cfg(unix)]
        super::terminate_pgid(pgid, name).await;
    }
}
