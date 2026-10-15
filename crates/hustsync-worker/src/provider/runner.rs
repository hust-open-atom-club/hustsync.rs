//! Starts and stops provider commands: on the host, inside a container or in a cgroup.

use std::{
    collections::BTreeMap,
    ffi::OsStr,
    io,
    sync::{Arc, atomic::AtomicU32},
    time::Duration,
};

use hustsync_config_parser::{MemBytes, MirrorConfig, WorkerDockerConfig};
use hustsync_internal::util::expand_tilde;
use tokio::process::Command;

use crate::cgroup::{self, Cgroups};

/// How a provider's command is launched.
#[derive(Debug, Default)]
pub enum Runner {
    #[default]
    Direct,
    Docker(Docker),
    Cgroup(Arc<Cgroups>, Option<MemBytes>),
}

/// Container settings merged from `[docker]` and the mirror's `docker_*` fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Docker {
    pub image: String,
    pub volumes: Vec<String>,
    pub options: Vec<String>,
    pub memory: Option<MemBytes>,
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
    /// Docker when enabled and the mirror sets `docker_image`, else a cgroup when enabled, else direct.
    pub fn new(
        docker: Option<&WorkerDockerConfig>,
        cg: Option<&Arc<Cgroups>>,
        m: &MirrorConfig,
    ) -> Self {
        let image = m.docker_image.as_deref().filter(|i| !i.is_empty());
        let (Some(d), Some(image)) = (docker.filter(|d| d.enable == Some(true)), image) else {
            return cg.map_or(Self::Direct, |cg| {
                Self::Cgroup(Arc::clone(cg), m.memory_limit)
            });
        };
        let ro = m
            .exclude_file
            .as_deref()
            .map(expand_tilde)
            .map(|f| format!("{f}:{f}:ro"));
        Self::Docker(Docker {
            image: image.to_owned(),
            volumes: d
                .volumes
                .iter()
                .chain(&m.docker_volumes)
                .flatten()
                .cloned()
                .chain(ro)
                .collect(),
            options: d
                .options
                .iter()
                .chain(&m.docker_options)
                .flatten()
                .cloned()
                .collect(),
            memory: m.memory_limit,
        })
    }

    /// Builds the command running `prog args` for `job`, creating its cgroup if any.
    pub async fn command(&self, prog: &str, args: &[String], job: Job<'_>) -> io::Result<Command> {
        let mut cmd = match self {
            Self::Docker(d) => {
                let mut cmd = Command::new("docker");
                cmd.args(d.args(job)).arg(prog);
                cmd
            }
            _ => Command::new(prog),
        };
        cmd.args(args)
            .current_dir(job.dir)
            .envs(job.env)
            .stdin(std::process::Stdio::null());
        // A session of its own keeps the job off the worker's terminal, so shells like fpsync never stop on SIGTTOU.
        #[cfg(unix)]
        // SAFETY: setsid is async-signal-safe.
        unsafe {
            cmd.pre_exec(|| nix::unistd::setsid().map(drop).map_err(Into::into));
        }
        if let Self::Cgroup(cg, mem) = self {
            cgroup::attach(&mut cmd, &cg.create(job.name, *mem).await?)?;
        }
        Ok(cmd)
    }

    /// Stops the running job: `docker stop` for containers, then the process group.
    #[cfg_attr(not(unix), allow(unused_variables))]
    pub async fn stop(&self, name: &str, pgid: &AtomicU32) {
        if let Self::Docker(_) = self {
            let _ = docker(["stop", "-t", "2", &container(name)]).output().await;
        }
        #[cfg(unix)]
        super::terminate_pgid(pgid, name).await;
    }

    /// Cleans up after a run; returns whether the OOM killer fired.
    pub async fn finish(&self, name: &str) -> bool {
        match self {
            Self::Direct => false,
            Self::Docker(_) => {
                wait_removed(&container(name)).await;
                false
            }
            Self::Cgroup(cg, _) => cg.finish(name).await,
        }
    }
}

/// Waits for `--rm` to remove the container so the next run can reuse its name.
async fn wait_removed(name: &str) {
    let filter = format!("name=^{name}$");
    for _ in 0..10 {
        match docker(["ps", "-a", "--filter", &filter, "--format", "{{.Status}}"])
            .output()
            .await
        {
            Ok(out) if out.stdout.is_empty() => return,
            Ok(_) => tokio::time::sleep(Duration::from_secs(1)).await,
            Err(e) => {
                tracing::error!("docker ps failed: {e}");
                return;
            }
        }
    }
    tracing::warn!("container {name} not removed automatically, next sync may fail");
}

impl Docker {
    /// `docker run` arguments up to the image, in tunasync's order.
    pub fn args(&self, job: Job<'_>) -> Vec<String> {
        let own = [job.log_dir, job.log_file, job.dir].map(|p| format!("{p}:{p}"));
        let mut args: Vec<String> = ["run", "--rm", "-a", "STDOUT", "-a", "STDERR", "--name"]
            .map(String::from)
            .into();
        args.extend([container(job.name), "-w".into(), job.dir.into()]);
        #[cfg(unix)]
        {
            use nix::unistd::{getgid, getuid};
            args.extend(["-u".into(), format!("{}:{}", getuid(), getgid())]);
        }
        args.extend(
            self.volumes
                .iter()
                .cloned()
                .chain(own)
                .flat_map(|v| ["-v".into(), v]),
        );
        args.extend(job.env.keys().flat_map(|k| ["-e".into(), k.clone()]));
        if let Some(m) = self.memory {
            args.extend(["-m".into(), m.0.to_string()]);
        }
        args.extend(self.options.iter().cloned());
        args.push(self.image.clone());
        args
    }
}

/// Container name for mirror `name`.
pub fn container(name: &str) -> String {
    format!("hustsync-job-{name}")
}

fn docker<I: IntoIterator<Item = S>, S: AsRef<OsStr>>(args: I) -> Command {
    let mut cmd = Command::new("docker");
    cmd.args(args);
    cmd
}
