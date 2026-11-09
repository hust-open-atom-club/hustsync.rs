//! Starts and stops provider commands, on the host or inside a container.

use std::{collections::BTreeMap, ffi::OsStr, sync::atomic::AtomicU32, time::Duration};

use hustsync_config_parser::{MemBytes, MirrorConfig, WorkerDockerConfig};
use hustsync_internal::util::expand_tilde;
use tokio::process::Command;

/// How a provider's command is launched.
#[derive(Debug, Default)]
pub enum Runner {
    #[default]
    Direct,
    Docker(Docker),
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
    /// Docker when `[docker]` is enabled and the mirror sets `docker_image`, else direct.
    pub fn new(docker: Option<&WorkerDockerConfig>, m: &MirrorConfig) -> Self {
        let (Some(d), Some(image)) = (docker, m.docker_image.as_deref()) else {
            return Self::Direct;
        };
        if d.enable != Some(true) || image.is_empty() {
            return Self::Direct;
        }
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

    /// Builds the command running `prog args` for `job`.
    pub fn command(&self, prog: &str, args: &[String], job: Job<'_>) -> Command {
        let mut cmd = match self {
            Self::Direct => Command::new(prog),
            Self::Docker(d) => {
                let mut cmd = Command::new("docker");
                cmd.args(d.args(job)).arg(prog);
                cmd
            }
        };
        cmd.args(args).current_dir(job.dir).envs(job.env);
        #[cfg(unix)]
        cmd.process_group(0);
        cmd
    }

    /// Stops the running job: `docker stop` for containers, then the process group.
    pub async fn stop(&self, name: &str, pgid: &AtomicU32) {
        if let Self::Docker(_) = self {
            let _ = docker(["stop", "-t", "2", &container(name)]).output().await;
        }
        #[cfg(unix)]
        super::terminate_pgid(pgid, name).await;
        #[cfg(windows)]
        kill_tree(pgid).await;
    }

    /// Waits for `--rm` to remove the container so the next run can reuse its name.
    pub async fn finish(&self, name: &str) {
        let Self::Docker(_) = self else { return };
        let name = container(name);
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

/// Ends the process `pid` and everything it started; Windows has no process groups to signal.
#[cfg(windows)]
async fn kill_tree(pid: &AtomicU32) {
    let pid = pid.load(std::sync::atomic::Ordering::Acquire);
    if pid != 0 && pid != u32::MAX {
        let _ = Command::new("taskkill")
            .args(["/T", "/F", "/PID", &pid.to_string()])
            .output()
            .await;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn mirror(image: Option<&str>) -> MirrorConfig {
        MirrorConfig {
            docker_image: image.map(Into::into),
            docker_volumes: Some(vec!["/m:/m".into()]),
            docker_options: Some(vec!["--net=host".into()]),
            exclude_file: Some("/etc/ex".into()),
            memory_limit: Some(MemBytes(1 << 30)),
            ..MirrorConfig::default()
        }
    }

    #[test]
    fn docker_needs_enable_and_image() {
        let on = WorkerDockerConfig {
            enable: Some(true),
            volumes: Some(vec!["/g:/g".into()]),
            options: Some(vec!["--init".into()]),
        };
        let off = WorkerDockerConfig {
            enable: Some(false),
            ..WorkerDockerConfig::default()
        };
        for (docker, image) in [
            (None, Some("img")),
            (Some(&off), Some("img")),
            (Some(&on), None),
            (Some(&on), Some("")),
        ] {
            assert!(matches!(
                Runner::new(docker, &mirror(image)),
                Runner::Direct
            ));
        }
        let want = Docker {
            image: "img".into(),
            volumes: vec!["/g:/g".into(), "/m:/m".into(), "/etc/ex:/etc/ex:ro".into()],
            options: vec!["--init".into(), "--net=host".into()],
            memory: Some(MemBytes(1 << 30)),
        };
        assert!(matches!(
            Runner::new(Some(&on), &mirror(Some("img"))),
            Runner::Docker(d) if d == want
        ));
    }

    #[test]
    fn docker_args_follow_tunasync_order() {
        let env = BTreeMap::from([("B".into(), "2".into()), ("A".into(), "1".into())]);
        let job = Job {
            name: "m",
            dir: "/data/m",
            log_dir: "/log/m",
            log_file: "/log/m/m.log",
            env: &env,
        };
        let d = Docker {
            image: "alpine:3.23".into(),
            volumes: vec!["/etc/x:/etc/x:ro".into()],
            options: vec!["--entrypoint=".into()],
            memory: Some(MemBytes(256 << 20)),
        };
        let mut want: Vec<String> = [
            "run",
            "--rm",
            "-a",
            "STDOUT",
            "-a",
            "STDERR",
            "--name",
            "hustsync-job-m",
            "-w",
            "/data/m",
        ]
        .map(String::from)
        .into();
        #[cfg(unix)]
        want.extend([
            "-u".into(),
            format!("{}:{}", nix::unistd::getuid(), nix::unistd::getgid()),
        ]);
        want.extend(
            [
                "-v",
                "/etc/x:/etc/x:ro",
                "-v",
                "/log/m:/log/m",
                "-v",
                "/log/m/m.log:/log/m/m.log",
                "-v",
                "/data/m:/data/m",
                "-e",
                "A",
                "-e",
                "B",
                "-m",
                "268435456",
                "--entrypoint=",
                "alpine:3.23",
            ]
            .map(String::from),
        );
        assert_eq!(d.args(job), want);
    }
}
