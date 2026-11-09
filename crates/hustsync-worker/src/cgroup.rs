//! One cgroup per job (v1 or v2): memory limit, leftover cleanup and OOM detection.

use std::{
    io,
    path::{Path, PathBuf},
    time::Duration,
};

use hustsync_config_parser::{MemBytes, WorkerCgroupConfig};
use tokio::fs;

/// Where the worker moves itself in implicit mode, so its parent group can hold children.
const OWN: &str = "__worker";

/// `ESRCH` on Linux: the process left before it could be moved.
const ESRCH: i32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    V1,
    V2,
}

/// The parent group every job group is created under.
#[derive(Debug)]
pub struct Cgroups {
    ver: Version,
    parent: PathBuf,
}

impl Cgroups {
    /// Finds the parent group; an empty `group` means the worker's own group, as in tunasync.
    pub fn init(cfg: &WorkerCgroupConfig) -> io::Result<Self> {
        if !cfg!(target_os = "linux") {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "cgroup needs Linux",
            ));
        }
        let base = Path::new(cfg.base_path.as_deref().unwrap_or("/sys/fs/cgroup"));
        let ver = if base.join("cgroup.controllers").exists() {
            Version::V2
        } else {
            Version::V1
        };
        let root = match ver {
            Version::V2 => base.to_path_buf(),
            Version::V1 => base.join("memory"),
        };
        let group = cfg.group.as_deref().unwrap_or_default().trim_matches('/');
        let implicit = group.is_empty();
        let rel = if implicit {
            own(ver)?
        } else {
            group.to_owned()
        };
        let cg = Self {
            ver,
            parent: root.join(rel.trim_start_matches('/')),
        };
        if implicit && ver == Version::V2 && cg.parent != root {
            cg.adopt()?;
        } else if !implicit && !pids(&cg.parent)?.is_empty() {
            return Err(io::Error::other(format!(
                "cgroup {} still has processes",
                cg.parent.display()
            )));
        }
        if ver == Version::V2 {
            std::fs::write(cg.parent.join("cgroup.subtree_control"), "+memory")?;
        }
        Ok(cg)
    }

    pub const fn version(&self) -> Version {
        self.ver
    }

    /// Directory of `name`'s group.
    pub fn dir(&self, name: &str) -> PathBuf {
        self.parent.join(name)
    }

    /// Creates `name`'s group with an optional hard memory limit (no swap), clearing a stale one first.
    pub async fn create(&self, name: &str, mem: Option<MemBytes>) -> io::Result<PathBuf> {
        let dir = self.dir(name);
        if fs::try_exists(&dir).await? {
            clear(&dir).await?;
        }
        fs::create_dir(&dir).await?;
        if let Some(m) = mem {
            let (limit, swap, cap) = match self.ver {
                Version::V2 => ("memory.max", "memory.swap.max", "0".to_owned()),
                Version::V1 => (
                    "memory.limit_in_bytes",
                    "memory.memsw.limit_in_bytes",
                    m.0.to_string(),
                ),
            };
            fs::write(dir.join(limit), m.0.to_string()).await?;
            if fs::try_exists(dir.join(swap)).await? {
                fs::write(dir.join(swap), cap).await?;
            }
        }
        Ok(dir)
    }

    /// Kills what is left in `name`'s group and removes it; returns whether the OOM killer fired.
    pub async fn finish(&self, name: &str) -> bool {
        let dir = self.dir(name);
        let file = match self.ver {
            Version::V2 => "memory.events",
            Version::V1 => "memory.oom_control",
        };
        let oom = fs::read_to_string(dir.join(file))
            .await
            .ok()
            .and_then(|s| {
                s.lines()
                    .find_map(|l| l.strip_prefix("oom_kill ")?.trim().parse::<u64>().ok())
            })
            .is_some_and(|n| n > 0);
        if let Err(e) = clear(&dir).await {
            tracing::warn!("cgroup {}: {e}", dir.display());
        }
        oom
    }

    /// Moves every process of the parent into `__worker`, since a v2 group with children must be empty.
    fn adopt(&self) -> io::Result<()> {
        let own = self.parent.join(OWN);
        if !own.exists() {
            std::fs::create_dir(&own)?;
        }
        loop {
            let left = pids(&self.parent)?;
            if left.is_empty() {
                return Ok(());
            }
            for pid in left {
                match std::fs::write(own.join("cgroup.procs"), pid.to_string()) {
                    Err(e) if e.raw_os_error() != Some(ESRCH) => return Err(e),
                    _ => {}
                }
            }
        }
    }
}

/// Moves the child into `dir` between fork and exec, so nothing it spawns escapes.
#[cfg(target_os = "linux")]
pub fn attach(cmd: &mut tokio::process::Command, dir: &Path) -> io::Result<()> {
    use nix::{
        fcntl::{OFlag, open},
        sys::stat::Mode,
        unistd::write,
    };
    use std::{ffi::CString, os::unix::ffi::OsStrExt};

    let procs = CString::new(dir.join("cgroup.procs").as_os_str().as_bytes())?;
    // SAFETY: the closure only calls open, write and close, all async-signal-safe.
    unsafe {
        cmd.pre_exec(move || {
            let fd = open(
                procs.as_c_str(),
                OFlag::O_WRONLY | OFlag::O_CLOEXEC,
                Mode::empty(),
            )?;
            write(&fd, b"0")?;
            Ok(())
        });
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn attach(_cmd: &mut tokio::process::Command, _dir: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "cgroup needs Linux",
    ))
}

/// The worker's own group, from `/proc/self/cgroup`.
fn own(ver: Version) -> io::Result<String> {
    let text = std::fs::read_to_string("/proc/self/cgroup")?;
    text.lines()
        .find_map(|l| {
            let mut f = l.splitn(3, ':');
            let (id, ctrls, path) = (f.next()?, f.next()?, f.next()?);
            match ver {
                Version::V2 => (id == "0" && ctrls.is_empty()).then_some(path),
                Version::V1 => ctrls.split(',').any(|c| c == "memory").then_some(path),
            }
        })
        .map(str::to_owned)
        .ok_or_else(|| io::Error::other("worker cgroup not found in /proc/self/cgroup"))
}

fn pids(dir: &Path) -> io::Result<Vec<i32>> {
    Ok(std::fs::read_to_string(dir.join("cgroup.procs"))?
        .lines()
        .filter_map(|l| l.trim().parse().ok())
        .collect())
}

/// SIGKILLs the group's processes for up to three rounds, as tunasync does, then removes it.
async fn clear(dir: &Path) -> io::Result<()> {
    for pause in [10, 1010, 2010] {
        let left = pids(dir)?;
        if left.is_empty() {
            break;
        }
        #[cfg(unix)]
        for pid in left {
            use nix::{
                sys::signal::{Signal, kill},
                unistd::Pid,
            };
            let _ = kill(Pid::from_raw(pid), Signal::SIGKILL);
        }
        tokio::time::sleep(Duration::from_millis(pause)).await;
    }
    fs::remove_dir(dir).await
}
