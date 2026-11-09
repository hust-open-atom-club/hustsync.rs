//! No per-job isolation on this platform; `[cgroup] enable` is refused at start.

use std::io;

use hustsync_config_parser::{MemBytes, WorkerCgroupConfig};
use tokio::process::{Child, Command};

#[derive(Debug)]
pub struct Cgroups;

impl Cgroups {
    pub fn init(_cfg: &WorkerCgroupConfig) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "cgroup needs Linux or Windows",
        ))
    }

    #[expect(clippy::unused_async, reason = "same signature as the Linux version")]
    pub async fn enter(
        &self,
        _name: &str,
        _mem: Option<MemBytes>,
        _cmd: &mut Command,
    ) -> io::Result<()> {
        Ok(())
    }

    pub const fn adopt(&self, _name: &str, _child: &Child) -> io::Result<()> {
        Ok(())
    }

    pub const fn kill(&self, _name: &str) {}

    #[expect(clippy::unused_async, reason = "same signature as the Linux version")]
    pub async fn finish(&self, _name: &str) -> bool {
        false
    }
}
