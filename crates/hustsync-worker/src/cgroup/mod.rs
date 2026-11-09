//! Per-job isolation: cgroups on Linux, Job Objects on Windows, nothing elsewhere.

#[cfg(windows)]
mod job;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(not(any(target_os = "linux", windows)))]
mod none;

#[cfg(windows)]
pub use job::Cgroups;
#[cfg(target_os = "linux")]
pub use linux::*;
#[cfg(not(any(target_os = "linux", windows)))]
pub use none::Cgroups;
