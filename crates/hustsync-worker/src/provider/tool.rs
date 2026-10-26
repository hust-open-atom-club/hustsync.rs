//! fpsync and lsyncd: sync tools whose source has to be a local directory.

use super::ProviderType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tool {
    /// Splits the tree with fpart and runs rsync jobs in parallel.
    Fpsync,
    /// Watches the source with inotify and keeps running.
    Lsyncd,
}

impl Tool {
    /// Arguments for one run: pull when `upstream` is a local path, push `dir` to it otherwise.
    pub fn argv(self, bin: &str, opts: &[String], upstream: &str, dir: &str) -> Vec<String> {
        let (src, dst) = if upstream.starts_with('/') {
            (upstream, dir)
        } else {
            (dir, upstream)
        };
        let ends = [src, dst].map(|p| format!("{}/", p.trim_end_matches('/')));
        let mut argv = vec![bin.to_owned()];
        if self == Self::Lsyncd {
            argv.push("-nodaemon".into());
        }
        argv.extend(opts.iter().cloned());
        if self == Self::Lsyncd {
            argv.push("-rsync".into());
        }
        argv.extend(ends);
        argv
    }

    /// Whether the tool keeps running instead of exiting after one pass.
    pub const fn daemon(self) -> bool {
        matches!(self, Self::Lsyncd)
    }
}

impl From<Tool> for ProviderType {
    fn from(t: Tool) -> Self {
        match t {
            Tool::Fpsync => Self::Fpsync,
            Tool::Lsyncd => Self::Lsyncd,
        }
    }
}
