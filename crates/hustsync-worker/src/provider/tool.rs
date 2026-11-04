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

#[cfg(test)]
mod tests {
    use super::Tool;

    #[test]
    fn local_upstream_pulls_and_remote_upstream_pushes() {
        let opts = ["-n".to_owned(), "4".to_owned()];
        assert_eq!(
            Tool::Fpsync.argv("fpsync", &opts, "/nfs/debian", "/srv/debian"),
            ["fpsync", "-n", "4", "/nfs/debian/", "/srv/debian/"]
        );
        assert_eq!(
            Tool::Fpsync.argv("fpsync", &[], "rsync://edge/debian", "/srv/debian/"),
            ["fpsync", "/srv/debian/", "rsync://edge/debian/"]
        );
        assert_eq!(
            Tool::Lsyncd.argv(
                "lsyncd",
                &["-delay".into(), "5".into()],
                "/nfs/debian/",
                "/srv/debian"
            ),
            [
                "lsyncd",
                "-nodaemon",
                "-delay",
                "5",
                "-rsync",
                "/nfs/debian/",
                "/srv/debian/"
            ]
        );
        assert_eq!(
            Tool::Lsyncd.argv("lsyncd", &[], "edge::debian", "/srv/debian"),
            [
                "lsyncd",
                "-nodaemon",
                "-rsync",
                "/srv/debian/",
                "edge::debian/"
            ]
        );
    }
}
