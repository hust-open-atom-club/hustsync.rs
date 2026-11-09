//! fpsync: a sync tool whose source has to be a local directory.

use super::ProviderType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tool {
    /// Splits the tree with fpart and runs rsync jobs in parallel.
    Fpsync,
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
        argv.extend(opts.iter().cloned());
        argv.extend(ends);
        argv
    }
}

impl From<Tool> for ProviderType {
    fn from(t: Tool) -> Self {
        match t {
            Tool::Fpsync => Self::Fpsync,
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
    }
}
