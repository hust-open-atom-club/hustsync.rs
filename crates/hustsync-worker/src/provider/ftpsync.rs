//! Debian's ftpsync, run with a config file generated from the mirror settings.

use std::{fs, io, path::Path};

/// Host and path of an `rsync://host/path/` or `host::path` upstream.
pub fn source(upstream: &str) -> Option<(&str, &str)> {
    let (host, path) = match upstream.strip_prefix("rsync://") {
        Some(rest) => rest.split_once('/')?,
        None => upstream.split_once("::")?,
    };
    let path = path.trim_matches('/');
    (!host.is_empty() && !path.is_empty()).then_some((host, path))
}

/// One ftpsync archive, as configured on a mirror.
pub struct Archive<'a> {
    pub name: &'a str,
    pub upstream: &'a str,
    pub to: &'a str,
    pub log_dir: &'a str,
    pub user: Option<&'a str>,
    pub extra: &'a [String],
}

impl Archive<'_> {
    /// Writes `ftpsync-<name>.conf` under `home`, the `HOME` ftpsync runs with; `extra` lines come last and win.
    pub fn write_config(&self, home: &Path) -> io::Result<()> {
        let (host, path) = source(self.upstream).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("bad ftpsync upstream `{}`", self.upstream),
            )
        })?;
        let mut conf = vec![
            format!("TO={}", quote(self.to)),
            format!("RSYNC_HOST={}", quote(host)),
            format!("RSYNC_PATH={}", quote(path)),
            format!("LOGDIR={}", quote(self.log_dir)),
            format!("BASEDIR={}", quote(&home.to_string_lossy())),
            format!("LOCKDIR={}", quote(&home.join("lock").to_string_lossy())),
            "MAILTO=root".to_owned(),
        ];
        conf.extend(self.user.map(|u| format!("RSYNC_USER={}", quote(u))));
        conf.extend(self.extra.iter().cloned());
        let dir = home.join(".config/ftpsync");
        fs::create_dir_all(&dir)?;
        fs::write(
            dir.join(format!("ftpsync-{}.conf", self.name)),
            conf.join("\n") + "\n",
        )
    }

    /// Command line syncing this archive with the ftpsync at `bin`.
    pub fn argv(&self, bin: &str) -> Vec<String> {
        vec![bin.to_owned(), format!("sync:archive:{}", self.name)]
    }
}

/// Single-quotes `s` for the shell that sources the config.
fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_both_upstream_forms() {
        assert_eq!(
            source("rsync://ftp.debian.org/debian/"),
            Some(("ftp.debian.org", "debian"))
        );
        assert_eq!(
            source("mirror.example::debian"),
            Some(("mirror.example", "debian"))
        );
        for bad in [
            "rsync://host/",
            "rsync://host",
            "::debian",
            "/srv/debian",
            "https://x/debian/",
        ] {
            assert_eq!(source(bad), None, "{bad}");
        }
    }

    #[test]
    fn writes_config_and_command() -> io::Result<()> {
        let home = tempfile::TempDir::new()?;
        let extra = ["ARCH_EXCLUDE='source'".to_owned()];
        let archive = Archive {
            name: "debian",
            upstream: "rsync://ftp.debian.org/debian/",
            to: "/srv/debian",
            log_dir: "/var/log/hustsync/debian",
            user: Some("mirror"),
            extra: &extra,
        };
        archive.write_config(home.path())?;
        assert_eq!(archive.argv("ftpsync"), ["ftpsync", "sync:archive:debian"]);
        let conf = fs::read_to_string(home.path().join(".config/ftpsync/ftpsync-debian.conf"))?;
        for line in [
            "TO='/srv/debian'",
            "RSYNC_HOST='ftp.debian.org'",
            "RSYNC_PATH='debian'",
            "LOGDIR='/var/log/hustsync/debian'",
            "RSYNC_USER='mirror'",
            "ARCH_EXCLUDE='source'",
        ] {
            assert!(conf.lines().any(|l| l == line), "{line}\n{conf}");
        }
        Ok(())
    }
}
