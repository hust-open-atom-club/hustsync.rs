//! The optional `[log]` table reaches both configs as it is written, for serlog to read.

use std::io::Write;

use hustsync_config_parser::{ManagerConfig, WorkerConfig, parse_config};
use tempfile::NamedTempFile;

type Res<T = ()> = Result<T, Box<dyn std::error::Error>>;

const LOG: &str = r#"
[log]
filter = "info"
sinks = [{ kind = "loki", url = "http://alloy:3100/loki/api/v1/push" }]
"#;

/// The shipped example `name` with the `[log]` table `before` or after its own tables.
fn with_log(name: &str, before: bool) -> Res<NamedTempFile> {
    let base = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../docs/example")
            .join(name),
    )?;
    let mut f = NamedTempFile::new()?;
    let text = if before {
        format!("{LOG}{base}")
    } else {
        format!("{base}{LOG}")
    };
    f.write_all(text.as_bytes())?;
    Ok(f)
}

#[test]
fn both_configs_keep_their_log_table() -> Res {
    let manager: ManagerConfig = parse_config(with_log("manager.conf", false)?.path())?;
    let worker: WorkerConfig = parse_config(with_log("worker.conf", true)?.path())?;
    for log in [manager.log, worker.log] {
        let log = log.ok_or("no [log] table")?;
        assert_eq!(log["filter"].as_str(), Some("info"));
        assert_eq!(log["sinks"][0]["kind"].as_str(), Some("loki"));
    }
    Ok(())
}
