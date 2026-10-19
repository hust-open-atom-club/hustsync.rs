use std::path::Path;

use serlog::{
    Guard, Layout, SinkExt,
    layout::When,
    sinks::{Console, File},
};
use tracing_subscriber::prelude::*;

/// Alert events stay on whatever level the flags or `[log]` pick.
const ALERT: &str = "hustsync::alert=info";

/// Logs to stdout, or appends to `file` when given, e.g. for a service with no console.
/// `RUST_LOG` overrides the level the flags choose.
pub fn init_logger(
    verbose: bool,
    debug: bool,
    with_systemd: bool,
    file: Option<&Path>,
) -> serlog::Result<Guard> {
    let level = match (debug, verbose) {
        (true, _) => "debug",
        (_, true) => "info",
        _ => "warn",
    };
    let filter = format!(
        "hustsync={level},hustsynctl={level},tracing_subscriber={level},rustls={level},{level},{ALERT}"
    );
    let layout = Layout::pattern(if debug {
        "{time:rfc3339} {level:>5} {spans}{file}:{line}: {msg}{fields}"
    } else {
        "{time:rfc3339} {level:>5} {spans}{msg}{fields}"
    })?;
    let colour = if with_systemd || file.is_some() {
        When::Never
    } else {
        When::Always
    };
    let builder = serlog::builder().filter(filter);
    let builder = match file {
        Some(p) => builder.sink(File::new(p).layout(layout).colour(colour)),
        None => builder.sink(Console::new().layout(layout).colour(colour)),
    };
    let guard = builder.init()?;
    if let Err(err) = tracing_subscriber::registry()
        .with(serlog::Layer)
        .try_init()
    {
        eprintln!("Failed to set global tracing subscriber: {err}");
    }
    Ok(guard)
}

/// Adds the sinks of a config's `[log]` table, such as Loki or mail, and applies its `filter` unless `RUST_LOG` is set;
/// `hustsync::alert` stays at `info` unless the filter names it.
pub fn extend(log: Option<&toml::Table>) -> serlog::Result<()> {
    let Some(table) = log else {
        return Ok(());
    };
    let bad = |msg: String| serlog::Error::Config(msg);
    let mut table = table.clone();
    let filter = table.remove("filter");
    let sinks = table.remove("sinks");
    if let Some(key) = table.keys().next() {
        return Err(bad(format!("[log]: unknown key `{key}`")));
    }
    if let Some(spec) = filter
        && std::env::var_os("RUST_LOG").is_none()
    {
        let spec = spec
            .as_str()
            .ok_or_else(|| bad("[log]: `filter` takes a string".into()))?;
        let full = if spec.contains("hustsync::alert") {
            spec.to_owned()
        } else {
            format!("{spec},{ALERT}")
        };
        serlog::set_filter(
            full.parse()
                .map_err(|e| bad(format!("[log]: filter `{spec}`: {e}")))?,
        );
    }
    let entries = match sinks {
        None => Vec::new(),
        Some(toml::Value::Array(a)) => a,
        Some(_) => return Err(bad("[log]: `sinks` takes a list of tables".into())),
    };
    for entry in entries {
        let tree = serde_json::to_value(&entry).map_err(serlog::Error::other)?;
        serlog::add(serlog::config::slot(tree)?)?;
    }
    Ok(())
}
