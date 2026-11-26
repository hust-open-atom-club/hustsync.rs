use std::{fs::OpenOptions, io, path::Path, sync::Mutex};

use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt, fmt::writer::BoxMakeWriter};

/// Logs to stdout, or appends to `file` when given, e.g. for a service with no console.
pub fn init_logger(
    verbose: bool,
    debug: bool,
    with_systemd: bool,
    file: Option<&Path>,
) -> io::Result<()> {
    let level = match (debug, verbose) {
        (true, _) => "debug",
        (_, true) => "info",
        _ => "warn",
    };

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(format!(
            "hustsync={lvl},hustsynctl={lvl},tracing_subscriber={lvl},rustls={lvl},{lvl}",
            lvl = level
        ))
    });

    let writer = match file {
        Some(p) => BoxMakeWriter::new(Mutex::new(
            OpenOptions::new().create(true).append(true).open(p)?,
        )),
        None => BoxMakeWriter::new(io::stdout),
    };

    let fmt_layer = fmt::layer()
        .with_writer(writer)
        .with_target(false)
        .with_ansi(!with_systemd && file.is_none())
        .with_file(debug)
        .with_line_number(debug)
        .with_timer(fmt::time::LocalTime::rfc_3339());

    let subscriber = tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer);

    if let Err(err) = tracing::subscriber::set_global_default(subscriber) {
        eprintln!("Failed to set global tracing subscriber: {err}");
    }
    Ok(())
}
