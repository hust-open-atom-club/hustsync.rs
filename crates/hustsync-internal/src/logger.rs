use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

pub fn init_logger(verbose: bool, debug: bool, with_systemd: bool) {
    let level = match (debug, verbose) {
        (true, _) => "debug",
        (_, true) => "info",
        _ => "warn",
    };

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(format!(
            "tunasync={lvl},tunasynctl={lvl},tracing_subscriber={lvl},rustls={lvl},{lvl}",
            lvl = level
        ))
    });

    let base_layer = fmt::layer()
        .with_target(false)
        .with_ansi(!with_systemd)
        .with_file(debug)
        .with_line_number(debug);

    // Use the same timer type in all cases so both branches have the same Layer type.
    let fmt_layer = base_layer.with_timer(fmt::time::LocalTime::rfc_3339());

    let subscriber = tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer);

    if let Err(err) = tracing::subscriber::set_global_default(subscriber) {
        eprintln!("Failed to set global tracing subscriber: {err}");
    }
}
