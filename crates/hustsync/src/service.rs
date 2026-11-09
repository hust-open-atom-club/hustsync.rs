//! Runs the manager or a worker under the Windows service control manager.

use std::{process, sync::Arc, thread};

use anyhow::Result;
use tokio::sync::Notify;
use windows_services::{Command, Service};

use crate::{Cli, block_on};

/// Serves `cli` until the service is stopped; a role failing on its own exits the process so recovery actions apply.
pub fn serve(cli: Cli) -> Result<()> {
    let stop = Arc::new(Notify::new());
    let (mut cli, mut task) = (Some(cli), None);
    Service::new()
        .can_stop()
        .run(|_, cmd| match cmd {
            Command::Start => {
                let stop = Arc::clone(&stop);
                task = cli.take().map(|cli| {
                    thread::spawn(move || {
                        if let Err(e) = block_on(cli, async move { stop.notified().await }) {
                            tracing::error!("{e:#}");
                            process::exit(1);
                        }
                    })
                });
            }
            Command::Stop => {
                stop.notify_one();
                if let Some(t) = task.take() {
                    let _ = t.join();
                }
            }
            _ => {}
        })
        .map_err(anyhow::Error::msg)
}
