//! `hustsync::alert` stays on at whatever level the flags or `[log]` pick.

use hustsync_internal::logger::{extend, init_logger};

fn alert(stage: &str) {
    tracing::info!(target: "hustsync::alert", "alert under {stage}");
}

fn routine(stage: &str) {
    tracing::info!(target: "hustsync::worker", "routine under {stage}");
}

#[test]
fn alerts_pass_the_flags_and_the_log_table() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("hustsync.log");
    let guard = init_logger(false, false, false, Some(&path))?;
    alert("flags");
    routine("flags");
    let log: toml::Table = toml::from_str(r#"filter = "error""#)?;
    extend(Some(&log))?;
    alert("table");
    routine("table");
    drop(guard);

    let text = std::fs::read_to_string(&path)?;
    assert!(text.contains("alert under flags"), "{text}");
    assert!(text.contains("alert under table"), "{text}");
    assert!(!text.contains("routine"), "{text}");
    Ok(())
}
