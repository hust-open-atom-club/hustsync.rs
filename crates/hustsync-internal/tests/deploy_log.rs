//! The `[log]` sinks shipped in `deploy/` build as serlog slots, so a mistyped key fails here, not in production.

use std::path::Path;

#[test]
fn shipped_log_tables_build() -> Result<(), Box<dyn std::error::Error>> {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for file in ["deploy/compose/manager.toml", "deploy/compose/worker.toml"] {
        let table: toml::Table = std::fs::read_to_string(repo.join(file))?.parse()?;
        let sinks = table
            .get("log")
            .and_then(|l| l.get("sinks"))
            .and_then(|s| s.as_array())
            .ok_or_else(|| format!("{file}: no [log] sinks"))?;
        assert!(!sinks.is_empty(), "{file}");
        for entry in sinks {
            let _slot = serlog::config::slot(serde_json::to_value(entry)?)
                .map_err(|e| format!("{file}: {e}"))?;
        }
    }
    Ok(())
}
