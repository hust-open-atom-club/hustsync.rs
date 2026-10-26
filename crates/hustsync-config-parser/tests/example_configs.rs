//! Every worker example under `docs/example/*/` and `deploy/` loads and validates; top-level files there are parser fixtures.

use std::path::{Path, PathBuf};

use hustsync_config_parser::{ConfigLoadOptions, load_worker_config, validate_worker_config};

fn collect(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let p = entry?.path();
        if p.is_dir() {
            collect(&p, out)?;
        } else if p
            .file_name()
            .is_some_and(|n| n.to_string_lossy().contains("worker"))
        {
            out.push(p);
        }
    }
    Ok(())
}

#[test]
fn worker_examples_load_and_validate() -> Result<(), Box<dyn std::error::Error>> {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut files = vec![repo.join("deploy/compose/worker.toml")];
    for entry in std::fs::read_dir(repo.join("docs/example"))? {
        let p = entry?.path();
        if p.is_dir() {
            collect(&p, &mut files)?;
        }
    }
    assert!(files.len() >= 5, "{files:?}");
    for f in &files {
        let at = |e: &dyn std::fmt::Display| format!("{}: {e}", f.display());
        let cfg = load_worker_config(f, &ConfigLoadOptions::default()).map_err(|e| at(&e))?;
        validate_worker_config(&cfg).map_err(|e| at(&e))?;
    }
    Ok(())
}
