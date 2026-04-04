#![cfg(test)]

use hustsync_config_parser::ManagerConfig;
use std::fs;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use tempfile::NamedTempFile;

fn make_cfg_blob() -> &'static str {
    r#"
debug = true
[server]
addr = "0.0.0.0"
port = 5000
ssl_cert = ""
ssl_key = ""

[files]
status_file = "/tmp/hustsync.json"
db_type = "redb"
db_file = "/var/lib/hustsync/hustsync.db"
ca_cert = ""
"#
}

#[test]
fn toml_decoding_should_work() {
    let cfg_blob = make_cfg_blob();
    let cfg: ManagerConfig = toml::from_str(cfg_blob).expect("decode toml");
    assert_eq!(
        cfg.server.addr,
        "0.0.0.0"
    );
    assert_eq!(cfg.server.port, 5000);
    assert_eq!(
        cfg.files.db_file,
        "/var/lib/hustsync/hustsync.db"
    );
}

// TODO 创建 clap 应用以测试更完整的加载流程
// 需要对应 hustsync config_test 部分
#[test]
fn toml_loading_should_work() {
    let cfg_blob = make_cfg_blob();
    let mut tmp = NamedTempFile::new().expect("create tempfile");
    tmp.write_all(cfg_blob.as_bytes()).expect("write tempfile");
    tmp.flush().expect("flush tempfile");

    #[cfg(unix)]
    fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o644)).expect("set perms");

    // let tempfile_path = tmp.path().to_path_buf();
}
