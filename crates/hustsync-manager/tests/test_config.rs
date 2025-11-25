#![cfg(test)]

use hustsync_config_parser::ManagerConfig;

#[test]
fn toml_decoding_should_work() {
    let cfg_blob = r#"
debug = true
[server]
addr = "0.0.0.0"
port = 5000

[files]
status_file = "/tmp/tunasync.json"
db_file = "/var/lib/tunasync/tunasync.db"
"#;

    let cfg: ManagerConfig = toml::from_str(cfg_blob).expect("decode toml");
    assert_eq!(
        cfg.server.as_ref().unwrap().addr.as_ref().unwrap(),
        "0.0.0.0"
    );
    assert_eq!(cfg.server.as_ref().unwrap().port.unwrap(), 5000);
    assert_eq!(
        cfg.files.as_ref().unwrap().db_file.as_ref().unwrap(),
        "/var/lib/tunasync/tunasync.db"
    );
}
