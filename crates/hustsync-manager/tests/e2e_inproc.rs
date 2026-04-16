//! In-process manager ↔ worker ↔ hustsynctl end-to-end test.
//!
//! Brings up a real manager HTTP server on an ephemeral port, starts a
//! worker pointing at it, issues a start command through the manager,
//! and waits for the sync to complete end-to-end. Uses the `cmd`
//! provider with `/bin/echo` so the sync finishes in milliseconds
//! without any network dependency.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod contract;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use hustsync_config_parser::{
    MirrorConfig, WorkerConfig, WorkerGlobalConfig, WorkerManagerConfig, WorkerServerConfig,
};
use hustsync_internal::msg::{ClientCmd, CmdVerb};
use hustsync_internal::status::SyncStatus;
use hustsync_manager::Manager;
use hustsync_worker::Worker;
use tempfile::TempDir;

/// Spawn a manager on `127.0.0.1:0` and return the bound address plus
/// a TempDir guard holding the redb file. The manager task runs until
/// the returned JoinHandle is aborted.
async fn spawn_manager_listening() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>, TempDir) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("manager.db");
    let cfg = Arc::new(hustsync_config_parser::ManagerConfig {
        debug: false,
        server: hustsync_config_parser::ManagerServerConfig {
            addr: "127.0.0.1".into(),
            port: 0,
            ssl_cert: "".into(),
            ssl_key: "".into(),
        },
        files: hustsync_config_parser::ManagerFileConfig {
            status_file: "".into(),
            db_type: "redb".into(),
            db_file: db_path.to_string_lossy().into_owned(),
            ca_cert: "".into(),
        },
    });

    let manager = Arc::new(Manager::new(cfg).expect("manager init"));
    let router = Arc::clone(&manager).make_router();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });
    (addr, handle, dir)
}

fn worker_config(
    worker_name: &str,
    mirror_name: &str,
    manager_addr: std::net::SocketAddr,
    worker_dir: &TempDir,
) -> WorkerConfig {
    let log_dir = worker_dir.path().join("log").to_string_lossy().into_owned();
    let mirror_dir = worker_dir
        .path()
        .join("mirrors")
        .to_string_lossy()
        .into_owned();

    WorkerConfig {
        global: Some(WorkerGlobalConfig {
            name: Some(worker_name.into()),
            log_dir: Some(log_dir),
            mirror_dir: Some(mirror_dir),
            concurrent: Some(2),
            rsync_options: None,
            dangerous_global_success_exit_codes: None,
            dangerous_global_rsync_success_exit_codes: None,
            retry: None,
            exec_on_status: None,
        }),
        manager: Some(WorkerManagerConfig {
            api_base: Some(format!("http://{}", manager_addr)),
            token: None,
            ca_cert: None,
        }),
        cgroup: None,
        server: Some(WorkerServerConfig {
            hostname: Some("127.0.0.1".into()),
            // Bind worker on a fixed port — no ephemeral here because
            // the register_worker self-URL needs to be predictable.
            // A port close to the default avoids clashes with parallel
            // test runs because each test owns its own TempDir and
            // process boundary. `0` would also work if we could read
            // it back, but the worker uses the config value in its URL.
            listen_addr: Some("127.0.0.1".into()),
            listen_port: Some(0),
            ssl_cert: None,
            ssl_key: None,
        }),
        mirrors: Some(vec![MirrorConfig {
            name: Some(mirror_name.into()),
            provider: Some("command".into()),
            command: Some("/bin/echo".into()),
            upstream: Some("unused://for-cmd-provider/".into()),
            retry: None,
            exec_on_status: None,
            exec_on_status_extra: None,
            use_ipv6: None,
            use_ipv4: None,
            mirror_dir: None,
            mirror_subdir: None,
            mirror_type: None,
            log_dir: None,
            env: None,
            role: None,
            fail_on_match: None,
            size_pattern: None,
            exclude_file: None,
            username: None,
            password: None,
            rsync_no_timeout: None,
            rsync_timeout: None,
            rsync_options: None,
            rsync_override: None,
            rsync_override_only: None,
            stage1_profile: None,
            memory_limit: None,
            success_exit_codes: None,
            rsync_success_exit_codes: None,
        }]),
        include: None,
    }
}

async fn get_json(url: &str) -> serde_json::Value {
    let client = reqwest::Client::new();
    let resp = client.get(url).send().await.unwrap();
    resp.json().await.unwrap()
}

async fn post_cmd(manager_url: &str, worker_id: &str, mirror_id: &str, cmd: CmdVerb) {
    let client = reqwest::Client::new();
    let body = ClientCmd {
        cmd,
        mirror_id: mirror_id.into(),
        worker_id: worker_id.into(),
        args: Vec::new(),
        options: HashMap::new(),
    };
    let _ = client
        .post(format!("{}/cmd", manager_url))
        .json(&body)
        .send()
        .await;
}

/// Poll `pred(json)` on `GET <url>` until it returns true or the
/// deadline passes. Returns the last-seen JSON on success; panics on
/// timeout.
async fn poll_until<F>(url: &str, within: Duration, mut pred: F) -> serde_json::Value
where
    F: FnMut(&serde_json::Value) -> bool,
{
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let j = get_json(url).await;
        if pred(&j) {
            return j;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting on {url}; last: {j}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn full_cmd_sync_round_trip() {
    let (manager_addr, manager_task, _manager_dir) = spawn_manager_listening().await;
    let manager_url = format!("http://{}", manager_addr);

    let worker_dir = TempDir::new().unwrap();
    let cfg = worker_config("e2e-worker", "e2e-mirror", manager_addr, &worker_dir);

    // Spawn worker in its own task — it registers, opens its own HTTP
    // server, then falls into the dispatch loop.
    let worker = Worker::new(cfg);
    let worker_task = tokio::spawn(async move {
        worker.run().await;
    });

    // Wait for the worker registration to hit the manager.
    let workers_url = format!("{}/workers", manager_url);
    poll_until(&workers_url, Duration::from_secs(10), |j| {
        j.as_array()
            .map(|arr| arr.iter().any(|w| w["id"] == "e2e-worker"))
            .unwrap_or(false)
    })
    .await;

    // Trigger the sync explicitly so we don't wait for the 5-second
    // scheduler tick in test wall time. The worker forwards through
    // its own /cmd endpoint which kicks the mirror's JobActor.
    post_cmd(&manager_url, "e2e-worker", "e2e-mirror", CmdVerb::Start).await;

    // Wait for a Success status to land back at the manager.
    let jobs_url = format!("{}/workers/e2e-worker/jobs", manager_url);
    let final_jobs = poll_until(&jobs_url, Duration::from_secs(15), |j| {
        j.as_array()
            .map(|arr| {
                arr.iter().any(|m| {
                    m["name"] == "e2e-mirror"
                        && m["status"] == serde_json::json!(SyncStatus::Success)
                })
            })
            .unwrap_or(false)
    })
    .await;

    // Spot-check the snapshot: worker field is populated, name matches,
    // status is a string the manager accepted.
    let arr = final_jobs.as_array().unwrap();
    let mirror = arr
        .iter()
        .find(|m| m["name"] == "e2e-mirror")
        .expect("mirror row");
    assert_eq!(mirror["worker"], "e2e-worker");

    manager_task.abort();
    worker_task.abort();
}
