use hustsync_manager::database::make_db_adapter;
use hustsync_internal::msg::{WorkerStatus, MirrorStatus};
use hustsync_internal::status::SyncStatus;
use chrono::Utc;
use tempfile::NamedTempFile;

#[test]
fn test_redb_adapter_basic() {
    let tmp_file = NamedTempFile::new().unwrap();
    let db_path = tmp_file.path().to_str().unwrap();

    let adapter = make_db_adapter("redb", db_path).expect("Failed to create adapter");
    adapter.init().expect("Failed to init database");

    // Test Worker
    let worker = WorkerStatus {
        id: "worker-1".to_string(),
        url: "http://localhost:8081".to_string(),
        token: "token123".to_string(),
        last_online: Utc::now(),
        last_register: Utc::now(),
    };

    adapter.create_worker(worker).expect("Failed to create worker");
    
    let workers = adapter.list_workers().expect("Failed to list workers");
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].id, "worker-1");

    let retrieved_worker = adapter.get_worker("worker-1").expect("Failed to get worker");
    assert_eq!(retrieved_worker.url, "http://localhost:8081");

    // Test Mirror Status
    let mirror = MirrorStatus {
        name: "ubuntu".to_string(),
        worker: "worker-1".to_string(),
        upstream: "http://archive.ubuntu.com/ubuntu".to_string(),
        size: "1TB".to_string(),
        error_msg: "".to_string(),
        last_update: Utc::now(),
        last_started: Utc::now(),
        last_ended: Utc::now(),
        next_scheduled: Utc::now(),
        status: SyncStatus::Success,
        is_master: true,
    };

    adapter.update_mirror_status("worker-1", "ubuntu", mirror).expect("Failed to update mirror status");

    let mirror_status = adapter.get_mirror_status("worker-1", "ubuntu").expect("Failed to get mirror status");
    assert_eq!(mirror_status.name, "ubuntu");
    assert_eq!(mirror_status.status, SyncStatus::Success);

    let all_mirrors = adapter.list_all_mirror_status().expect("Failed to list all mirror status");
    assert_eq!(all_mirrors.len(), 1);

    // Test delete
    adapter.delete_worker("worker-1").expect("Failed to delete worker");
    let workers_after = adapter.list_workers().expect("Failed to list workers after delete");
    assert_eq!(workers_after.len(), 0);
}
