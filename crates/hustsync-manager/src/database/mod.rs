use std::str::FromStr;

use hustsync_internal::msg::{MirrorStatus, WorkerStatus};
use thiserror::Error;

use crate::database::db_redb::RedbAdapter;

mod db_redb;

pub(super) const WORKER_BUCKETKEY: &str = "workers";
pub(super) const STATUS_BUCKETKEY: &str = "mirror_status";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DbType {
    Redb,
}

impl FromStr for DbType {
    type Err = AdapterError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "redb" => Ok(DbType::Redb),
            _ => Err(AdapterError::UnsupportedDbType(s.into())),
        }
    }
}

#[derive(Error, Debug)]
pub enum AdapterError {
    #[error("unsupported db type: {0}")]
    UnsupportedDbType(String),
    #[error("adapter initialization error: {0}")]
    InitError(String),
    #[error("create bucket: {0}, error: {1}")]
    CreateBucketError(String, String),
    #[error("record not found: {0}")]
    NotFound(String),
    #[error("serialization error: {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error(transparent)]
    RdbDatabaseError(#[from] redb::DatabaseError),
    #[error(transparent)]
    RdbTransactionError(#[from] redb::TransactionError),
    #[error(transparent)]
    RdbTableError(#[from] redb::TableError),
    #[error(transparent)]
    RdbCommitError(#[from] redb::CommitError),
    #[error(transparent)]
    RdbStorageError(#[from] redb::StorageError),
}

pub trait DbAdapterTrait: Send + Sync {
    fn init(&self) -> Result<(), AdapterError>;
    fn list_workers(&self) -> Result<Vec<WorkerStatus>, AdapterError>;
    fn get_worker(&self, worker_id: &str) -> Result<WorkerStatus, AdapterError>;
    fn delete_worker(&self, worker_id: &str) -> Result<(), AdapterError>;
    fn create_worker(&self, w: WorkerStatus) -> Result<WorkerStatus, AdapterError>;
    fn refresh_worker(&self, worker_id: &str) -> Result<WorkerStatus, AdapterError>;
    fn update_mirror_status(
        &self,
        worker_id: &str,
        mirror_id: &str,
        status: MirrorStatus,
    ) -> Result<MirrorStatus, AdapterError>;
    fn get_mirror_status(
        &self,
        worker_id: &str,
        mirror_id: &str,
    ) -> Result<MirrorStatus, AdapterError>;
    fn list_mirror_status(&self, worker_id: &str) -> Result<Vec<MirrorStatus>, AdapterError>;
    fn list_all_mirror_status(&self) -> Result<Vec<MirrorStatus>, AdapterError>;
    fn flush_disabled_jobs(&self) -> Result<(), AdapterError>;
    fn close(&self) -> Result<(), AdapterError>;
}

pub fn make_db_adapter(
    db_type: impl AsRef<str>,
    db_file: impl AsRef<str>,
) -> Result<Box<dyn DbAdapterTrait>, AdapterError> {
    let db_type = DbType::from_str(db_type.as_ref())?;
    let adapter: Box<dyn DbAdapterTrait> = match db_type {
        DbType::Redb => {
            let db = redb::Database::create(db_file.as_ref())?;
            Box::new(RedbAdapter { db })
        }
    };
    Ok(adapter)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use hustsync_internal::status::SyncStatus;
    use tempfile::NamedTempFile;

    fn setup_test_db() -> (Box<dyn DbAdapterTrait>, NamedTempFile) {
        let tmp_file = NamedTempFile::new().unwrap();
        let db_path = tmp_file.path().to_str().unwrap();
        let adapter = make_db_adapter("redb", db_path).unwrap();
        adapter.init().unwrap();
        (adapter, tmp_file)
    }

    #[test]
    fn test_worker_operations() {
        let (db, _tmp) = setup_test_db();

        let worker = WorkerStatus {
            id: "test-worker-1".to_string(),
            url: "http://localhost:8080".to_string(),
            token: "secret".to_string(),
            last_online: chrono::Utc::now(),
            last_register: chrono::Utc::now(),
        };

        // Create
        db.create_worker(worker).unwrap();

        // Get
        let w = db.get_worker("test-worker-1").unwrap();
        assert_eq!(w.id, "test-worker-1");
        assert_eq!(w.url, "http://localhost:8080");

        // List
        let workers = db.list_workers().unwrap();
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].id, "test-worker-1");

        // Delete
        db.delete_worker("test-worker-1").unwrap();
        let workers_after = db.list_workers().unwrap();
        assert_eq!(workers_after.len(), 0);
    }

    #[test]
    fn test_mirror_status_operations() {
        let (db, _tmp) = setup_test_db();

        let mirror = MirrorStatus {
            name: "debian".to_string(),
            worker: "worker-1".to_string(),
            upstream: "https://deb.debian.org".to_string(),
            size: "1TB".to_string(),
            error_msg: "".to_string(),
            last_update: chrono::Utc::now(),
            last_started: chrono::Utc::now(),
            last_ended: chrono::Utc::now(),
            next_scheduled: chrono::Utc::now(),
            status: SyncStatus::Success,
            is_master: true,
        };

        // Update/Create
        db.update_mirror_status("worker-1", "debian", mirror)
            .unwrap();

        // Get
        let m = db.get_mirror_status("worker-1", "debian").unwrap();
        assert_eq!(m.name, "debian");
        assert_eq!(m.status, SyncStatus::Success);

        // List per worker
        let statuses = db.list_mirror_status("worker-1").unwrap();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].name, "debian");

        // List all
        let all_statuses = db.list_all_mirror_status().unwrap();
        assert_eq!(all_statuses.len(), 1);
    }
}
