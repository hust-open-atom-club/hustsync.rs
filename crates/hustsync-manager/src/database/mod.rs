use std::{collections::HashMap, fmt, str::FromStr};

use hustsync_internal::msg::{MirrorStatus, WorkerStatus};
use thiserror::Error;

mod db_redb;

const WORKER_BUCKETKEY: &str = "workers";
const STATUS_BUCKETKEY: &str = "mirror_status";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DbType {
    // TODO current only redb is supported
    Redb,
    // Redis,
    // Badger,
    // LevelDb,
}

impl fmt::Display for DbType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FromStr for DbType {
    type Err = AdapterError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            // TODO current only redb is supported
            "redb" => Ok(DbType::Redb),
            // "redis" => Ok(DbType::Redis),
            // "badger" => Ok(DbType::Badger),
            // "leveldb" => Ok(DbType::LevelDb),
            _ => Err(AdapterError::UnsupportedDbType(s.into())),
        }
    }
}

#[derive(Error, Debug)]
enum AdapterError {
    #[error("unsupported db type: {0}")]
    UnsupportedDbType(String),
    #[error("adapter initialization error: {0}")]
    InitError(String),
    #[error("create bucket: {0}, error: {1}")]
    CreateBucketError(String, String),
    // TODO: more error variants
}

trait DbAdapterTrait: Send + Sync {
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
}
trait KvAdapterTrait: Send + Sync {
    fn init_bucket(&self, bucket: &str) -> Result<(), AdapterError>;
    // TODO should be bytes return
    fn get(&self, bucket: &str, key: &str) -> Result<Option<Vec<u8>>, AdapterError>;
    // TODO should be bytes return
    fn get_all(&self, bucket: &str) -> Result<HashMap<String, Vec<u8>>, AdapterError>;
    fn put(&self, bucket: &str, key: &str, value: &[u8]) -> Result<(), AdapterError>;
    fn delete(&self, bucket: &str, key: &str) -> Result<(), AdapterError>;
    fn close(&self) -> Result<(), AdapterError>;
}

struct KvDBAdapter {
    inner: Box<dyn KvAdapterTrait>,
}

impl KvDBAdapter {
    fn init(&self) -> Result<(), AdapterError> {
        self.inner.init_bucket(WORKER_BUCKETKEY)?;
        self.inner.init_bucket(STATUS_BUCKETKEY)?;
        Ok(())
    }

    fn list_workers(&self) -> Result<Vec<WorkerStatus>, AdapterError> {
        todo!()
    }

    fn get_worker(&self, worker_id: &str) -> Result<WorkerStatus, AdapterError> {
        todo!()
    }

    fn delete_worker(&self, worker_id: &str) -> Result<(), AdapterError> {
        todo!()
    }

    fn create_worker(&self, w: WorkerStatus) -> Result<WorkerStatus, AdapterError> {
        todo!()
    }

    fn refresh_worker(&self, worker_id: &str) -> Result<WorkerStatus, AdapterError> {
        todo!()
    }

    fn update_mirror_status(
        &self,
        worker_id: &str,
        mirror_id: &str,
        status: MirrorStatus,
    ) -> Result<MirrorStatus, AdapterError> {
        todo!()
    }

    fn get_mirror_status(
        &self,
        worker_id: &str,
        mirror_id: &str,
    ) -> Result<MirrorStatus, AdapterError> {
        todo!()
    }

    fn list_mirror_status(&self, worker_id: &str) -> Result<Vec<MirrorStatus>, AdapterError> {
        todo!()
    }

    fn list_all_mirror_status(&self) -> Result<Vec<MirrorStatus>, AdapterError> {
        todo!()
    }

    fn flush_disabled_jobs(&self) -> Result<(), AdapterError> {
        todo!()
    }

    fn close(&self) -> Result<(), AdapterError> {
        self.inner.close()
    }
}

// TODO: implement a real Redb adapter and return it here.
fn make_db_adapter(
    db_type: impl AsRef<str>,
    db_file: impl AsRef<str>,
) -> Result<Box<dyn DbAdapterTrait>, AdapterError> {
    let db_type = DbType::from_str(db_type.as_ref())?;
    todo!()
}
