use std::{collections::HashMap, fmt, str::FromStr};

use hustsync_internal::msg::{MirrorStatus, WorkerStatus};
use thiserror::Error;

use crate::database::db_redb::RedbAdapter;
use redb;

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
pub enum AdapterError {
    #[error("unsupported db type: {0}")]
    UnsupportedDbType(String),
    #[error("adapter initialization error: {0}")]
    InitError(String),
    #[error("create bucket: {0}, error: {1}")]
    CreateBucketError(String, String),
    // This should be more specific in real implementation
    #[error("anyhow error: {0}")]
    Anyhow(String),
    #[error(transparent)]
    RdbError(#[from] redb::Error),
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
    // TODO: more error variants
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
        let workers_map = self.inner.get_all(WORKER_BUCKETKEY)?;
        let mut workers = Vec::new();

        for (_, v) in workers_map {
            let w: WorkerStatus = serde_json::from_slice(&v)
                .map_err(|e| AdapterError::Anyhow(format!("json unmarshal error: {}", e)))?;
            workers.push(w);
        }
        Ok(workers)
    }

    fn get_worker(&self, worker_id: &str) -> Result<WorkerStatus, AdapterError> {
        let v = self.inner.get(WORKER_BUCKETKEY, worker_id)?;
        match v {
            Some(bytes) => {
                let w: WorkerStatus = serde_json::from_slice(&bytes)
                    .map_err(|e| AdapterError::Anyhow(format!("json unmarshal error: {}", e)))?;
                Ok(w)
            }
            None => Err(AdapterError::Anyhow(format!(
                "invalid workerID {}",
                worker_id
            ))),
        }
    }

    fn delete_worker(&self, worker_id: &str) -> Result<(), AdapterError> {
        // Check existence first to match Go behavior (optional but good for error reporting)
        let v = self.inner.get(WORKER_BUCKETKEY, worker_id)?;
        if v.is_none() {
            return Err(AdapterError::Anyhow(format!(
                "invalid workerID {}",
                worker_id
            )));
        }
        self.inner.delete(WORKER_BUCKETKEY, worker_id)
    }

    fn create_worker(&self, w: WorkerStatus) -> Result<WorkerStatus, AdapterError> {
        let v = serde_json::to_vec(&w)
            .map_err(|e| AdapterError::Anyhow(format!("json marshal error: {}", e)))?;
        self.inner.put(WORKER_BUCKETKEY, &w.id, &v)?;
        Ok(w)
    }

    fn refresh_worker(&self, worker_id: &str) -> Result<WorkerStatus, AdapterError> {
        let mut w = self.get_worker(worker_id)?;
        w.last_online = chrono::Utc::now();
        self.create_worker(w)
    }

    fn update_mirror_status(
        &self,
        worker_id: &str,
        mirror_id: &str,
        status: MirrorStatus,
    ) -> Result<MirrorStatus, AdapterError> {
        let id = format!("{}/{}", mirror_id, worker_id);
        let v = serde_json::to_vec(&status)
            .map_err(|e| AdapterError::Anyhow(format!("json marshal error: {}", e)))?;
        self.inner.put(STATUS_BUCKETKEY, &id, &v)?;
        Ok(status)
    }

    fn get_mirror_status(
        &self,
        worker_id: &str,
        mirror_id: &str,
    ) -> Result<MirrorStatus, AdapterError> {
        let id = format!("{}/{}", mirror_id, worker_id);
        let v = self.inner.get(STATUS_BUCKETKEY, &id)?;
        match v {
            Some(bytes) => {
                let m: MirrorStatus = serde_json::from_slice(&bytes)
                    .map_err(|e| AdapterError::Anyhow(format!("json unmarshal error: {}", e)))?;
                Ok(m)
            }
            None => Err(AdapterError::Anyhow(format!(
                "no mirror '{}' exists in worker '{}'",
                mirror_id, worker_id
            ))),
        }
    }

    fn list_mirror_status(&self, worker_id: &str) -> Result<Vec<MirrorStatus>, AdapterError> {
        let vals = self.inner.get_all(STATUS_BUCKETKEY)?;
        let mut result = Vec::new();

        for (k, v) in vals {
            // key format: mirrorID/workerID
            let parts: Vec<&str> = k.split('/').collect();
            if parts.len() > 1 && parts[1] == worker_id {
                let m: MirrorStatus = serde_json::from_slice(&v)
                    .map_err(|e| AdapterError::Anyhow(format!("json unmarshal error: {}", e)))?;
                result.push(m);
            }
        }
        Ok(result)
    }

    fn list_all_mirror_status(&self) -> Result<Vec<MirrorStatus>, AdapterError> {
        let vals = self.inner.get_all(STATUS_BUCKETKEY)?;
        let mut result = Vec::new();

        for (_, v) in vals {
            let m: MirrorStatus = serde_json::from_slice(&v)
                .map_err(|e| AdapterError::Anyhow(format!("json unmarshal error: {}", e)))?;
            result.push(m);
        }
        Ok(result)
    }

    fn flush_disabled_jobs(&self) -> Result<(), AdapterError> {
        let vals = self.inner.get_all(STATUS_BUCKETKEY)?;
        for (k, v) in vals {
            let m: MirrorStatus = serde_json::from_slice(&v)
                .map_err(|e| AdapterError::Anyhow(format!("json unmarshal error: {}", e)))?;

            if m.status == hustsync_internal::status::SyncStatus::Disabled || m.name.is_empty() {
                self.inner.delete(STATUS_BUCKETKEY, &k)?;
            }
        }
        Ok(())
    }

    fn close(&self) -> Result<(), AdapterError> {
        self.inner.close()
    }
}

impl DbAdapterTrait for KvDBAdapter {
    fn init(&self) -> Result<(), AdapterError> {
        KvDBAdapter::init(self)
    }

    fn list_workers(&self) -> Result<Vec<WorkerStatus>, AdapterError> {
        KvDBAdapter::list_workers(self)
    }

    fn get_worker(&self, worker_id: &str) -> Result<WorkerStatus, AdapterError> {
        KvDBAdapter::get_worker(self, worker_id)
    }

    fn delete_worker(&self, worker_id: &str) -> Result<(), AdapterError> {
        KvDBAdapter::delete_worker(self, worker_id)
    }

    fn create_worker(&self, w: WorkerStatus) -> Result<WorkerStatus, AdapterError> {
        KvDBAdapter::create_worker(self, w)
    }

    fn refresh_worker(&self, worker_id: &str) -> Result<WorkerStatus, AdapterError> {
        KvDBAdapter::refresh_worker(self, worker_id)
    }

    fn update_mirror_status(
        &self,
        worker_id: &str,
        mirror_id: &str,
        status: MirrorStatus,
    ) -> Result<MirrorStatus, AdapterError> {
        KvDBAdapter::update_mirror_status(self, worker_id, mirror_id, status)
    }

    fn get_mirror_status(
        &self,
        worker_id: &str,
        mirror_id: &str,
    ) -> Result<MirrorStatus, AdapterError> {
        KvDBAdapter::get_mirror_status(self, worker_id, mirror_id)
    }

    fn list_mirror_status(&self, worker_id: &str) -> Result<Vec<MirrorStatus>, AdapterError> {
        KvDBAdapter::list_mirror_status(self, worker_id)
    }

    fn list_all_mirror_status(&self) -> Result<Vec<MirrorStatus>, AdapterError> {
        KvDBAdapter::list_all_mirror_status(self)
    }

    fn flush_disabled_jobs(&self) -> Result<(), AdapterError> {
        KvDBAdapter::flush_disabled_jobs(self)
    }

    fn close(&self) -> Result<(), AdapterError> {
        KvDBAdapter::close(self)
    }
}

pub fn make_db_adapter(
    db_type: impl AsRef<str>,
    db_file: impl AsRef<str>,
) -> Result<Box<dyn DbAdapterTrait>, AdapterError> {
    let db_type = DbType::from_str(db_type.as_ref())?;
    let adapter: Box<dyn DbAdapterTrait> = match db_type {
        DbType::Redb => {
            let inner_db = redb::Database::create(db_file.as_ref())?;
            let db = RedbAdapter { db: inner_db };
            let kv = KvDBAdapter {
                inner: Box::new(db),
            };
            Box::new(kv)
        }
    };
    Ok(adapter)
}
