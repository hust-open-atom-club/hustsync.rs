use std::collections::HashMap;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use super::{AdapterError, DbAdapterTrait, STATUS_BUCKETKEY, WORKER_BUCKETKEY};
use hustsync_internal::msg::{MirrorStatus, WorkerStatus};

pub(super) struct RedbAdapter {
    pub(super) db: Database,
}

impl RedbAdapter {
    fn init_bucket(&self, bucket: &str) -> Result<(), AdapterError> {
        let table_def: TableDefinition<&str, &[u8]> = TableDefinition::new(bucket);
        let write_txn = self.db.begin_write()?;
        write_txn.open_table(table_def)?;
        write_txn.commit()?;
        Ok(())
    }

    fn kv_get(&self, bucket: &str, key: &str) -> Result<Option<Vec<u8>>, AdapterError> {
        let table_def: TableDefinition<&str, &[u8]> = TableDefinition::new(bucket);
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(table_def)?;
        match table.get(key)? {
            Some(value) => Ok(Some(value.value().to_vec())),
            None => Ok(None),
        }
    }

    fn kv_get_all(&self, bucket: &str) -> Result<HashMap<String, Vec<u8>>, AdapterError> {
        let table_def: TableDefinition<&str, &[u8]> = TableDefinition::new(bucket);
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(table_def)?;
        let mut map = HashMap::new();
        for item in table.iter()? {
            let (key_guard, value_guard) = item?;
            let key = key_guard.value().to_string();
            let value = value_guard.value().to_vec();
            map.insert(key, value);
        }
        Ok(map)
    }

    fn kv_put(&self, bucket: &str, key: &str, value: &[u8]) -> Result<(), AdapterError> {
        let table_def: TableDefinition<&str, &[u8]> = TableDefinition::new(bucket);
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(table_def)?;
            table.insert(key, value)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn kv_delete(&self, bucket: &str, key: &str) -> Result<(), AdapterError> {
        let table_def: TableDefinition<&str, &[u8]> = TableDefinition::new(bucket);
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(table_def)?;
            table.remove(key)?;
        }
        write_txn.commit()?;
        Ok(())
    }
}

impl DbAdapterTrait for RedbAdapter {
    fn init(&self) -> Result<(), AdapterError> {
        self.init_bucket(WORKER_BUCKETKEY)?;
        self.init_bucket(STATUS_BUCKETKEY)?;
        Ok(())
    }

    fn list_workers(&self) -> Result<Vec<WorkerStatus>, AdapterError> {
        let workers_map = self.kv_get_all(WORKER_BUCKETKEY)?;
        let mut workers = Vec::new();
        for (_, v) in workers_map {
            let w: WorkerStatus = serde_json::from_slice(&v)?;
            workers.push(w);
        }
        Ok(workers)
    }

    fn get_worker(&self, worker_id: &str) -> Result<WorkerStatus, AdapterError> {
        let v = self.kv_get(WORKER_BUCKETKEY, worker_id)?;
        let Some(bytes) = v else {
            return Err(AdapterError::NotFound(format!("worker '{}'", worker_id)));
        };
        let w: WorkerStatus = serde_json::from_slice(&bytes)?;
        Ok(w)
    }

    fn delete_worker(&self, worker_id: &str) -> Result<(), AdapterError> {
        let v = self.kv_get(WORKER_BUCKETKEY, worker_id)?;
        if v.is_none() {
            return Err(AdapterError::NotFound(format!("worker '{}'", worker_id)));
        }
        self.kv_delete(WORKER_BUCKETKEY, worker_id)
    }

    fn create_worker(&self, w: WorkerStatus) -> Result<WorkerStatus, AdapterError> {
        let v = serde_json::to_vec(&w)?;
        self.kv_put(WORKER_BUCKETKEY, &w.id, &v)?;
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
        let v = serde_json::to_vec(&status)?;
        self.kv_put(STATUS_BUCKETKEY, &id, &v)?;
        Ok(status)
    }

    fn get_mirror_status(
        &self,
        worker_id: &str,
        mirror_id: &str,
    ) -> Result<MirrorStatus, AdapterError> {
        let id = format!("{}/{}", mirror_id, worker_id);
        let v = self.kv_get(STATUS_BUCKETKEY, &id)?;
        match v {
            Some(bytes) => Ok(serde_json::from_slice(&bytes)?),
            None => Err(AdapterError::NotFound(format!(
                "mirror '{}' in worker '{}'",
                mirror_id, worker_id
            ))),
        }
    }

    fn list_mirror_status(&self, worker_id: &str) -> Result<Vec<MirrorStatus>, AdapterError> {
        let vals = self.kv_get_all(STATUS_BUCKETKEY)?;
        let mut result = Vec::new();
        for (k, v) in vals {
            // key format: mirrorID/workerID
            let parts: Vec<&str> = k.split('/').collect();
            if parts.len() > 1 && parts[1] == worker_id {
                let m: MirrorStatus = serde_json::from_slice(&v)?;
                result.push(m);
            }
        }
        Ok(result)
    }

    fn list_all_mirror_status(&self) -> Result<Vec<MirrorStatus>, AdapterError> {
        let vals = self.kv_get_all(STATUS_BUCKETKEY)?;
        let mut result = Vec::new();
        for (_, v) in vals {
            let m: MirrorStatus = serde_json::from_slice(&v)?;
            result.push(m);
        }
        Ok(result)
    }

    fn flush_disabled_jobs(&self) -> Result<(), AdapterError> {
        let vals = self.kv_get_all(STATUS_BUCKETKEY)?;
        for (k, v) in vals {
            let m: MirrorStatus = serde_json::from_slice(&v)?;
            if m.status == hustsync_internal::status::SyncStatus::Disabled || m.name.is_empty() {
                self.kv_delete(STATUS_BUCKETKEY, &k)?;
            }
        }
        Ok(())
    }

    fn close(&self) -> Result<(), AdapterError> {
        Ok(())
    }
}
