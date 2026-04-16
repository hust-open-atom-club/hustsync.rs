use std::collections::HashMap;

use redis::{Client, Commands};

use super::{AdapterError, DbAdapterTrait, STATUS_BUCKETKEY, WORKER_BUCKETKEY};
use hustsync_internal::msg::{MirrorStatus, WorkerStatus};

pub(super) struct RedisAdapter {
    client: Client,
}

impl RedisAdapter {
    pub(super) fn new(url: &str) -> Result<Self, AdapterError> {
        let client = Client::open(url)
            .map_err(|e| AdapterError::InitError(format!("redis open: {e}")))?;
        // Eagerly verify the connection is reachable before handing back the adapter.
        let mut conn = client
            .get_connection()
            .map_err(|e| AdapterError::InitError(format!("redis connect: {e}")))?;
        let _: String = redis::cmd("PING")
            .query(&mut conn)
            .map_err(|e| AdapterError::InitError(format!("redis ping: {e}")))?;
        Ok(Self { client })
    }

    fn conn(&self) -> Result<redis::Connection, AdapterError> {
        self.client
            .get_connection()
            .map_err(|e| AdapterError::RedisError(format!("get connection: {e}")))
    }

    fn kv_get(&self, bucket: &str, key: &str) -> Result<Option<Vec<u8>>, AdapterError> {
        let mut conn = self.conn()?;
        let val: Option<Vec<u8>> = conn
            .hget(bucket, key)
            .map_err(|e| AdapterError::RedisError(format!("HGET {bucket} {key}: {e}")))?;
        Ok(val)
    }

    fn kv_get_all(&self, bucket: &str) -> Result<HashMap<String, Vec<u8>>, AdapterError> {
        let mut conn = self.conn()?;
        let map: HashMap<String, Vec<u8>> = conn
            .hgetall(bucket)
            .map_err(|e| AdapterError::RedisError(format!("HGETALL {bucket}: {e}")))?;
        Ok(map)
    }

    fn kv_put(&self, bucket: &str, key: &str, value: &[u8]) -> Result<(), AdapterError> {
        let mut conn = self.conn()?;
        conn.hset(bucket, key, value)
            .map_err(|e| AdapterError::RedisError(format!("HSET {bucket} {key}: {e}")))
    }

    fn kv_delete(&self, bucket: &str, key: &str) -> Result<(), AdapterError> {
        let mut conn = self.conn()?;
        conn.hdel(bucket, key)
            .map_err(|e| AdapterError::RedisError(format!("HDEL {bucket} {key}: {e}")))
    }
}

impl DbAdapterTrait for RedisAdapter {
    /// Redis hashes are created on first write; no explicit bucket init needed.
    fn init(&self) -> Result<(), AdapterError> {
        Ok(())
    }

    fn list_workers(&self) -> Result<Vec<WorkerStatus>, AdapterError> {
        let map = self.kv_get_all(WORKER_BUCKETKEY)?;
        let mut workers = Vec::with_capacity(map.len());
        for (_, v) in map {
            let w: WorkerStatus = serde_json::from_slice(&v)?;
            workers.push(w);
        }
        Ok(workers)
    }

    fn get_worker(&self, worker_id: &str) -> Result<WorkerStatus, AdapterError> {
        match self.kv_get(WORKER_BUCKETKEY, worker_id)? {
            Some(bytes) => Ok(serde_json::from_slice(&bytes)?),
            None => Err(AdapterError::NotFound(format!("worker '{worker_id}'"))),
        }
    }

    fn delete_worker(&self, worker_id: &str) -> Result<(), AdapterError> {
        if self.kv_get(WORKER_BUCKETKEY, worker_id)?.is_none() {
            return Err(AdapterError::NotFound(format!("worker '{worker_id}'")));
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
        let id = format!("{mirror_id}/{worker_id}");
        let v = serde_json::to_vec(&status)?;
        self.kv_put(STATUS_BUCKETKEY, &id, &v)?;
        Ok(status)
    }

    fn get_mirror_status(
        &self,
        worker_id: &str,
        mirror_id: &str,
    ) -> Result<MirrorStatus, AdapterError> {
        let id = format!("{mirror_id}/{worker_id}");
        match self.kv_get(STATUS_BUCKETKEY, &id)? {
            Some(bytes) => Ok(serde_json::from_slice(&bytes)?),
            None => Err(AdapterError::NotFound(format!(
                "mirror '{mirror_id}' in worker '{worker_id}'"
            ))),
        }
    }

    fn list_mirror_status(&self, worker_id: &str) -> Result<Vec<MirrorStatus>, AdapterError> {
        let map = self.kv_get_all(STATUS_BUCKETKEY)?;
        let mut result = Vec::new();
        for (k, v) in map {
            // key format: mirrorID/workerID
            let mut parts = k.splitn(2, '/');
            parts.next(); // mirror id
            if parts.next() == Some(worker_id) {
                let m: MirrorStatus = serde_json::from_slice(&v)?;
                result.push(m);
            }
        }
        Ok(result)
    }

    fn list_all_mirror_status(&self) -> Result<Vec<MirrorStatus>, AdapterError> {
        let map = self.kv_get_all(STATUS_BUCKETKEY)?;
        let mut result = Vec::with_capacity(map.len());
        for (_, v) in map {
            let m: MirrorStatus = serde_json::from_slice(&v)?;
            result.push(m);
        }
        Ok(result)
    }

    fn flush_disabled_jobs(&self) -> Result<(), AdapterError> {
        let map = self.kv_get_all(STATUS_BUCKETKEY)?;
        for (k, v) in map {
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use hustsync_internal::status::SyncStatus;

    /// Run with: REDIS_URL=redis://127.0.0.1:6379/15 cargo test -p hustsync-manager redis
    fn redis_url() -> Option<String> {
        std::env::var("REDIS_URL").ok()
    }

    fn setup_adapter() -> Option<RedisAdapter> {
        let url = redis_url()?;
        let adapter = RedisAdapter::new(&url).expect("redis adapter init");
        // Flush the test DB to start clean.
        let mut conn = adapter.client.get_connection().expect("connection");
        let _: () = redis::cmd("FLUSHDB")
            .query(&mut conn)
            .expect("FLUSHDB");
        Some(adapter)
    }

    #[test]
    fn test_redis_worker_operations() {
        let Some(db) = setup_adapter() else {
            eprintln!("REDIS_URL not set, skipping Redis tests");
            return;
        };

        let worker = WorkerStatus {
            id: "redis-worker-1".to_string(),
            url: "http://localhost:8080".to_string(),
            token: "secret".to_string(),
            last_online: chrono::Utc::now(),
            last_register: chrono::Utc::now(),
        };

        // Create
        db.create_worker(worker).unwrap();

        // Get
        let w = db.get_worker("redis-worker-1").unwrap();
        assert_eq!(w.id, "redis-worker-1");
        assert_eq!(w.url, "http://localhost:8080");

        // List
        let workers = db.list_workers().unwrap();
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].id, "redis-worker-1");

        // Delete
        db.delete_worker("redis-worker-1").unwrap();
        let workers_after = db.list_workers().unwrap();
        assert_eq!(workers_after.len(), 0);

        // NotFound on missing
        let err = db.get_worker("redis-worker-1").unwrap_err();
        assert!(matches!(err, AdapterError::NotFound(_)));
    }

    #[test]
    fn test_redis_mirror_status_operations() {
        let Some(db) = setup_adapter() else {
            eprintln!("REDIS_URL not set, skipping Redis tests");
            return;
        };

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
        let all = db.list_all_mirror_status().unwrap();
        assert_eq!(all.len(), 1);
    }

    #[test]
    fn test_redis_flush_disabled_jobs() {
        let Some(db) = setup_adapter() else {
            eprintln!("REDIS_URL not set, skipping Redis tests");
            return;
        };

        let active = MirrorStatus {
            name: "ubuntu".to_string(),
            worker: "worker-1".to_string(),
            upstream: "https://archive.ubuntu.com".to_string(),
            size: "".to_string(),
            error_msg: "".to_string(),
            last_update: chrono::Utc::now(),
            last_started: chrono::Utc::now(),
            last_ended: chrono::Utc::now(),
            next_scheduled: chrono::Utc::now(),
            status: SyncStatus::Success,
            is_master: false,
        };
        let disabled = MirrorStatus {
            name: "arch".to_string(),
            worker: "worker-1".to_string(),
            upstream: "https://archive.archlinux.org".to_string(),
            size: "".to_string(),
            error_msg: "".to_string(),
            last_update: chrono::Utc::now(),
            last_started: chrono::Utc::now(),
            last_ended: chrono::Utc::now(),
            next_scheduled: chrono::Utc::now(),
            status: SyncStatus::Disabled,
            is_master: false,
        };

        db.update_mirror_status("worker-1", "ubuntu", active)
            .unwrap();
        db.update_mirror_status("worker-1", "arch", disabled)
            .unwrap();

        db.flush_disabled_jobs().unwrap();

        let all = db.list_all_mirror_status().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "ubuntu");
    }
}
