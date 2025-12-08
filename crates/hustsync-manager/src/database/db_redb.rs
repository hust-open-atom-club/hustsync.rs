use std::collections::HashMap;

use redb;
use redb::Database;

use super::AdapterError;
use super::KvAdapterTrait;

struct RedbAdapter {
    db: Database,
}

impl KvAdapterTrait for RedbAdapter {
    fn init_bucket(&self, bucket: &str) -> Result<(), AdapterError> {
        todo!()
    }
    fn get(&self, bucket: &str, key: &str) -> Result<Option<Vec<u8>>, AdapterError> {
        todo!()
    }
    fn get_all(&self, bucket: &str) -> Result<HashMap<String, Vec<u8>>, AdapterError> {
        todo!()
    }
    fn put(&self, bucket: &str, key: &str, value: &[u8]) -> Result<(), AdapterError> {
        todo!()
    }
    fn delete(&self, bucket: &str, key: &str) -> Result<(), AdapterError> {
        todo!()
    }
    fn close(&self) -> Result<(), AdapterError> {
        todo!()
    }
}
