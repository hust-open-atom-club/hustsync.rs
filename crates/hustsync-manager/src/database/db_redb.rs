use std::collections::HashMap;

use redb;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use super::AdapterError;
use super::KvAdapterTrait;

pub(super) struct RedbAdapter {
    pub(super) db: Database,
}

impl KvAdapterTrait for RedbAdapter {
    fn init_bucket(&self, bucket: &str) -> Result<(), AdapterError> {
        let table_def: TableDefinition<&str, &[u8]> = TableDefinition::new(bucket);
        let write_txn = self.db.begin_write()?;
        write_txn.open_table(table_def)?;
        write_txn.commit()?;
        Ok(())
    }

    fn get(&self, bucket: &str, key: &str) -> Result<Option<Vec<u8>>, AdapterError> {
        let table_def: TableDefinition<&str, &[u8]> = TableDefinition::new(bucket);
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(table_def)?;
        match table.get(key)? {
            Some(value) => Ok(Some(value.value().to_vec())),
            None => Ok(None),
        }
    }

    fn get_all(&self, bucket: &str) -> Result<HashMap<String, Vec<u8>>, AdapterError> {
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

    fn put(&self, bucket: &str, key: &str, value: &[u8]) -> Result<(), AdapterError> {
        let table_def: TableDefinition<&str, &[u8]> = TableDefinition::new(bucket);
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(table_def)?;
            table.insert(key, value)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn delete(&self, bucket: &str, key: &str) -> Result<(), AdapterError> {
        let table_def: TableDefinition<&str, &[u8]> = TableDefinition::new(bucket);
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(table_def)?;
            table.remove(key)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn close(&self) -> Result<(), AdapterError> {
        Ok(())
    }
}
