use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::table::{Table, TableConfig};

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct DatabaseConfig {
    pub tables: Vec<TableConfig>,
}

pub struct Database {
    pub tables: Vec<Table>,
}

impl Database {
    pub fn new(config: DatabaseConfig) -> Result<Self, String> {
        let mut tables: Vec<Table> = Vec::new();
        for table_config in config.tables {
            tables.push(Table::new(table_config)?);
        }
        Ok(Self { tables })
    }

    pub fn clear(&mut self) -> Result<&mut Self, String> {
        for table in self.tables.iter_mut() {
            table.clear()?;
        }
        Ok(self)
    }
}

pub struct WriteTransaction<'a> {
    database: RwLockWriteGuard<'a, Database>,
    changes_for_tables: Vec<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl<'a> WriteTransaction<'a> {
    pub fn new(database_lock: &'a RwLock<Database>) -> Self {
        let tables_count = database_lock.read().unwrap().tables.len();
        Self {
            database: database_lock.write().unwrap(),
            changes_for_tables: vec![const { BTreeMap::new() }; tables_count],
        }
    }

    pub fn set(
        &mut self,
        table_index: usize,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<&mut Self, String> {
        self.changes_for_tables
            .get_mut(table_index)
            .ok_or(format!("No table at index {table_index}"))?
            .insert(key, value);
        Ok(self)
    }

    pub fn get(&self, table_index: usize, key: &Vec<u8>) -> Result<Option<Vec<u8>>, String> {
        match self
            .changes_for_tables
            .get(table_index)
            .ok_or(format!("No table at index {table_index}"))?
            .get(key)
        {
            Some(result_from_changes) => Ok(Some(result_from_changes.clone())),
            None => Ok(self
                .database
                .tables
                .get(table_index)
                .ok_or(format!("No table at index {table_index}"))?
                .get(key)
                .map_or(None, |result_from_memtable| {
                    Some(result_from_memtable.clone())
                })),
        }
    }

    pub fn commit(&mut self) -> Result<(), String> {
        for (table_index, table_changes) in self.changes_for_tables.iter_mut().enumerate() {
            self.database.tables[table_index].merge(table_changes);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, thread};

    use super::*;
    use crate::{
        index::IndexConfig, table::TableConfig, variable_data_pool::VariableDataPoolConfig,
    };

    #[test]
    fn test_transactions() {
        let database = Arc::new(RwLock::new(
            Database::new(DatabaseConfig {
                tables: vec![TableConfig {
                    index: IndexConfig {
                        path: Path::new("/tmp/lawn/test/database/0/index.idx").to_path_buf(),
                        container_size: 4 as u8,
                    },
                    data_pool: Box::new(VariableDataPoolConfig {
                        directory: Path::new("/tmp/lawn/test/database/0/data_pool").to_path_buf(),
                        max_element_size: 65536 as usize,
                    }),
                }],
            })
            .unwrap(),
        ));
        {
            database.write().unwrap().clear().unwrap();
        }

        let mut threads_handles = vec![];

        {
            let database_clone = Arc::clone(&database);
            threads_handles.push(thread::spawn(move || {
                let transaction = WriteTransaction::new(&database_clone);
                loop {
                    if transaction
                        .get(0 as usize, &"key".as_bytes().to_vec())
                        .unwrap()
                        .is_some()
                    {
                        break;
                    }
                }
            }));
        }
        {
            let database_clone = Arc::clone(&database);
            threads_handles.push(thread::spawn(move || {
                let mut transaction = WriteTransaction::new(&database_clone);
                transaction
                    .set(
                        0 as usize,
                        "key".as_bytes().to_vec(),
                        "value".as_bytes().to_vec(),
                    )
                    .unwrap()
                    .commit()
                    .unwrap();
            }));
        }

        for thread_handle in threads_handles {
            thread_handle.join().unwrap();
        }
    }
}
