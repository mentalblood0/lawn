use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
    thread::{self, JoinHandle},
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

pub struct ReadTransaction<'a> {
    database_lock_guard: RwLockReadGuard<'a, Database>,
}

impl<'a> ReadTransaction<'a> {
    pub fn new(database_lock: &'a RwLock<Database>) -> Self {
        Self {
            database_lock_guard: database_lock.read().unwrap(),
        }
    }

    pub fn get(&self, table_index: usize, key: &Vec<u8>) -> Result<Option<&Vec<u8>>, String> {
        Ok(self
            .database_lock_guard
            .tables
            .get(table_index)
            .ok_or(format!("No table at index {table_index}"))?
            .get(key))
    }
}

pub struct WriteTransaction<'a> {
    database_lock_guard: RwLockWriteGuard<'a, Database>,
    changes_for_tables: Vec<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl<'a> WriteTransaction<'a> {
    pub fn new(database_lock: &'a RwLock<Database>) -> Self {
        let lock_guard = database_lock.write().unwrap();
        let tables_count = lock_guard.tables.len();
        Self {
            database_lock_guard: lock_guard,
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

    pub fn get(&self, table_index: usize, key: &Vec<u8>) -> Result<Option<&Vec<u8>>, String> {
        match self
            .changes_for_tables
            .get(table_index)
            .ok_or(format!("No table at index {table_index}"))?
            .get(key)
        {
            Some(result_from_changes) => Ok(Some(result_from_changes)),
            None => Ok(self
                .database_lock_guard
                .tables
                .get(table_index)
                .ok_or(format!("No table at index {table_index}"))?
                .get(key)),
        }
    }

    pub fn commit(&mut self) -> Result<(), String> {
        for (table_index, table_changes) in self.changes_for_tables.iter_mut().enumerate() {
            self.database_lock_guard.tables[table_index].merge(table_changes);
        }
        Ok(())
    }
}

pub fn lock_all_writes_and_read(
    database: &Arc<RwLock<Database>>,
    f: fn(ReadTransaction) -> (),
) -> () {
    f(ReadTransaction::new(&Arc::clone(database)))
}

pub fn lock_all_and_write(database: &Arc<RwLock<Database>>, f: fn(WriteTransaction) -> ()) -> () {
    f(WriteTransaction::new(&Arc::clone(database)))
}

pub fn lock_all_writes_and_spawn_read(
    database: &Arc<RwLock<Database>>,
    f: fn(ReadTransaction) -> (),
) -> JoinHandle<()> {
    let database_clone = Arc::clone(database);
    thread::spawn(move || f(ReadTransaction::new(&database_clone)))
}

pub fn lock_all_and_spawn_write(
    database: &Arc<RwLock<Database>>,
    f: fn(WriteTransaction) -> (),
) -> JoinHandle<()> {
    let database_clone = Arc::clone(database);
    thread::spawn(move || f(WriteTransaction::new(&database_clone)))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        index::IndexConfig, table::TableConfig, variable_data_pool::VariableDataPoolConfig,
    };
    use std::{path::Path, sync::Arc};

    #[test]
    fn test_transactions_concurrency() {
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

        lock_all_and_write(&database, |mut transaction| {
            let key = "key".as_bytes().to_vec();
            transaction
                .set(0 as usize, key, (0 as usize).to_le_bytes().to_vec())
                .unwrap()
                .commit()
                .unwrap();
        });

        const THREADS_COUNT: usize = 10;
        const INCREMENTS_PER_THREAD_COUNT: usize = 100_000;
        const FINAL_VALUE: usize = THREADS_COUNT * INCREMENTS_PER_THREAD_COUNT;

        let mut threads_handles = vec![];
        for _ in 0..THREADS_COUNT {
            threads_handles.push(lock_all_and_spawn_write(&database, |mut transaction| {
                let key = "key".as_bytes().to_vec();
                for _ in 0..INCREMENTS_PER_THREAD_COUNT {
                    transaction
                        .set(
                            0 as usize,
                            key.clone(),
                            (usize::from_le_bytes(
                                transaction
                                    .get(0 as usize, &key)
                                    .unwrap()
                                    .unwrap()
                                    .clone()
                                    .as_slice()
                                    .try_into()
                                    .unwrap(),
                            ) + 1)
                                .to_le_bytes()
                                .to_vec(),
                        )
                        .unwrap()
                        .commit()
                        .unwrap();
                }
            }));
        }
        for thread_handle in threads_handles {
            thread_handle.join().unwrap();
        }

        lock_all_writes_and_read(&database, |transaction| {
            assert_eq!(
                transaction
                    .get(0 as usize, &"key".as_bytes().to_vec())
                    .unwrap()
                    .unwrap(),
                &FINAL_VALUE.to_le_bytes().to_vec()
            );
        });
    }
}
