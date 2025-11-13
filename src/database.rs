use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
    thread::{self, JoinHandle},
};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::log::{Log, LogConfig};
use crate::table;

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct DatabaseConfig {
    pub tables: HashMap<String, table::TableConfig>,
    pub log: LogConfig,
}

struct DatabaseLockableInternals {
    tables: HashMap<String, table::Table>,
    log: Log,
}

pub struct ReadTransaction<'a> {
    database_locked_internals: RwLockReadGuard<'a, DatabaseLockableInternals>,
}

impl<'a> ReadTransaction<'a> {
    fn new(database_lock: &'a RwLock<DatabaseLockableInternals>) -> Result<Self, String> {
        Ok(Self {
            database_locked_internals: database_lock
                .read()
                .map_err(|error| format!("Can not acquire read lock on database: {error}"))?,
        })
    }

    fn get(&self, table_name: &String, key: &Vec<u8>) -> Result<Option<&Vec<u8>>, String> {
        Ok(self
            .database_locked_internals
            .tables
            .get(table_name)
            .ok_or(format!("No table named '{table_name}'"))?
            .get(key))
    }
}

pub struct WriteTransaction<'a> {
    database_locked_internals: RwLockWriteGuard<'a, DatabaseLockableInternals>,
    changes_for_tables: HashMap<String, BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl<'a> WriteTransaction<'a> {
    fn new(database_lock: &'a RwLock<DatabaseLockableInternals>) -> Result<Self, String> {
        Ok(Self {
            database_locked_internals: database_lock
                .write()
                .map_err(|error| format!("Can not acquire write lock on database: {error}"))?,
            changes_for_tables: HashMap::new(),
        })
    }

    fn set(
        &mut self,
        table_name: &String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<&mut Self, String> {
        self.changes_for_tables
            .entry(table_name.clone())
            .and_modify(|changes_for_table| {
                changes_for_table.insert(key.clone(), value.clone());
            })
            .or_insert_with(|| BTreeMap::from_iter([(key.clone(), value.clone())]));
        Ok(self)
    }

    fn get(&self, table_name: &String, key: &Vec<u8>) -> Result<Option<&Vec<u8>>, String> {
        if let Some(changes_for_table) = self.changes_for_tables.get(table_name) {
            if let Some(result_from_changes) = changes_for_table.get(key) {
                return Ok(Some(result_from_changes));
            }
        }
        Ok(self
            .database_locked_internals
            .tables
            .get(table_name)
            .ok_or(format!("No table named '{table_name}'"))?
            .get(key))
    }

    fn commit(&mut self) -> Result<(), String> {
        self.database_locked_internals
            .log
            .write(&self.changes_for_tables)?;
        for (table_name, table_changes) in self.changes_for_tables.iter_mut() {
            self.database_locked_internals
                .tables
                .get_mut(table_name)
                .ok_or_else(|| format!("No table named '{table_name}'"))?
                .merge(table_changes);
        }
        Ok(())
    }
}

pub struct Database {
    lockable_internals: Arc<RwLock<DatabaseLockableInternals>>,
}

impl Database {
    pub fn new(config: DatabaseConfig) -> Result<Self, String> {
        let mut tables: HashMap<String, table::Table> = HashMap::new();
        for (table_name, table_config) in config.tables {
            tables.insert(table_name, table::Table::new(table_config)?);
        }
        Ok(Self {
            lockable_internals: Arc::new(RwLock::new(DatabaseLockableInternals {
                tables,
                log: Log::new(config.log)?,
            })),
        })
    }

    pub fn lock_all_writes_and_read(&self, f: fn(ReadTransaction) -> ()) -> Result<&Self, String> {
        f(ReadTransaction::new(&Arc::clone(&self.lockable_internals))?);
        Ok(self)
    }

    pub fn lock_all_and_write(&self, f: fn(WriteTransaction) -> ()) -> Result<&Self, String> {
        f(WriteTransaction::new(&Arc::clone(
            &self.lockable_internals,
        ))?);
        Ok(self)
    }

    pub fn lock_all_and_clear(&self) -> Result<&Self, String> {
        let mut locked_internals = self
            .lockable_internals
            .write()
            .map_err(|error| format!("Can not acquire write lock on database: {error}"))?;

        for (_, table) in locked_internals.tables.iter_mut() {
            table.clear()?;
        }
        locked_internals.log.clear()?;
        Ok(self)
    }

    pub fn lock_all_writes_and_spawn_read(
        &self,
        f: fn(ReadTransaction) -> (),
    ) -> JoinHandle<Result<(), String>> {
        let locked_internals = Arc::clone(&self.lockable_internals);
        thread::spawn(move || Ok(f(ReadTransaction::new(&locked_internals)?)))
    }

    pub fn lock_all_and_spawn_write(
        &self,
        f: fn(WriteTransaction) -> (),
    ) -> JoinHandle<Result<(), String>> {
        let locked_internals = Arc::clone(&self.lockable_internals);
        thread::spawn(move || Ok(f(WriteTransaction::new(&locked_internals)?)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        index::IndexConfig, table::TableConfig, variable_data_pool::VariableDataPoolConfig,
    };
    use std::path::Path;

    #[test]
    fn test_transactions_concurrency() {
        const THREADS_COUNT: usize = 10;
        const INCREMENTS_PER_THREAD_COUNT: usize = 100_000;
        const FINAL_VALUE: usize = THREADS_COUNT * INCREMENTS_PER_THREAD_COUNT;
        const TABLE_NAME: &str = "test";

        let database = Database::new(DatabaseConfig {
            tables: HashMap::from_iter([(
                TABLE_NAME.to_string(),
                TableConfig {
                    index: IndexConfig {
                        path: Path::new("/tmp/lawn/test/database/0/index.idx").to_path_buf(),
                        container_size: 4 as u8,
                    },
                    data_pool: Box::new(VariableDataPoolConfig {
                        directory: Path::new("/tmp/lawn/test/database/0/data_pool").to_path_buf(),
                        max_element_size: 65536 as usize,
                    }),
                },
            )]),
            log: LogConfig {
                path: Path::new("/tmp/lawn/test/database/log.dat").to_path_buf(),
            },
        })
        .unwrap();

        database
            .lock_all_and_clear()
            .unwrap()
            .lock_all_and_write(|mut transaction| {
                transaction
                    .set(
                        &TABLE_NAME.to_string(),
                        "key".as_bytes().to_vec(),
                        (0 as usize).to_le_bytes().to_vec(),
                    )
                    .unwrap()
                    .commit()
                    .unwrap();
            })
            .unwrap();

        let threads_handles: Vec<JoinHandle<Result<(), String>>> = (0..THREADS_COUNT)
            .map(|_| {
                database.lock_all_and_spawn_write(|mut transaction| {
                    let key = "key".as_bytes().to_vec();
                    for _ in 0..INCREMENTS_PER_THREAD_COUNT {
                        transaction
                            .set(
                                &TABLE_NAME.to_string(),
                                key.clone(),
                                (usize::from_le_bytes(
                                    transaction
                                        .get(&TABLE_NAME.to_string(), &key)
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
                })
            })
            .collect();
        for thread_handle in threads_handles {
            thread_handle.join().unwrap().unwrap();
        }

        database
            .lock_all_writes_and_read(|transaction| {
                assert_eq!(
                    transaction
                        .get(&TABLE_NAME.to_string(), &"key".as_bytes().to_vec())
                        .unwrap()
                        .unwrap(),
                    &FINAL_VALUE.to_le_bytes().to_vec()
                );
            })
            .unwrap();
    }
}
