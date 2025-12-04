use std::ops::Bound::{Included, Unbounded};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
    thread::{self, JoinHandle},
};

use fallible_iterator::FallibleIterator;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::keyvalue::{Key, Value};
use crate::log::{Log, LogConfig};
use crate::merging_iterator::MergingIterator;
use crate::table;

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct DatabaseConfig {
    pub tables: Vec<table::TableConfig<K, V>>,
    pub log: LogConfig,
}

struct DatabaseLockableInternals {
    tables: Vec<table::Table<K, V>>,
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

    pub fn get_raw(&self, table_index: usize, key: &Vec<u8>) -> Result<Option<Vec<u8>>, String> {
        match self.database_locked_internals.tables.get(table_index) {
            Some(table) => table.get(key),
            None => Ok(None),
        }
    }

    pub fn get<K, V>(&self, table_index: usize, key: &K) -> Result<Option<V>, String>
    where
        K: bincode::Encode,
        V: bincode::Decode<()>,
    {
        Ok(
            if let Some(encoded_value) = self.get_raw(
                table_index,
                &bincode::encode_to_vec(key, bincode::config::standard())
                    .map_err(|error| format!("Can not encode key into bytes: {error}"))?,
            )? {
                Some(
                    bincode::decode_from_slice::<V, _>(&encoded_value, bincode::config::standard())
                        .map_err(|error| format!("Can not decode value from bytes: {error}"))?
                        .0,
                )
            } else {
                None
            },
        )
    }

    pub fn iter_raw(
        &'_ self,
        table_index: usize,
        from_key: Option<&Vec<u8>>,
    ) -> Result<Box<dyn FallibleIterator<Item = (Vec<u8>, Vec<u8>), Error = String> + '_>, String>
    {
        Ok(self
            .database_locked_internals
            .tables
            .get(table_index)
            .ok_or(format!("No table at index {table_index}"))?
            .iter(from_key)?)
    }

    pub fn iter<K, V>(
        &'_ self,
        table_index: usize,
        from_key: Option<&K>,
    ) -> Result<Box<dyn FallibleIterator<Item = (K, V), Error = String> + '_>, String>
    where
        K: bincode::Decode<()> + bincode::Encode + Clone,
        V: bincode::Decode<()>,
    {
        Ok(Box::new(
            self.iter_raw(
                table_index,
                if let Some(from_key) = from_key {
                    Some(
                        bincode::encode_to_vec(from_key, bincode::config::standard())
                            .map_err(|error| format!("Can not encode key into bytes: {error}"))?,
                    )
                } else {
                    None
                }
                .as_ref(),
            )?
            .map(|(encoded_key, encoded_value)| {
                Ok((
                    bincode::decode_from_slice::<K, _>(&encoded_key, bincode::config::standard())
                        .map_err(|error| format!("Can not decode key from bytes: {error}"))?
                        .0,
                    bincode::decode_from_slice::<V, _>(&encoded_value, bincode::config::standard())
                        .map_err(|error| format!("Can not decode value from bytes: {error}"))?
                        .0,
                ))
            }),
        ))
    }
}

pub struct WriteTransaction<'a> {
    database_locked_internals: RwLockWriteGuard<'a, DatabaseLockableInternals>,
    changes_for_tables: Vec<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
}

impl<'a> WriteTransaction<'a> {
    fn new(database_lock: &'a RwLock<DatabaseLockableInternals>) -> Result<Self, String> {
        let locked_internals = database_lock
            .write()
            .map_err(|error| format!("Can not acquire write lock on database: {error}"))?;
        let tables_count = locked_internals.tables.len();
        Ok(Self {
            database_locked_internals: locked_internals,
            changes_for_tables: vec![const { BTreeMap::new() }; tables_count],
        })
    }

    pub fn insert_raw(
        &mut self,
        table_index: usize,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<&mut Self, String> {
        self.changes_for_tables
            .get_mut(table_index)
            .ok_or(format!("No table at index {table_index}"))?
            .insert(key, Some(value));
        Ok(self)
    }

    pub fn insert<K, V>(
        &mut self,
        table_index: usize,
        key: &K,
        value: &V,
    ) -> Result<&mut Self, String>
    where
        K: bincode::Encode,
        V: bincode::Encode,
    {
        self.insert_raw(
            table_index,
            bincode::encode_to_vec(key, bincode::config::standard())
                .map_err(|error| format!("Can not encode key into bytes: {error}"))?,
            bincode::encode_to_vec(value, bincode::config::standard())
                .map_err(|error| format!("Can not encode value into bytes: {error}"))?,
        )
    }

    pub fn remove_raw(&mut self, table_index: usize, key: Vec<u8>) -> Result<&mut Self, String> {
        self.changes_for_tables
            .get_mut(table_index)
            .ok_or(format!("No table at index {table_index}"))?
            .insert(key, None);
        Ok(self)
    }

    pub fn remove<K>(&mut self, table_index: usize, key: &K) -> Result<&mut Self, String>
    where
        K: bincode::Encode,
    {
        self.remove_raw(
            table_index,
            bincode::encode_to_vec(key, bincode::config::standard())
                .map_err(|error| format!("Can not encode key into bytes: {error}"))?,
        )
    }

    pub fn get_raw(&self, table_index: usize, key: &Vec<u8>) -> Result<Option<Vec<u8>>, String> {
        match self
            .changes_for_tables
            .get(table_index)
            .ok_or(format!("No table at index {table_index}"))?
            .get(key)
        {
            Some(result_from_changes) => Ok(result_from_changes.clone()),
            None => match self.database_locked_internals.tables.get(table_index) {
                Some(table) => table.get(key),
                None => Ok(None),
            },
        }
    }

    pub fn get<K, V>(&self, table_index: usize, key: &K) -> Result<Option<V>, String>
    where
        K: bincode::Encode,
        V: bincode::Decode<()>,
    {
        Ok(
            if let Some(encoded_value) = self.get_raw(
                table_index,
                &bincode::encode_to_vec(key, bincode::config::standard())
                    .map_err(|error| format!("Can not encode key into bytes: {error}"))?,
            )? {
                Some(
                    bincode::decode_from_slice::<V, _>(&encoded_value, bincode::config::standard())
                        .map_err(|error| format!("Can not decode value from bytes: {error}"))?
                        .0,
                )
            } else {
                None
            },
        )
    }

    pub fn commit(&mut self) -> Result<(), String> {
        self.database_locked_internals
            .log
            .write(&self.changes_for_tables)?;
        for (table_index, table_changes) in self.changes_for_tables.iter_mut().enumerate() {
            self.database_locked_internals.tables[table_index].merge(table_changes);
        }
        Ok(())
    }

    pub fn iter_raw(
        &'_ self,
        table_index: usize,
        from_key: Option<&Vec<u8>>,
    ) -> Result<Box<dyn FallibleIterator<Item = (Vec<u8>, Vec<u8>), Error = String> + '_>, String>
    {
        Ok(Box::new(MergingIterator::new(
            self.changes_for_tables
                .get(table_index)
                .ok_or(format!("No table at index {table_index}"))?
                .range::<Vec<u8>, _>((
                    (if let Some(from_key) = from_key {
                        Included(from_key)
                    } else {
                        Unbounded
                    }),
                    Unbounded,
                )),
            self.database_locked_internals
                .tables
                .get(table_index)
                .ok_or(format!("No table at index {table_index}"))?
                .iter(from_key)?,
        )?))
    }

    pub fn iter<K, V>(
        &'_ self,
        table_index: usize,
        from_key: Option<&K>,
    ) -> Result<Box<dyn FallibleIterator<Item = (K, V), Error = String> + '_>, String>
    where
        K: bincode::Decode<()> + bincode::Encode + Clone,
        V: bincode::Decode<()>,
    {
        Ok(Box::new(
            self.iter_raw(
                table_index,
                if let Some(from_key) = from_key {
                    Some(
                        bincode::encode_to_vec(from_key, bincode::config::standard())
                            .map_err(|error| format!("Can not encode key into bytes: {error}"))?,
                    )
                } else {
                    None
                }
                .as_ref(),
            )?
            .map(|(encoded_key, encoded_value)| {
                Ok((
                    bincode::decode_from_slice::<K, _>(&encoded_key, bincode::config::standard())
                        .map_err(|error| format!("Can not decode key from bytes: {error}"))?
                        .0,
                    bincode::decode_from_slice::<V, _>(&encoded_value, bincode::config::standard())
                        .map_err(|error| format!("Can not decode value from bytes: {error}"))?
                        .0,
                ))
            }),
        ))
    }
}

pub struct Database {
    lockable_internals: Arc<RwLock<DatabaseLockableInternals>>,
}

impl Database {
    pub fn new(config: DatabaseConfig) -> Result<Self, String> {
        let mut tables: Vec<table::Table> = Vec::new();
        for table_config in config.tables {
            tables.push(table::Table::new(table_config)?);
        }
        Ok(Self {
            lockable_internals: Arc::new(RwLock::new(DatabaseLockableInternals {
                tables,
                log: Log::new(config.log)?,
            })),
        })
    }

    pub fn lock_all_writes_and_read<F>(&self, closure: F) -> Result<&Self, String>
    where
        F: Fn(ReadTransaction) -> (),
    {
        closure(ReadTransaction::new(&Arc::clone(&self.lockable_internals))?);
        Ok(self)
    }

    pub fn lock_all_and_write<F>(&self, f: F) -> Result<&Self, String>
    where
        F: Fn(WriteTransaction) -> (),
    {
        f(WriteTransaction::new(&Arc::clone(
            &self.lockable_internals,
        ))?);
        Ok(self)
    }

    pub fn lock_all_and_recover(&self) -> Result<&Self, String> {
        let mut locked_internals = self
            .lockable_internals
            .write()
            .map_err(|error| format!("Can not acquire write lock on database: {error}"))?;

        for (table_id, memtable) in locked_internals.log.read()?.into_iter().enumerate() {
            locked_internals
                .tables
                .get_mut(table_id)
                .ok_or_else(|| format!("No table with id {table_id}"))?
                .memtable = memtable;
        }
        Ok(self)
    }

    pub fn lock_all_and_clear(&self) -> Result<&Self, String> {
        let mut locked_internals = self
            .lockable_internals
            .write()
            .map_err(|error| format!("Can not acquire write lock on database: {error}"))?;

        for table in locked_internals.tables.iter_mut() {
            table.clear()?;
        }
        locked_internals.log.clear()?;
        Ok(self)
    }

    pub fn lock_all_and_checkpoint(&self, parallel: bool) -> Result<&Self, String> {
        let mut locked_internals = self
            .lockable_internals
            .write()
            .map_err(|error| format!("Can not acquire write lock on database: {error}"))?;

        if parallel {
            thread::scope(|s| {
                for result in locked_internals
                    .tables
                    .iter_mut()
                    .enumerate()
                    .map(|(table_index, table)| {
                        s.spawn(move || {
                            table.checkpoint().map_err(|error| {
                                format!(
                                    "Checkpoint of table with index {table_index:?} failed: {error}"
                                )
                            })
                        })
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(|handle| handle.join())
                    .collect::<Vec<_>>()
                {
                    match result {
                        Ok(_) => {}
                        Err(error_string) => {
                            if let Some(s) = error_string.downcast_ref::<&'static str>() {
                                return Err((*s).to_string());
                            } else if let Some(s) = error_string.downcast_ref::<String>() {
                                return Err(s.as_str().into());
                            } else {
                                return Err("Unknown error in checkpointing thread".into());
                            }
                        }
                    }
                }
                Ok(())
            })?;
        } else {
            for table in locked_internals.tables.iter_mut() {
                table.checkpoint()?;
            }
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
    use nanorand::{Rng, WyRand};
    use std::path::Path;
    use std::sync::Arc;

    use pretty_assertions::assert_eq;

    fn new_default_database(test_name_for_isolation: String) -> Database {
        let database_dir =
            Path::new(format!("/tmp/lawn/test/{test_name_for_isolation}").as_str()).to_path_buf();
        Database::new(DatabaseConfig {
            tables: vec![TableConfig {
                index: IndexConfig {
                    path: database_dir
                        .join("database")
                        .join("0")
                        .join("index.idx")
                        .to_path_buf(),
                },
                data_pool: Box::new(VariableDataPoolConfig {
                    directory: database_dir
                        .join("database")
                        .join("0")
                        .join("data_pool")
                        .to_path_buf(),
                    max_element_size: 65536 as usize,
                }),
            }],
            log: LogConfig {
                path: database_dir.join("log.dat").to_path_buf(),
            },
        })
        .unwrap()
    }

    #[test]
    fn test_generative() {
        let database = new_default_database("test_generative".to_string());
        database.lock_all_and_clear().unwrap();

        let mut previously_added_keyvalues: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut rng = WyRand::new_seed(0);

        for _ in 0..100 {
            for _ in 0..100 {
                let action_id = if previously_added_keyvalues.is_empty() {
                    1
                } else {
                    rng.generate_range(1..=2)
                };
                match action_id {
                    1 => {
                        let key: Vec<u8> = {
                            let mut result = vec![0u8; rng.generate_range(1..2)];
                            rng.fill(&mut result);
                            result
                        };
                        let value: Vec<u8> = {
                            let mut result = vec![0u8; rng.generate_range(1..2)];
                            rng.fill(&mut result);
                            result
                        };
                        previously_added_keyvalues.insert(key.clone(), value.clone());
                        database
                            .lock_all_and_write(|mut transaction| {
                                transaction
                                    .insert_raw(0, key.clone(), value.clone())
                                    .unwrap()
                                    .commit()
                                    .unwrap();
                            })
                            .unwrap();
                    }
                    2 => {
                        let key_to_remove: Vec<u8> = previously_added_keyvalues
                            .keys()
                            .nth(rng.generate_range(0..previously_added_keyvalues.len()))
                            .unwrap()
                            .clone();
                        previously_added_keyvalues.remove(&key_to_remove);
                        database
                            .lock_all_and_write(|mut transaction| {
                                transaction
                                    .remove_raw(0, key_to_remove.clone())
                                    .unwrap()
                                    .commit()
                                    .unwrap();
                            })
                            .unwrap();
                    }
                    _ => {}
                }
            }
            let previously_added_keyvalues_arc = Arc::new(&previously_added_keyvalues).clone();
            database
                .lock_all_writes_and_read(|transaction| {
                    for (key, value) in previously_added_keyvalues_arc.iter() {
                        assert_eq!(
                            transaction.get_raw(0, &key).unwrap().clone(),
                            Some(value.clone())
                        );
                    }
                })
                .unwrap();
            database.lock_all_and_checkpoint(true).unwrap();
        }
    }

    #[test]
    fn test_transactions_concurrency() {
        const THREADS_COUNT: usize = 10;
        const INCREMENTS_PER_THREAD_COUNT: usize = 1000;
        const FINAL_VALUE: usize = THREADS_COUNT * INCREMENTS_PER_THREAD_COUNT;

        let database = new_default_database("test_transactions_concurrency".to_string());
        database
            .lock_all_and_clear()
            .unwrap()
            .lock_all_and_write(|mut transaction| {
                transaction
                    .insert(0 as usize, &"key".to_string(), &(0 as usize))
                    .unwrap()
                    .commit()
                    .unwrap();
            })
            .unwrap();

        let threads_handles: Vec<JoinHandle<Result<(), String>>> = (0..THREADS_COUNT)
            .map(|_| {
                database.lock_all_and_spawn_write(|mut transaction| {
                    for _ in 0..INCREMENTS_PER_THREAD_COUNT {
                        transaction
                            .insert(
                                0 as usize,
                                &"key".to_string(),
                                &(transaction
                                    .get::<String, usize>(0, &"key".to_string())
                                    .unwrap()
                                    .unwrap()
                                    + 1),
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
                        .get::<String, usize>(0, &"key".to_string())
                        .unwrap()
                        .unwrap(),
                    FINAL_VALUE
                );
            })
            .unwrap();
    }

    #[test]
    fn test_recover_from_log() {
        new_default_database("test_recover_from_log".to_string())
            .lock_all_and_clear()
            .unwrap()
            .lock_all_and_write(|mut transaction| {
                transaction
                    .insert_raw(0, "key".as_bytes().to_vec(), "value".as_bytes().to_vec())
                    .unwrap()
                    .commit()
                    .unwrap();
            })
            .unwrap();
        new_default_database("test_recover_from_log".to_string())
            .lock_all_and_recover()
            .unwrap()
            .lock_all_writes_and_read(|transaction| {
                assert_eq!(
                    transaction
                        .get_raw(0, &"key".as_bytes().to_vec())
                        .unwrap()
                        .clone()
                        .unwrap(),
                    "value".as_bytes().to_vec()
                );
            })
            .unwrap();
    }
}
