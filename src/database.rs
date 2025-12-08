#[macro_export]
macro_rules! define_database {
    ($database_name:ident {
        $($table_name:ident<$key_type:ty, $value_type:ty>),+ $(,)?
    }
    use { $($use_item:item),* $(,)? }) => {
        #[allow(dead_code)]
        mod $database_name {
            $( $use_item )*

            use std::ops::Bound::{Included, Unbounded};
            use std::path::PathBuf;
            use std::io::{BufReader, BufRead, Write};
            use std::fs;
            use std::{
                collections::BTreeMap,
                sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
                thread::{self, JoinHandle},
            };

            use fallible_iterator::FallibleIterator;

            #[cfg(feature = "serde")]
            use serde::{Deserialize, Serialize};

            use $crate::keyvalue::{Key, Value};
            use $crate::merging_iterator::MergingIterator;
            use $crate::table;

            #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
            pub struct LogConfig {
                pub path: PathBuf,
            }

            pub struct Log {
                pub config: LogConfig,
                file: fs::File,
            }

            #[derive(bincode::Encode, bincode::Decode)]
            struct LogRecord {
                $(
                    $table_name: Vec<($key_type, Option<$value_type>)>,
                )+
            }

            impl Log {
                fn new(config: LogConfig) -> Result<Self, String> {
                    if let Some(path_parent_directory) = config.path.parent() {
                        std::fs::create_dir_all(path_parent_directory).map_err(|error| {
                            format!(
                                "Can not create parent directories for path {}: {error}",
                                &config.path.display()
                            )
                        })?;
                    }
                    let file = fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .write(true)
                        .open(&config.path)
                        .map_err(|error| {
                            format!(
                                "Can not open file at path {}: {error}",
                                config.path.display()
                            )
                        })?;
                    Ok(Self { config, file })
                }

                fn write(&mut self, record: LogRecord) -> Result<(), String> {
                    let buffer = bincode::encode_to_vec(record, bincode::config::standard())
                        .map_err(|error| format!("Can not encode transaction to log record: {error}"))?;
                    self.file
                        .write_all(&buffer.as_slice())
                        .map_err(|error| format!("Can not write transaction log record to file: {error}"))?;
                    Ok(())
                }

                fn clear(&mut self) -> Result<(), String> {
                    self.file
                        .set_len(0)
                        .map_err(|error| format!("Can not truncate log file {:?}: {error}", self.file))?;
                    Ok(())
                }

                fn iter(&mut self) -> Result<LogIterator, String> {
                    Ok(
                        LogIterator {
                            reader: BufReader::new(
                                fs::File::open(&self.config.path).map_err(|error| {
                                    format!(
                                        "Can not open file at path {} for read: {error}",
                                        &self.config.path.display()
                                    )
                                })?
                            )
                        }
                    )
                }
            }

            struct LogIterator {
                reader: BufReader<fs::File>,
            }
            impl FallibleIterator for LogIterator {
                type Item = LogRecord;
                type Error = String;

                fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
                    Ok(
                        if self.reader
                               .fill_buf()
                                      .map_err(|error| format!("Can not read log: {error}"))?.is_empty() {
                            None
                        } else {
                            Some(bincode::decode_from_std_read(&mut self.reader, bincode::config::standard())
                                    .map_err(|error| format!("Can not decode log record: {error}"))?)
                        }
                    )
                }
            }

            pub struct TablesConfig {
                $(
                    pub $table_name: table::TableConfig<$key_type, $value_type>,
                )+
            }

            #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
            pub struct DatabaseConfig {
                pub tables: TablesConfig,
                pub log: LogConfig,
            }

            pub struct TableTransaction<K: Key, V: Value>{
                table: table::Table<K, V>,
                changes: BTreeMap<K, Option<V>>,
            }
            impl<K: Key, V: Value> TableTransaction<K, V> {
                pub fn insert(&mut self, key: K, value: V) -> &mut Self {
                    self.changes.insert(key, Some(value));
                    self
                }

                pub fn remove(&mut self, key: &K) -> &mut Self {
                    self.changes.remove(key);
                    self
                }

                pub fn get(&self, key: &K) -> Result<Option<V>, String> {
                    match self.changes.get(key) {
                        Some(result_from_changes) => Ok(result_from_changes.clone()),
                        None => self.table.get(key),
                    }
                }

                pub fn iter(
                    &'_ self,
                    from_key: Option<&K>,
                ) -> Result<Box<dyn FallibleIterator<Item = (K, V), Error = String> + '_>, String>
                {
                    Ok(Box::new(MergingIterator::new(
                        self.changes
                            .range::<K, _>((
                                (if let Some(from_key) = from_key {
                                    Included(from_key)
                                } else {
                                    Unbounded
                                }),
                                Unbounded,
                            )),
                        self.table
                            .iter(from_key)?,
                    )?))
                }
            }

        pub struct TablesTransactions {
            $(
                pub $table_name: TableTransaction<$key_type, $value_type>,
            )+
        }

        pub struct DatabaseLockableInternals {
            pub tables: TablesTransactions,
            log: Log,
        }

        pub struct ReadTransaction<'a> {
            pub database_locked_internals: RwLockReadGuard<'a, DatabaseLockableInternals>,
        }
        impl<'a> ReadTransaction<'a> {
            fn new(database_lock: &'a RwLock<DatabaseLockableInternals>) -> Result<Self, String> {
                Ok(Self {
                    database_locked_internals: database_lock
                        .read()
                        .map_err(|error| format!("Can not acquire read lock on database: {error}"))?,
                })
            }
        }

        pub struct WriteTransaction<'a> {
            pub database_locked_internals: RwLockWriteGuard<'a, DatabaseLockableInternals>,
        }
        impl<'a> WriteTransaction<'a> {
            fn new(database_lock: &'a RwLock<DatabaseLockableInternals>) -> Result<Self, String> {
                Ok(Self {
                    database_locked_internals: database_lock
                        .write()
                        .map_err(|error| format!("Can not acquire write lock on database: {error}"))?,
                })
            }

            pub fn commit(&mut self) -> Result<(), String> {
                let log_record = LogRecord {
                    $(
                        $table_name: self.database_locked_internals
                                            .tables
                                            .$table_name
                                            .changes
                                            .iter()
                                            .map(|(key, value)| (key.clone(), value.clone()))
                                            .collect(),
                    )+
                };
                self.database_locked_internals
                    .log
                    .write(log_record)?;
                $({
                    let mut table_changes = std::mem::take(&mut self.database_locked_internals
                                                                    .tables
                                                                    .$table_name.changes);
                    self.database_locked_internals
                        .tables
                        .$table_name
                        .table
                        .merge(&mut table_changes);
                })+
                Ok(())
            }
        }

        impl<'a> std::ops::Deref for WriteTransaction<'a> {
            type Target = TablesTransactions;

            fn deref(&self) -> &Self::Target {
                &self.database_locked_internals.tables
            }
        }

        impl<'a> std::ops::DerefMut for WriteTransaction<'a> {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.database_locked_internals.tables
            }
        }

        impl<'a> std::ops::Deref for ReadTransaction<'a> {
            type Target = TablesTransactions;

            fn deref(&self) -> &Self::Target {
                &self.database_locked_internals.tables
            }
        }

        pub struct Database {
            lockable_internals: Arc<RwLock<DatabaseLockableInternals>>,
        }

        impl Database {
            pub fn new(config: DatabaseConfig) -> Result<Self, String> {
                Ok(Self {
                    lockable_internals: Arc::new(RwLock::new(DatabaseLockableInternals {
                        tables: TablesTransactions {
                            $(
                                $table_name: TableTransaction {
                                    table: table::Table::<$key_type, $value_type>::new(config.tables.$table_name)?,
                                    changes: BTreeMap::new(),
                                },
                            )+
                        },
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

                locked_internals.log.iter()?.for_each(|log_record| {
                    $(
                        for (key, value) in log_record.$table_name {
                            locked_internals.tables.$table_name.table.memtable.insert(key, value);
                        }
                    )+
                    Ok(())
                })?;
                Ok(self)
            }

            pub fn lock_all_and_clear(&self) -> Result<&Self, String> {
                let mut locked_internals = self
                    .lockable_internals
                    .write()
                    .map_err(|error| format!("Can not acquire write lock on database: {error}"))?;

                $(
                    locked_internals.tables.$table_name.table.clear()?;
                )+
                locked_internals.log.clear()?;
                Ok(self)
            }

            pub fn lock_all_and_checkpoint(&self) -> Result<&Self, String> {
                let mut locked_internals = self
                    .lockable_internals
                    .write()
                    .map_err(|error| format!("Can not acquire write lock on database: {error}"))?;

                $(
                    locked_internals.tables.$table_name.table.checkpoint()?;
                )+
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
        }
    };

    ($database_name:ident { $($table_name:ident<$key_type:ty, $value_type:ty>),+ $(,)? }) => {
        define_database!($database_name {
            $($table_name<$key_type, $value_type>),+
        } use {});
    };
}

pub use crate::define_database;

#[derive(bincode::Encode, bincode::Decode, Clone, Debug, PartialEq)]
struct Data {
    data: Vec<u8>,
}

define_database!(test_database {
    vecs<Vec<u8>, Data>,
    count<(), usize>
} use {
    use super::Data;
});

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        index::IndexConfig, table::TableConfig, variable_data_pool::VariableDataPoolConfig,
    };
    use nanorand::{Rng, WyRand};
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::sync::Arc;

    use pretty_assertions::assert_eq;

    fn new_default_database(test_name_for_isolation: String) -> test_database::Database {
        let database_dir =
            Path::new(format!("/tmp/lawn/test/{test_name_for_isolation}").as_str()).to_path_buf();
        test_database::Database::new(test_database::DatabaseConfig {
            tables: test_database::TablesConfig {
                vecs: TableConfig {
                    index: IndexConfig {
                        path: database_dir
                            .join("tables")
                            .join("vecs")
                            .join("index.idx")
                            .to_path_buf(),
                    },
                    data_pool: Box::new(VariableDataPoolConfig {
                        directory: database_dir
                            .join("tables")
                            .join("vecs")
                            .join("data_pool")
                            .to_path_buf(),
                        max_element_size: 65536 as usize,
                    }),
                },
                count: TableConfig {
                    index: IndexConfig {
                        path: database_dir
                            .join("tables")
                            .join("count")
                            .join("index.idx")
                            .to_path_buf(),
                    },
                    data_pool: Box::new(VariableDataPoolConfig {
                        directory: database_dir
                            .join("tables")
                            .join("count")
                            .join("data_pool")
                            .to_path_buf(),
                        max_element_size: 65536 as usize,
                    }),
                },
            },
            log: test_database::LogConfig {
                path: database_dir.join("log.dat").to_path_buf(),
            },
        })
        .unwrap()
    }

    #[test]
    fn test_generative() {
        let database = new_default_database("test_generative".to_string());
        database.lock_all_and_clear().unwrap();

        let mut previously_added_keyvalues: BTreeMap<Vec<u8>, Data> = BTreeMap::new();
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
                        let value = {
                            let mut result = Data {
                                data: vec![0u8; rng.generate_range(1..2)],
                            };
                            rng.fill(&mut result.data);
                            result
                        };
                        previously_added_keyvalues.insert(key.clone(), value.clone());
                        database
                            .lock_all_and_write(|mut transaction| {
                                transaction.vecs.insert(key.clone(), value.clone());
                                transaction.commit().unwrap();
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
                                transaction.vecs.remove(&key_to_remove);
                                transaction.commit().unwrap();
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
                            transaction.vecs.get(&key).unwrap().clone(),
                            Some(value.clone())
                        );
                    }
                })
                .unwrap();
            database.lock_all_and_checkpoint().unwrap();
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
                transaction.count.insert((), 0);
                transaction.commit().unwrap();
            })
            .unwrap();

        let threads_handles = (0..THREADS_COUNT)
            .map(|_| {
                database.lock_all_and_spawn_write(|mut transaction| {
                    for _ in 0..INCREMENTS_PER_THREAD_COUNT {
                        let current_value = transaction.count.get(&()).unwrap().unwrap();
                        transaction.count.insert((), current_value + 1);
                        transaction.commit().unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for thread_handle in threads_handles {
            thread_handle.join().unwrap().unwrap();
        }

        database
            .lock_all_writes_and_read(|transaction| {
                assert_eq!(transaction.count.get(&()).unwrap().unwrap(), FINAL_VALUE);
            })
            .unwrap();
    }

    #[test]
    fn test_recover_from_log() {
        new_default_database("test_recover_from_log".to_string())
            .lock_all_and_clear()
            .unwrap()
            .lock_all_and_write(|mut transaction| {
                transaction.vecs.insert(
                    "key".as_bytes().to_vec(),
                    Data {
                        data: "value".as_bytes().to_vec(),
                    },
                );
                transaction.commit().unwrap();
            })
            .unwrap();
        new_default_database("test_recover_from_log".to_string())
            .lock_all_and_recover()
            .unwrap()
            .lock_all_writes_and_read(|transaction| {
                assert_eq!(
                    transaction
                        .vecs
                        .get(&"key".as_bytes().to_vec())
                        .unwrap()
                        .clone()
                        .unwrap(),
                    Data {
                        data: "value".as_bytes().to_vec()
                    }
                );
            })
            .unwrap();
    }
}
