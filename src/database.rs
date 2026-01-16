pub extern crate anyhow;
pub extern crate fallible_iterator;
pub extern crate parking_lot;
pub extern crate serde;

#[macro_export]
macro_rules! define_database {
    ($database_name:ident {
        $($table_name:ident<$key_type:ty, $value_type:ty>),+ $(,)?
    }
    use { $($use_item:item)* }) => {
        #[allow(dead_code)]
        pub mod $database_name {
            $( $use_item )*

            use std::ops::Bound::{Included, Unbounded};
            use std::path::PathBuf;
            use std::io::{BufReader, BufRead, Write};
            use std::fs;
            use std::{
                collections::BTreeMap,
                sync::Arc,
                thread::{self, JoinHandle},
            };

            use $crate::database::anyhow::{Context, Result, Error};
            use $crate::database::fallible_iterator::FallibleIterator;
            use $crate::database::parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

            use $crate::database::serde::{Deserialize, Serialize};

            use $crate::keyvalue::{Key, Value};
            use $crate::merging_iterator::MergingIterator;
            use $crate::table;

            #[derive(Serialize, Deserialize, Debug, Clone)]
            pub struct LogConfig {
                pub path: PathBuf,
            }

            #[derive(Debug)]
            pub struct Log {
                pub config: LogConfig,
                file: fs::File,
            }

            #[derive(bincode::Encode, bincode::Decode, Debug, Clone)]
            struct LogRecord {
                $(
                    $table_name: Vec<($key_type, Option<$value_type>)>,
                )+
            }

            impl Log {
                fn new(config: LogConfig) -> Result<Self> {
                    if let Some(path_parent_directory) = config.path.parent() {
                        std::fs::create_dir_all(path_parent_directory).with_context(|| {
                            format!(
                                "Can not create parent directories for path {}",
                                &config.path.display()
                            )
                        })?;
                    }
                    let file = fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .write(true)
                        .open(&config.path)
                        .with_context(|| {
                            format!(
                                "Can not open file at path {}",
                                config.path.display()
                            )
                        })?;
                    Ok(Self { config, file })
                }

                fn write(&mut self, record: LogRecord) -> Result<()> {
                    let buffer = bincode::encode_to_vec(record, bincode::config::standard())
                        .with_context(|| format!("Can not encode transaction to log record"))?;
                    self.file
                        .write_all(&buffer.as_slice())
                        .with_context(|| format!("Can not write transaction log record to file"))?;
                    Ok(())
                }

                fn clear(&mut self) -> Result<()> {
                    self.file
                        .set_len(0)
                        .with_context(|| format!("Can not truncate log file {:?}", self.file))?;
                    Ok(())
                }

                fn iter(&mut self) -> Result<LogIterator> {
                    Ok(
                        LogIterator {
                            reader: BufReader::new(
                                fs::File::open(&self.config.path).with_context(|| {
                                    format!(
                                        "Can not open file at path {} for read",
                                        &self.config.path.display()
                                    )
                                })?
                            )
                        }
                    )
                }
            }

            #[derive(Debug)]
            struct LogIterator {
                reader: BufReader<fs::File>,
            }
            impl FallibleIterator for LogIterator {
                type Item = LogRecord;
                type Error = Error;

                fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
                    Ok(
                        if self.reader
                               .fill_buf()
                                      .with_context(|| format!("Can not read log"))?.is_empty() {
                            None
                        } else {
                            Some(bincode::decode_from_std_read(&mut self.reader, bincode::config::standard())
                                    .with_context(|| format!("Can not decode log record"))?)
                        }
                    )
                }
            }

            #[derive(Serialize, Deserialize, Debug, Clone)]
            pub struct TablesConfig {
                $(
                    pub $table_name: table::TableConfig<$key_type, $value_type>,
                )+
            }

            #[derive(Serialize, Deserialize, Debug, Clone)]
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

                pub fn get(&self, key: &K) -> Result<Option<V>> {
                    match self.changes.get(key) {
                        Some(result_from_changes) => Ok(result_from_changes.clone()),
                        None => self.table.get(key),
                    }
                }

                pub fn iter(
                    &'_ self,
                    from_key: Option<&K>,
                ) -> Result<Box<dyn FallibleIterator<Item = (K, V), Error = Error> + '_>>
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
                            .iter(from_key).with_context(|| format!("Can not initiate iteration over table starting from key {from_key:?}"))?,
                    ).with_context(|| "Can not initiate merging-with-uncommitted-changes iteration over table starting from key {from_key:?}")?))
                }
            }

        pub struct TablesTransactions {
            $(
                pub $table_name: TableTransaction<$key_type, $value_type>,
            )+
        }

        struct DatabaseLockableInternals {
            tables: TablesTransactions,
            log: Log,
        }

        pub struct ReadTransaction<'a> {
            database_locked_internals: RwLockReadGuard<'a, DatabaseLockableInternals>,
        }
        impl<'a> ReadTransaction<'a> {
            fn new(database_lock: &'a RwLock<DatabaseLockableInternals>) -> Result<Self> {
                Ok(Self {
                    database_locked_internals: database_lock
                        .read()
                })
            }
        }

        pub struct WriteTransaction<'a> {
            database_locked_internals: RwLockWriteGuard<'a, DatabaseLockableInternals>,
        }
        impl<'a> WriteTransaction<'a> {
            fn new(database_lock: &'a RwLock<DatabaseLockableInternals>) -> Result<Self> {
                Ok(Self {
                    database_locked_internals: database_lock
                        .write()
                })
            }

            pub fn commit(&mut self) -> Result<()> {
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
                    .write(log_record.clone()).with_context(|| format!("Can not write log record {log_record:?} to database while committing write transaction"))?;
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
            pub fn new(config: DatabaseConfig) -> Result<Self> {
                Ok(Self {
                    lockable_internals: Arc::new(RwLock::new(DatabaseLockableInternals {
                        tables: TablesTransactions {
                            $(
                                $table_name: TableTransaction {
                                    table: table::Table::<$key_type, $value_type>::new(config.tables.$table_name.clone()).with_context(|| format!("Can not create new table {:?} from config {:?}", stringify!($table_name), config.tables.$table_name))?,
                                    changes: BTreeMap::new(),
                                },
                            )+
                        },
                        log: Log::new(config.log.clone()).with_context(|| format!("Can not create new log from config {:?}", config.log))?,
                    })),
                })
            }

            pub fn lock_all_writes_and_read<F>(&self, mut f: F) -> Result<&Self>
            where
                F: FnMut(ReadTransaction) -> Result<()>,
            {
                f(ReadTransaction::new(&Arc::clone(&self.lockable_internals)).with_context(|| "Can not create new read transaction from lockable internals of database")?).with_context(|| "Can not execute user-provided function for read transaction")?;
                Ok(self)
            }

            pub fn lock_all_and_write<F>(&self, mut f: F) -> Result<&Self>
            where
                F: FnMut(&mut WriteTransaction) -> Result<()>,
            {
                let cloned_internals = &Arc::clone(&self.lockable_internals);
                let mut transaction = WriteTransaction::new(cloned_internals).with_context(|| "Can not create new write transaction from arc-cloned internals of database")?;
                f(&mut transaction).with_context(|| "Can not execute user-provided function for read transaction")?;
                transaction.commit().with_context(|| "Can not commit write transaction to database")?;
                Ok(self)
            }

            pub fn lock_all_and_recover(&self) -> Result<&Self> {
                let mut locked_internals = self
                    .lockable_internals
                    .write();

                locked_internals.log.iter().with_context(|| format!("Can not initiate iteration over log {:?} to recover database", locked_internals.log))?.for_each(|log_record| {
                    $(
                        for (key, value) in log_record.$table_name {
                            locked_internals.tables.$table_name.table.memtable.insert(key, value);
                        }
                    )+
                    Ok(())
                }).with_context(|| format!("Can not recover database from log {:?}", locked_internals.log))?;
                Ok(self)
            }

            pub fn lock_all_and_clear(&self) -> Result<&Self> {
                let mut locked_internals = self
                    .lockable_internals
                    .write();

                $(
                    locked_internals.tables.$table_name.table.clear().with_context(|| format!("Can not clear table {:?} while clearing database", stringify!($table_name)))?;
                )+
                locked_internals.log.clear().with_context(|| format!("Can not clear log {:?} while clearing database", locked_internals.log))?;
                Ok(self)
            }

            pub fn lock_all_and_checkpoint(&self) -> Result<&Self> {
                let mut locked_internals = self
                    .lockable_internals
                    .write();

                $(
                    locked_internals.tables.$table_name.table.checkpoint().with_context(|| format!("Can not checkpoint table {:?}", stringify!($table_name)))?;
                )+
                locked_internals.log.clear().with_context(|| format!("Can not clear log {:?} while checkpointing database", locked_internals.log))?;
                Ok(self)
            }

            pub fn lock_all_writes_and_spawn_read(
                &self,
                f: fn(ReadTransaction) -> Result<()>,
            ) -> JoinHandle<Result<()>> {
                let locked_internals = Arc::clone(&self.lockable_internals);
                thread::spawn(move || f(ReadTransaction::new(&locked_internals).with_context(|| "Can not create new read transaction from database locked internals")?))
            }

            pub fn lock_all_and_spawn_write(
                &self,
                f: fn(WriteTransaction) -> Result<()>,
            ) -> JoinHandle<Result<()>> {
                let locked_internals = Arc::clone(&self.lockable_internals);
                thread::spawn(move || f(WriteTransaction::new(&locked_internals).with_context(|| "Can not create new write transaction from database locked internals")?))
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

#[cfg(test)]
mod tests {
    use super::*;

    use nanorand::{Rng, WyRand};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use pretty_assertions::assert_eq;

    #[derive(bincode::Encode, bincode::Decode, Clone, Debug, PartialEq)]
    pub struct Data {
        data: Vec<u8>,
    }

    define_database!(test_database {
        vecs<Vec<u8>, Data>,
        count<(), usize>
    } use {
        use super::Data;
    });

    fn new_default_database(test_name_for_isolation: &str) -> test_database::Database {
        test_database::Database::new(
            serde_saphyr::from_str(
                &std::fs::read_to_string("src/test_database_config.yml")
                    .unwrap()
                    .replace("TEST_NAME", test_name_for_isolation),
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn test_generative() {
        let database = new_default_database("test_generative");
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
                            .lock_all_and_write(|transaction| {
                                transaction.vecs.insert(key.clone(), value.clone());
                                Ok(())
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
                            .lock_all_and_write(|transaction| {
                                transaction.vecs.remove(&key_to_remove);
                                Ok(())
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
                    Ok(())
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

        let database = new_default_database("test_transactions_concurrency");
        database
            .lock_all_and_clear()
            .unwrap()
            .lock_all_and_write(|transaction| {
                transaction.count.insert((), 0);
                Ok(())
            })
            .unwrap();

        let threads_handles = (0..THREADS_COUNT)
            .map(|_| {
                database.lock_all_and_spawn_write(|mut transaction| {
                    for _ in 0..INCREMENTS_PER_THREAD_COUNT {
                        let current_value = transaction.count.get(&())?.unwrap();
                        transaction.count.insert((), current_value + 1);
                    }
                    Ok(())
                })
            })
            .collect::<Vec<_>>();
        for thread_handle in threads_handles {
            thread_handle.join().unwrap().unwrap();
        }

        database
            .lock_all_writes_and_read(|transaction| {
                assert_eq!(transaction.count.get(&()).unwrap().unwrap(), FINAL_VALUE);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn test_recover_from_log() {
        new_default_database("test_recover_from_log")
            .lock_all_and_clear()
            .unwrap()
            .lock_all_and_write(|transaction| {
                transaction.vecs.insert(
                    "key".as_bytes().to_vec(),
                    Data {
                        data: "value".as_bytes().to_vec(),
                    },
                );
                Ok(())
            })
            .unwrap();
        new_default_database("test_recover_from_log")
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
                Ok(())
            })
            .unwrap();
    }
}
