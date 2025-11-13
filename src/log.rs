use bincode;
use std::collections::HashMap;
use std::io::{BufReader, Write};
use std::path::PathBuf;
use std::{collections::BTreeMap, fs};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct LogConfig {
    pub path: PathBuf,
}

pub struct Log {
    pub config: LogConfig,
    file: fs::File,
}

#[derive(bincode::Encode, bincode::Decode)]
struct KeyValueChangeRecord {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

#[derive(bincode::Encode, bincode::Decode)]
struct TableChangeRecord {
    table_name: String,
    keyvalues_changes: Vec<KeyValueChangeRecord>,
}

#[derive(bincode::Encode, bincode::Decode)]
struct TransactionRecord {
    tables_changes: Vec<TableChangeRecord>,
}

impl Log {
    pub fn new(config: LogConfig) -> Result<Self, String> {
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

    pub fn write(
        &mut self,
        changes_for_tables: &HashMap<String, BTreeMap<Vec<u8>, Vec<u8>>>,
    ) -> Result<(), String> {
        let buffer = bincode::encode_to_vec(
            TransactionRecord {
                tables_changes: changes_for_tables
                    .iter()
                    .map(|(table_name, table_changes)| TableChangeRecord {
                        table_name: table_name.clone(),
                        keyvalues_changes: table_changes
                            .iter()
                            .map(|(key, value)| KeyValueChangeRecord {
                                key: key.clone(),
                                value: Some(value.clone()),
                            })
                            .collect(),
                    })
                    .collect(),
            },
            bincode::config::standard(),
        )
        .map_err(|error| format!("Can not encode transaction to log record: {error}"))?;
        self.file
            .write_all(&buffer.as_slice())
            .map_err(|error| format!("Can not write transaction log record to file: {error}"))?;
        Ok(())
    }

    pub fn read(&self) -> Result<HashMap<String, BTreeMap<Vec<u8>, Vec<u8>>>, String> {
        let mut result: HashMap<String, BTreeMap<Vec<u8>, Vec<u8>>> = HashMap::new();
        let file = fs::File::open(&self.config.path).map_err(|error| {
            format!(
                "Can not open file at path {} for read: {error}",
                &self.config.path.display()
            )
        })?;
        let mut reader = BufReader::new(file);
        loop {
            let transactcion_record: TransactionRecord =
                match bincode::decode_from_std_read(&mut reader, bincode::config::standard()) {
                    Ok(transaction_record) => transaction_record,
                    Err(_) => break,
                };
            for table_changes_record in transactcion_record.tables_changes {
                result
                    .entry(table_changes_record.table_name.clone())
                    .and_modify(|changes_for_table| {
                        for keyvalue_change in table_changes_record.keyvalues_changes.iter() {
                            changes_for_table.insert(
                                keyvalue_change.key.clone(),
                                keyvalue_change.value.clone().unwrap(),
                            );
                        }
                    })
                    .or_insert_with(|| {
                        BTreeMap::from_iter(table_changes_record.keyvalues_changes.iter().map(
                            |keyvalue_change| {
                                (
                                    keyvalue_change.key.clone(),
                                    keyvalue_change.value.clone().unwrap(),
                                )
                            },
                        ))
                    });
            }
        }
        Ok(result)
    }

    pub fn clear(&mut self) -> Result<(), String> {
        self.file
            .set_len(0)
            .map_err(|error| format!("Can not truncate file {:?}: {error}", self.file))?;
        Ok(())
    }
}
