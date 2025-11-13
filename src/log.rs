use bincode;
use std::io::{BufWriter, Write};
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
    table_index: usize,
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
        changes_for_tables: &Vec<BTreeMap<Vec<u8>, Vec<u8>>>,
    ) -> Result<(), String> {
        let buffer = bincode::encode_to_vec(
            TransactionRecord {
                tables_changes: changes_for_tables
                    .iter()
                    .enumerate()
                    .map(|(table_index, table_changes)| TableChangeRecord {
                        table_index,
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
        self.file.write_all(&buffer.as_slice());
        Ok(())
    }

    pub fn clear(&mut self) -> Result<(), String> {
        self.file
            .set_len(0)
            .map_err(|error| format!("Can not truncate file {:?}: {error}", self.file))?;
        Ok(())
    }
}
