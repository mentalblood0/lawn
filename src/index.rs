use std::io::{BufReader, BufWriter, Seek};
use std::path::PathBuf;
use std::{fs, os::unix::fs::FileExt};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::variable_data_pool;

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct IndexConfig {
    pub path: PathBuf,
}

#[derive(bincode::Encode, bincode::Decode)]
struct IndexHeader {
    record_size: u8,
}

pub struct Index {
    pub config: IndexConfig,
    file: fs::File,
    header: IndexHeader,
    header_size: usize,
    records_count: u64,
    bytes_on_disk: u64,
}

impl Index {
    pub fn new(config: IndexConfig) -> Result<Self, String> {
        if let Some(path_parent_directory) = config.path.parent() {
            std::fs::create_dir_all(path_parent_directory).map_err(|error| {
                format!(
                    "Can not create parent directories for path {}: {error}",
                    &config.path.display()
                )
            })?;
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&config.path)
            .map_err(|error| {
                format!(
                    "Can not open file at path {}: {error}",
                    config.path.display()
                )
            })?;
        let bytes_on_disk = file
            .metadata()
            .map_err(|error| format!("Can not get metadata of file {:?}: {error}", file))?
            .len() as u64;
        if bytes_on_disk == 0 {
            let header = IndexHeader { record_size: 2 };
            let bytes_on_disk = {
                let mut writer = BufWriter::new(&mut file);
                bincode::encode_into_std_write(&header, &mut writer, bincode::config::standard())
                    .map_err(|error| {
                        format!(
                            "Can not encode header to file at path {}: {error}",
                            config.path.display()
                        )
                    })? as u64
            };
            Ok(Index {
                config,
                file,
                header,
                header_size: bytes_on_disk as usize,
                records_count: 0,
                bytes_on_disk,
            })
        } else {
            let header: IndexHeader = {
                let mut reader = BufReader::new(&mut file);
                bincode::decode_from_std_read(&mut reader, bincode::config::standard()).map_err(
                    |error| {
                        format!(
                            "Can not read header from file at path {}: {error}",
                            config.path.display()
                        )
                    },
                )?
            };
            let header_size = file.stream_position().map_err(|error| {
                format!(
                    "Can not get position in file at path {}: {error}",
                    config.path.display()
                )
            })? as usize;
            let records_count = (bytes_on_disk - header_size as u64) / header.record_size as u64;
            Ok(Index {
                config,
                file,
                header,
                header_size,
                records_count,
                bytes_on_disk,
            })
        }
    }

    pub fn get(&self, record_index: u64) -> Result<Option<variable_data_pool::Id>, String> {
        let mut buffer = vec![0 as u8; self.header.record_size as usize];
        if self
            .file
            .read_exact_at(
                &mut buffer,
                self.header_size as u64 + record_index * self.header.record_size as u64,
            )
            .is_ok()
        {
            let result: variable_data_pool::Id =
                bincode::decode_from_slice(&mut buffer, bincode::config::standard())
                    .map_err(|error| format!("Can not decode data id from index record: {error}"))?
                    .0;
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    pub fn get_records_count(&self) -> u64 {
        self.records_count
    }

    pub fn clear(&mut self) -> Result<(), String> {
        self.file
            .set_len(0)
            .map_err(|error| format!("Can not truncate file {:?}: {error}", self.file))?;
        Ok(())
    }
}
