use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::path::PathBuf;
use std::{fs, os::unix::fs::FileExt};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct IndexConfig {
    pub path: PathBuf,
}

#[derive(bincode::Encode, bincode::Decode, Debug)]
pub struct IndexHeader {
    pub record_size: u8,
}

pub struct Index {
    pub config: IndexConfig,
    file: fs::File,
    pub header: IndexHeader,
    header_size: usize,
    pub records_count: u64,
    bytes_on_disk: u64,
}

impl Index {
    pub fn write_header(mut file: &fs::File, header: &IndexHeader) -> Result<u64, String> {
        let mut writer = BufWriter::new(&mut file);
        let result =
            bincode::encode_into_std_write(&header, &mut writer, bincode::config::standard())
                .map_err(|error| format!("Can not encode header: {error}",))? as u64;
        writer
            .flush()
            .map_err(|error| format!("Can not flush new index file header: {error}"))?;
        Ok(result)
    }
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
                let result = bincode::encode_into_std_write(
                    &header,
                    &mut writer,
                    bincode::config::standard(),
                )
                .map_err(|error| {
                    format!(
                        "Can not encode header to file at path {}: {error}",
                        config.path.display()
                    )
                })? as u64;
                writer
                    .flush()
                    .map_err(|error| format!("Can not flush new index file header: {error}"))?;
                result
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
            let (header, header_size): (IndexHeader, usize) = {
                let mut reader = BufReader::new(&mut file);
                let header =
                    bincode::decode_from_std_read(&mut reader, bincode::config::standard())
                        .map_err(|error| {
                            format!(
                                "Can not read header from file at path {}: {error}",
                                config.path.display()
                            )
                        })?;
                (
                    header,
                    reader.stream_position().map_err(|error| {
                        format!(
                            "Can not get position in file at path {}: {error}",
                            config.path.display()
                        )
                    })? as usize,
                )
            };
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

    pub fn get(&self, record_index: u64) -> Result<Option<u64>, String> {
        let mut buffer = vec![0 as u8; self.header.record_size as usize];
        if self
            .file
            .read_exact_at(
                &mut buffer,
                self.header_size as u64 + record_index * self.header.record_size as u64,
            )
            .is_ok()
        {
            let mut result: u64 = 0;
            for byte in buffer {
                result = (result << 8) + byte as u64;
            }
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    pub fn clear(&mut self) -> Result<(), String> {
        self.file
            .set_len(0)
            .map_err(|error| format!("Can not truncate file {:?}: {error}", self.file))?;
        Ok(())
    }

    pub fn iter(&self) -> Result<IndexIterator, String> {
        let index_file = std::fs::OpenOptions::new()
            .create(false)
            .read(true)
            .open(&self.config.path)
            .map_err(|error| {
                format!(
                    "Can not open file at path {} for reading: {error}",
                    &self.config.path.display()
                )
            })?;
        Ok(IndexIterator {
            reader: BufReader::new(index_file),
            current_record_index: 0,
            records_count: self.records_count,
            buffer: vec![0 as u8; self.header.record_size as usize],
        })
    }
}
pub struct IndexIterator {
    reader: BufReader<fs::File>,
    current_record_index: u64,
    records_count: u64,
    buffer: Vec<u8>,
}

impl Iterator for IndexIterator {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.records_count - self.current_record_index <= 1 {
            return None;
        }

        match self
            .reader
            .read_exact(&mut self.buffer)
            .map_err(|error| format!("Can not read index record (data id): {error}"))
        {
            Ok(_) => {
                let mut result: u64 = 0;
                for byte in &self.buffer {
                    result = (result << 8) + *byte as u64;
                }
                Some(result)
            }
            Err(_) => None,
        }
    }
}
