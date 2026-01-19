use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::path::PathBuf;
use std::{fs, os::unix::fs::FileExt};

use anyhow::{Context, Error, Result};
use fallible_iterator::FallibleIterator;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IndexConfig {
    pub path: PathBuf,
}

#[derive(bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct IndexHeader {
    pub record_size: u8,
}

#[derive(Debug)]
pub struct Index {
    pub config: IndexConfig,
    file: fs::File,
    pub header: IndexHeader,
    header_size: usize,
    pub records_count: u64,
}

impl Index {
    pub fn write_header(mut file: &fs::File, header: &IndexHeader) -> Result<u64> {
        let mut writer = BufWriter::new(&mut file);
        let result =
            bincode::encode_into_std_write(&header, &mut writer, bincode::config::standard())
                .with_context(|| format!("Can not encode header",))? as u64;
        writer
            .flush()
            .with_context(|| format!("Can not flush new index file header"))?;
        Ok(result)
    }
    pub fn new(config: IndexConfig) -> Result<Self> {
        if let Some(path_parent_directory) = config.path.parent() {
            std::fs::create_dir_all(path_parent_directory).with_context(|| {
                format!(
                    "Can not create parent directories for path {}",
                    &config.path.display()
                )
            })?;
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&config.path)
            .with_context(|| format!("Can not open file at path {}", config.path.display()))?;
        let bytes_on_disk = file
            .metadata()
            .with_context(|| format!("Can not get metadata of file {:?}", file))?
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
                .with_context(|| {
                    format!(
                        "Can not encode header to file at path {}",
                        config.path.display()
                    )
                })? as u64;
                writer
                    .flush()
                    .with_context(|| format!("Can not flush new index file header"))?;
                result
            };
            Ok(Index {
                config,
                file,
                header,
                header_size: bytes_on_disk as usize,
                records_count: 0,
            })
        } else {
            let (header, header_size): (IndexHeader, usize) = {
                let mut reader = BufReader::new(&mut file);
                let header =
                    bincode::decode_from_std_read(&mut reader, bincode::config::standard())
                        .with_context(|| {
                            format!(
                                "Can not read header from file at path {}",
                                config.path.display()
                            )
                        })?;
                (
                    header,
                    reader.stream_position().with_context(|| {
                        format!(
                            "Can not get position in file at path {}",
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
            })
        }
    }

    pub fn get(&self, record_index: u64) -> Result<Option<u64>> {
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
            for byte in buffer.iter().rev() {
                result = (result << 8) + *byte as u64;
            }
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    pub fn clear(&mut self) -> Result<()> {
        self.file
            .set_len(0)
            .with_context(|| format!("Can not truncate file {:?}", self.file))?;
        Ok(())
    }

    pub fn iter(&self, from_record_index: u64, backwards: bool) -> Result<IndexIterator> {
        println!("iter");
        let mut index_file = std::fs::OpenOptions::new()
            .create(false)
            .read(true)
            .open(&self.config.path)
            .with_context(|| {
                format!(
                    "Can not open file at path {} for reading",
                    &self.config.path.display()
                )
            })?;
        index_file
            .seek(std::io::SeekFrom::Start(
                self.header_size as u64 + from_record_index * self.header.record_size as u64,
            ))
            .with_context(|| format!("Can not seek in index file at {self:?}"))?;
        Ok(IndexIterator {
            reader: BufReader::new(index_file),
            current_record_index: from_record_index,
            records_count: self.records_count,
            buffer: vec![0 as u8; self.header.record_size as usize],
            header_size: self.header_size,
            backwards,
            first_read: true,
        })
    }
}

#[derive(Debug)]
pub struct IndexIterator {
    reader: BufReader<fs::File>,
    current_record_index: u64,
    records_count: u64,
    buffer: Vec<u8>,
    header_size: usize,
    backwards: bool,
    first_read: bool,
}

impl FallibleIterator for IndexIterator {
    type Item = u64;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Self::Item>> {
        if self.records_count <= self.current_record_index {
            return Ok(None);
        }
        if self.backwards {
            if !self.first_read {
                self.reader
                    .seek(std::io::SeekFrom::Start(
                        self.header_size as u64
                            + self.current_record_index * self.buffer.len() as u64,
                    ))
                    .with_context(|| format!("Can not seek backwards while iterating index"))?;
            }
            self.reader.read_exact(&mut self.buffer).with_context(|| {
                format!("Can not read index record (data id) while iterating index")
            })?;
            self.current_record_index = self
                .current_record_index
                .checked_sub(1)
                .unwrap_or(self.records_count);
        } else {
            self.reader.read_exact(&mut self.buffer).with_context(|| {
                format!("Can not read index record (data id) while iterating index")
            })?;
            self.current_record_index += 1;
        }
        let mut result: u64 = 0;
        for byte in self.buffer.iter().rev() {
            result = (result << 8) + *byte as u64;
        }
        self.first_read = false;
        Ok(Some(result))
    }
}
