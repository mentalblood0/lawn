//! A simple index implementation that maps record indices to data offsets.
//!
//! This module provides a persistent index structure that stores u64 values
//! (typically representing file offsets or record positions) in a binary file.
//! The index supports both random access via [`Index::get`] and sequential
//! iteration via [`Index::iter`].
//!
//! # Structure
//!
//! The index file consists of:
//! 1. A header containing metadata (record size, etc.)
//! 2. A sequence of records, each containing a u64 value
//!
//! # Example
//!
//! ```ignore
//! use index_rs::Index;
//! use std::path::PathBuf;
//!
//! let config = IndexConfig {
//!     path: PathBuf::from("/tmp/my_index.idx"),
//! };
//! let mut index = Index::new(config).unwrap();
//!
//! // Write a value at record index 0
//! // (Implementation would provide a write method)
//!
//! // Read the value back
//! let value = index.get(0).unwrap();
//! assert_eq!(value, Some(12345));
//! ```

use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::path::PathBuf;
use std::{fs, os::unix::fs::FileExt};

use anyhow::{Context, Error, Result};
use fallible_iterator::FallibleIterator;

use serde::{Deserialize, Serialize};

/// Configuration for creating or opening an index.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IndexConfig {
/// The path where the index file is stored.
    pub path: PathBuf,
}

/// The header metadata for an index file.
///
/// Contains information about the index structure that is stored
/// at the beginning of every index file.
#[derive(bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct IndexHeader {
/// The size of each record in bytes.
    pub record_size: u8,
}

#[derive(Debug)]
pub struct Index {
/// The index configuration.
    pub config: IndexConfig,
    /// The underlying file handle for the index.
    file: fs::File,
    /// The parsed header from the index file.
    pub header: IndexHeader,
    header_size: usize,
    /// The total number of records stored in the index.
    pub records_count: u64,
}

impl Index {
    /// Writes an index header to the given file.
    ///
    /// This is a utility function for creating new index files.
    /// The header is encoded using bincode and written to the beginning
    /// of the file.
    ///
    /// # Arguments
    ///
    /// * `file` - The file to write the header to (must be opened for writing)
    /// * `header` - The header data to write
    ///
    /// # Returns
    ///
    /// The number of bytes written (header size)
    ///
    /// # Errors
    ///
    /// Returns an error if encoding or flushing fails.
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
    /// Creates a new index or opens an existing one.
    ///
    /// If the file at `config.path` exists, it is opened and the header
    /// is read. If it doesn't exist, a new index is created with a
    /// default header (record_size = 2).
    ///
    /// # Arguments
    ///
    /// * `config` - The index configuration containing the file path
    ///
    /// # Returns
    ///
    /// A new `Index` instance
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The parent directory cannot be created
    /// - The file cannot be opened
    /// - The header cannot be read or written
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

    /// Retrieves the value at the specified record index.
    ///
    /// Reads the u64 value stored at the given record position in the index.
    ///
    /// # Arguments
    ///
    /// * `record_index` - The zero-based index of the record to retrieve
    ///
    /// # Returns
    ///
    /// `Ok(Some(value))` if the record exists, `Ok(None)` if the index
    /// is out of bounds or the read fails
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

    /// Clears all records from the index.
    ///
    /// Truncates the index file to zero length, removing all stored records.
    /// The header will need to be rewritten if the index is to be reused.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be truncated.
    pub fn clear(&mut self) -> Result<()> {
        self.file
            .set_len(0)
            .with_context(|| format!("Can not truncate file {:?}", self.file))?;
        Ok(())
    }

    /// Creates an iterator over the index records.
    ///
    /// Returns an iterator that can traverse the index records either
    /// forwards or backwards starting from the specified record index.
    ///
    /// # Arguments
    ///
    /// * `from_record_index` - The record index to start iterating from
    /// * `backwards` - If true, iterates in reverse order
    ///
    /// # Returns
    ///
    /// An `IndexIterator` for sequential access to records
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or seeked.
    pub fn iter(&self, from_record_index: u64, backwards: bool) -> Result<IndexIterator> {
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

/// An iterator over index records.
///
/// Provides sequential access to u64 values stored in the index.
/// Can iterate forwards or backwards through the records.
#[derive(Debug)]
pub struct IndexIterator {
    /// The reader for the index file.
    reader: BufReader<fs::File>,
    /// The current position in the iteration (record index).
    current_record_index: u64,
    /// The total number of records in the index.
    records_count: u64,
    /// Buffer for reading record data.
    buffer: Vec<u8>,
    /// The size of the header in bytes.
    header_size: usize,
    /// Whether to iterate in reverse order.
    backwards: bool,
    /// Tracks whether this is the first read operation.
    first_read: bool,
}

impl FallibleIterator for IndexIterator {
    type Item = u64;
    type Error = Error;

    /// Advances the iterator and returns the next value.
    ///
    /// Reads the next record from the index based on the iteration direction.
    ///
    /// # Returns
    ///
    /// `Ok(Some(value))` with the next record value, or `Ok(None)` if
    /// there are no more records.
    ///
    /// # Errors
    ///
    /// Returns an error if a read or seek operation fails.
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
