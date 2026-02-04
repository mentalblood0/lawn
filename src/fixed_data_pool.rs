//! Fixed-size data pool implementation.
//!
//! This module provides a [`FixedDataPool`] that stores data records in fixed-size containers
//! within a file. It implements a free-list pattern for efficient memory reuse after deletions.
//! The pool uses a bitmap (head) at the beginning of the file to track free and allocated containers.

use std::cmp::Ordering;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::{fs, io::BufWriter};

use anyhow::{Context, Result, anyhow};

use serde::{Deserialize, Serialize};

use crate::data_pool::*;
use crate::keyvalue::Value;

/// Configuration for creating a [`FixedDataPool`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixedDataPoolConfig {
    /// The file path where the data pool will be stored.
    pub path: PathBuf,
    /// The fixed size of each container in bytes. All data records are stored in containers of this size.
    pub container_size: usize,
}

/// A data pool that stores fixed-size containers in a file.
///
/// The `FixedDataPool` uses a bitmap at the beginning of the file (called the "head") to track
/// which containers are allocated and which are free. When containers are removed, they are
/// added to a free-list for reuse. If no freed containers are available, new containers are
/// appended to the end of the file.
///
/// # File Structure
/// The file consists of:
/// 1. **Head (bitmap)**: A sequence of bytes where each byte represents a container slot.
///    - Value `255` (all bits set) means the container is allocated
///    - Other values point to the next free container in the free-list chain
/// 2. **Containers**: Fixed-size slots for storing data records
#[derive(Debug)]
pub struct FixedDataPool {
    /// Configuration for this data pool.
    pub config: FixedDataPoolConfig,
    /// The total number of containers that have been allocated.
    containers_allocated: u64,
    /// The total size of the file on disk in bytes.
    bytesize_on_disk: u64,
    /// The underlying file handle for the data pool storage.
    file: fs::File,
    /// The size of the head (bitmap) in bytes.
    head_size: u8,
    /// The current head (bitmap) cached in memory.
    head: Vec<u8>,
    /// Whether there are no free containers left in the free-list.
    no_holes_left: bool,
    /// Buffered writer for appending new containers to the file.
    writer: Option<BufWriter<fs::File>>,
    /// Buffer of pointers to containers that have been removed but not yet flushed.
    buffer_of_pointers_to_data_to_remove: Vec<u64>,
}

impl<D: Value> DataPoolConfig<D> for FixedDataPoolConfig {
    fn new_data_pool(&self) -> Result<Box<dyn DataPool<D> + Send + Sync>> {
        Ok(Box::new(FixedDataPool::new(self).with_context(|| {
            format!("Can not create new fixed data pool from config {self:?}")
        })?))
    }
}

impl FixedDataPool {
    /// Creates a new `FixedDataPool` with the given configuration.
    ///
    /// If the file at the specified path exists, it will be opened and the existing
    /// data pool state will be recovered. If the file doesn't exist, a new empty
    /// data pool will be created.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The parent directory cannot be created
    /// - The file cannot be opened
    /// - The file metadata cannot be read
    /// - The existing file is corrupt or invalid
    pub fn new(config: &FixedDataPoolConfig) -> Result<Self> {
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
            .read(true)
            .write(true)
            .open(&config.path)
            .with_context(|| format!("Can not open file at path {}", config.path.display()))?;
        let mut result = Self {
            config: config.clone(),
            file: file,
            head_size: 0,
            head: vec![],
            no_holes_left: true,
            containers_allocated: 0,
            bytesize_on_disk: 0,
            writer: None,
            buffer_of_pointers_to_data_to_remove: Vec::new(),
        };
        result.head_size = std::cmp::min(config.container_size, 8) as u8;
        result.bytesize_on_disk = result
            .file
            .metadata()
            .with_context(|| format!("Can not get metadata of file {:?}", result.file))?
            .len() as u64;
        if result.bytesize_on_disk == 0 {
            result.initialize_empty_file().with_context(|| {
                format!("Can not initialize empty file for fixed data pool with config {config:?}")
            })?;
        } else {
            result
                .file
                .read_exact_at(&mut result.head, 0)
                .with_context(|| format!("Can not read head of {result:?}"))?;
            result.containers_allocated =
                (result.bytesize_on_disk - result.head_size as u64) / config.container_size as u64;
            result.no_holes_left = result.head.iter().all(|byte| *byte == 255);
        }
        Ok(result)
    }

    /// Initializes an empty file structure for a new data pool.
    ///
    /// Creates a head (bitmap) filled with 255 (all containers free) and sets
    /// initial metadata.
    fn initialize_empty_file(&mut self) -> Result<()> {
        self.set_head(vec![255; self.head_size as usize])
            .with_context(|| "Can not set head for file of empty fixed data pool")?;
        self.bytesize_on_disk = self.head_size as u64;
        self.containers_allocated = 0;
        self.no_holes_left = true;
        Ok(())
    }

    /// Writes the head (bitmap) to the file and updates the cached head.
    ///
    /// The head tracks free containers using a free-list chain. When a container
    /// is freed, its slot is set to point to the current head, and the freed
    /// container's position becomes the new head.
    fn set_head(&mut self, value: Vec<u8>) -> Result<&Self> {
        self.file
            .write_all_at(value.as_slice(), 0)
            .with_context(|| format!("Can not write head for file {:?}", self.file))?;
        self.no_holes_left = self.head.iter().all(|byte| *byte == 255);
        Ok(self)
    }

    /// Extracts a pointer value from a container's bytes.
    ///
    /// The pointer is stored in little-endian format in the first min(8, container_size) bytes.
    fn pointer_from_container(&self, container: &Vec<u8>) -> u64 {
        let mut result: u64 = 0;
        for byte in container[..std::cmp::min(8, container.len())].iter() {
            result = (result << 8) + *byte as u64;
        }
        result
    }

    /// Converts a pointer value to bytes suitable for storage in a container.
    ///
    /// If container_size is >= 8, stores the full 8-byte pointer.
    /// Otherwise, stores only the last container_size bytes.
    fn pointer_to_container(&self, pointer: u64) -> Vec<u8> {
        let result_not_cutted = pointer.to_le_bytes();
        if self.config.container_size >= 8 {
            result_not_cutted.to_vec()
        } else {
            result_not_cutted[8 - self.config.container_size..].to_vec()
        }
    }

    /// Reads data of a specific size from a container at the given pointer.
    fn get_of_size(&self, pointer: u64, size: usize) -> Result<Vec<u8>> {
        let mut result = vec![0; size];
        self.file
            .read_exact_at(
                &mut result,
                self.head_size as u64 + pointer * self.config.container_size as u64,
            )
            .with_context(|| {
                format!("Can not get container of size {size} at pointer {pointer} of {self:?}")
            })?;
        Ok(result)
    }

    /// Writes data to a container at the given pointer.
    fn set(&mut self, pointer: u64, container: &Vec<u8>) -> Result<()> {
        self.file
            .write_all_at(
                container,
                self.head_size as u64 + pointer * self.config.container_size as u64,
            )
            .with_context(|| {
                format!(
                    "Can not set container of size {} at pointer {pointer} of {self:?}",
                    container.len()
                )
            })?;
        Ok(())
    }

    /// Inserts raw data into the data pool and returns its pointer.
    ///
    /// If there are previously removed containers available for reuse, one will be reused.
    /// Otherwise, a new container will be appended to the end of the file.
    ///
    /// Data smaller than `container_size` will be zero-padded to fill the container.
    /// Data larger than `container_size` will return an error.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The data size exceeds the container size
    /// - Writing to the file fails
    pub fn insert_raw(&mut self, mut data: Vec<u8>) -> Result<u64> {
        if let Some(pointer_to_data_to_remove) = self.buffer_of_pointers_to_data_to_remove.pop() {
            self.set(pointer_to_data_to_remove, &data)
                .with_context(|| {
                    format!("Can not set replace current data at pointer {pointer_to_data_to_remove:?} with data {data:?}")
                })?;
            Ok(pointer_to_data_to_remove)
        } else {
            if self.no_holes_left {
                if self.writer.is_none() {
                    let file = fs::OpenOptions::new()
                        .create(false)
                        .read(false)
                        .write(true)
                        .append(true)
                        .open(&self.config.path)
                        .with_context(|| {
                            format!("Can not open file at path {}", self.config.path.display())
                        })?;
                    self.writer = Some(BufWriter::new(file));
                }
                match data.len().cmp(&self.config.container_size) {
                    Ordering::Greater => {
                        return Err(anyhow!(
                            "Can not insert data of size {} into fixed data pool for containers of size {} at {self:?}",
                            data.len(),
                            self.config.container_size
                        ));
                    }
                    Ordering::Less => {
                        data.resize(self.config.container_size, 0);
                    }
                    Ordering::Equal => {}
                }
                self.writer
                    .as_mut()
                    .ok_or_else(|| anyhow!("Logical error: no writer"))?
                    .write_all(&data)
                    .with_context(|| format!("Can not write to file {:?}", self.file))?;
                self.containers_allocated += 1;
                self.bytesize_on_disk += self.config.container_size as u64;
                Ok(self.containers_allocated - 1)
            } else {
                let pointer = self.pointer_from_container(&self.head);
                let new_head_size = self.head_size as usize;
                let new_head = self.get_of_size(pointer, new_head_size).with_context(|| {
                    format!(
                        "Can not get {new_head_size:?} bytes at pointer {pointer:?} of {self:?}"
                    )
                })?;
                self.set(pointer, &data).with_context(|| {
                    format!("Can not set data {data:?} at pointer {pointer:?} of {self:?}")
                })?;
                self.set_head(new_head.clone())
                    .with_context(|| format!("Can not set new head {new_head:?}"))?;
                Ok(pointer)
            }
        }
    }

    /// Retrieves raw data from the container at the given pointer.
    ///
    /// Returns the full container contents of `container_size` bytes.
    pub fn get_raw(&self, pointer: u64) -> Result<Vec<u8>> {
        self.get_of_size(pointer, self.config.container_size)
    }
}

impl<D: Value> DataPool<D> for FixedDataPool {
    /// Inserts a data record into the pool and returns its unique identifier.
    ///
    /// The data record is serialized using bincode before storage.
    fn insert(&mut self, data_record: D) -> Result<u64> {
        self.insert_raw(
            bincode::encode_to_vec(data_record, bincode::config::standard())
                .with_context(|| format!("Can not encode data record"))?,
        )
    }

    /// Marks a container for removal.
    ///
    /// The container will be added to the free-list for reuse when `flush` is called.
    fn remove(&mut self, id: u64) -> Result<()> {
        self.buffer_of_pointers_to_data_to_remove.push(id);
        Ok(())
    }

    /// Flushes all pending operations to disk.
    ///
    /// This includes:
    /// - Reclaiming removed containers by updating the free-list
    /// - Flushing any buffered writes to the file
    fn flush(&mut self) -> Result<()> {
        if !self.buffer_of_pointers_to_data_to_remove.is_empty() {
            let mut cached_head = self.head.clone();
            for pointer_to_data_to_remove in
                std::mem::take(&mut self.buffer_of_pointers_to_data_to_remove).into_iter()
            {
                self.set(pointer_to_data_to_remove, &cached_head).with_context(|| format!("Can not set cached head {cached_head:?} at pointer to data to remove {pointer_to_data_to_remove:?} at {self:?} while flushing"))?;
                cached_head = self.pointer_to_container(pointer_to_data_to_remove);
            }
            self.set_head(cached_head.clone()).with_context(|| {
                format!("Can not set cached head {cached_head:?} for while flushing")
            })?;
        }
        if let Some(writer) = self.writer.as_mut() {
            writer
                .flush()
                .with_context(|| format!("Can not flush file for fixed data pool update"))?;
        }
        self.writer = None;
        Ok(())
    }

    /// Clears all data from the pool and resets it to an empty state.
    fn clear(&mut self) -> Result<()> {
        self.file
            .set_len(0)
            .with_context(|| format!("Can not truncate file {:?}", self.file))?;
        self.initialize_empty_file()?;
        Ok(())
    }

    /// Retrieves a data record by its identifier.
    ///
    /// The data is deserialized using bincode before being returned.
    fn get(&self, id: u64) -> Result<D> {
        Ok(bincode::decode_from_slice::<D, _>(
            &self
                .get_raw(id)
                .with_context(|| format!("Can not get raw value by id {id:?} at {self:?}"))?,
            bincode::config::standard(),
        )
        .with_context(|| {
            format!("Can not decode data record got by id {id:?} from fixed data pool {self:?}")
        })?
        .0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nanorand::{Rng, WyRand};
    use std::collections::BTreeMap;
    use std::path::Path;

    use pretty_assertions::assert_eq;

    #[test]
    fn test_generative() {
        const CONTAINER_SIZE: usize = 16;
        let path = Path::new("/tmp/lawn/test/fixed_data_pool.dat");
        let mut rng = WyRand::new_seed(0);

        let mut fixed_data_pool: Box<dyn DataPool<([u8; 8], [u8; 8])>> = Box::new(
            FixedDataPool::new(&FixedDataPoolConfig {
                path: path.to_path_buf(),
                container_size: CONTAINER_SIZE,
            })
            .unwrap(),
        );
        fixed_data_pool.clear().unwrap();
        let mut previously_inserted_data: BTreeMap<u64, ([u8; 8], [u8; 8])> = BTreeMap::new();

        for _ in 0..1000 {
            let pointers_to_data_to_remove: Vec<u64> = previously_inserted_data
                .keys()
                .take(rng.generate_range(0..=previously_inserted_data.len()))
                .cloned()
                .collect();
            for pointer in pointers_to_data_to_remove {
                previously_inserted_data.remove(&pointer);
                fixed_data_pool.remove(pointer).unwrap();
            }
            for _ in 0..rng.generate_range(1..=16) {
                let mut data_to_insert = ([0 as u8; 8], [0 as u8; 8]);
                rng.fill(&mut data_to_insert.0);
                rng.fill(&mut data_to_insert.1);
                let data_id = fixed_data_pool.insert(data_to_insert.clone()).unwrap();
                previously_inserted_data.insert(data_id, data_to_insert);
            }
            fixed_data_pool.flush().unwrap();

            for (pointer, data) in &previously_inserted_data {
                assert_eq!(&fixed_data_pool.get(*pointer).unwrap(), data);
            }
        }

        let mut fixed_data_pool: Box<dyn DataPool<([u8; 8], [u8; 8])>> = Box::new(
            FixedDataPool::new(&FixedDataPoolConfig {
                path: path.to_path_buf(),
                container_size: CONTAINER_SIZE,
            })
            .unwrap(),
        );
        for (pointer, data) in &previously_inserted_data {
            assert_eq!(&fixed_data_pool.get(*pointer).unwrap(), data);
        }
        fixed_data_pool.clear().unwrap();
    }
}
