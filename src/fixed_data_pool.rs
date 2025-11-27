use std::cmp::Ordering;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::{fs, io::BufWriter};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::data_pool::*;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct FixedDataPoolConfig {
    pub path: PathBuf,
    pub container_size: usize,
}

#[derive(Debug)]
pub struct FixedDataPool {
    pub config: FixedDataPoolConfig,
    containers_allocated: u64,
    bytesize_on_disk: u64,
    file: fs::File,
    head_size: u8,
    head: Vec<u8>,
    no_holes_left: bool,
    writer: Option<BufWriter<fs::File>>,
    buffer_of_pointers_to_data_to_remove: Vec<u64>,
}

#[cfg_attr(feature = "serde", typetag::serde)]
impl DataPoolConfig for FixedDataPoolConfig {
    fn new_data_pool(&self) -> Result<Box<dyn DataPool + Send + Sync>, String> {
        Ok(Box::new(FixedDataPool::new(self)?))
    }
}

impl FixedDataPool {
    pub fn new(config: &FixedDataPoolConfig) -> Result<Self, String> {
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
            .read(true)
            .write(true)
            .open(&config.path)
            .map_err(|error| {
                format!(
                    "Can not open file at path {}: {error}",
                    config.path.display()
                )
            })?;
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
            .map_err(|error| format!("Can not get metadata of file {:?}: {error}", result.file))?
            .len() as u64;
        if result.bytesize_on_disk == 0 {
            result.initialize_empty_file()?;
        } else {
            result
                .file
                .read_exact_at(&mut result.head, 0)
                .map_err(|error| format!("Can not read head of {result:?}: {error}"))?;
            result.containers_allocated =
                (result.bytesize_on_disk - result.head_size as u64) / config.container_size as u64;
            result.no_holes_left = result.head.iter().all(|byte| *byte == 255);
        }
        Ok(result)
    }

    fn initialize_empty_file(&mut self) -> Result<(), String> {
        self.set_head(vec![255; self.head_size as usize])?;
        self.bytesize_on_disk = self.head_size as u64;
        self.containers_allocated = 0;
        self.no_holes_left = true;
        Ok(())
    }

    fn set_head(&mut self, value: Vec<u8>) -> Result<&Self, String> {
        self.file
            .write_all_at(value.as_slice(), 0)
            .map_err(|error| format!("Can not write head for file {:?}: {error}", self.file))?;
        self.no_holes_left = self.head.iter().all(|byte| *byte == 255);
        Ok(self)
    }

    fn pointer_from_container(&self, container: &Vec<u8>) -> u64 {
        let mut result: u64 = 0;
        for byte in container[..std::cmp::min(8, container.len())].iter() {
            result = (result << 8) + *byte as u64;
        }
        result
    }

    fn pointer_to_container(&self, pointer: u64) -> Vec<u8> {
        let result_not_cutted = pointer.to_le_bytes();
        if self.config.container_size >= 8 {
            result_not_cutted.to_vec()
        } else {
            result_not_cutted[8 - self.config.container_size..].to_vec()
        }
    }

    fn get_of_size(&self, pointer: u64, size: usize) -> Result<Vec<u8>, String> {
        let mut result = vec![0; size];
        self.file
            .read_exact_at(
                &mut result,
                self.head_size as u64 + pointer * self.config.container_size as u64,
            )
            .map_err(|error| {
                format!(
                    "Can not get container of size {size} at pointer {pointer} of {self:?}: {error}"
                )
            })?;
        Ok(result)
    }

    fn set(&mut self, pointer: u64, container: &Vec<u8>) -> Result<(), String> {
        self.file
            .write_all_at(
                container,
                self.head_size as u64 + pointer * self.config.container_size as u64,
            )
            .map_err(|error| {
                format!(
                    "Can not set container of size {} at pointer {pointer} of {self:?}: {error}",
                    container.len()
                )
            })?;
        Ok(())
    }
}

impl DataPool for FixedDataPool {
    fn insert(&mut self, mut data: Vec<u8>) -> Result<u64, String> {
        if let Some(pointer_to_data_to_remove) = self.buffer_of_pointers_to_data_to_remove.pop() {
            self.set(pointer_to_data_to_remove, &data)?;
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
                        .map_err(|error| {
                            format!(
                                "Can not open file at path {}: {error}",
                                self.config.path.display()
                            )
                        })?;
                    self.writer = Some(BufWriter::new(file));
                }
                match data.len().cmp(&self.config.container_size) {
                    Ordering::Greater => {
                        return Err(format!(
                            "Can not insert data of size {} into fixed data pool for containers of size {}",
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
                    .ok_or_else(|| format!("Logical error: no writer"))?
                    .write_all(&data)
                    .map_err(|error| format!("Can not write to file {:?}: {error}", self.file))?;
                self.containers_allocated += 1;
                self.bytesize_on_disk += self.config.container_size as u64;
                Ok(self.containers_allocated - 1)
            } else {
                let pointer = self.pointer_from_container(&self.head);
                let new_head = self.get_of_size(pointer, self.head_size as usize)?;
                self.set(pointer, &data)?;
                self.set_head(new_head)?;
                Ok(pointer)
            }
        }
    }

    fn remove(&mut self, id: u64) -> Result<(), String> {
        self.buffer_of_pointers_to_data_to_remove.push(id);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), String> {
        let mut cached_head = self.head.clone();
        for pointer_to_data_to_remove in
            std::mem::take(&mut self.buffer_of_pointers_to_data_to_remove).into_iter()
        {
            self.set(pointer_to_data_to_remove, &cached_head)?;
            cached_head = self.pointer_to_container(pointer_to_data_to_remove);
        }
        self.set_head(cached_head)?;
        if let Some(writer) = self.writer.as_mut() {
            writer
                .flush()
                .map_err(|error| format!("Can not flush fixed data pool update: {error}"))?;
        }
        self.writer = None;
        Ok(())
    }

    fn clear(&mut self) -> Result<(), String> {
        self.file
            .set_len(0)
            .map_err(|error| format!("Can not truncate file {:?}: {error}", self.file))?;
        self.initialize_empty_file()?;
        Ok(())
    }

    fn get(&self, pointer: u64) -> Result<Vec<u8>, String> {
        self.get_of_size(pointer, self.config.container_size)
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

        let mut fixed_data_pool = FixedDataPool::new(&FixedDataPoolConfig {
            path: path.to_path_buf(),
            container_size: CONTAINER_SIZE,
        })
        .unwrap();
        fixed_data_pool.clear().unwrap();
        let mut previously_inserted_data: BTreeMap<u64, Vec<u8>> = BTreeMap::new();

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
                let mut data_to_insert = vec![0u8; CONTAINER_SIZE];
                rng.fill(&mut data_to_insert);
                let data_id = fixed_data_pool.insert(data_to_insert.clone()).unwrap();
                previously_inserted_data.insert(data_id, data_to_insert);
            }
            fixed_data_pool.flush().unwrap();

            for (pointer, data) in &previously_inserted_data {
                assert_eq!(&fixed_data_pool.get(*pointer).unwrap(), data);
            }
        }

        assert_eq!(
            fixed_data_pool.bytesize_on_disk,
            fixed_data_pool.file.metadata().unwrap().len() as u64
        );
        let mut fixed_data_pool = FixedDataPool::new(&FixedDataPoolConfig {
            path: path.to_path_buf(),
            container_size: CONTAINER_SIZE,
        })
        .unwrap();
        for (pointer, data) in &previously_inserted_data {
            assert_eq!(&fixed_data_pool.get(*pointer).unwrap(), data);
        }
        fixed_data_pool.clear().unwrap();
    }
}
