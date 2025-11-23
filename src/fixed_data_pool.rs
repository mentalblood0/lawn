use std::fs;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;

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
        self.set_head(&vec![255; self.head_size as usize])?;
        self.bytesize_on_disk = self.head_size as u64;
        self.containers_allocated = 0;
        self.no_holes_left = true;
        Ok(())
    }

    fn set_head(&mut self, value: &Vec<u8>) -> Result<&Self, String> {
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
    fn update(
        &mut self,
        data_to_add: &Vec<Vec<u8>>,
        pointers_to_data_to_remove: &Vec<u64>,
    ) -> Result<Vec<u64>, String> {
        let mut result = Vec::with_capacity(data_to_add.len());

        let mut replaced = 0 as usize;
        while replaced < data_to_add.len() && replaced < pointers_to_data_to_remove.len() {
            let pointer = pointers_to_data_to_remove[replaced];
            self.set(pointer, &data_to_add[replaced])?;
            result.push(pointer);
            replaced += 1;
        }
        if replaced < pointers_to_data_to_remove.len() {
            self.set(pointers_to_data_to_remove[replaced], &self.head.clone())?;
            for pointer_index in replaced + 1..pointers_to_data_to_remove.len() {
                self.set(
                    pointers_to_data_to_remove[pointer_index],
                    &self.pointer_to_container(pointers_to_data_to_remove[pointer_index - 1]),
                )?;
            }
            self.set_head(
                &self.pointer_to_container(
                    *pointers_to_data_to_remove
                        .last()
                        .ok_or("Can not get last pointer to data to remove".to_string())?,
                ),
            )?;
        }

        for pointer_index in replaced..data_to_add.len() {
            if self.no_holes_left {
                for pointer in self.containers_allocated
                    ..(self.containers_allocated + data_to_add.len() as u64 - pointer_index as u64)
                {
                    result.push(pointer);
                }
                let mut data_to_add_left: Vec<u8> =
                    vec![0; (data_to_add.len() - pointer_index) * self.config.container_size];
                data_to_add
                    .into_iter()
                    .skip(pointer_index)
                    .enumerate()
                    .for_each(|(data_index, data)| {
                        let target_slice = &mut data_to_add_left[data_index
                            * self.config.container_size
                            ..data_index * self.config.container_size + data.len()];
                        target_slice.copy_from_slice(data);
                    });
                self.set(self.containers_allocated, &data_to_add_left)?;
                self.containers_allocated += (data_to_add.len() - pointer_index) as u64;
                self.bytesize_on_disk += data_to_add_left.len() as u64;
                break;
            } else {
                let pointer = self.pointer_from_container(&self.head);
                let new_head = self.get_of_size(pointer, self.head_size as usize)?;
                self.set(pointer, &data_to_add[pointer_index])?;
                result.push(pointer);
                self.set_head(&new_head)?;
            }
        }

        Ok(result)
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
    use std::collections::HashMap;
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
        let mut previously_added_data: HashMap<u64, Vec<u8>> = HashMap::new();

        for _ in 0..1000 {
            let data_to_add: Vec<Vec<u8>> = (0..rng.generate_range(1..=16))
                .map(|_| {
                    let mut data = vec![0u8; CONTAINER_SIZE];
                    rng.fill(&mut data);
                    data
                })
                .collect();
            let pointers_to_data_to_remove: Vec<u64> = previously_added_data
                .keys()
                .take(rng.generate_range(0..=previously_added_data.len()))
                .cloned()
                .collect();
            for pointer in &pointers_to_data_to_remove {
                previously_added_data.remove(&pointer);
            }

            let pointers_to_added_data = fixed_data_pool
                .update(&data_to_add, &pointers_to_data_to_remove)
                .unwrap();
            pointers_to_added_data
                .iter()
                .enumerate()
                .for_each(|(pointer_index, pointer)| {
                    previously_added_data.insert(*pointer, data_to_add[pointer_index].clone());
                });

            for (pointer, data) in &previously_added_data {
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
        for (pointer, data) in &previously_added_data {
            assert_eq!(&fixed_data_pool.get(*pointer).unwrap(), data);
        }
        fixed_data_pool.clear().unwrap();
    }
}
