use bincode;
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};

use serde::{Deserialize, Serialize};

use crate::data_pool::*;
use crate::fixed_data_pool::*;
use crate::keyvalue::Value;

const CONTAINERS_SIZES_COUNT: usize = 256;
const CONTAINER_SIZE_MIN: usize = 2;

fn split_scale_logarithmically(max_value: usize) -> Result<[usize; CONTAINERS_SIZES_COUNT]> {
    if max_value < CONTAINERS_SIZES_COUNT {
        return Err(anyhow!(
            "Can not split scale: maximum value {max_value} must be greater or equal to values count {CONTAINERS_SIZES_COUNT}"
        ));
    }

    let mut result = [max_value; CONTAINERS_SIZES_COUNT];
    let mut current_values_count = 0 as usize;

    for iterations_count in 0.. {
        let new_value = (2 as f64).powf(
            (max_value as f64).log2() * iterations_count as f64
                / (CONTAINERS_SIZES_COUNT - 1) as f64,
        ) as usize;
        if new_value < CONTAINER_SIZE_MIN {
            continue;
        }
        if new_value > max_value {
            break;
        }
        if current_values_count == 0 || result[current_values_count - 1] != new_value {
            result[current_values_count] = new_value;
            current_values_count += 1;
        }
    }
    result[std::cmp::min(CONTAINERS_SIZES_COUNT, current_values_count) - 1] = max_value;

    let mut additional_values = [0 as usize; CONTAINERS_SIZES_COUNT];
    loop {
        let target_additional_values_count = CONTAINERS_SIZES_COUNT - current_values_count;
        if target_additional_values_count == 0 {
            break;
        }
        let mut current_additional_values_count = 0 as usize;
        for two_values in result.windows(2) {
            if two_values[1] - two_values[0] == 1 {
                continue;
            }
            additional_values[current_additional_values_count] =
                (two_values[0] + two_values[1]) / 2;
            current_additional_values_count += 1;
            if current_additional_values_count == target_additional_values_count {
                break;
            }
        }
        result[current_values_count..current_values_count + current_additional_values_count]
            .copy_from_slice(&additional_values[..current_additional_values_count]);
        current_values_count += current_additional_values_count;
        result.sort();
    }

    Ok(result)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariableDataPoolConfig {
    pub directory: PathBuf,
    pub max_element_size: usize,
}

#[derive(Debug)]
pub struct VariableDataPool {
    pub config: VariableDataPoolConfig,
    container_size_index_to_fixed_data_pool: [FixedDataPool; CONTAINERS_SIZES_COUNT],
    jump_point: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, bincode::Encode, bincode::Decode)]
pub struct Id {
    pub container_size_index: u8,
    pub pointer: u64,
}

impl From<u64> for Id {
    fn from(item: u64) -> Self {
        Id {
            container_size_index: (item % 256) as u8,
            pointer: item / 256,
        }
    }
}

impl From<Id> for u64 {
    fn from(item: Id) -> Self {
        item.pointer * 256 + item.container_size_index as u64
    }
}

#[derive(bincode::Encode, bincode::Decode, Debug)]
pub struct Container {
    pub data: Vec<u8>,
}

impl<D: Value> DataPoolConfig<D> for VariableDataPoolConfig {
    fn new_data_pool(&self) -> Result<Box<dyn DataPool<D> + Send + Sync>> {
        Ok(Box::new(VariableDataPool::new(self).with_context(
            || format!("Can not create variable data pool from config {self:?}"),
        )?))
    }
}

impl VariableDataPool {
    pub fn new(config: &VariableDataPoolConfig) -> Result<Self> {
        let mut fixed_data_pools: Vec<FixedDataPool> = Vec::with_capacity(CONTAINERS_SIZES_COUNT);
        let mut jump_point: Option<usize> = None;
        for (size_index, container_size) in split_scale_logarithmically(config.max_element_size)
            .with_context(|| {
                format!(
                    "Can not split scale logarithmically for maximum element size {:?}",
                    config.max_element_size
                )
            })?
            .into_iter()
            .enumerate()
        {
            if jump_point.is_none() && container_size - size_index > CONTAINER_SIZE_MIN {
                jump_point = Some(size_index - 1 + CONTAINER_SIZE_MIN);
            }
            fixed_data_pools.push(
                FixedDataPool::new(&FixedDataPoolConfig {
                    path: config
                        .directory
                        .join(format!("containers_of_size_{container_size:0>10}.dat")),
                    container_size,
                })
                .with_context(|| {
                    format!(
                        "Can not create {:?}-nth fixed data pool for variable data pool",
                        fixed_data_pools.len() + 2
                    )
                })?,
            );
        }
        Ok(Self {
            config: config.clone(),
            container_size_index_to_fixed_data_pool: fixed_data_pools.try_into().map_err(|source_type|
                anyhow!("Can not convert fixed data pools vec to static array with required size: {source_type:?}"),
            )?,
            jump_point: jump_point.unwrap_or(0 as usize),
        })
    }

    fn insert_raw(&mut self, data: Vec<u8>) -> Result<u64> {
        let encoded_data = if data.len() > self.jump_point {
            bincode::encode_to_vec(
                Container { data: data.clone() },
                bincode::config::standard(),
            )
            .with_context(|| format!("Can not encode data to container structure"))?
        } else {
            data
        };
        let container_size_index = self
            .container_size_index_to_fixed_data_pool
            .partition_point(|fixed_data_pool| {
                fixed_data_pool.config.container_size < encoded_data.len()
            });
        let pointer = self.container_size_index_to_fixed_data_pool[container_size_index]
            .insert_raw(encoded_data.clone()).with_context(|| format!("Can not insert encoded data {encoded_data:?} into {:?}-nth fixed data pool of variable data pool", container_size_index + 1))?;
        Ok(u64::from(Id {
            container_size_index: container_size_index as u8,
            pointer: pointer,
        }))
    }

    fn get_raw(&self, id: u64) -> Result<Vec<u8>> {
        let parsed_id = Id::from(id);
        self.container_size_index_to_fixed_data_pool
            .get(parsed_id.container_size_index as usize)
            .with_context(|| format!("Can not get {id:?}: no such container index"))?
            .get_raw(parsed_id.pointer)
    }
}

impl<D: Value> DataPool<D> for VariableDataPool {
    fn insert(&mut self, data_record: D) -> Result<u64> {
        self.insert_raw(
            bincode::encode_to_vec(data_record, bincode::config::standard())
                .with_context(|| format!("Can not encode data record"))?,
        )
    }

    fn remove(&mut self, id: u64) -> Result<()> {
        let parsed_id = Id::from(id);
        <FixedDataPool as DataPool<D>>::remove(
            &mut self.container_size_index_to_fixed_data_pool
                [parsed_id.container_size_index as usize],
            parsed_id.pointer,
        )
    }

    fn flush(&mut self) -> Result<()> {
        for fixed_data_pool in self.container_size_index_to_fixed_data_pool.iter_mut() {
            <FixedDataPool as DataPool<D>>::flush(fixed_data_pool).with_context(|| {
                format!("Can not flush fixed data pool {fixed_data_pool:?} of variable data pool")
            })?;
        }
        Ok(())
    }

    fn clear(&mut self) -> Result<()> {
        for fixed_data_pool in self.container_size_index_to_fixed_data_pool.iter_mut() {
            <FixedDataPool as DataPool<D>>::clear(fixed_data_pool).with_context(|| {
                format!("Can not clear fixed data pool {fixed_data_pool:?} of variable data pool")
            })?;
        }
        Ok(())
    }

    fn get(&self, id: u64) -> Result<D> {
        let encoded_container_or_data_record = self
            .get_raw(id)
            .with_context(|| format!("Can not get raw data from fixed data pool by id {id:?}"))?;
        let container: Container = if encoded_container_or_data_record.len() > self.jump_point {
            bincode::decode_from_slice(
                &encoded_container_or_data_record,
                bincode::config::standard(),
            )
            .with_context(|| format!("Can not decode data container"))?
            .0
        } else {
            Container {
                data: encoded_container_or_data_record,
            }
        };
        Ok(
            bincode::decode_from_slice::<D, _>(&container.data, bincode::config::standard())
                .with_context(|| format!("Can not decode data record"))?
                .0,
        )
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
    fn test_splitting_scale() {
        for max_value in (CONTAINERS_SIZES_COUNT..(2 as usize).pow(16)).step_by(19) {
            let result = split_scale_logarithmically(max_value).unwrap();
            assert!(result.is_sorted());
            assert_eq!(result.len(), CONTAINERS_SIZES_COUNT);
            assert_eq!(*result.last().unwrap(), max_value);
        }
    }

    #[test]
    fn test_generative() {
        let mut variable_data_pool: Box<dyn DataPool<(Vec<u8>, Vec<u8>)>> = Box::new(
            VariableDataPool::new(&VariableDataPoolConfig {
                directory: Path::new("/tmp/lawn/test/variable_data_pool").to_path_buf(),
                max_element_size: 65536,
            })
            .unwrap(),
        );
        variable_data_pool.clear().unwrap();

        let mut previously_inserted_data: BTreeMap<u64, (Vec<u8>, Vec<u8>)> = BTreeMap::new();
        let mut rng = WyRand::new_seed(0);

        for _ in 0..1000 {
            let ids_of_data_to_remove: Vec<u64> = previously_inserted_data
                .keys()
                .take(rng.generate_range(0..=previously_inserted_data.len()))
                .cloned()
                .collect();
            for id in &ids_of_data_to_remove {
                previously_inserted_data.remove(&id);
                variable_data_pool.remove(*id).unwrap();
            }
            for _ in 0..rng.generate_range(1..=16) {
                let mut data_to_insert = (
                    vec![0u8; rng.generate_range(1..512)],
                    vec![0u8; rng.generate_range(1..512)],
                );
                rng.fill(&mut data_to_insert.0);
                rng.fill(&mut data_to_insert.1);
                let data_id = variable_data_pool.insert(data_to_insert.clone()).unwrap();
                previously_inserted_data.insert(data_id, data_to_insert);
            }
            variable_data_pool.flush().unwrap();

            for (id, data) in &previously_inserted_data {
                assert_eq!(&variable_data_pool.get(id.clone()).unwrap(), data);
            }
        }
    }
}
