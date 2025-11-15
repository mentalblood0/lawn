use bincode;
use std::path::PathBuf;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::data_pool::*;
use super::fixed_data_pool::*;

const CONTAINERS_SIZES_COUNT: usize = 256;

fn split_scale_logarithmically(
    max_value: usize,
) -> Result<[usize; CONTAINERS_SIZES_COUNT], String> {
    if max_value < CONTAINERS_SIZES_COUNT {
        return Err(format!(
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

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct VariableDataPoolConfig {
    pub directory: PathBuf,
    pub max_element_size: usize,
}

#[derive(Debug)]
pub struct VariableDataPool {
    pub config: VariableDataPoolConfig,
    container_size_index_to_fixed_data_pool: [FixedDataPool; CONTAINERS_SIZES_COUNT],
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Id {
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

#[derive(bincode::Encode, bincode::Decode)]
pub struct Container {
    pub data: Vec<u8>,
}

#[cfg_attr(feature = "serde", typetag::serde)]
impl DataPoolConfig for VariableDataPoolConfig {
    fn new_data_pool(&self) -> Result<Box<dyn DataPool + Send + Sync>, String> {
        Ok(Box::new(VariableDataPool::new(self)?))
    }
}

impl VariableDataPool {
    pub fn new(config: &VariableDataPoolConfig) -> Result<Self, String> {
        let mut fixed_data_pools: Vec<FixedDataPool> = Vec::with_capacity(CONTAINERS_SIZES_COUNT);
        for container_size in split_scale_logarithmically(config.max_element_size)? {
            fixed_data_pools.push(FixedDataPool::new(&FixedDataPoolConfig {
                path: config
                    .directory
                    .join(format!("containers_of_size_{container_size:0>10}.dat")),
                container_size,
            })?);
        }
        Ok(Self {
            config: config.clone(),
            container_size_index_to_fixed_data_pool: fixed_data_pools.try_into().map_err(|source_type| {
                format!("Can not convert fixed data pools vec to static array with required size: {source_type:?}")
            })?,
        })
    }
}

impl DataPool for VariableDataPool {
    fn update(
        &mut self,
        data_to_add: &Vec<Vec<u8>>,
        ids_of_data_to_remove: &Vec<u64>,
    ) -> Result<Vec<u64>, String> {
        let mut container_size_index_to_encoded_data_to_add: [Vec<Vec<u8>>;
            CONTAINERS_SIZES_COUNT] = [const { Vec::new() }; CONTAINERS_SIZES_COUNT];
        let mut container_size_index_to_data_to_add_initial_indexes: [Vec<usize>;
            CONTAINERS_SIZES_COUNT] = [const { Vec::new() }; CONTAINERS_SIZES_COUNT];
        for (initial_index, data) in data_to_add.iter().enumerate() {
            let encoded_data = bincode::encode_to_vec(
                Container { data: data.clone() },
                bincode::config::standard(),
            )
            .map_err(|error| format!("Can not encode data to container structure: {error}"))?;
            let container_size_index = self
                .container_size_index_to_fixed_data_pool
                .partition_point(|fixed_data_pool| {
                    fixed_data_pool.config.container_size < encoded_data.len()
                });
            container_size_index_to_data_to_add_initial_indexes[container_size_index]
                .push(initial_index);
            container_size_index_to_encoded_data_to_add[container_size_index].push(encoded_data);
        }

        let mut container_size_index_to_pointers_to_remove: [Vec<u64>; CONTAINERS_SIZES_COUNT] =
            [const { Vec::new() }; CONTAINERS_SIZES_COUNT];
        for id in ids_of_data_to_remove {
            let parsed_id = Id::from(*id);
            container_size_index_to_pointers_to_remove[parsed_id.container_size_index as usize]
                .push(parsed_id.pointer);
        }

        let mut result: Vec<u64> = vec![0; data_to_add.len()];
        for container_size_index in 0..CONTAINERS_SIZES_COUNT {
            let encoded_data_to_add =
                &container_size_index_to_encoded_data_to_add[container_size_index];
            let pointers_to_data_to_remove =
                &container_size_index_to_pointers_to_remove[container_size_index];
            if encoded_data_to_add.len() == 0 && pointers_to_data_to_remove.len() == 0 {
                continue;
            }
            let encoded_data_to_add_pointers = self.container_size_index_to_fixed_data_pool
                [container_size_index]
                .update(encoded_data_to_add, pointers_to_data_to_remove)?;
            for (encoded_data_to_add_index, pointer) in
                encoded_data_to_add_pointers.iter().enumerate()
            {
                let initial_index = container_size_index_to_data_to_add_initial_indexes
                    [container_size_index][encoded_data_to_add_index];
                result[initial_index] = u64::from(Id {
                    container_size_index: container_size_index as u8,
                    pointer: *pointer,
                });
            }
        }

        Ok(result)
    }

    fn clear(&mut self) -> Result<(), String> {
        for fixed_data_pool in self.container_size_index_to_fixed_data_pool.iter_mut() {
            fixed_data_pool.clear()?;
        }
        Ok(())
    }

    fn get(&self, id: u64) -> Result<Vec<u8>, String> {
        let parsed_id = Id::from(id);
        let encoded_data = self
            .container_size_index_to_fixed_data_pool
            .get(parsed_id.container_size_index as usize)
            .ok_or_else(|| format!("Can not get {id:?}: no such container index"))?
            .get(parsed_id.pointer)?;
        let container: Container =
            bincode::decode_from_slice(&encoded_data, bincode::config::standard())
                .map_err(|error| format!("Can not decode got data: {error}"))?
                .0;
        Ok(container.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use nanorand::{Rng, WyRand};
    use std::collections::HashMap;
    use std::path::Path;

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
        let mut variable_data_pool = VariableDataPool::new(&VariableDataPoolConfig {
            directory: Path::new("/tmp/lawn/test/variable_data_pool").to_path_buf(),
            max_element_size: 65536,
        })
        .unwrap();
        variable_data_pool.clear().unwrap();

        let mut previously_added_data: HashMap<u64, Vec<u8>> = HashMap::new();
        let mut rng = WyRand::new_seed(0);

        for _ in 0..1000 {
            let data_to_add: Vec<Vec<u8>> = (0..rng.generate_range(1..=16))
                .map(|_| {
                    let mut data = vec![0u8; rng.generate_range(1..1024)];
                    rng.fill(&mut data);
                    data
                })
                .collect();
            let ids_of_data_to_remove: Vec<u64> = previously_added_data
                .keys()
                .take(rng.generate_range(0..=previously_added_data.len()))
                .cloned()
                .collect();
            for id in &ids_of_data_to_remove {
                previously_added_data.remove(&id);
            }

            let ids_of_added_data = variable_data_pool
                .update(&data_to_add, &ids_of_data_to_remove)
                .unwrap();
            ids_of_added_data
                .iter()
                .enumerate()
                .for_each(|(id_index, id)| {
                    previously_added_data.insert(id.clone(), data_to_add[id_index].clone());
                });

            for (id, data) in &previously_added_data {
                assert_eq!(&variable_data_pool.get(id.clone()).unwrap(), data);
            }
        }
    }
}
