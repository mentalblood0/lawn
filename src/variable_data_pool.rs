use bincode;
use serde::Deserialize;
use std::path::PathBuf;

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

#[derive(Debug, Deserialize)]
pub struct VariableDataPoolConfig {
    pub directory: PathBuf,
    pub max_element_size: usize,
}

#[derive(Debug)]
pub struct VariableDataPool {
    pub config: VariableDataPoolConfig,
    container_size_index_to_fixed_data_pool: [FixedDataPool; CONTAINERS_SIZES_COUNT],
}

#[derive(Debug, Clone)]
pub struct Id {
    pub container_size_index: u8,
    pub pointer: usize,
}

#[derive(bincode::Encode, bincode::Decode)]
pub struct Container {
    pub data: Vec<u8>,
}

impl VariableDataPool {
    pub fn new(config: VariableDataPoolConfig) -> Result<Self, String> {
        let mut fixed_data_pools: Vec<FixedDataPool> = Vec::with_capacity(CONTAINERS_SIZES_COUNT);
        for container_size in split_scale_logarithmically(config.max_element_size)? {
            fixed_data_pools.push(FixedDataPool::new(FixedDataPoolConfig {
                path: config
                    .directory
                    .join(format!("containers_of_size_{container_size:0>10}.dat")),
                container_size,
            })?);
        }
        Ok(Self {
            config,
            container_size_index_to_fixed_data_pool: fixed_data_pools.try_into().map_err(|source_type| {
                format!("Can not convert fixed data pools vec to static array with required size: {source_type:?}")
            })?,
        })
    }

    pub fn update(
        &mut self,
        data_to_add: &Vec<Vec<u8>>,
        ids_of_data_to_delete: &Vec<Id>,
    ) -> Result<Vec<Id>, String> {
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

        let mut container_size_index_to_pointers_to_delete: [Vec<usize>; CONTAINERS_SIZES_COUNT] =
            [const { Vec::new() }; CONTAINERS_SIZES_COUNT];
        for id in ids_of_data_to_delete {
            container_size_index_to_pointers_to_delete[id.container_size_index as usize]
                .push(id.pointer);
        }

        let mut result: Vec<Id> = vec![
            Id {
                container_size_index: 0,
                pointer: 0
            };
            data_to_add.len()
        ];
        for container_size_index in 0..CONTAINERS_SIZES_COUNT {
            let encoded_data_to_add =
                &container_size_index_to_encoded_data_to_add[container_size_index];
            let pointers_to_data_to_delete =
                &container_size_index_to_pointers_to_delete[container_size_index];
            if encoded_data_to_add.len() == 0 && pointers_to_data_to_delete.len() == 0 {
                continue;
            }
            dbg!(&encoded_data_to_add);
            let encoded_data_to_add_pointers = self.container_size_index_to_fixed_data_pool
                [container_size_index]
                .update(encoded_data_to_add, pointers_to_data_to_delete)?;
            dbg!(&encoded_data_to_add_pointers);
            for (encoded_data_to_add_index, pointer) in
                encoded_data_to_add_pointers.iter().enumerate()
            {
                let initial_index = container_size_index_to_data_to_add_initial_indexes
                    [container_size_index][encoded_data_to_add_index];
                result[initial_index] = Id {
                    container_size_index: container_size_index as u8,
                    pointer: *pointer,
                };
            }
        }

        Ok(result)
    }
    pub fn clear(&mut self) -> Result<&Self, String> {
        for fixed_data_pool in self.container_size_index_to_fixed_data_pool.iter_mut() {
            fixed_data_pool.clear()?;
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_update_simple() {
        let path = Path::new("/tmp/lawn/test/variable_data_pool");
        let mut variable_data_pool = VariableDataPool::new(VariableDataPoolConfig {
            directory: path.to_path_buf(),
            max_element_size: 65536,
        })
        .unwrap();
        variable_data_pool.clear().unwrap();

        let ids = variable_data_pool
            .update(&vec!["lalala".to_string().into_bytes()], &vec![])
            .unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0].container_size_index, 6);
        assert_eq!(ids[0].pointer, 0);
    }
}
