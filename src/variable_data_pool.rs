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
    fn test_new() {
        let path = Path::new("/tmp/lawn/test/variable_data_pool");
        VariableDataPool::new(VariableDataPoolConfig {
            directory: path.to_path_buf(),
            max_element_size: 65536,
        })
        .unwrap();
    }
}
