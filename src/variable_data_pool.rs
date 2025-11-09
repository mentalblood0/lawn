use serde::Deserialize;
use std::path::PathBuf;

pub fn split_scale_logarithmically(max_value: usize) -> Result<Vec<usize>, String> {
    const VALUES_COUNT: usize = 256;
    if max_value < VALUES_COUNT {
        return Err(format!(
            "Can not split scale: maximum value {max_value} must be greater or equal to values count {VALUES_COUNT}"
        ));
    }

    let mut result: Vec<usize> = Vec::with_capacity(VALUES_COUNT);

    for current_values_count in 0..(VALUES_COUNT - 3) {
        let new_value = (2 as f64).powf(
            (max_value as f64).log2() * current_values_count as f64 / (VALUES_COUNT - 1) as f64,
        ) as usize;
        if result.len() == 0 || *result.last().unwrap() != new_value {
            result.push(new_value);
        }
    }
    result.push(max_value);

    let mut additional_values_count = VALUES_COUNT - result.len();
    let mut additional_values: Vec<usize> = Vec::with_capacity(additional_values_count);
    loop {
        if additional_values_count == 0 {
            break;
        }
        for two_values in result.windows(2) {
            if two_values[1] - two_values[0] == 1 {
                continue;
            }
            additional_values.push((two_values[0] + two_values[1]) / 2);
            if additional_values.len() == additional_values_count {
                break;
            }
        }
        result.append(&mut additional_values);
        result.sort();
        additional_values_count = VALUES_COUNT - result.len();
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use crate::variable_data_pool::split_scale_logarithmically;

    #[test]
    fn test_splitting_scale() {
        for max_value in (256..(2 as usize).pow(16)).step_by(19) {
            let result = split_scale_logarithmically(max_value).unwrap();
            assert!(result.is_sorted());
            assert_eq!(result.len(), 256);
            assert_eq!(*result.last().unwrap(), max_value);
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct VariableDataPoolConfig {
    pub directory: PathBuf,
    pub max_element_size: usize,
}

#[derive(Debug)]
pub struct VariableDataPool {
    pub config: VariableDataPoolConfig,
}
