//! Variable-sized data pool implementation for efficient storage of data elements with varying sizes.
//!
//! This module provides a data pool that optimizes storage by using multiple fixed-size containers
//! organized logarithmically by size. Data elements are placed in the smallest container that can
//! accommodate them, reducing internal fragmentation.
//!
//! # Architecture
//!
//! The [`VariableDataPool`] uses a collection of [`FixedDataPool`] instances, each configured
//! with a different container size. Container sizes are distributed logarithmically between a
//! minimum size and a configured maximum element size. This allows efficient storage of data
//! elements ranging from small to large sizes.
//!
//! When inserting data, the pool determines the smallest container that can hold the encoded data
//! and stores it there. When retrieving, it reads from the appropriate container using a compact
//! ID that encodes both the container size index and the pointer within that container.
//!
//! # Key Features
//!
//! - **Logarithmic size distribution**: Container sizes grow logarithmically to efficiently cover
//!   a wide range of data sizes with minimal overhead
//! - **Compact IDs**: Each element is identified by a 64-bit ID containing both container index
//!   and pointer
//! - **Overflow handling**: Large elements exceeding the jump point threshold are wrapped in
//!   a container structure for proper serialization
//! - **Thread-safe**: Implements [`DataPool`] trait with Send + Sync bounds
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};

use serde::{Deserialize, Serialize};

use crate::data_pool::*;
use crate::fixed_data_pool::*;
use crate::keyvalue::Value;

/// Number of distinct container sizes in the logarithmic scale.
/// This determines how many fixed-size data pools will be created.
const CONTAINERS_SIZES_COUNT: usize = 256;

/// Minimum size for a container in the scale.
/// Any calculated size below this threshold is clamped up to this value.
const CONTAINER_SIZE_MIN: usize = 2;

/// Generates a logarithmic scale of container sizes from minimum to maximum.
///
/// This function creates an array of container sizes distributed logarithmically
/// between `CONTAINER_SIZE_MIN` and `max_value`. The distribution ensures efficient
/// storage by having more smaller containers and fewer larger ones.
///
/// The algorithm:
/// 1. Calculates logarithmic steps from 0 to `max_value`
/// 2. Fills in any gaps between steps with intermediate values
/// 3. Ensures exactly `CONTAINERS_SIZES_COUNT` entries are produced
///
/// # Arguments
///
/// * `max_value` - The maximum container size to include in the scale
///
/// # Returns
///
/// An array of `CONTAINERS_SIZES_COUNT` container sizes, sorted in ascending order
///
/// # Errors
///
/// Returns an error if `max_value` is less than `CONTAINERS_SIZES_COUNT`
fn split_scale_logarithmically(max_value: usize) -> Result<[usize; CONTAINERS_SIZES_COUNT]> {
    if max_value < CONTAINERS_SIZES_COUNT {
        return Err(anyhow!(
            "Can not split scale: maximum value {max_value} must be greater or equal to values \
             count {CONTAINERS_SIZES_COUNT}"
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

/// Configuration for creating a [`VariableDataPool`].
///
/// This struct holds the settings required to initialize a variable-sized data pool.
/// It specifies where the pool stores its data files and the maximum size of elements
/// it can accommodate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariableDataPoolConfig {
    /// The directory where container files will be stored.
    /// Each container size gets its own file in this directory.
    pub directory: PathBuf,

    /// The maximum size of any single element that can be stored.
    /// This determines the upper bound of the logarithmic container size scale.
    pub max_element_size: usize,
}

/// A variable-sized data pool that efficiently stores elements of different sizes.
///
/// This data pool uses multiple fixed-size containers organized logarithmically by size.
/// Each element is stored in the smallest container that can accommodate it, minimizing
/// internal fragmentation while maintaining efficient access.
///
/// # Storage Model
///
/// The pool maintains 256 ([`CONTAINERS_SIZES_COUNT`]) fixed-size data pools, each with a
/// different container size. Container sizes grow logarithmically from a minimum size
/// to `max_element_size`. When storing data:
///
/// 1. The data is encoded using bincode
/// 2. The smallest container that fits the encoded data is selected
/// 3. A compact 64-bit ID is returned for later retrieval
///
/// # Thread Safety
///
/// This struct implements [`DataPool`] with `Send + Sync` bounds, making it safe to use
/// across multiple threads.
///
/// # Note
///
/// The pool is typically created through the [`DataPoolConfig`] trait implementation,
/// which ensures proper configuration and initialization.
#[derive(Debug)]
pub struct VariableDataPool {
    /// The configuration used to create this pool.
    pub config: VariableDataPoolConfig,

    /// Array mapping container size indices to their corresponding fixed data pools.
    /// Index 0 contains the smallest containers, increasing to the largest at index 255.
    container_size_index_to_fixed_data_pool: [FixedDataPool; CONTAINERS_SIZES_COUNT],

    /// Threshold for determining when to wrap data in a container structure.
    /// Data larger than this threshold is serialized as a [`Container`] to ensure
    /// proper binary encoding across all container sizes.
    jump_point: usize,
}

/// A compact identifier for an element stored in a [`VariableDataPool`].
///
/// This struct encodes both the container size index and the pointer position
/// within that container. It can be converted to and from a 64-bit integer for
/// compact storage and transmission.
///
/// # ID Structure
///
/// The 64-bit ID is composed of:
/// - **Lower 8 bits (0-255)**: The `container_size_index` identifying which
///   fixed-size pool contains the element
/// - **Upper 56 bits**: The `pointer` offset within that container
///
/// # Serialization
///
/// This struct implements [`bincode::Encode`] and [`bincode::Decode`] for
/// efficient binary serialization.
#[derive(Debug, Clone, PartialEq, Eq, Hash, bincode::Encode, bincode::Decode)]
pub struct Id {
    /// Index into the container size array, indicating which fixed-size pool
    /// contains the element. Valid range: 0-255.
    pub container_size_index: u8,

    /// Offset within the fixed-size container where the element is stored.
    pub pointer: u64,
}

impl From<u64> for Id {
    /// Decodes a 64-bit ID into its component parts.
    ///
    /// The lower 8 bits are extracted as the container size index,
    /// and the remaining upper bits form the pointer.
    fn from(item: u64) -> Self {
        Id {
            container_size_index: (item % 256) as u8,
            pointer: item / 256,
        }
    }
}

impl From<Id> for u64 {
    /// Encodes an ID into a compact 64-bit integer.
    ///
    /// The container size index occupies the lower 8 bits,
    /// and the pointer forms the upper bits.
    fn from(item: Id) -> Self {
        item.pointer * 256 + item.container_size_index as u64
    }
}

/// Wrapper structure for encoding variable-sized data.
///
/// This struct wraps data that exceeds the `jump_point` threshold
/// to ensure proper bincode serialization. When data is larger than
/// the jump point, it is wrapped in this container for encoding.
///
/// # Note
///
/// The container is bincode-serializable, allowing variable-length
/// data to be stored in fixed-size containers by adding proper length
/// metadata.
#[derive(bincode::Encode, bincode::Decode, Debug)]
pub struct Container {
    /// The wrapped data bytes.
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
    /// Creates a new `VariableDataPool` from the given configuration.
    ///
    /// This method initializes the pool by:
    /// 1. Computing a logarithmic scale of container sizes up to `max_element_size`
    /// 2. Creating a [`FixedDataPool`] for each size in the scale
    /// 3. Calculating the `jump_point` threshold for overflow handling
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration specifying the storage directory and maximum element size
    ///
    /// # Returns
    ///
    /// A new `VariableDataPool` instance, or an error if initialization fails
    ///
    /// # Errors
    ///
    /// This method can fail if:
    /// - Container size scale computation fails
    /// - Any of the fixed data pools cannot be created
    /// - The conversion from Vec to array fails
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
            container_size_index_to_fixed_data_pool: fixed_data_pools.try_into().map_err(
                |source_type| {
                    anyhow!(
                        "Can not convert fixed data pools vec to static array with required size: \
                         {source_type:?}"
                    )
                },
            )?,
            jump_point: jump_point.unwrap_or(0 as usize),
        })
    }

    /// Inserts raw bytes into the pool and returns a compact ID.
    ///
    /// This internal method handles the core insertion logic:
    /// 1. If data exceeds `jump_point`, wraps it in a [`Container`] for serialization
    /// 2. Finds the smallest container that can fit the encoded data
    /// 3. Inserts into the corresponding fixed data pool
    /// 4. Returns a 64-bit ID encoding the container index and pointer
    ///
    /// # Arguments
    ///
    /// * `data` - The raw bytes to store
    ///
    /// # Returns
    ///
    /// A 64-bit ID that can be used to retrieve the data later
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
            .insert_raw(encoded_data.clone())
            .with_context(|| {
                format!(
                    "Can not insert encoded data {encoded_data:?} into {:?}-nth fixed data pool \
                     of variable data pool",
                    container_size_index + 1
                )
            })?;
        Ok(u64::from(Id {
            container_size_index: container_size_index as u8,
            pointer: pointer,
        }))
    }

    /// Retrieves raw bytes from the pool using a compact ID.
    ///
    /// This internal method decodes the ID to locate the element:
    /// 1. Extracts the container size index from the lower 8 bits
    /// 2. Retrieves the pointer from the upper bits
    /// 3. Fetches the data from the appropriate fixed data pool
    ///
    /// # Arguments
    ///
    /// * `id` - The 64-bit ID returned by [`insert_raw`]
    ///
    /// # Returns
    ///
    /// The raw bytes stored at the given ID
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
