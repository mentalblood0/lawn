use bincode;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::io::{BufReader, BufWriter, Read};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::data_pool::{DataPool, DataPoolConfig};
use crate::index::{Index, IndexConfig};
use crate::partition_point::PartitionPoint;

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct TableConfig {
    pub index: IndexConfig,
    pub data_pool: Box<dyn DataPoolConfig>,
}

pub struct Table {
    pub index: Index,
    pub data_pool: Box<dyn DataPool + Send + Sync>,
    pub memtable: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

#[derive(bincode::Encode, bincode::Decode)]
struct DataRecord {
    key: Vec<u8>,
    value: Vec<u8>,
}

struct MemtableRecord {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

impl PartialEq for MemtableRecord {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for MemtableRecord {}

impl PartialOrd for MemtableRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.key.cmp(&other.key))
    }
}

impl Ord for MemtableRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl Table {
    pub fn new(config: TableConfig) -> Result<Self, String> {
        Ok(Self {
            index: Index::new(config.index)?,
            data_pool: config.data_pool.new_data_pool()?,
            memtable: BTreeMap::new(),
        })
    }

    pub fn merge(&mut self, changes: &mut BTreeMap<Vec<u8>, Option<Vec<u8>>>) {
        self.memtable.append(changes);
    }

    fn get_from_index_by_id(&self, id: u64) -> Result<DataRecord, String> {
        let result_encoded = self.data_pool.get(id)?;
        let result: DataRecord =
            bincode::decode_from_slice(&result_encoded, bincode::config::standard())
                .map_err(|error| format!("Can not decode data record: {error}"))?
                .0;
        Ok(result)
    }

    fn get_from_index(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, String> {
        Ok(
            PartitionPoint::new(0, self.index.get_records_count(), |record_index| {
                let data_record_id = self.index.get(record_index)?.ok_or(format!(
                    "Can not get data record id at index {record_index}"
                ))?;
                let data_record = self.get_from_index_by_id(data_record_id)?;
                Ok((data_record.key.cmp(key), data_record.value, ()))
            })?
            .filter(|partition_point| partition_point.is_exact)
            .map(|partition_point| partition_point.first_satisfying.value),
        )
    }

    pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, String> {
        match self.memtable.get(key) {
            Some(value) => Ok(value.clone()),
            None => Ok(self.get_from_index(key)?),
        }
    }

    pub fn clear(&mut self) -> Result<(), String> {
        self.index.clear()?;
        self.data_pool.clear()?;
        self.memtable.clear();
        Ok(())
    }

    pub fn checkpoint(&mut self) -> Result<(), String> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        let mut memtable_records: Vec<MemtableRecord> = std::mem::take(&mut self.memtable)
            .into_iter()
            .map(|(key, value)| MemtableRecord { key, value })
            .collect();

        let merge_locations = sparse_merge(
            self.index.get_records_count(),
            |data_record_id_index| {
                let data_record_id = self.index.get(data_record_id_index)?.ok_or(format!(
                    "Can not get data record id at index {data_record_id_index}"
                ))?;
                let data_record = self.get_from_index_by_id(data_record_id)?;
                Ok(Some((
                    MemtableRecord {
                        key: data_record.key,
                        value: Some(data_record.value),
                    },
                    data_record_id,
                )))
            },
            &memtable_records,
        )?;

        let mut memtable_records_to_add: Vec<Vec<u8>> = Vec::new();
        let mut memtable_records_to_add_merge_locations: Vec<&MergeLocation<u64>> = Vec::new();
        let mut ids_to_delete: Vec<u64> = Vec::new();

        for (current_record_index, merge_location) in merge_locations.into_iter().enumerate().rev()
        {
            if merge_location.replace {
                ids_to_delete.push(merge_location.additional_data);
            }
            let current_record = &memtable_records[current_record_index];
            if let Some(value) = &current_record.value {
                let current_record_as_data_record_encoded = bincode::encode_to_vec(
                    DataRecord {
                        key: current_record.key.clone(),
                        value: value.clone(),
                    },
                    bincode::config::standard(),
                )
                .map_err(|error| format!("Can not encode data record"))?;
                memtable_records_to_add.push(current_record_as_data_record_encoded);
            };
        }

        let memtable_records_new_ids = self
            .data_pool
            .update(&memtable_records_to_add, &ids_to_delete);

        let new_index_file_path = self.index.config.path.with_extension("part");
        let mut new_index_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&new_index_file_path)
            .map_err(|error| {
                format!(
                    "Can not create file at path {} for writing: {error}",
                    &new_index_file_path.display()
                )
            })?;
        let mut new_index_writer = BufWriter::new(new_index_file);

        let ids_to_delete_set: HashSet<u64> = ids_to_delete.into_iter().collect();
        let mut memtable_records_new_ids_iter = memtable_records_new_ids.iter().enumerate();
        for (old_index_data_id_index, old_index_data_id) in self.index.iter()?.enumerate() {
            if ids_to_delete_set.contains(&old_index_data_id) {
                continue;
            }
            for (record_index, new_id) in memtable_records_new_ids.iter().enumerate() {
                let merge_location = memtable_records_to_add_merge_locations[record_index];
            }
        }

        Ok(())
    }
}

struct Middles {
    source_size: usize,
    queue: VecDeque<(usize, usize)>,
}

impl Middles {
    fn new(source_size: usize) -> Self {
        let mut queue: VecDeque<(usize, usize)> = VecDeque::new();
        queue.push_back((0, source_size - 1));
        Self { source_size, queue }
    }
}

struct Middle {
    left_index: usize,
    middle_index: usize,
    right_index: usize,
}

impl Iterator for Middles {
    type Item = Middle;

    fn next(&mut self) -> Option<Self::Item> {
        match self.queue.pop_front() {
            Some((left_index, right_index)) => {
                let middle_index = (left_index + right_index) / 2;
                let result = Middle {
                    left_index,
                    middle_index,
                    right_index,
                };

                if left_index + 1 <= middle_index {
                    self.queue.push_back((left_index, middle_index - 1));
                }

                if middle_index + 1 <= right_index {
                    self.queue.push_back((middle_index + 1, right_index));
                }

                Some(result)
            }
            None => None,
        }
    }
}

#[derive(Clone, Debug)]
struct MergeLocation<A: Clone + Ord> {
    index: u64,
    replace: bool,
    additional_data: A,
}

fn sparse_merge<F, T, A>(
    big_len: u64,
    mut big_get_element: F,
    small: &Vec<T>,
) -> Result<Vec<MergeLocation<A>>, String>
where
    F: FnMut(u64) -> Result<Option<(T, A)>, String>,
    T: Ord,
    A: Clone + Ord + Default,
{
    let mut result_insert_indices: Vec<Option<MergeLocation<A>>> = vec![None; small.len()];
    for middle in Middles::new(small.len()) {
        let element_to_insert = &small[middle.middle_index];

        let left_bound = result_insert_indices[if middle.left_index > 1 {
            middle.left_index - 1
        } else {
            0
        }]
        .clone()
        .map(|merge_location| merge_location.index)
        .unwrap_or(0);
        let right_bound = result_insert_indices
            [std::cmp::min(middle.right_index + 1, result_insert_indices.len() - 1)]
        .clone()
        .map(|merge_location| merge_location.index)
        .unwrap_or(big_len);

        result_insert_indices[middle.middle_index] = Some({
            PartitionPoint::new(left_bound, right_bound, |element_index| {
                let current = big_get_element(element_index)?.unwrap();
                Ok((current.0.cmp(element_to_insert), current.0, current.1))
            })?
            .map_or(
                MergeLocation {
                    index: right_bound,
                    replace: false,
                    additional_data: A::default(),
                },
                |partition_point| MergeLocation {
                    index: partition_point.first_satisfying.index,
                    replace: partition_point.is_exact,
                    additional_data: partition_point.first_satisfying.additional_data,
                },
            )
        });
    }
    Ok(result_insert_indices
        .into_iter()
        .map(Option::unwrap)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nanorand::{Rng, WyRand};

    use pretty_assertions::assert_eq;

    #[test]
    fn test_binary_search() {
        let source: Vec<u64> = (0..10).collect();
        let mut result: Vec<u64> = Vec::with_capacity(source.len());

        for element_to_find in &source {
            result.push(
                PartitionPoint::new(0, (&source.len() - 1) as u64, |element_index| {
                    let current = source[element_index as usize];
                    Ok((current.cmp(element_to_find), current, ()))
                })
                .unwrap()
                .map_or(*source.last().unwrap(), |partition_point| {
                    partition_point.first_satisfying.index
                }),
            );
        }

        assert_eq!(result, source);
    }

    #[test]
    fn test_middles() {
        let middles: Vec<usize> = Middles::new(10).map(|middle| middle.middle_index).collect();
        assert_eq!(middles, vec![4, 1, 7, 0, 2, 5, 8, 3, 6, 9]);
    }

    #[test]
    fn test_sparse_merge() {
        let mut rng = WyRand::new_seed(0);

        let mut big: Vec<u8> = (0..256).map(|_| rng.generate()).collect();
        big.sort();
        big.dedup();
        let mut small: Vec<u8> = (0..256).map(|_| rng.generate()).collect();
        small.sort();
        small.dedup();

        let mut insert_indices: Vec<MergeLocation<()>> = sparse_merge(
            big.len() as u64,
            |element_index| {
                Ok(big
                    .get(element_index as usize)
                    .cloned()
                    .and_then(|element| Some((element, ()))))
            },
            &small,
        )
        .unwrap();

        let mut result = big.clone();
        for (element_index, merge_location) in insert_indices.iter().enumerate().rev() {
            let element = small[element_index];
            if merge_location.replace {
                result[merge_location.index as usize] = element.clone();
            } else {
                result.insert(merge_location.index as usize, element.clone());
            }
        }

        let mut correct_result = [big, small].concat();
        correct_result.sort();
        correct_result.dedup();

        assert_eq!(result, correct_result);
    }
}
