use bincode;
use std::collections::{BTreeMap, VecDeque};

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
                let data_record_id = self
                    .index
                    .get(record_index)?
                    .ok_or(format!("Can not get data_id at index {record_index}"))?;
                let data_record = self.get_from_index_by_id(data_record_id)?;
                Ok((data_record.key.cmp(key), data_record.value))
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

fn sparse_merge<F, T>(
    big_len: u64,
    mut big_get_element: F,
    small: &Vec<T>,
) -> Result<Vec<u64>, String>
where
    F: FnMut(u64) -> Result<Option<T>, String>,
    T: Ord,
    T: std::fmt::Debug,
{
    let mut result_insert_indexes: Vec<Option<u64>> = vec![None; small.len()];
    for middle in Middles::new(small.len()) {
        let element_to_insert = &small[middle.middle_index];

        let left_bound = result_insert_indexes[if middle.left_index > 1 {
            middle.left_index - 1
        } else {
            0
        }]
        .unwrap_or(0);
        let right_bound = result_insert_indexes
            [std::cmp::min(middle.right_index + 1, result_insert_indexes.len() - 1)]
        .unwrap_or(big_len - 1);

        result_insert_indexes[middle.middle_index] = Some({
            PartitionPoint::new(left_bound, right_bound, |element_index| {
                let current = big_get_element(element_index)?.unwrap();
                Ok((current.cmp(element_to_insert), current))
            })?
            .map_or(right_bound, |partition_point| {
                partition_point.first_satisfying.index
            })
        });
    }
    Ok(result_insert_indexes
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
                    Ok((current.cmp(element_to_find), current))
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
        const ELEMENT_SIZE: usize = 16;

        let mut rng = WyRand::new_seed(0);

        let mut big: Vec<usize> = (0..1000).map(|_| rng.generate()).collect();
        big.sort();
        let mut small: Vec<usize> = (0..100).map(|_| rng.generate()).collect();
        small.sort();

        let mut insert_indexes: Vec<(usize, u64)> = sparse_merge(
            big.len() as u64,
            |element_index| Ok(big.get(element_index as usize).cloned()),
            &small,
        )
        .unwrap()
        .iter()
        .enumerate()
        .map(|(element_index, insert_index)| (small[element_index], *insert_index))
        .collect();
        insert_indexes.sort_by_key(|(element, insert_index)| *insert_index);

        let mut result = big.clone();
        for (element, insert_index) in insert_indexes.iter().rev() {
            result.insert(*insert_index as usize, element.clone());
        }

        let mut correct_result = [big, small].concat();
        correct_result.sort();

        assert_eq!(result, correct_result);
    }
}
