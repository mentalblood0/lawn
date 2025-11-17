use bincode;
use std::collections::{BTreeMap, VecDeque};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::binary_search::{Direction, binary_search};
use crate::data_pool::{DataPool, DataPoolConfig};
use crate::index::{Index, IndexConfig};

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
        let low_and_high = binary_search(
            (0 as u64, ()),
            (self.index.get_records_count(), ()),
            |record_index| {
                let data_record_id = self
                    .index
                    .get(record_index)?
                    .ok_or(format!("Can not get data_id at index {record_index}"))?;
                let data_record = self.get_from_index_by_id(data_record_id)?;
                if &data_record.key > key {
                    Ok(Direction::Low(()))
                } else {
                    Ok(Direction::High(()))
                }
            },
        )?;
        let result_data_record_id = low_and_high.1.0;
        let result_data_record = self.get_from_index_by_id(result_data_record_id)?;
        if &result_data_record.key == key {
            Ok(Some(result_data_record.value))
        } else {
            Ok(None)
        }
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

fn sparse_merge<F, A, B>(
    big_size: u64,
    mut big_get_element: F,
    small: Vec<Vec<u8>>,
) -> Result<Vec<u64>, String>
where
    F: FnMut(u64) -> Result<Option<Vec<u8>>, String>,
{
    let mut result_insert_indexes: Vec<Option<u64>> = vec![None; small.len()];
    for middle in Middles::new(small.len()) {
        let element_to_insert = &small[middle.middle_index];

        let left_bound =
            result_insert_indexes[std::cmp::max(0, middle.left_index - 1)].unwrap_or(0);
        let right_bound = result_insert_indexes
            [std::cmp::min(middle.right_index + 1, result_insert_indexes.len() - 1)]
        .unwrap_or(big_size - 1);

        let insert_index = binary_search((left_bound, ()), (right_bound, ()), |element_index| {
            match big_get_element(element_index)? {
                Some(current) => {
                    if current >= *element_to_insert {
                        Ok(Direction::Low(()))
                    } else {
                        Ok(Direction::High(()))
                    }
                }
                None => Ok(Direction::Low(())),
            }
        })?
        .1
        .0;

        result_insert_indexes[middle.middle_index] = Some(insert_index);
    }
    Ok(result_insert_indexes
        .into_iter()
        .map(Option::unwrap)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_middles() {
        let middles: Vec<usize> = Middles::new(10).map(|middle| middle.middle_index).collect();
        assert_eq!(middles, vec![4, 1, 7, 0, 2, 5, 8, 3, 6, 9]);
    }
}
