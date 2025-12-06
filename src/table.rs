use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::ops::Bound::{Included, Unbounded};

use fallible_iterator::FallibleIterator;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::data_pool::{DataPool, DataPoolConfig, DataRecord};
use crate::index::{Index, IndexConfig, IndexHeader, IndexIterator};
use crate::keyvalue::{Key, Value};
use crate::merging_iterator::MergingIterator;
use crate::partition_point::PartitionPoint;

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct TableConfig<K: Key, V: Value> {
    pub index: IndexConfig,
    pub data_pool: Box<dyn DataPoolConfig<K, V>>,
}

pub struct Table<K: Key, V: Value> {
    pub index: Index,
    pub data_pool: Box<dyn DataPool<K, V> + Send + Sync>,
    pub memtable: BTreeMap<K, Option<V>>,
}

#[derive(Debug)]
struct MemtableRecord<K: Key, V: Value> {
    key: K,
    value: Option<V>,
}

impl<K: Key, V: Value> PartialEq for MemtableRecord<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<K: Key, V: Value> Eq for MemtableRecord<K, V> {}

impl<K: Key, V: Value> PartialOrd for MemtableRecord<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.key.cmp(&other.key))
    }
}

impl<K: Key, V: Value> Ord for MemtableRecord<K, V> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

fn write_data_id(
    writer: &mut BufWriter<File>,
    data_id: u64,
    record_size: u8,
) -> Result<(), String> {
    let data_id_encoded = data_id.to_le_bytes()[..record_size as usize].to_vec();
    writer
        .write_all(&data_id_encoded)
        .map_err(|error| format!("Can not write data id to file: {error}"))?;
    Ok(())
}

#[derive(Debug)]
struct LinearMergeElement<K: Key> {
    key: K,
    id: Option<u64>,
}

impl<K: Key, V: Value> Table<K, V> {
    pub fn new(config: TableConfig<K, V>) -> Result<Self, String> {
        Ok(Self {
            index: Index::new(config.index)?,
            data_pool: config.data_pool.new_data_pool()?,
            memtable: BTreeMap::new(),
        })
    }

    pub fn merge(&mut self, changes: &mut BTreeMap<K, Option<V>>) {
        self.memtable.append(changes);
    }

    fn get_from_index_by_id(&self, id: u64) -> Result<DataRecord<K, V>, String> {
        self.data_pool.get(id)
    }

    fn get_from_index(&self, key: &K) -> Result<Option<V>, String> {
        Ok(
            PartitionPoint::new(0, self.index.records_count, |record_index| {
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

    pub fn get(&self, key: &K) -> Result<Option<V>, String> {
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
        if self.index.records_count == 0 {
            self.checkpoint_using_dump()
        } else if self.index.records_count <= 2 * self.memtable.len() as u64 {
            self.checkpoint_using_linear_merge()
        } else {
            self.checkpoint_using_sparse_merge()
        }
    }

    fn checkpoint_using_dump(&mut self) -> Result<(), String> {
        let mut ids: Vec<u64> = Vec::new();
        let mut max_id: u64 = 0;
        for current_record in std::mem::take(&mut self.memtable).into_iter() {
            if let Some(value) = current_record.1 {
                let id = self.data_pool.insert(DataRecord {
                    key: current_record.0,
                    value: value,
                })?;
                if id > max_id {
                    max_id = id;
                }
                ids.push(id);
            }
        }
        self.data_pool.flush()?;
        let index_record_size = (max_id as f64).log(256.0).ceil() as u8;

        let index_file_path = self.index.config.path.with_extension("part");
        let mut index_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(&index_file_path)
            .map_err(|error| {
                format!(
                    "Can not create file at path {} for writing: {error}",
                    &index_file_path.display()
                )
            })?;
        Index::write_header(
            &mut index_file,
            &IndexHeader {
                record_size: index_record_size,
            },
        )?;
        let mut index_writer = BufWriter::new(index_file);

        for id in ids.into_iter() {
            write_data_id(&mut index_writer, id, index_record_size)?;
        }
        index_writer
            .flush()
            .map_err(|error| format!("Can not flush new index file: {error}"))?;

        fs::rename(&index_file_path, &self.index.config.path).map_err(|error| {
            format!(
                "Can not overwrite old index at {} with new index at {}: {error}",
                self.index.config.path.display(),
                index_file_path.display()
            )
        })?;
        self.index = Index::new(IndexConfig {
            path: self.index.config.path.clone(),
        })?;

        Ok(())
    }

    fn checkpoint_using_linear_merge(&mut self) -> Result<(), String> {
        let mut new_elements: Vec<LinearMergeElement<K>> = Vec::new();
        let mut max_new_id: u64 = 0;
        for current_new_record in std::mem::take(&mut self.memtable).into_iter() {
            if let Some(value) = current_new_record.1 {
                let id = self.data_pool.insert(DataRecord {
                    key: current_new_record.0.clone(),
                    value: value,
                })?;
                if id > max_new_id {
                    max_new_id = id;
                }
                new_elements.push(LinearMergeElement {
                    key: current_new_record.0,
                    id: Some(id),
                });
            } else {
                new_elements.push(LinearMergeElement {
                    key: current_new_record.0,
                    id: None,
                });
            }
        }
        let new_index_record_size = self
            .index
            .header
            .record_size
            .max((max_new_id as f64).log(256.0).ceil() as u8);

        let new_index_file_path = self.index.config.path.with_extension("part");
        let mut new_index_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(&new_index_file_path)
            .map_err(|error| {
                format!(
                    "Can not create file at path {} for writing: {error}",
                    &new_index_file_path.display()
                )
            })?;
        Index::write_header(
            &mut new_index_file,
            &IndexHeader {
                record_size: new_index_record_size,
            },
        )?;
        let mut new_index_writer = BufWriter::new(new_index_file);

        let mut old_ids_iter = self.index.iter(0)?;
        let mut current_old_id_and_key_option = if let Some(current_old_id) = old_ids_iter.next()? {
            Some((
                current_old_id,
                self.get_from_index_by_id(current_old_id)?.key,
            ))
        } else {
            None
        };

        let mut new_elements_iter = new_elements.into_iter();
        let mut current_new_element_option = new_elements_iter.next();

        loop {
            match (&current_new_element_option, &current_old_id_and_key_option) {
                (Some(current_new_element), Some(current_old_id_and_key)) => {
                    match current_new_element.key.cmp(&current_old_id_and_key.1) {
                        Ordering::Less => {
                            if let Some(current_new_element_id) = current_new_element.id {
                                write_data_id(
                                    &mut new_index_writer,
                                    current_new_element_id,
                                    new_index_record_size,
                                )?;
                            }
                            current_new_element_option = new_elements_iter.next();
                        }
                        Ordering::Greater => {
                            write_data_id(
                                &mut new_index_writer,
                                current_old_id_and_key.0,
                                new_index_record_size,
                            )?;
                            current_old_id_and_key_option =
                                if let Some(current_old_id) = old_ids_iter.next()? {
                                    Some((
                                        current_old_id,
                                        self.get_from_index_by_id(current_old_id)?.key,
                                    ))
                                } else {
                                    None
                                };
                        }
                        Ordering::Equal => {
                            self.data_pool.remove(current_old_id_and_key.0)?;
                            if let Some(current_new_element_id) = current_new_element.id {
                                write_data_id(
                                    &mut new_index_writer,
                                    current_new_element_id,
                                    new_index_record_size,
                                )?;
                            }
                            current_new_element_option = new_elements_iter.next();
                            current_old_id_and_key_option =
                                if let Some(current_old_id) = old_ids_iter.next()? {
                                    Some((
                                        current_old_id,
                                        self.get_from_index_by_id(current_old_id)?.key,
                                    ))
                                } else {
                                    None
                                };
                        }
                    }
                }
                (Some(current_new_element), None) => {
                    if let Some(current_new_element_id) = current_new_element.id {
                        write_data_id(
                            &mut new_index_writer,
                            current_new_element_id,
                            new_index_record_size,
                        )?;
                    }
                    current_new_element_option = new_elements_iter.next();
                }
                (None, Some(current_old_id_and_key)) => {
                    write_data_id(
                        &mut new_index_writer,
                        current_old_id_and_key.0,
                        new_index_record_size,
                    )?;
                    current_old_id_and_key_option =
                        if let Some(current_old_id) = old_ids_iter.next()? {
                            Some((
                                current_old_id,
                                self.get_from_index_by_id(current_old_id)?.key,
                            ))
                        } else {
                            None
                        };
                }
                (None, None) => break,
            }
        }
        self.data_pool.flush()?;
        new_index_writer
            .flush()
            .map_err(|error| format!("Can not flush new index file: {error}"))?;

        fs::rename(&new_index_file_path, &self.index.config.path).map_err(|error| {
            format!(
                "Can not overwrite old index at {} with new index at {}: {error}",
                self.index.config.path.display(),
                new_index_file_path.display()
            )
        })?;
        self.index = Index::new(IndexConfig {
            path: self.index.config.path.clone(),
        })?;

        Ok(())
    }

    fn checkpoint_using_sparse_merge(&mut self) -> Result<(), String> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        let memtable_records: Vec<MemtableRecord<K, V>> = std::mem::take(&mut self.memtable)
            .into_iter()
            .map(|(key, value)| MemtableRecord { key, value })
            .collect();

        let merge_locations = sparse_merge(
            self.index.records_count,
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

        let mut effective_merge_locations: Vec<MergeLocation<u64>> = Vec::new();
        let mut old_ids_to_remove_with_no_replacement: Vec<u64> = Vec::new();
        let mut max_new_id: u64 = 0;
        for (merge_location, current_record) in merge_locations
            .into_iter()
            .zip(memtable_records.into_iter())
        {
            if merge_location.replace {
                self.data_pool.remove(merge_location.additional_data)?;
            }
            if let Some(value) = &current_record.value {
                let new_id = self.data_pool.insert(DataRecord {
                    key: current_record.key.clone(),
                    value: value.clone(),
                })?;
                if new_id > max_new_id {
                    max_new_id = new_id;
                }
                effective_merge_locations.push(MergeLocation {
                    additional_data: new_id,
                    ..merge_location
                });
            } else if merge_location.replace {
                old_ids_to_remove_with_no_replacement.push(merge_location.additional_data);
            }
        }
        self.data_pool.flush()?;

        let new_index_record_size = self
            .index
            .header
            .record_size
            .max((max_new_id as f64).log(256.0).ceil() as u8);

        let new_index_file_path = self.index.config.path.with_extension("part");
        let mut new_index_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(&new_index_file_path)
            .map_err(|error| {
                format!(
                    "Can not create file at path {} for writing: {error}",
                    &new_index_file_path.display()
                )
            })?;
        Index::write_header(
            &mut new_index_file,
            &IndexHeader {
                record_size: new_index_record_size,
            },
        )?;
        let mut new_index_writer = BufWriter::new(new_index_file);

        let mut old_ids_iter = self.index.iter(0)?.enumerate();
        let mut current_old_id_option = old_ids_iter.next()?;

        let mut old_ids_to_remove_with_no_replacement_iter =
            old_ids_to_remove_with_no_replacement.into_iter();
        let mut current_old_id_to_remove_with_no_replacement_option =
            old_ids_to_remove_with_no_replacement_iter.next();

        let mut effective_merge_locations_iter = effective_merge_locations.into_iter();
        let mut current_effective_merge_location_option = effective_merge_locations_iter.next();

        loop {
            if current_old_id_option.is_some_and(|(_, current_old_id)| {
                current_old_id_to_remove_with_no_replacement_option.is_some_and(
                    |current_old_id_to_remove_with_no_replacement| {
                        current_old_id_to_remove_with_no_replacement == current_old_id
                    },
                )
            }) {
                current_old_id_option = old_ids_iter.next()?;
                current_old_id_to_remove_with_no_replacement_option =
                    old_ids_to_remove_with_no_replacement_iter.next();
                continue;
            }
            match (
                &current_effective_merge_location_option,
                current_old_id_option,
            ) {
                (
                    Some(current_effective_merge_location),
                    Some((current_old_id_index, current_old_id)),
                ) => {
                    match &current_old_id_index
                        .cmp(&(current_effective_merge_location.index as usize))
                    {
                        Ordering::Less => {
                            write_data_id(
                                &mut new_index_writer,
                                current_old_id,
                                new_index_record_size,
                            )?;
                            current_old_id_option = old_ids_iter.next()?;
                        }
                        Ordering::Greater => {
                            write_data_id(
                                &mut new_index_writer,
                                current_effective_merge_location.additional_data,
                                new_index_record_size,
                            )?;
                            current_effective_merge_location_option =
                                effective_merge_locations_iter.next();
                        }
                        Ordering::Equal => {
                            if current_effective_merge_location.replace {
                                write_data_id(
                                    &mut new_index_writer,
                                    current_effective_merge_location.additional_data,
                                    new_index_record_size,
                                )?;
                                current_effective_merge_location_option =
                                    effective_merge_locations_iter.next();
                                current_old_id_option = old_ids_iter.next()?;
                            } else {
                                write_data_id(
                                    &mut new_index_writer,
                                    current_effective_merge_location.additional_data,
                                    new_index_record_size,
                                )?;
                                current_effective_merge_location_option =
                                    effective_merge_locations_iter.next();
                            }
                        }
                    }
                }
                (Some(current_effective_merge_location), None) => {
                    write_data_id(
                        &mut new_index_writer,
                        current_effective_merge_location.additional_data,
                        new_index_record_size,
                    )?;
                    current_effective_merge_location_option = effective_merge_locations_iter.next();
                }
                (None, Some((_, current_old_id))) => {
                    write_data_id(&mut new_index_writer, current_old_id, new_index_record_size)?;
                    current_old_id_option = old_ids_iter.next()?;
                }
                (None, None) => break,
            }
        }
        new_index_writer
            .flush()
            .map_err(|error| format!("Can not flush new index file: {error}"))?;

        fs::rename(&new_index_file_path, &self.index.config.path).map_err(|error| {
            format!(
                "Can not overwrite old index at {} with new index at {}: {error}",
                self.index.config.path.display(),
                new_index_file_path.display()
            )
        })?;
        self.index = Index::new(IndexConfig {
            path: self.index.config.path.clone(),
        })?;

        Ok(())
    }

    fn iter_index(&'_ self, from_key: Option<&K>) -> Result<TableIndexIterator<'_, K, V>, String> {
        if let Some(from_key) = from_key {
            Ok(TableIndexIterator {
                data_pool: &self.data_pool,
                index_iter: self.index.iter(
                    PartitionPoint::new(0, self.index.records_count, |record_index| {
                        let data_record_id = self.index.get(record_index)?.ok_or(format!(
                            "Can not get data record id at index {record_index}"
                        ))?;
                        let data_record = self.get_from_index_by_id(data_record_id)?;
                        Ok((data_record.key.cmp(from_key), data_record.value, ()))
                    })?
                    .map(|partition_point| partition_point.first_satisfying.index)
                    .unwrap_or(self.index.records_count + 1),
                )?,
            })
        } else {
            Ok(TableIndexIterator {
                data_pool: &self.data_pool,
                index_iter: self.index.iter(0)?,
            })
        }
    }

    pub fn iter(
        &'_ self,
        from_key: Option<&K>,
    ) -> Result<Box<dyn FallibleIterator<Item = (K, V), Error = String> + '_>, String> {
        Ok(Box::new(MergingIterator::new(
            self.memtable.range::<K, _>((
                (if let Some(from_key) = from_key {
                    Included(from_key)
                } else {
                    Unbounded
                }),
                Unbounded,
            )),
            Box::new(self.iter_index(from_key)?),
        )?))
    }
}

struct TableIndexIterator<'a, K: Key, V: Value> {
    data_pool: &'a Box<dyn DataPool<K, V> + Send + Sync>,
    index_iter: IndexIterator,
}

impl<'a, K: Key, V: Value> FallibleIterator for TableIndexIterator<'a, K, V> {
    type Item = (K, V);
    type Error = String;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        Ok(match self.index_iter.next()? {
            Some(id) => {
                let data_record = self.data_pool.get(id)?;
                Some((data_record.key, data_record.value))
            }
            None => None,
        })
    }
}

struct Middles {
    queue: VecDeque<(usize, usize)>,
}

impl Middles {
    fn new(source_size: usize) -> Self {
        let mut queue: VecDeque<(usize, usize)> = VecDeque::new();
        queue.push_back((0, source_size - 1));
        Self { queue }
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
                let current = big_get_element(element_index)?
                    .ok_or(format!("Can not get element at index {element_index}"))?;
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
    let mut result: Vec<MergeLocation<A>> = Vec::with_capacity(result_insert_indices.len());
    for insert_index in result_insert_indices.into_iter() {
        result.push(insert_index.ok_or("Can not find where to insert element using sparse merge")?);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::variable_data_pool::VariableDataPoolConfig;

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
    fn test_sparse_merge_simple() {
        let big = vec![vec![0 as u8], vec![2 as u8], vec![4 as u8]];
        let small = vec![vec![1 as u8], vec![3 as u8]];

        let insert_indices: Vec<MergeLocation<()>> = sparse_merge(
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

        assert_eq!(insert_indices[0].index, 1);
        assert_eq!(insert_indices[1].index, 2);
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

        let insert_indices: Vec<MergeLocation<()>> = sparse_merge(
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

    fn new_default_table<K: Key, V: Value>(test_name_for_isolation: &str) -> Table<K, V> {
        let table_dir =
            Path::new(format!("/tmp/lawn/test/table/{test_name_for_isolation}/").as_str())
                .to_path_buf();
        let mut result = Table::new(TableConfig {
            index: IndexConfig {
                path: table_dir.join("index.idx").to_path_buf(),
            },
            data_pool: Box::new(VariableDataPoolConfig {
                directory: table_dir.join("data_pool").to_path_buf(),
                max_element_size: 65536 as usize,
            }),
        })
        .unwrap();
        result.clear().unwrap();
        result
    }

    #[test]
    fn test_checkpoint_2_in_3() {
        let mut table = new_default_table::<Vec<u8>, Vec<u8>>("test_checkpoint_2_in_3");

        let first_keyvalues = vec![
            (vec![0 as u8, 0 as u8], Some(vec![1 as u8, 0 as u8])),
            (vec![0 as u8, 2 as u8], Some(vec![1 as u8, 2 as u8])),
            (vec![0 as u8, 4 as u8], Some(vec![1 as u8, 4 as u8])),
        ];
        {
            let keyvalues = &first_keyvalues;
            for (key, value) in keyvalues.iter() {
                table.memtable.insert(key.clone(), value.clone());
            }
            table.checkpoint().unwrap();
            for (key, value) in keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
        }
        println!("after first checkpoint");
        {
            let keyvalues = vec![
                (vec![0 as u8, 1 as u8], Some(vec![1 as u8, 1 as u8])),
                (vec![0 as u8, 3 as u8], Some(vec![1 as u8, 3 as u8])),
            ];
            for (key, value) in keyvalues.iter() {
                table.memtable.insert(key.clone(), value.clone());
            }
            table.checkpoint().unwrap();
            for (key, value) in keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
            for (key, value) in first_keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
        }
    }

    #[test]
    fn test_checkpoint_3_in_2() {
        let mut table = new_default_table::<Vec<u8>, Vec<u8>>("test_checkpoint_3_in_2");

        let first_keyvalues = vec![
            (vec![0 as u8, 1 as u8], Some(vec![1 as u8, 1 as u8])),
            (vec![0 as u8, 3 as u8], Some(vec![1 as u8, 3 as u8])),
        ];
        {
            let keyvalues = &first_keyvalues;
            for (key, value) in keyvalues.iter() {
                table.memtable.insert(key.clone(), value.clone());
            }
            table.checkpoint().unwrap();
            for (key, value) in keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
            for (key, value) in first_keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
        }
        {
            let keyvalues = vec![
                (vec![0 as u8, 0 as u8], Some(vec![1 as u8, 0 as u8])),
                (vec![0 as u8, 2 as u8], Some(vec![1 as u8, 2 as u8])),
                (vec![0 as u8, 4 as u8], Some(vec![1 as u8, 4 as u8])),
            ];
            for (key, value) in keyvalues.iter() {
                table.memtable.insert(key.clone(), value.clone());
            }
            table.checkpoint().unwrap();
            for (key, value) in keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
        }
    }

    #[test]
    fn test_checkpoint_2_after_3() {
        let mut table = new_default_table::<Vec<u8>, Vec<u8>>("test_checkpoint_2_after_3");

        let first_keyvalues = vec![
            (vec![0 as u8, 0 as u8], Some(vec![1 as u8, 0 as u8])),
            (vec![0 as u8, 2 as u8], Some(vec![1 as u8, 2 as u8])),
            (vec![0 as u8, 4 as u8], Some(vec![1 as u8, 4 as u8])),
        ];
        {
            let keyvalues = &first_keyvalues;
            for (key, value) in keyvalues.iter() {
                table.memtable.insert(key.clone(), value.clone());
            }
            table.checkpoint().unwrap();
            for (key, value) in keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
        }
        {
            let keyvalues = vec![
                (vec![0 as u8, 5 as u8], Some(vec![1 as u8, 5 as u8])),
                (vec![0 as u8, 6 as u8], Some(vec![1 as u8, 6 as u8])),
            ];
            for (key, value) in keyvalues.iter() {
                table.memtable.insert(key.clone(), value.clone());
            }
            table.checkpoint().unwrap();
            for (key, value) in keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
            for (key, value) in first_keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
        }
    }

    #[test]
    fn test_checkpoint_2_remove_1_add_after_10() {
        let mut table =
            new_default_table::<Vec<u8>, Vec<u8>>("test_checkpoint_2_remove_1_add_after_10");

        let first_keyvalues = vec![
            (vec![0 as u8, 0 as u8], Some(vec![1 as u8, 0 as u8])),
            (vec![0 as u8, 1 as u8], Some(vec![1 as u8, 1 as u8])),
            (vec![0 as u8, 2 as u8], Some(vec![1 as u8, 2 as u8])),
            (vec![0 as u8, 3 as u8], Some(vec![1 as u8, 3 as u8])),
            (vec![0 as u8, 4 as u8], Some(vec![1 as u8, 4 as u8])),
            (vec![0 as u8, 5 as u8], Some(vec![1 as u8, 5 as u8])),
            (vec![0 as u8, 6 as u8], Some(vec![1 as u8, 6 as u8])),
            (vec![0 as u8, 7 as u8], Some(vec![1 as u8, 7 as u8])),
            (vec![0 as u8, 8 as u8], Some(vec![1 as u8, 8 as u8])),
            (vec![0 as u8, 9 as u8], Some(vec![1 as u8, 9 as u8])),
        ];
        {
            let keyvalues = &first_keyvalues;
            for (key, value) in keyvalues.iter() {
                table.memtable.insert(key.clone(), value.clone());
            }
            table.checkpoint().unwrap();
            for (key, value) in keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
        }
        {
            let keyvalues = vec![
                (vec![0 as u8, 10 as u8], None),
                (vec![0 as u8, 11 as u8], None),
                (vec![0 as u8, 12 as u8], Some(vec![1 as u8, 12 as u8])),
            ];
            for (key, value) in keyvalues.iter() {
                table.memtable.insert(key.clone(), value.clone());
            }
            table.checkpoint().unwrap();
            for (key, value) in keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
            for (key, value) in first_keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
        }
    }

    #[test]
    fn test_checkpoint_2_before_3() {
        let mut table = new_default_table::<Vec<u8>, Vec<u8>>("test_checkpoint_2_before_3");

        let first_keyvalues = vec![
            (vec![0 as u8, 2 as u8], Some(vec![1 as u8, 2 as u8])),
            (vec![0 as u8, 3 as u8], Some(vec![1 as u8, 3 as u8])),
            (vec![0 as u8, 4 as u8], Some(vec![1 as u8, 4 as u8])),
        ];
        {
            let keyvalues = &first_keyvalues;
            for (key, value) in keyvalues.iter() {
                table.memtable.insert(key.clone(), value.clone());
            }
            table.checkpoint().unwrap();
            for (key, value) in keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
        }
        {
            let keyvalues = vec![
                (vec![0 as u8, 0 as u8], Some(vec![1 as u8, 0 as u8])),
                (vec![0 as u8, 1 as u8], Some(vec![1 as u8, 1 as u8])),
            ];
            for (key, value) in keyvalues.iter() {
                table.memtable.insert(key.clone(), value.clone());
            }
            table.checkpoint().unwrap();
            for (key, value) in keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
            for (key, value) in first_keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
        }
    }

    #[test]
    fn test_checkpoint_replace() {
        let mut table = new_default_table::<Vec<u8>, Vec<u8>>("test_checkpoint_replace");

        {
            let keyvalues = vec![
                (vec![0 as u8, 0 as u8], Some(vec![1 as u8, 0 as u8])),
                (vec![0 as u8, 1 as u8], Some(vec![1 as u8, 1 as u8])),
                (vec![0 as u8, 2 as u8], Some(vec![1 as u8, 2 as u8])),
            ];
            for (key, value) in keyvalues.iter() {
                table.memtable.insert(key.clone(), value.clone());
            }
            table.checkpoint().unwrap();
            for (key, value) in keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
        }
        {
            let keyvalues = vec![
                (vec![0 as u8, 0 as u8], Some(vec![1 as u8, 3 as u8])),
                (vec![0 as u8, 1 as u8], Some(vec![1 as u8, 4 as u8])),
                (vec![0 as u8, 2 as u8], Some(vec![1 as u8, 5 as u8])),
            ];
            for (key, value) in keyvalues.iter() {
                table.memtable.insert(key.clone(), value.clone());
            }
            table.checkpoint().unwrap();
            for (key, value) in keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
        }
    }

    #[test]
    fn test_checkpoint_delete_middle() {
        let mut table = new_default_table::<Vec<u8>, Vec<u8>>("test_checkpoint_delete_middle");

        let mut keyvalues = vec![
            (vec![0 as u8, 0 as u8], Some(vec![1 as u8, 0 as u8])),
            (vec![0 as u8, 1 as u8], Some(vec![1 as u8, 1 as u8])),
            (vec![0 as u8, 2 as u8], Some(vec![1 as u8, 2 as u8])),
        ];
        {
            for (key, value) in keyvalues.iter() {
                table.memtable.insert(key.clone(), value.clone());
            }
            table.checkpoint().unwrap();
            for (key, value) in keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
        }
        {
            table
                .memtable
                .insert(keyvalues[1].0.clone(), keyvalues[1].1.clone());
            keyvalues.remove(1);
            table.checkpoint().unwrap();
            for (key, value) in keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
        }
    }

    #[test]
    fn test_checkpoint_delete_first() {
        let mut table = new_default_table::<Vec<u8>, Vec<u8>>("test_checkpoint_delete_first");

        let mut keyvalues = vec![
            (vec![0 as u8, 0 as u8], Some(vec![1 as u8, 0 as u8])),
            (vec![0 as u8, 1 as u8], Some(vec![1 as u8, 1 as u8])),
            (vec![0 as u8, 2 as u8], Some(vec![1 as u8, 2 as u8])),
        ];
        {
            for (key, value) in keyvalues.iter() {
                table.memtable.insert(key.clone(), value.clone());
            }
            table.checkpoint().unwrap();
            for (key, value) in keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
        }
        {
            table.memtable.insert(
                keyvalues.first().unwrap().0.clone(),
                keyvalues.first().unwrap().1.clone(),
            );
            keyvalues.remove(0);
            table.checkpoint().unwrap();
            for (key, value) in keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
        }
    }

    #[test]
    fn test_checkpoint_delete_last() {
        let mut table = new_default_table::<Vec<u8>, Vec<u8>>("test_checkpoint_delete_last");

        let mut keyvalues = vec![
            (vec![0 as u8, 0 as u8], Some(vec![1 as u8, 0 as u8])),
            (vec![0 as u8, 1 as u8], Some(vec![1 as u8, 1 as u8])),
            (vec![0 as u8, 2 as u8], Some(vec![1 as u8, 2 as u8])),
        ];
        {
            for (key, value) in keyvalues.iter() {
                table.memtable.insert(key.clone(), value.clone());
            }
            table.checkpoint().unwrap();
            for (key, value) in keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
        }
        {
            table.memtable.insert(
                keyvalues.last().unwrap().0.clone(),
                keyvalues.last().unwrap().1.clone(),
            );
            keyvalues.remove(keyvalues.len() - 1);
            table.checkpoint().unwrap();
            for (key, value) in keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), *value);
            }
        }
    }

    #[test]
    fn test_checkpoint_generative() {
        let mut table = new_default_table::<Vec<u8>, Vec<u8>>("test_checkpoint_generative");

        let mut previously_added_keyvalues: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut rng = WyRand::new_seed(0);

        for checkpoint_number in 1..=20 {
            for _ in 1..=20 {
                let random_byte = rng.generate_range(0..256) as u8;
                let key = vec![0 as u8, random_byte];
                let value = if rng.generate_range(0..=1) == 0 {
                    None
                } else {
                    Some(vec![1 as u8, random_byte])
                };
                if let Some(value) = &value {
                    println!("insert {random_byte}");
                    previously_added_keyvalues.insert(key.clone(), value.clone());
                } else {
                    println!("remove {random_byte}");
                    previously_added_keyvalues.remove(&key);
                }
                table.memtable.insert(key, value);
            }

            table
                .iter(None)
                .unwrap()
                .map(|(key, _)| Ok(key))
                .unwrap()
                .zip(previously_added_keyvalues.keys())
                .for_each(|(table_key, correct_table_key)| {
                    assert_eq!(&table_key, correct_table_key)
                });

            let correct_table_keys: Vec<Vec<u8>> =
                previously_added_keyvalues.keys().cloned().collect();
            for key_index in 0..correct_table_keys.len() {
                let table_keys: Vec<Vec<u8>> = table
                    .iter(Some(&correct_table_keys[key_index]))
                    .unwrap()
                    .map(|(key, _)| Ok(key))
                    .unwrap()
                    .collect();
                assert_eq!(table_keys, correct_table_keys[key_index..]);
            }

            println!("checkpoint {checkpoint_number}\n");
            table.checkpoint().unwrap();
            for (key, value) in previously_added_keyvalues.iter() {
                assert_eq!(table.get_from_index(&key).unwrap(), Some(value.clone()));
            }
        }
    }
}
