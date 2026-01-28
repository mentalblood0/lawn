use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::ops::Bound;

use anyhow::{Context, Error, Result, anyhow};
use fallible_iterator::FallibleIterator;

use serde::{Deserialize, Serialize};

use crate::data_pool::{DataPool, DataPoolConfig};
use crate::fixed_data_pool::FixedDataPoolConfig;
use crate::index::{Index, IndexConfig, IndexHeader};
use crate::keyvalue::{Key, Value};
use crate::merging_iterator::MergingIterator;
use crate::partition_point::PartitionPoint;
use crate::variable_data_pool::VariableDataPoolConfig;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataPoolConfigEnum {
    #[serde(alias = "fixed")]
    Fixed(FixedDataPoolConfig),
    #[serde(alias = "variable")]
    Variable(VariableDataPoolConfig),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableConfig<K: Key, V: Value> {
    pub index: IndexConfig,
    pub data_pool: DataPoolConfigEnum,

    #[serde(skip)]
    pub _key: PhantomData<K>,
    #[serde(skip)]
    pub _value: PhantomData<V>,
}

pub struct Table<K: Key, V: Value> {
    pub index: Index,
    pub data_pool: Box<dyn DataPool<DataRecord<K, V>> + Send + Sync>,
    pub memtable: BTreeMap<K, Option<V>>,
}

#[derive(Debug, Clone)]
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

#[derive(bincode::Encode, bincode::Decode, Debug, Clone, PartialEq)]
pub struct DataRecord<K: Key, V: Value> {
    key: K,
    value: V,
}

fn write_data_id(writer: &mut BufWriter<File>, data_id: u64, record_size: u8) -> Result<()> {
    let data_id_encoded = data_id.to_le_bytes()[..record_size as usize].to_vec();
    writer
        .write_all(&data_id_encoded)
        .with_context(|| format!("Can not write data id to file"))?;
    Ok(())
}

#[derive(Debug, Clone)]
struct LinearMergeElement<K: Key> {
    key: K,
    id: Option<u64>,
}

impl<K: Key, V: Value> Table<K, V> {
    pub fn new(config: TableConfig<K, V>) -> Result<Self> {
        Ok(Self {
            index: Index::new(config.index.clone()).with_context(|| {
                format!("Can not create table with index config {:?}", config.index)
            })?,
            data_pool: match config.data_pool {
                DataPoolConfigEnum::Fixed(config) => <FixedDataPoolConfig as DataPoolConfig<
                    DataRecord<K, V>,
                >>::new_data_pool(&config)
                .with_context(|| {
                    format!("Can not create fixed data pool from config {config:?}")
                })?,
                DataPoolConfigEnum::Variable(config) => {
                    <VariableDataPoolConfig as DataPoolConfig<DataRecord<K, V>>>::new_data_pool(
                        &config,
                    )
                    .with_context(|| {
                        format!("Can not create variable data pool from config {config:?}")
                    })?
                }
            },
            memtable: BTreeMap::new(),
        })
    }

    pub fn merge(&mut self, changes: &mut BTreeMap<K, Option<V>>) {
        self.memtable.append(changes);
    }

    fn get_from_index_by_id(&self, id: u64) -> Result<DataRecord<K, V>> {
        self.data_pool.get(id)
    }

    fn get_from_index(&self, key: &K) -> Result<Option<V>> {
        Ok(
            PartitionPoint::new(0, self.index.records_count, |record_index| {
                let data_record_id = self
                    .index
                    .get(record_index)
                    .with_context(|| {
                        format!(
                            "Can not get data record id as record with index {record_index:?} from {:?}", self.index
                        )
                    })?
                    .ok_or(anyhow!(
                        "Can not get data record id at index {record_index:?} from {:?} (got nothing)", self.index
                    ))?;
                let data_record = self.get_from_index_by_id(data_record_id).with_context(|| format!("Can not get data record from index {:?} using id {data_record_id:?}", self.index))?;
                Ok((data_record.key.cmp(key), data_record.value, ()))
            }).with_context(|| format!("Can not create partition point for {:?} to search key {key:?}", self.index))?
            .filter(|partition_point| partition_point.is_exact)
            .map(|partition_point| partition_point.first_satisfying.value),
        )
    }

    pub fn get(&self, key: &K) -> Result<Option<V>> {
        match self.memtable.get(key) {
            Some(value) => Ok(value.clone()),
            None => Ok(self.get_from_index(key).with_context(|| {
                format!(
                    "Can not get value by key {key:?} from index {:?}",
                    self.index
                )
            })?),
        }
    }

    pub fn clear(&mut self) -> Result<()> {
        self.index.clear().with_context(|| {
            format!("Can not clear index {:?} while clearing table", self.index)
        })?;
        self.data_pool
            .clear()
            .with_context(|| "Can not clear data pool while clearing table")?;
        self.memtable.clear();
        Ok(())
    }

    pub fn checkpoint(&mut self) -> Result<()> {
        if self.memtable.is_empty() {
            Ok(())
        } else {
            if self.index.records_count == 0 {
                self.checkpoint_using_dump()
            } else if self.index.records_count <= 2 * self.memtable.len() as u64 {
                self.checkpoint_using_linear_merge()
            } else {
                self.checkpoint_using_sparse_merge()
            }
        }
    }

    fn checkpoint_using_dump(&mut self) -> Result<()> {
        let mut ids: Vec<u64> = Vec::new();
        let mut max_id: u64 = 0;
        for current_record in std::mem::take(&mut self.memtable).into_iter() {
            if let Some(value) = current_record.1 {
                let data_record_to_insert = DataRecord {
                    key: current_record.0,
                    value: value,
                };
                let id = self
                    .data_pool
                    .insert(data_record_to_insert.clone())
                    .with_context(|| format!("Can not insert {data_record_to_insert:?} while checkpointing table using dump method"))?;
                if id > max_id {
                    max_id = id;
                }
                ids.push(id);
            }
        }
        self.data_pool
            .flush()
            .with_context(|| "Can not flush data pool")?;
        let index_record_size = (max_id as f64).log(256.0).ceil() as u8;

        let index_file_path = self.index.config.path.with_extension("part");
        let mut index_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(&index_file_path)
            .with_context(|| {
                format!(
                    "Can not create file at path {} for writing",
                    &index_file_path.display()
                )
            })?;
        let header_to_write = IndexHeader {
            record_size: index_record_size,
        };
        Index::write_header(&mut index_file, &header_to_write)
            .with_context(|| format!("Can not write header {header_to_write:?} for index while checkpointing table using dump method"))?;
        let mut index_writer = BufWriter::new(index_file);

        for id in ids.into_iter() {
            write_data_id(&mut index_writer, id, index_record_size).with_context(|| format!("Can not write data id {id:?} using {index_writer:?} aligning it with index record size {index_record_size:?}"))?;
        }
        index_writer
            .flush()
            .with_context(|| format!("Can not flush new index file"))?;

        fs::rename(&index_file_path, &self.index.config.path).with_context(|| {
            format!(
                "Can not overwrite old index at {} with new index at {}",
                self.index.config.path.display(),
                index_file_path.display()
            )
        })?;
        let new_index_config = IndexConfig {
            path: self.index.config.path.clone(),
        };
        self.index = Index::new(new_index_config.clone()).with_context(|| {
            format!("Can not create new index with config {new_index_config:?}")
        })?;

        Ok(())
    }

    fn checkpoint_using_linear_merge(&mut self) -> Result<()> {
        let mut new_elements: Vec<LinearMergeElement<K>> = Vec::new();
        let mut max_new_id: u64 = 0;
        for current_new_record in std::mem::take(&mut self.memtable).into_iter() {
            if let Some(value) = current_new_record.1 {
                let data_record_to_insert = DataRecord {
                    key: current_new_record.0.clone(),
                    value: value,
                };
                let id = self
                    .data_pool
                    .insert(data_record_to_insert.clone())
                    .with_context(|| {
                        format!(
                            "Can not insert data record {data_record_to_insert:?} into data pool"
                        )
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
            .with_context(|| {
                format!(
                    "Can not create file at path {} for writing",
                    &new_index_file_path.display()
                )
            })?;
        let header_to_write = IndexHeader {
            record_size: new_index_record_size,
        };
        Index::write_header(
            &mut new_index_file,
            &header_to_write,
        ).with_context(|| format!("Can not write header {header_to_write:?} for index while checkpointing table using dump method"))?;
        let mut new_index_writer = BufWriter::new(new_index_file);

        let mut old_ids_iter = self
            .index
            .iter(0, false)
            .with_context(|| "Can not initiate iteration over index from the beginning")?;
        let mut current_old_id_and_key_option = if let Some(current_old_id) =
            old_ids_iter.next().with_context(|| {
                format!("Can not get first old identifier (even if there is no such)")
            })? {
            Some((
                current_old_id,
                self.get_from_index_by_id(current_old_id)
                    .with_context(|| {
                        format!("Can not get value from index by id {current_old_id:?}")
                    })?
                    .key,
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
                                )
                                .with_context(|| format!("Can not write data id (current new element id) {current_new_element_id:?} using {new_index_writer:?} aligning it with index record size {new_index_record_size:?}"))?;
                            }
                            current_new_element_option = new_elements_iter.next();
                        }
                        Ordering::Greater => {
                            write_data_id(
                                &mut new_index_writer,
                                current_old_id_and_key.0,
                                new_index_record_size,
                            ).with_context(|| {
                                format!(
                                    "Can not write data id (current old id) {:?} using {new_index_writer:?} aligning it with index record size {new_index_record_size:?}",
                                    current_old_id_and_key.0
                                )
                            })?;
                            current_old_id_and_key_option =
                                if let Some(current_old_id) = old_ids_iter.next().with_context(|| format!("Can not propagate old identifiers iterator further (even getting nothing)"))? {
                                    Some((
                                        current_old_id,
                                        self.get_from_index_by_id(current_old_id).with_context(|| {
                                            format!("Can not get value from index by id (current old id) {current_old_id:?}")
                                        })?.key,
                                    ))
                                } else {
                                    None
                                };
                        }
                        Ordering::Equal => {
                            self.data_pool
                                .remove(current_old_id_and_key.0)
                                .with_context(|| {
                                    format!(
                                        "Can not remove value from data pool by id {:?}",
                                        current_old_id_and_key.0
                                    )
                                })?;
                            if let Some(current_new_element_id) = current_new_element.id {
                                write_data_id(
                                    &mut new_index_writer,
                                    current_new_element_id,
                                    new_index_record_size,
                                ).with_context(|| {
                                    format!(
                                        "Can not write data id (current new element id) {:?} using {new_index_writer:?} aligning it with index record size {new_index_record_size:?}",
                                        current_new_element_id
                                    )
                                })?;
                            }
                            current_new_element_option = new_elements_iter.next();
                            current_old_id_and_key_option =
                                if let Some(current_old_id) = old_ids_iter.next().with_context(|| format!("Can not propagate old identifiers iterator further (even getting nothing)"))? {
                                    Some((
                                        current_old_id,
                                        self.get_from_index_by_id(current_old_id).with_context(|| {
                                            format!("Can not get value from index by id (current old id) {current_old_id:?}")
                                        })?.key,
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
                        ).with_context(|| {
                            format!(
                                "Can not write data id (current new element id) {:?} using {new_index_writer:?} aligning it with index record size {new_index_record_size:?}",
                                current_new_element_id
                            )
                        })?;
                    }
                    current_new_element_option = new_elements_iter.next();
                }
                (None, Some(current_old_id_and_key)) => {
                    write_data_id(
                        &mut new_index_writer,
                        current_old_id_and_key.0,
                        new_index_record_size,
                    ).with_context(|| {
                        format!(
                            "Can not write data id (current old id) {:?} using {new_index_writer:?} aligning it with index record size {new_index_record_size:?}",
                            current_old_id_and_key.0
                        )
                    })?;
                    current_old_id_and_key_option =
                        if let Some(current_old_id) = old_ids_iter.next().with_context(|| format!("Can not propagate old identifiers iterator further (even getting nothing)"))? {
                            Some((
                                current_old_id,
                                self.get_from_index_by_id(current_old_id).with_context(|| {
                                    format!("Can not get value from index by id (current old id) {current_old_id:?}")
                                })?.key,
                            ))
                        } else {
                            None
                        };
                }
                (None, None) => break,
            }
        }
        self.data_pool
            .flush()
            .with_context(|| "Can not flush data pool")?;
        new_index_writer
            .flush()
            .with_context(|| format!("Can not flush new index file"))?;

        fs::rename(&new_index_file_path, &self.index.config.path).with_context(|| {
            format!(
                "Can not overwrite old index at {} with new index at {}",
                self.index.config.path.display(),
                new_index_file_path.display()
            )
        })?;
        let new_index_config = IndexConfig {
            path: self.index.config.path.clone(),
        };
        self.index = Index::new(new_index_config.clone()).with_context(|| {
            format!("Can not create new index with config {new_index_config:?}")
        })?;

        Ok(())
    }

    fn checkpoint_using_sparse_merge(&mut self) -> Result<()> {
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
                let data_record_id = self.index.get(data_record_id_index)?.with_context(|| {
                    format!("Can not get data record id at index {data_record_id_index}")
                })?;
                let data_record = self.get_from_index_by_id(data_record_id).with_context(|| {
                    format!("Can not get value from index by id {data_record_id:?}")
                })?;
                Ok(Some((
                    MemtableRecord {
                        key: data_record.key,
                        value: Some(data_record.value),
                    },
                    data_record_id,
                )))
            },
            &memtable_records,
        )
        .with_context(|| {
            format!(
                "Can not get merge locations using sparse merge on index {:?}",
                self.index
            )
        })?;

        let mut effective_merge_locations: Vec<MergeLocation<u64>> = Vec::new();
        let mut old_ids_to_remove_with_no_replacement: Vec<u64> = Vec::new();
        let mut max_new_id: u64 = 0;
        for (merge_location, current_record) in merge_locations
            .into_iter()
            .zip(memtable_records.into_iter())
        {
            if merge_location.replace {
                self.data_pool.remove(merge_location.additional_data).with_context(|| format!("Can not remove value from data pool by id {:?} so to replace it with another", merge_location.additional_data))?;
            }
            if let Some(value) = &current_record.value {
                let data_record_to_insert = DataRecord {
                    key: current_record.key.clone(),
                    value: value.clone(),
                };
                let new_id = self
                    .data_pool
                    .insert(data_record_to_insert.clone())
                    .with_context(|| {
                        format!(
                            "Can not insert data record {data_record_to_insert:?} into data pool"
                        )
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
        self.data_pool
            .flush()
            .with_context(|| format!("Can not flush new index file"))?;

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
            .with_context(|| {
                format!(
                    "Can not create file at path {} for writing",
                    &new_index_file_path.display()
                )
            })?;
        let header_to_write = IndexHeader {
            record_size: new_index_record_size,
        };
        Index::write_header(
            &mut new_index_file,
            &header_to_write,
        ).with_context(|| format!("Can not write header {header_to_write:?} for index while checkpointing table using dump method"))?;
        let mut new_index_writer = BufWriter::new(new_index_file);

        let mut old_ids_iter = self
            .index
            .iter(0, false)
            .with_context(|| "Can not initiate iteration over index from the beginning")?
            .enumerate();
        let mut current_old_id_option = old_ids_iter.next().with_context(|| {
            format!(
                "Can not get first identifier (or even nothing) from old index {:?}",
                self.index
            )
        })?;

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
                current_old_id_option = old_ids_iter.next().with_context(|| format!("Can not propagate old index {:?} iterator further (even if there is no identifiers to receive)", self.index))?;
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
                            ).with_context(|| {
                                format!(
                                    "Can not write data id (current old id) {:?} using {new_index_writer:?} aligning it with index record size {new_index_record_size:?}",
                                    new_index_record_size
                                )
                            })?;
                            current_old_id_option = old_ids_iter.next()?;
                        }
                        Ordering::Greater => {
                            write_data_id(
                                &mut new_index_writer,
                                current_effective_merge_location.additional_data,
                                new_index_record_size,
                            ).with_context(|| {
                                format!(
                                    "Can not write data id (current effective merge location additional data) {:?} using {new_index_writer:?} aligning it with index record size {new_index_record_size:?}",
                                    new_index_record_size
                                )
                            })?;
                            current_effective_merge_location_option =
                                effective_merge_locations_iter.next();
                        }
                        Ordering::Equal => {
                            if current_effective_merge_location.replace {
                                write_data_id(
                                    &mut new_index_writer,
                                    current_effective_merge_location.additional_data,
                                    new_index_record_size,
                                ).with_context(|| {
                                    format!(
                                        "Can not write data id (current effective merge location additional data) {:?} using {new_index_writer:?} aligning it with index record size {new_index_record_size:?}",
                                        new_index_record_size
                                    )
                                })?;
                                current_effective_merge_location_option =
                                    effective_merge_locations_iter.next();
                                current_old_id_option = old_ids_iter.next()?;
                            } else {
                                write_data_id(
                                    &mut new_index_writer,
                                    current_effective_merge_location.additional_data,
                                    new_index_record_size
                                ).with_context(|| {
                                    format!(
                                        "Can not write data id (current effective merge location additional data) {:?} using {new_index_writer:?} aligning it with index record size {new_index_record_size:?}",
                                        new_index_record_size
                                    )
                                })?;
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
                    ).with_context(|| {
                        format!(
                            "Can not write data id (current effective merge location additional data) {:?} using {new_index_writer:?} aligning it with index record size {new_index_record_size:?}",
                            new_index_record_size
                        )
                    })?;
                    current_effective_merge_location_option = effective_merge_locations_iter.next();
                }
                (None, Some((_, current_old_id))) => {
                    write_data_id(&mut new_index_writer, current_old_id, new_index_record_size).with_context(|| {
                        format!(
                            "Can not write data id (current old id) {:?} using {new_index_writer:?} aligning it with index record size {new_index_record_size:?}",
                            new_index_record_size
                        )
                    })?;
                    current_old_id_option = old_ids_iter.next().with_context(|| format!("Can not propagate old index {:?} iterator further (even if there is no identifiers to receive)", self.index))?;
                }
                (None, None) => break,
            }
        }
        new_index_writer
            .flush()
            .with_context(|| format!("Can not flush new index file"))?;

        fs::rename(&new_index_file_path, &self.index.config.path).with_context(|| {
            format!(
                "Can not overwrite old index at {} with new index at {}",
                self.index.config.path.display(),
                new_index_file_path.display()
            )
        })?;
        let new_index_config = IndexConfig {
            path: self.index.config.path.clone(),
        };
        self.index = Index::new(new_index_config.clone()).with_context(|| {
            format!("Can not create new index with config {new_index_config:?}")
        })?;

        Ok(())
    }

    fn iter_index(
        &'_ self,
        start_bound: Bound<&K>,
        backwards: bool,
    ) -> Result<TableIndexIterator<'_, K, V>> {
        match start_bound {
            Bound::Included(from_key) | Bound::Excluded(from_key) => {
                let from_record_index = PartitionPoint::new(
                    0,
                    self.index.records_count, |record_index| {
                        let data_record_id = self.index.get(if backwards {self.index.records_count - 1 - record_index} else {record_index})?.with_context(|| {
                            format!("Can not get data record id at index {record_index}")
                        })?;
                        let data_record =
                            self.get_from_index_by_id(data_record_id).with_context(|| {
                                format!("Can not get record from index by id {data_record_id:?}")
                            })?;
                        Ok((if backwards {from_key.cmp(&data_record.key)} else {data_record.key.cmp(from_key)}, data_record.value, ()))
                    }
                ).with_context(|| {
                        format!(
                            "Can not create partition point for {:?} to iterate table from key {from_key:?}",
                            self.index
                        )
                    }
                )?.and_then(|mut partition_point| {
                    if backwards {
                        partition_point.first_satisfying.index = self.index.records_count - 1 - partition_point.first_satisfying.index;
                    }
                    if let Bound::Included(_) = start_bound {
                        Some(partition_point.first_satisfying.index)
                    } else {
                        if backwards {
                            if partition_point.is_exact {
                                partition_point.first_satisfying.index.checked_sub(1)
                            } else {
                                Some(partition_point.first_satisfying.index)
                            }
                        } else {
                            if partition_point.is_exact {
                                Some(partition_point.first_satisfying.index + 1)
                            } else {
                                Some(partition_point.first_satisfying.index)
                            }
                        }
                    }
                });
                Ok(TableIndexIterator {
                    data_pool: &self.data_pool,
                    index_iter: if let Some(from_record_index) = from_record_index {
                        Box::new(self.index.iter(
                            from_record_index,
                            backwards
                        ).with_context(|| format!("Can not initiate iteration over index {:?} from key {from_key:?}", self.index))?)
                    } else {
                        Box::new(fallible_iterator::convert(
                            [0u64; 0].into_iter().map(|u| Ok(u)),
                        ))
                    },
                })
            }
            Bound::Unbounded => Ok(TableIndexIterator {
                data_pool: &self.data_pool,
                index_iter: if backwards && self.index.records_count == 0 {
                    Box::new(fallible_iterator::convert(
                        [0u64; 0].into_iter().map(|u| Ok(u)),
                    ))
                } else {
                    Box::new(
                        self.index
                            .iter(
                                if backwards {
                                    self.index.records_count - 1
                                } else {
                                    0
                                },
                                backwards,
                            )
                            .with_context(|| {
                                format!(
                                    "Can not initiate iteration over index {:?} from the beginning",
                                    self.index
                                )
                            })?,
                    )
                },
            }),
        }
    }

    pub fn iter(
        &'_ self,
        start_bound: Bound<&K>,
        backwards: bool,
    ) -> Result<Box<dyn FallibleIterator<Item = (K, V), Error = Error> + '_>> {
        Ok(Box::new(
            MergingIterator::new(
                if backwards {
                    Box::new(
                        self.memtable
                            .range::<K, _>((Bound::Unbounded, start_bound))
                            .rev(),
                    )
                } else {
                    Box::new(self.memtable.range::<K, _>((start_bound, Bound::Unbounded)))
                },
                Box::new(self.iter_index(start_bound, backwards).with_context(|| {
                    format!("Can not initiate iteration over table index from key {start_bound:?}")
                })?),
                backwards,
            )
            .with_context(|| {
                format!("Can not initiate merging iteration over table from key {start_bound:?}")
            })?,
        ))
    }
}

struct TableIndexIterator<'a, K: Key, V: Value> {
    data_pool: &'a Box<dyn DataPool<DataRecord<K, V>> + Send + Sync>,
    index_iter: Box<dyn FallibleIterator<Item = u64, Error = Error> + Send + Sync>,
}

impl<'a, K: Key, V: Value> FallibleIterator for TableIndexIterator<'a, K, V> {
    type Item = (K, V);
    type Error = Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        Ok(
            match self.index_iter.next().with_context(|| {
                format!(
                    "Can not propagate index iterator further (even if there is nothing to receive)"
                )
            })? {
                Some(id) => {
                    let data_record = self.data_pool.get(id).with_context(|| {
                        format!("Can not get record from data pool by id {id:?}")
                    })?;
                    Some((data_record.key, data_record.value))
                }
                None => None,
            },
        )
    }
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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
) -> Result<Vec<MergeLocation<A>>>
where
    F: FnMut(u64) -> Result<Option<(T, A)>>,
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
                let current = big_get_element(element_index).with_context(|| format!("Can not get element at index {element_index:?} from big merging part"))?
                    .with_context(|| format!("Can not get element at index {element_index:?}"))?;
                Ok((current.0.cmp(element_to_insert), current.0, current.1))
            }).with_context(|| format!("Can not create partition point from left bound {left_bound:?} to {right_bound:?} to maybe get {:?}-nth insert indice", middle.middle_index + 1))?
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
        result.push(insert_index.ok_or(anyhow!(
            "Can not find where to insert element using sparse merge"
        ))?);
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
            data_pool: DataPoolConfigEnum::Variable(VariableDataPoolConfig {
                directory: table_dir.join("data_pool").to_path_buf(),
                max_element_size: 65536 as usize,
            }),
            _key: PhantomData,
            _value: PhantomData,
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
    fn test_sort_complex() {
        let mut table = new_default_table::<([u8; 16], String), u8>("test_sort_complex");

        let keyvalues = vec![
            (
                (
                    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                    "0000000000.0000000002".to_string(),
                ),
                0u8,
            ),
            (
                (
                    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                    "0000000000.0000000003".to_string(),
                ),
                0u8,
            ),
            (
                (
                    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                    "0000000001".to_string(),
                ),
                0u8,
            ),
            (
                (
                    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                    "0000000002".to_string(),
                ),
                0u8,
            ),
        ];
        for (key, value) in keyvalues.iter() {
            table.memtable.insert(key.clone(), Some(value.clone()));
        }
        // table.checkpoint().unwrap();
        assert_eq!(
            table
                .iter(
                    Bound::Included(&(
                        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "0000000000.9".to_string(),
                    )),
                    true
                )
                .unwrap()
                .collect::<Vec<_>>()
                .unwrap(),
            [
                (
                    (
                        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "0000000000.0000000003".to_string(),
                    ),
                    0u8,
                ),
                (
                    (
                        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "0000000000.0000000002".to_string(),
                    ),
                    0u8,
                ),
            ],
        );
    }

    fn generative(use_checkpoints: bool) {
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

            let previously_added_keys = previously_added_keyvalues
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            let previously_added_keys_reversed = previously_added_keyvalues
                .keys()
                .rev()
                .cloned()
                .collect::<Vec<_>>();

            assert_eq!(
                table
                    .iter(Bound::Unbounded, false)
                    .unwrap()
                    .map(|(key, _)| Ok(key))
                    .collect::<Vec<_>>()
                    .unwrap(),
                previously_added_keys
            );
            assert_eq!(
                table
                    .iter(Bound::Unbounded, true)
                    .unwrap()
                    .map(|(key, _)| Ok(key))
                    .collect::<Vec<_>>()
                    .unwrap(),
                previously_added_keys_reversed
            );

            for (key_index, key) in previously_added_keys.iter().enumerate() {
                println!("key_index = {}/{}", key_index, previously_added_keys.len());
                assert_eq!(
                    table
                        .iter(Bound::Included(key), false)
                        .unwrap()
                        .map(|(key, _)| Ok(key))
                        .collect::<Vec<_>>()
                        .unwrap(),
                    previously_added_keys[key_index..]
                );
                assert_eq!(
                    table
                        .iter(Bound::Included(key), true)
                        .unwrap()
                        .map(|(key, _)| Ok(key))
                        .collect::<Vec<_>>()
                        .unwrap(),
                    previously_added_keys_reversed
                        [previously_added_keys_reversed.len() - 1 - key_index..]
                );

                let current = previously_added_keys[key_index][1];
                if let Some(next) = previously_added_keys
                    .get(key_index + 1)
                    .and_then(|next| Some(next[1]))
                {
                    if let Some(direct_next) = current.checked_add(1) {
                        if direct_next < next {
                            assert_eq!(
                                table
                                    .iter(Bound::Included(&vec![0u8, direct_next]), false)
                                    .unwrap()
                                    .map(|(key, _)| Ok(key))
                                    .collect::<Vec<_>>()
                                    .unwrap(),
                                previously_added_keys[key_index + 1..]
                            );
                            assert_eq!(
                                table
                                    .iter(Bound::Included(&vec![0u8, direct_next]), true)
                                    .unwrap()
                                    .map(|(key, _)| Ok(key))
                                    .collect::<Vec<_>>()
                                    .unwrap(),
                                previously_added_keys_reversed
                                    [previously_added_keys_reversed.len() - 1 - key_index..]
                            );
                        }
                    }
                }
            }

            let correct_table_keys: Vec<Vec<u8>> =
                previously_added_keyvalues.keys().cloned().collect();
            for key_index in 0..correct_table_keys.len() {
                let table_keys: Vec<Vec<u8>> = table
                    .iter(Bound::Included(&correct_table_keys[key_index]), false)
                    .unwrap()
                    .map(|(key, _)| Ok(key))
                    .unwrap()
                    .collect();
                assert_eq!(table_keys, correct_table_keys[key_index..]);
            }

            if use_checkpoints {
                println!("checkpoint {checkpoint_number}\n");
                table.checkpoint().unwrap();
            }
            for (key, value) in previously_added_keyvalues.iter() {
                assert_eq!(table.get(&key).unwrap(), Some(value.clone()));
            }
        }
    }

    #[test]
    fn test_memtable_generative() {
        generative(false);
    }

    #[test]
    fn test_checkpoint_generative() {
        generative(true);
    }
}
