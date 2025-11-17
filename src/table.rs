use bincode;
use std::collections::BTreeMap;

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
                if &data_record.key < key {
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
