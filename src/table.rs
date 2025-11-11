use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::data_pool::{DataPool, DataPoolConfig};
use crate::index::{Index, IndexConfig};

#[derive(Serialize, Deserialize)]
pub struct TableConfig {
    index: IndexConfig,
    data_pool: Box<dyn DataPoolConfig>,
}

pub struct Table {
    pub index: Index,
    pub data_pool: Box<dyn DataPool>,
    memtable: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Table {
    pub fn new(config: TableConfig) -> Result<Self, String> {
        Ok(Table {
            index: Index::new(config.index)?,
            data_pool: config.data_pool.new_data_pool()?,
            memtable: BTreeMap::new(),
        })
    }
}
