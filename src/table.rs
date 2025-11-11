use std::collections::BTreeMap;

use crate::index::{Index, IndexConfig};
use crate::{
    fixed_data_pool::{FixedDataPool, FixedDataPoolConfig},
    variable_data_pool::{VariableDataPool, VariableDataPoolConfig},
};

enum DataPoolConfig {
    Fixed(FixedDataPoolConfig),
    Variable(VariableDataPoolConfig),
}

pub enum DataPool {
    Fixed(FixedDataPool),
    Variable(VariableDataPool),
}

pub struct TableConfig {
    index: IndexConfig,
    data_pool: DataPoolConfig,
}

pub struct Table {
    pub index: Index,
    pub data_pool: DataPool,
    memtable: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Table {
    pub fn new(config: TableConfig) -> Result<Self, String> {
        Ok(Table {
            index: Index::new(config.index)?,
            data_pool: match config.data_pool {
                DataPoolConfig::Fixed(config) => DataPool::Fixed(FixedDataPool::new(config)?),
                DataPoolConfig::Variable(config) => {
                    DataPool::Variable(VariableDataPool::new(config)?)
                }
            },
            memtable: BTreeMap::new(),
        })
    }
}
