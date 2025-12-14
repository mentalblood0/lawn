use anyhow::Result;

use crate::keyvalue::Value;

pub trait DataPoolConfig<D: Value> {
    fn new_data_pool(&self) -> Result<Box<dyn DataPool<D> + Send + Sync>>;
}

pub trait DataPool<D: Value> {
    fn insert(&mut self, data_record: D) -> Result<u64>;

    fn remove(&mut self, id: u64) -> Result<()>;

    fn flush(&mut self) -> Result<()>;

    fn clear(&mut self) -> Result<()>;

    fn get(&self, id: u64) -> Result<D>;
}
