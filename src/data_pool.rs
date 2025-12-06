#[cfg(feature = "serde")]
use typetag;

use crate::keyvalue::Value;

#[cfg_attr(feature = "serde", typetag::serde(tag = "type"))]
pub trait DataPoolConfig<D: Value> {
    fn new_data_pool(&self) -> Result<Box<dyn DataPool<D> + Send + Sync>, String>;
}

pub trait DataPool<D: Value> {
    fn insert(&mut self, data_record: D) -> Result<u64, String>;

    fn remove(&mut self, id: u64) -> Result<(), String>;

    fn flush(&mut self) -> Result<(), String>;

    fn clear(&mut self) -> Result<(), String>;

    fn get(&self, id: u64) -> Result<D, String>;
}
