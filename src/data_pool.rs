#[cfg(feature = "serde")]
use typetag;

use crate::keyvalue::{Key, Value};

#[derive(bincode::Encode, bincode::Decode, Debug)]
pub struct DataRecord<K: Key, V: Value> {
    pub key: K,
    pub value: V,
}

#[cfg_attr(feature = "serde", typetag::serde(tag = "type"))]
pub trait DataPoolConfig<K: Key, V: Value> {
    fn new_data_pool(&self) -> Result<Box<dyn DataPool<K, V> + Send + Sync>, String>;
}

pub trait DataPool<K: Key, V: Value> {
    fn insert_raw(&mut self, data: Vec<u8>) -> Result<u64, String>;

    fn insert(&mut self, data_record: DataRecord<K, V>) -> Result<u64, String>;

    fn remove(&mut self, id: u64) -> Result<(), String>;

    fn flush(&mut self) -> Result<(), String>;

    fn clear(&mut self) -> Result<(), String>;

    fn get(&self, id: u64) -> Result<DataRecord<K, V>, String>;
}
