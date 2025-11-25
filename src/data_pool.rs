#[cfg(feature = "serde")]
use typetag;

#[cfg_attr(feature = "serde", typetag::serde(tag = "type"))]
pub trait DataPoolConfig {
    fn new_data_pool(&self) -> Result<Box<dyn DataPool + Send + Sync>, String>;
}

pub trait DataPool {
    fn insert(&mut self, data: Vec<u8>) -> Result<u64, String>;

    fn remove(&mut self, id: u64) -> Result<(), String>;

    fn flush(&mut self) -> Result<(), String>;

    fn clear(&mut self) -> Result<(), String>;

    fn get(&self, id: u64) -> Result<Vec<u8>, String>;
}
