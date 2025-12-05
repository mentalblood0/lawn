#[cfg(feature = "serde")]
use typetag;

#[cfg_attr(feature = "serde", typetag::serde(tag = "type"))]
pub trait DataPoolConfig<K, V>
where
    K: bincode::Encode + bincode::Decode<()> + Ord + Clone,
    V: bincode::Encode + bincode::Decode<()> + Ord + Clone,
{
    fn new_data_pool(&self) -> Result<Box<dyn DataPool<K, V> + Send + Sync>, String>;
}

pub trait DataPool<K, V>
where
    K: bincode::Encode + bincode::Decode<()> + Ord + Clone,
    V: bincode::Encode + bincode::Decode<()> + Ord + Clone,
{
    fn insert(&mut self, data: Vec<u8>) -> Result<u64, String>;

    fn remove(&mut self, id: u64) -> Result<(), String>;

    fn flush(&mut self) -> Result<(), String>;

    fn clear(&mut self) -> Result<(), String>;

    fn get(&self, id: u64) -> Result<Vec<u8>, String>;
}
