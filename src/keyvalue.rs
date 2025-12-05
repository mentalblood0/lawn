pub trait Key: bincode::Encode + bincode::Decode<()> + Ord + Clone + Send + Sync {}
impl<T> Key for T where T: bincode::Encode + bincode::Decode<()> + Ord + Clone + Send + Sync {}

pub trait Value: bincode::Encode + bincode::Decode<()> + Ord + Clone + Send + Sync {}
impl<T> Value for T where T: bincode::Encode + bincode::Decode<()> + Ord + Clone + Send + Sync {}
