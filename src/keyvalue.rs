pub trait Key:
    bincode::Encode + bincode::Decode<()> + Ord + Clone + Send + Sync + std::fmt::Debug
{
}
impl<T> Key for T where
    T: bincode::Encode + bincode::Decode<()> + Ord + Clone + Send + Sync + std::fmt::Debug
{
}

pub trait Value:
    bincode::Encode + bincode::Decode<()> + Clone + Send + Sync + std::fmt::Debug
{
}
impl<T> Value for T where
    T: bincode::Encode + bincode::Decode<()> + Clone + Send + Sync + std::fmt::Debug
{
}
