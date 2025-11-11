pub trait DataPoolConfig {
    fn new_data_pool(&self) -> Result<Box<dyn DataPool>, String>;
}

pub trait DataPool {
    fn update(
        &mut self,
        data_to_add: &Vec<Vec<u8>>,
        ids_of_data_to_delete: &Vec<u64>,
    ) -> Result<Vec<u64>, String>;

    fn clear(&mut self) -> Result<(), String>;

    fn get(&self, id: u64) -> Result<Vec<u8>, String>;
}
