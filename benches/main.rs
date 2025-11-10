use lawn::variable_data_pool::*;
use nanorand::{Rng, WyRand};
use std::collections::HashMap;
use std::path::Path;

fn main() {
    divan::main();
}

#[divan::bench]
fn variable_data_pool(bencher: divan::Bencher) {
    const MIN_BATCH_SIZE: usize = 1000;
    const MAX_BATCH_SIZE: usize = 1000;
    const MIN_DATA_SIZE: usize = 1;
    const MAX_DATA_SIZE: usize = 1024;
    const ITERATIONS: usize = 10;
    const PATH: &str = "/tmp/lawn/benchmark/variable_data_pool";

    bencher.bench(|| {
        let mut variable_data_pool = VariableDataPool::new(VariableDataPoolConfig {
            directory: Path::new(PATH).to_path_buf(),
            max_element_size: 65536,
        })
        .unwrap();
        variable_data_pool.clear().unwrap();

        let mut previously_added_data: HashMap<Id, Vec<u8>> = HashMap::new();
        let mut rng = WyRand::new_seed(0);

        for _ in 0..ITERATIONS {
            let data_to_add: Vec<Vec<u8>> = (0..rng
                .generate_range(MIN_BATCH_SIZE..=MAX_BATCH_SIZE))
                .map(|_| {
                    let mut data = vec![0u8; rng.generate_range(MIN_DATA_SIZE..=MAX_DATA_SIZE)];
                    rng.fill(&mut data);
                    data
                })
                .collect();
            let ids_of_data_to_delete: Vec<Id> = previously_added_data
                .keys()
                .take(rng.generate_range(0..=previously_added_data.len()))
                .cloned()
                .collect();
            for id in &ids_of_data_to_delete {
                previously_added_data.remove(&id);
            }

            let ids_of_added_data = variable_data_pool
                .update(&data_to_add, &ids_of_data_to_delete)
                .unwrap();
            ids_of_added_data
                .iter()
                .enumerate()
                .for_each(|(id_index, id)| {
                    previously_added_data.insert(id.clone(), data_to_add[id_index].clone());
                });
        }
    });
}
