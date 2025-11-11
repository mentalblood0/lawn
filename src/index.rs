use std::fs;
use std::path::PathBuf;

pub struct IndexConfig {
    pub path: PathBuf,
    pub container_size: u8,
}

pub struct Index {
    pub config: IndexConfig,
    file: fs::File,
}

impl Index {
    pub fn new(config: IndexConfig) -> Result<Self, String> {
        let file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&config.path)
            .map_err(|error| {
                format!(
                    "Can not open file at path {}: {error}",
                    config.path.display()
                )
            })?;
        Ok(Index { config, file: file })
    }
}
