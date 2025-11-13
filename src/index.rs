use std::fs;
use std::path::PathBuf;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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
        if let Some(path_parent_directory) = config.path.parent() {
            std::fs::create_dir_all(path_parent_directory).map_err(|error| {
                format!(
                    "Can not create parent directories for path {}: {error}",
                    &config.path.display()
                )
            })?;
        }
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
        Ok(Index { config, file })
    }

    pub fn clear(&mut self) -> Result<(), String> {
        self.file
            .set_len(0)
            .map_err(|error| format!("Can not truncate file {:?}: {error}", self.file))?;
        Ok(())
    }
}
