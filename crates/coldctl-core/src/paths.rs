use crate::error::Error;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct StatePaths {
    pub data_dir: PathBuf,
}

impl StatePaths {
    pub fn resolve(data_dir: Option<&Path>) -> Result<Self, Error> {
        let data_dir = match data_dir {
            Some(path) => path.to_path_buf(),
            None => directories::ProjectDirs::from("com", "coldctl", "coldctl")
                .ok_or(Error::DataDirectoryUnavailable)?
                .data_local_dir()
                .to_path_buf(),
        };
        Ok(Self { data_dir })
    }

    pub fn database(&self) -> PathBuf {
        self.data_dir.join("state.db")
    }
}
