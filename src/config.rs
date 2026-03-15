use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchedFolder {
    pub path: String,
    pub sanitize_pii: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MementoConfig {
    pub watched_folders: Vec<WatchedFolder>,
}

pub fn get_config_path() -> PathBuf {
    let mut path = dirs::config_dir().unwrap_or_else(|| PathBuf::from("."));
    path.push("memento");
    fs::create_dir_all(&path).unwrap_or_default();
    path.push("config.json");
    path
}

pub fn load_config() -> MementoConfig {
    let path = get_config_path();
    if path.exists() {
        if let Ok(content) = fs::read_to_string(&path) {
            if let Ok(config) = serde_json::from_str(&content) {
                return config;
            }
        }
    }
    MementoConfig::default()
}

pub fn save_config(config: &MementoConfig) -> anyhow::Result<()> {
    let path = get_config_path();
    let content = serde_json::to_string_pretty(config)?;
    fs::write(path, content)?;
    Ok(())
}
