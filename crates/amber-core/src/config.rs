use std::{
    fmt, fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use thiserror::Error;

const DEFAULT_LOCAL_STORAGE_PATH: &str = "./amber_data";
const DEFAULT_WAL_ROTATION_MAX_SIZE_MB: u64 = 256;
const DEFAULT_WAL_ROTATION_MAX_DURATION_SEC: u64 = 300;
const DEFAULT_COMPACTION_TARGET_FILE_MB: u64 = 256;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct AmberConfig {
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub wal: WalConfig,
    #[serde(default)]
    pub compaction: CompactionConfig,
    #[serde(default)]
    pub nodes: Vec<NodeConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageBackend {
    Local,
    S3,
}

impl StorageBackend {
    pub const fn is_supported(&self) -> bool {
        matches!(self, Self::Local)
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::S3 => "s3",
        }
    }
}

impl fmt::Display for StorageBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(default = "default_storage_backend")]
    pub backend: StorageBackend,
    #[serde(default)]
    pub path: Option<PathBuf>,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub access_key: Option<String>,
    #[serde(default)]
    pub secret_key: Option<String>,
}

impl StorageConfig {
    fn resolve_path_relative_to(&mut self, config_dir: &Path) {
        if self.backend != StorageBackend::Local {
            return;
        }

        let path = self.path.clone().unwrap_or_else(default_local_storage_path);
        self.path = Some(if path.is_relative() {
            config_dir.join(path)
        } else {
            path
        });
    }

    pub fn ensure_supported(&self) -> Result<(), ConfigError> {
        if self.backend.is_supported() {
            Ok(())
        } else {
            Err(ConfigError::UnsupportedStorageBackend {
                backend: self.backend.clone(),
            })
        }
    }

    pub fn resolved_local_path(&self) -> PathBuf {
        self.path.clone().unwrap_or_else(default_local_storage_path)
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: default_storage_backend(),
            path: None,
            bucket: None,
            prefix: None,
            endpoint: None,
            access_key: None,
            secret_key: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct WalConfig {
    #[serde(default)]
    pub rotation: WalRotationConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalRotationConfig {
    #[serde(default = "default_wal_rotation_max_size_mb")]
    pub max_size_mb: u64,
    #[serde(default = "default_wal_rotation_max_duration_sec")]
    pub max_duration_sec: u64,
}

impl Default for WalRotationConfig {
    fn default() -> Self {
        Self {
            max_size_mb: default_wal_rotation_max_size_mb(),
            max_duration_sec: default_wal_rotation_max_duration_sec(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionConfig {
    #[serde(default = "default_compaction_target_file_mb")]
    pub target_file_mb: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            target_file_mb: default_compaction_target_file_mb(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    pub id: String,
    #[serde(default)]
    pub outputs: Vec<OutputConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputConfig {
    pub id: String,
    #[serde(default)]
    pub every_n_frames: Option<u64>,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file '{}': {source}", path.display())]
    ReadConfigFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config file '{}': {source}", path.display())]
    ParseConfigFile {
        path: PathBuf,
        #[source]
        source: serde_yaml::Error,
    },
    #[error("storage backend '{backend}' is not supported; only 'local' is currently available")]
    UnsupportedStorageBackend { backend: StorageBackend },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AmberFile {
    amber: AmberConfig,
}

fn default_storage_backend() -> StorageBackend {
    StorageBackend::Local
}

fn default_local_storage_path() -> PathBuf {
    PathBuf::from(DEFAULT_LOCAL_STORAGE_PATH)
}

fn default_wal_rotation_max_size_mb() -> u64 {
    DEFAULT_WAL_ROTATION_MAX_SIZE_MB
}

fn default_wal_rotation_max_duration_sec() -> u64 {
    DEFAULT_WAL_ROTATION_MAX_DURATION_SEC
}

fn default_compaction_target_file_mb() -> u64 {
    DEFAULT_COMPACTION_TARGET_FILE_MB
}

impl AmberConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(|source| ConfigError::ReadConfigFile {
            path: path.to_path_buf(),
            source,
        })?;
        let file: AmberFile =
            serde_yaml::from_str(&contents).map_err(|source| ConfigError::ParseConfigFile {
                path: path.to_path_buf(),
                source,
            })?;

        let mut config = file.amber;
        let config_dir = path.parent().unwrap_or_else(|| Path::new("."));
        config.storage.resolve_path_relative_to(config_dir);
        config.storage.ensure_supported()?;

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time should be after epoch")
                .as_nanos();
            let path = std::env::temp_dir().join(format!("amber-config-test-{unique}"));
            fs::create_dir_all(&path).expect("temp dir should be created");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn yaml_defaults_local_storage_and_sizes() {
        let parsed: AmberFile = serde_yaml::from_str(
            r#"
amber: {}
"#,
        )
        .expect("config should deserialize");

        assert_eq!(parsed.amber.storage.backend, StorageBackend::Local);
        assert_eq!(parsed.amber.storage.path, None);
        assert_eq!(
            parsed.amber.storage.resolved_local_path(),
            PathBuf::from("./amber_data")
        );
        assert_eq!(parsed.amber.wal.rotation.max_size_mb, 256);
        assert_eq!(parsed.amber.wal.rotation.max_duration_sec, 300);
        assert_eq!(parsed.amber.compaction.target_file_mb, 256);
        assert!(parsed.amber.nodes.is_empty());
    }

    #[test]
    fn yaml_rejects_unknown_keys() {
        let err = serde_yaml::from_str::<AmberFile>(
            r#"
amber:
  storage:
    backend: local
    unknown_key: true
"#,
        )
        .expect_err("unknown key should be rejected");

        assert!(err.to_string().contains("unknown field `unknown_key`"));
    }

    #[test]
    fn rejects_unsupported_storage_backends() {
        let parsed: AmberFile = serde_yaml::from_str(
            r#"
amber:
  storage:
    backend: s3
    bucket: amber
"#,
        )
        .expect("future-compatible storage shape should deserialize");

        let err = parsed
            .amber
            .storage
            .ensure_supported()
            .expect_err("non-local backend should be rejected");

        assert_eq!(
            err.to_string(),
            "storage backend 's3' is not supported; only 'local' is currently available"
        );
    }

    #[test]
    fn from_file_loads_yaml_and_applies_defaults() {
        let temp_dir = TestDir::new();
        let config_path = temp_dir.path().join("amber.yaml");
        fs::write(
            &config_path,
            r#"
amber:
  storage:
    backend: local
  compaction:
    target_file_mb: 512
"#,
        )
        .expect("config file should be written");

        let config = AmberConfig::from_file(&config_path).expect("config should load");

        assert_eq!(config.storage.backend, StorageBackend::Local);
        assert_eq!(
            config.storage.path,
            Some(temp_dir.path().join("amber_data"))
        );
        assert_eq!(config.wal.rotation.max_size_mb, 256);
        assert_eq!(config.wal.rotation.max_duration_sec, 300);
        assert_eq!(config.compaction.target_file_mb, 512);
        assert!(config.nodes.is_empty());
    }

    #[test]
    fn from_file_resolves_relative_storage_path_from_config_directory() {
        let temp_dir = TestDir::new();
        let config_dir = temp_dir.path().join("configs");
        let config_path = config_dir.join("amber.yaml");
        fs::create_dir_all(&config_dir).expect("config dir should be created");
        fs::write(
            &config_path,
            r#"
amber:
  storage:
    backend: local
    path: ./data
"#,
        )
        .expect("config file should be written");

        let config = AmberConfig::from_file(&config_path).expect("config should load");

        assert_eq!(config.storage.path, Some(config_dir.join("data")));
    }

    #[test]
    fn from_file_reports_parse_errors_with_path_context() {
        let temp_dir = TestDir::new();
        let config_path = temp_dir.path().join("broken.yaml");
        fs::write(
            &config_path,
            r#"
amber:
  storage:
    backend: [
"#,
        )
        .expect("config file should be written");

        let error = AmberConfig::from_file(&config_path).expect_err("config should fail");
        let message = error.to_string();

        assert!(message.contains(config_path.to_string_lossy().as_ref()));
        assert!(message.contains("failed to parse config file"));
    }

    #[test]
    fn from_file_reports_io_errors_with_path_context() {
        let temp_dir = TestDir::new();
        let config_path = temp_dir.path().join("missing.yaml");

        let error = AmberConfig::from_file(&config_path).expect_err("config should fail");
        let message = error.to_string();

        assert!(matches!(
            error,
            ConfigError::ReadConfigFile {
                path,
                source: _,
            } if path == config_path
        ));
        assert!(message.contains("failed to read config file"));
    }
}
