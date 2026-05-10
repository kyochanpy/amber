//! amber-core crate root.

pub mod config;

pub use config::{
    AmberConfig, CompactionConfig, ConfigError, NodeConfig, OutputConfig, StorageBackend,
    StorageConfig, WalConfig, WalRotationConfig,
};
