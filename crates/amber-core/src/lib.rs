//! amber-core crate root.

pub mod config;
pub mod session;
pub mod storage;

pub use config::{
    AmberConfig, CompactionConfig, ConfigError, NodeConfig, OutputConfig, StorageBackend,
    StorageConfig, WalConfig, WalRotationConfig,
};
pub use session::{SessionId, SessionIdError};
pub use storage::{ObjectPath, Storage, StorageError, paths};
