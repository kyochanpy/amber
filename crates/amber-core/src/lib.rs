//! amber-core crate root.

pub mod config;
pub mod schema;
pub mod session;
pub mod storage;

pub use config::{
    AmberConfig, CompactionConfig, ConfigError, NodeConfig, OutputConfig, StorageBackend,
    StorageConfig, WalConfig, WalRotationConfig,
};
pub use schema::{
    AMBER_TIMESTAMP_COLUMN, METADATA_COLUMNS, NODE_ID_COLUMN, NODE_TIMESTAMP_COLUMN,
    OUTPUT_ID_COLUMN, SESSION_ID_COLUMN, is_metadata_column, metadata_field_names, metadata_fields,
    metadata_schema, payload_fields, payload_schema, schema_fingerprint,
    schema_fingerprint_for_payload,
};
pub use session::{
    ClosedWalStreamUpdate, ObservedStreamSummary, SessionId, SessionIdError, SessionManifest,
    SessionManifestError, SessionStatus,
};
pub use storage::{ObjectPath, Storage, StorageError, paths};
