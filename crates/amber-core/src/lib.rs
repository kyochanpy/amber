//! amber-core crate root.

pub mod catalog;
pub mod compactor;
pub mod config;
pub mod image;
pub mod read_set;
pub mod schema;
pub mod session;
pub mod storage;
pub mod writer;

pub use catalog::{
    CatalogError, CatalogEvent, CatalogEventId, CatalogState, CompactionCommittedEvent,
    CompactionId, FoldedWalSegment, FoldedWalSegmentState, ParquetFileId, PublishedParquetFile,
    SchemaCatalogEntry, UuidV7Id, UuidV7IdError, WalSegmentClosedEvent, WalSegmentDeletedEvent,
    WalSegmentId,
};
pub use compactor::{Compactor, CompactorError};
pub use config::{
    AmberConfig, CompactionConfig, ConfigError, NodeConfig, OutputConfig, StorageBackend,
    StorageConfig, WalConfig, WalRotationConfig,
};
pub use image::{ImageError, PreparedImageBatch, prepare_compressed_image_batch};
pub use read_set::{
    ParquetSource, SessionSourceError, SessionSourceFilter, SessionSourceGroup, SessionSourceSet,
    WalSource,
};
pub use schema::{
    AMBER_TIMESTAMP_COLUMN, METADATA_COLUMNS, MetadataColumnsError, NODE_ID_COLUMN,
    NODE_TIMESTAMP_COLUMN, NormalizedDataType, NormalizedField, NormalizedPayloadSchema,
    OUTPUT_ID_COLUMN, RecordBatchMetadata, SESSION_ID_COLUMN, is_metadata_column,
    metadata_field_names, metadata_fields, metadata_schema, normalized_payload_schema,
    payload_fields, payload_schema, prepend_metadata_columns, schema_fingerprint,
    schema_fingerprint_for_payload,
};
pub use session::{
    ClosedWalStreamUpdate, ObservedStreamSummary, SessionId, SessionIdError, SessionManifest,
    SessionManifestError, SessionStatus,
};
pub use storage::{ObjectPath, Storage, StorageError, paths};
pub use writer::{
    WalRotateReceipt, WalRotateRequest, WalWriteReceipt, WalWriteRequest, WalWriter,
    WalWriterHandle,
    WalWriterError, WriteCommand,
};
