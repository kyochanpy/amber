use std::{collections::BTreeMap, fmt, str::FromStr};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::{Uuid, Version};

use crate::{
    SessionId, Storage, StorageError,
    schema::NormalizedPayloadSchema,
    storage::{ObjectPath, paths},
};

pub type CatalogEventId = UuidV7Id;
pub type WalSegmentId = UuidV7Id;
pub type CompactionId = UuidV7Id;
pub type ParquetFileId = UuidV7Id;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UuidV7Id(String);

impl UuidV7Id {
    pub fn new() -> Self {
        Self(Uuid::now_v7().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn parse(value: impl AsRef<str>) -> Result<Self, UuidV7IdError> {
        value.as_ref().parse()
    }
}

impl Default for UuidV7Id {
    fn default() -> Self {
        Self::new()
    }
}

impl AsRef<str> for UuidV7Id {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for UuidV7Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for UuidV7Id {
    type Err = UuidV7IdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let uuid = Uuid::parse_str(value).map_err(|source| UuidV7IdError::InvalidUuid {
            value: value.to_owned(),
            source,
        })?;

        if uuid.get_version() != Some(Version::SortRand) {
            return Err(UuidV7IdError::WrongVersion {
                value: value.to_owned(),
            });
        }

        Ok(Self(uuid.to_string()))
    }
}

#[derive(Debug, Error)]
pub enum UuidV7IdError {
    #[error("invalid UUIDv7 ID '{value}': {source}")]
    InvalidUuid {
        value: String,
        #[source]
        source: uuid::Error,
    },
    #[error("ID '{value}' is not a UUIDv7")]
    WrongVersion { value: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum CatalogEvent {
    WalSegmentClosed(WalSegmentClosedEvent),
    CompactionCommitted(CompactionCommittedEvent),
    WalSegmentDeleted(WalSegmentDeletedEvent),
}

impl CatalogEvent {
    pub fn event_id(&self) -> &CatalogEventId {
        match self {
            Self::WalSegmentClosed(event) => &event.event_id,
            Self::CompactionCommitted(event) => &event.event_id,
            Self::WalSegmentDeleted(event) => &event.event_id,
        }
    }

    pub fn event_type(&self) -> &'static str {
        match self {
            Self::WalSegmentClosed(_) => "wal_segment_closed",
            Self::CompactionCommitted(_) => "compaction_committed",
            Self::WalSegmentDeleted(_) => "wal_segment_deleted",
        }
    }

    pub fn path(&self) -> ObjectPath {
        paths::catalog_event(&format!("{}-{}.json", self.event_id(), self.event_type()))
    }

    pub async fn save(&self, storage: &Storage) -> Result<(), CatalogError> {
        let path = self.path();
        storage
            .put_json(&path, self)
            .await
            .map_err(|source| CatalogError::SaveEvent {
                event_id: self.event_id().clone(),
                path,
                source: Box::new(source),
            })
    }

    pub async fn list(storage: &Storage) -> Result<Vec<Self>, CatalogError> {
        let mut events = Vec::new();
        for meta in storage
            .list_prefix(&paths::catalog_events_prefix())
            .await
            .map_err(|source| CatalogError::ListEvents { source })?
        {
            let path = meta.location.clone();
            let event = storage
                .get_json::<CatalogEvent>(&path)
                .await
                .map_err(|source| CatalogError::LoadEvent {
                    path,
                    source: Box::new(source),
                })?;
            events.push(event);
        }

        events.sort_by(|left, right| left.event_id().cmp(right.event_id()));
        Ok(events)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalSegmentClosedEvent {
    pub event_id: CatalogEventId,
    pub segment_id: WalSegmentId,
    pub session_id: SessionId,
    pub node_id: String,
    pub output_id: String,
    pub schema_fingerprint: String,
    pub path: String,
    pub row_count: u64,
    pub byte_size: u64,
    pub min_node_timestamp: i64,
    pub max_node_timestamp: i64,
    pub min_amber_timestamp: i64,
    pub max_amber_timestamp: i64,
    pub opened_at: DateTime<Utc>,
    pub closed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionCommittedEvent {
    pub event_id: CatalogEventId,
    pub compaction_id: CompactionId,
    pub source_wal_segments: Vec<WalSegmentId>,
    pub created_parquet_files: Vec<PublishedParquetFile>,
    pub committed_at: DateTime<Utc>,
}

impl CompactionCommittedEvent {
    pub fn new(
        compaction_id: CompactionId,
        source_wal_segments: Vec<WalSegmentId>,
        created_parquet_files: Vec<PublishedParquetFile>,
        committed_at: DateTime<Utc>,
    ) -> Self {
        Self {
            event_id: CatalogEventId::new(),
            compaction_id,
            source_wal_segments,
            created_parquet_files,
            committed_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedParquetFile {
    pub file_id: ParquetFileId,
    pub node_id: String,
    pub output_id: String,
    pub schema_fingerprint: String,
    pub path: String,
    pub row_count: u64,
    pub byte_size: u64,
    pub min_node_timestamp: i64,
    pub max_node_timestamp: i64,
    pub min_amber_timestamp: i64,
    pub max_amber_timestamp: i64,
    pub created_at: DateTime<Utc>,
}

impl PublishedParquetFile {
    pub fn new(
        file_id: ParquetFileId,
        node_id: impl Into<String>,
        output_id: impl Into<String>,
        schema_fingerprint: impl Into<String>,
        path: ObjectPath,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            file_id,
            node_id: node_id.into(),
            output_id: output_id.into(),
            schema_fingerprint: schema_fingerprint.into(),
            path: path.to_string(),
            row_count: 0,
            byte_size: 0,
            min_node_timestamp: 0,
            max_node_timestamp: 0,
            min_amber_timestamp: 0,
            max_amber_timestamp: 0,
            created_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalSegmentDeletedEvent {
    pub event_id: CatalogEventId,
    pub segment_id: WalSegmentId,
    pub path: String,
    pub deleted_at: DateTime<Utc>,
}

impl WalSegmentDeletedEvent {
    pub fn new(segment_id: WalSegmentId, path: ObjectPath, deleted_at: DateTime<Utc>) -> Self {
        Self {
            event_id: CatalogEventId::new(),
            segment_id,
            path: path.to_string(),
            deleted_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaCatalogEntry {
    pub schema_fingerprint: String,
    pub normalized_payload_schema: NormalizedPayloadSchema,
}

impl SchemaCatalogEntry {
    pub fn new(
        schema_fingerprint: impl Into<String>,
        normalized_payload_schema: NormalizedPayloadSchema,
    ) -> Self {
        Self {
            schema_fingerprint: schema_fingerprint.into(),
            normalized_payload_schema,
        }
    }

    pub fn path(&self) -> ObjectPath {
        paths::schema_file(&self.schema_fingerprint)
    }

    pub async fn save_if_absent(&self, storage: &Storage) -> Result<bool, CatalogError> {
        let path = self.path();
        if storage
            .exists(&path)
            .await
            .map_err(|source| CatalogError::CheckSchemaCatalogEntry {
                schema_fingerprint: self.schema_fingerprint.clone(),
                path: path.clone(),
                source: Box::new(source),
            })?
        {
            return Ok(false);
        }

        storage.put_json(&path, self).await.map_err(|source| {
            CatalogError::SaveSchemaCatalogEntry {
                schema_fingerprint: self.schema_fingerprint.clone(),
                path,
                source: Box::new(source),
            }
        })?;
        Ok(true)
    }

    pub async fn load(storage: &Storage, schema_fingerprint: &str) -> Result<Self, CatalogError> {
        let path = paths::schema_file(schema_fingerprint);
        storage
            .get_json::<SchemaCatalogEntry>(&path)
            .await
            .map_err(|source| CatalogError::LoadSchemaCatalogEntry {
                schema_fingerprint: schema_fingerprint.to_owned(),
                path,
                source: Box::new(source),
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogState {
    pub wal_segments: BTreeMap<WalSegmentId, FoldedWalSegment>,
    pub published_parquet_files: BTreeMap<ParquetFileId, PublishedParquetFile>,
}

impl CatalogState {
    pub async fn load(storage: &Storage) -> Result<Self, CatalogError> {
        let events = CatalogEvent::list(storage).await?;
        Self::from_events(events)
    }

    pub fn from_events(
        events: impl IntoIterator<Item = CatalogEvent>,
    ) -> Result<Self, CatalogError> {
        let mut wal_segments = BTreeMap::new();
        let mut published_parquet_files = BTreeMap::new();

        for event in events {
            match event {
                CatalogEvent::WalSegmentClosed(event) => {
                    wal_segments.insert(
                        event.segment_id.clone(),
                        FoldedWalSegment::from_closed(event),
                    );
                }
                CatalogEvent::CompactionCommitted(event) => {
                    for segment_id in &event.source_wal_segments {
                        let segment = wal_segments.get_mut(segment_id).ok_or_else(|| {
                            CatalogError::MissingWalSegment {
                                segment_id: segment_id.clone(),
                                event_id: event.event_id.clone(),
                            }
                        })?;
                        segment.state = FoldedWalSegmentState::Compacted;
                    }

                    for file in event.created_parquet_files {
                        published_parquet_files.insert(file.file_id.clone(), file);
                    }
                }
                CatalogEvent::WalSegmentDeleted(event) => {
                    let segment = wal_segments.get_mut(&event.segment_id).ok_or_else(|| {
                        CatalogError::MissingWalSegment {
                            segment_id: event.segment_id.clone(),
                            event_id: event.event_id.clone(),
                        }
                    })?;
                    segment.state = FoldedWalSegmentState::Deleted;
                }
            }
        }

        Ok(Self {
            wal_segments,
            published_parquet_files,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldedWalSegment {
    pub segment_id: WalSegmentId,
    pub session_id: SessionId,
    pub node_id: String,
    pub output_id: String,
    pub schema_fingerprint: String,
    pub path: String,
    pub row_count: u64,
    pub byte_size: u64,
    pub min_node_timestamp: i64,
    pub max_node_timestamp: i64,
    pub min_amber_timestamp: i64,
    pub max_amber_timestamp: i64,
    pub opened_at: DateTime<Utc>,
    pub closed_at: DateTime<Utc>,
    pub state: FoldedWalSegmentState,
}

impl FoldedWalSegment {
    fn from_closed(event: WalSegmentClosedEvent) -> Self {
        Self {
            segment_id: event.segment_id,
            session_id: event.session_id,
            node_id: event.node_id,
            output_id: event.output_id,
            schema_fingerprint: event.schema_fingerprint,
            path: event.path,
            row_count: event.row_count,
            byte_size: event.byte_size,
            min_node_timestamp: event.min_node_timestamp,
            max_node_timestamp: event.max_node_timestamp,
            min_amber_timestamp: event.min_amber_timestamp,
            max_amber_timestamp: event.max_amber_timestamp,
            opened_at: event.opened_at,
            closed_at: event.closed_at,
            state: FoldedWalSegmentState::Pending,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldedWalSegmentState {
    Pending,
    Compacted,
    Deleted,
}

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("failed to save catalog event '{event_id}' at '{path}': {source}")]
    SaveEvent {
        event_id: CatalogEventId,
        path: ObjectPath,
        #[source]
        source: Box<StorageError>,
    },
    #[error("failed to list catalog events: {source}")]
    ListEvents {
        #[source]
        source: StorageError,
    },
    #[error("failed to load catalog event at '{path}': {source}")]
    LoadEvent {
        path: ObjectPath,
        #[source]
        source: Box<StorageError>,
    },
    #[error("catalog event '{event_id}' references unknown WAL segment '{segment_id}'")]
    MissingWalSegment {
        segment_id: WalSegmentId,
        event_id: CatalogEventId,
    },
    #[error("failed to check schema catalog entry '{schema_fingerprint}' at '{path}': {source}")]
    CheckSchemaCatalogEntry {
        schema_fingerprint: String,
        path: ObjectPath,
        #[source]
        source: Box<StorageError>,
    },
    #[error("failed to save schema catalog entry '{schema_fingerprint}' at '{path}': {source}")]
    SaveSchemaCatalogEntry {
        schema_fingerprint: String,
        path: ObjectPath,
        #[source]
        source: Box<StorageError>,
    },
    #[error("failed to load schema catalog entry '{schema_fingerprint}' at '{path}': {source}")]
    LoadSchemaCatalogEntry {
        schema_fingerprint: String,
        path: ObjectPath,
        #[source]
        source: Box<StorageError>,
    },
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};
    use chrono::{Duration, TimeZone};
    use tempfile::TempDir;

    use crate::normalized_payload_schema;

    use super::*;
    #[test]
    fn event_paths_use_uuidv7_and_type_suffixes() {
        let started_at = Utc.with_ymd_and_hms(2026, 5, 10, 12, 0, 0).unwrap();
        let closed = CatalogEvent::WalSegmentClosed(WalSegmentClosedEvent {
            event_id: CatalogEventId::new(),
            segment_id: WalSegmentId::new(),
            session_id: SessionId::new(),
            node_id: "joint_states".to_owned(),
            output_id: "state".to_owned(),
            schema_fingerprint: "abcd1234".to_owned(),
            path: paths::wal_segment(
                "session",
                "joint_states",
                "state",
                &format!("segment-{}.arrow", WalSegmentId::new()),
            )
            .to_string(),
            row_count: 0,
            byte_size: 0,
            min_node_timestamp: 0,
            max_node_timestamp: 0,
            min_amber_timestamp: 0,
            max_amber_timestamp: 0,
            opened_at: started_at,
            closed_at: started_at + Duration::seconds(30),
        });

        let path = closed.path().to_string();
        assert!(path.starts_with("catalog/events/"));
        assert!(path.ends_with("-wal_segment_closed.json"));
        assert_eq!(
            Uuid::parse_str(closed.event_id().as_str())
                .expect("event id should parse")
                .get_version(),
            Some(Version::SortRand)
        );
    }

    #[tokio::test]
    async fn schema_catalog_is_idempotent() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let storage = Storage::new_local(temp_dir.path(), None::<&str>).expect("storage");
        let entry = SchemaCatalogEntry::new(
            "abcd1234",
            normalized_payload_schema(&arrow::datatypes::Schema::new(vec![Field::new(
                "payload",
                DataType::Int32,
                true,
            )])),
        );

        assert!(
            entry
                .save_if_absent(&storage)
                .await
                .expect("first save should work")
        );
        assert!(
            !entry
                .save_if_absent(&storage)
                .await
                .expect("second save should be a no-op")
        );

        let loaded = SchemaCatalogEntry::load(&storage, "abcd1234")
            .await
            .expect("entry should load");
        assert_eq!(loaded, entry);
    }

    #[tokio::test]
    async fn catalog_state_lists_sorts_and_folds_events() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let storage = Storage::new_local(temp_dir.path(), None::<&str>).expect("storage");
        let session_id = SessionId::new();
        let segment_id = WalSegmentId::new();
        let parquet_file_id = ParquetFileId::new();
        let base = Utc.with_ymd_and_hms(2026, 5, 10, 12, 0, 0).unwrap();
        let wal_path = paths::wal_segment(
            session_id.as_str(),
            "joint_states",
            "state",
            &format!("segment-{segment_id}.arrow"),
        );
        let parquet_path = paths::parquet_file(
            "joint_states",
            "state",
            "abcd1234",
            &format!("part-{parquet_file_id}.parquet"),
        );

        CatalogEvent::WalSegmentClosed(WalSegmentClosedEvent {
            event_id: CatalogEventId::new(),
            segment_id: segment_id.clone(),
            session_id: session_id.clone(),
            node_id: "joint_states".to_owned(),
            output_id: "state".to_owned(),
            schema_fingerprint: "abcd1234".to_owned(),
            path: wal_path.to_string(),
            row_count: 1200,
            byte_size: 345678,
            min_node_timestamp: 1,
            max_node_timestamp: 9,
            min_amber_timestamp: 2,
            max_amber_timestamp: 10,
            opened_at: base,
            closed_at: base + Duration::seconds(30),
        })
        .save(&storage)
        .await
        .expect("closed event should save");

        CatalogEvent::CompactionCommitted(CompactionCommittedEvent {
            event_id: CatalogEventId::new(),
            compaction_id: CompactionId::new(),
            source_wal_segments: vec![segment_id.clone()],
            created_parquet_files: vec![PublishedParquetFile {
                file_id: parquet_file_id.clone(),
                node_id: "joint_states".to_owned(),
                output_id: "state".to_owned(),
                schema_fingerprint: "abcd1234".to_owned(),
                path: parquet_path.to_string(),
                row_count: 50000,
                byte_size: 268435456,
                min_node_timestamp: 1,
                max_node_timestamp: 100,
                min_amber_timestamp: 2,
                max_amber_timestamp: 101,
                created_at: base + Duration::hours(1),
            }],
            committed_at: base + Duration::hours(1),
        })
        .save(&storage)
        .await
        .expect("compaction event should save");

        CatalogEvent::WalSegmentDeleted(WalSegmentDeletedEvent {
            event_id: CatalogEventId::new(),
            segment_id: segment_id.clone(),
            path: wal_path.to_string(),
            deleted_at: base + Duration::hours(2),
        })
        .save(&storage)
        .await
        .expect("delete event should save");

        let state = CatalogState::load(&storage)
            .await
            .expect("catalog state should load");
        let segment = state
            .wal_segments
            .get(&segment_id)
            .expect("segment should be folded");
        assert_eq!(segment.state, FoldedWalSegmentState::Deleted);
        assert_eq!(segment.path, wal_path.to_string());
        assert_eq!(
            state.published_parquet_files.get(&parquet_file_id),
            Some(&PublishedParquetFile {
                file_id: parquet_file_id,
                node_id: "joint_states".to_owned(),
                output_id: "state".to_owned(),
                schema_fingerprint: "abcd1234".to_owned(),
                path: parquet_path.to_string(),
                row_count: 50000,
                byte_size: 268435456,
                min_node_timestamp: 1,
                max_node_timestamp: 100,
                min_amber_timestamp: 2,
                max_amber_timestamp: 101,
                created_at: base + Duration::hours(1),
            })
        );
    }

    #[tokio::test]
    async fn parse_failures_return_catalog_errors() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let storage = Storage::new_local(temp_dir.path(), None::<&str>).expect("storage");
        let path = paths::catalog_event("broken-wal_segment_closed.json");

        storage
            .put_bytes(&path, b"{not-json}".to_vec())
            .await
            .expect("broken event should be written");

        let error = CatalogEvent::list(&storage)
            .await
            .expect_err("listing should fail");
        let message = error.to_string();

        assert!(matches!(error, CatalogError::LoadEvent { .. }));
        assert!(message.contains("catalog event"));
        assert!(message.contains("broken-wal_segment_closed.json"));
    }
}
