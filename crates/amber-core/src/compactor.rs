use std::{collections::BTreeMap, io::Cursor, sync::Arc};

use arrow::{
    datatypes::SchemaRef, error::ArrowError, ipc::reader::StreamReader, record_batch::RecordBatch,
};
use bytes::Bytes;
use chrono::Utc;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    errors::ParquetError,
};
use thiserror::Error;

use crate::{
    AMBER_TIMESTAMP_COLUMN, CatalogError, CatalogEvent, CatalogState, CompactionCommittedEvent,
    CompactionId, FoldedWalSegment, FoldedWalSegmentState, NODE_TIMESTAMP_COLUMN, ObjectPath,
    ParquetFileId, PublishedParquetFile, Storage, StorageError, paths,
};

const BYTES_PER_MB: u64 = 1024 * 1024;

pub struct Compactor {
    storage: Storage,
    target_file_mb: u64,
}

impl Compactor {
    pub fn new(storage: Storage, target_file_mb: u64) -> Self {
        Self {
            storage,
            target_file_mb,
        }
    }

    pub async fn compact_pending(
        &self,
    ) -> Result<Option<CompactionCommittedEvent>, CompactorError> {
        let catalog = CatalogState::load(&self.storage).await.map_err(|source| {
            CompactorError::LoadCatalog {
                source: Box::new(source),
            }
        })?;
        let pending_segments = catalog
            .wal_segments
            .values()
            .filter(|segment| segment.state == FoldedWalSegmentState::Pending)
            .cloned()
            .collect::<Vec<_>>();

        if pending_segments.is_empty() {
            return Ok(None);
        }

        let mut grouped = BTreeMap::<CompactionGroupKey, Vec<LoadedWalSegment>>::new();
        for segment in pending_segments {
            let loaded = self.load_segment(segment).await?;
            grouped.entry(loaded.group_key()).or_default().push(loaded);
        }

        let compaction_id = CompactionId::new();
        let mut source_wal_segments = Vec::new();
        let mut created_parquet_files = Vec::new();
        for loaded_segments in grouped.into_values() {
            for part in partition_segments(loaded_segments, self.target_file_mb) {
                source_wal_segments
                    .extend(part.segments.iter().map(|segment| segment.segment_id()));
                created_parquet_files.push(self.write_part(&part).await?);
            }
        }

        let event = CompactionCommittedEvent::new(
            compaction_id,
            source_wal_segments,
            created_parquet_files,
            Utc::now(),
        );
        CatalogEvent::CompactionCommitted(event.clone())
            .save(&self.storage)
            .await
            .map_err(|source| CompactorError::SaveCatalogEvent {
                source: Box::new(source),
            })?;

        Ok(Some(event))
    }

    async fn load_segment(
        &self,
        segment: FoldedWalSegment,
    ) -> Result<LoadedWalSegment, CompactorError> {
        let path = ObjectPath::from(segment.path.clone());
        let bytes = self.storage.get_bytes(&path).await.map_err(|source| {
            CompactorError::ReadWalSegment {
                path: path.clone(),
                source: Box::new(source),
            }
        })?;
        let reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(|source| {
            CompactorError::DecodeWalSegment {
                path: path.clone(),
                source: Box::new(source),
            }
        })?;

        let mut batches = Vec::new();
        let mut schema = None::<SchemaRef>;
        for batch in reader {
            let batch = batch.map_err(|source| CompactorError::DecodeWalSegment {
                path: path.clone(),
                source: Box::new(source),
            })?;
            if let Some(existing) = &schema {
                if existing.as_ref() != batch.schema().as_ref() {
                    return Err(CompactorError::SchemaMismatch {
                        path,
                        schema_fingerprint: segment.schema_fingerprint,
                    });
                }
            } else {
                schema = Some(batch.schema());
            }
            batches.push(batch);
        }

        Ok(LoadedWalSegment {
            segment,
            schema: schema.unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty())),
            batches,
        })
    }

    async fn write_part(
        &self,
        part: &CompactionPart,
    ) -> Result<PublishedParquetFile, CompactorError> {
        let file_id = ParquetFileId::new();
        let path = paths::parquet_file(
            &part.key.node_id,
            &part.key.output_id,
            &part.key.schema_fingerprint,
            &format!("part-{file_id}.parquet"),
        );

        let bytes = write_parquet_bytes(&part.schema, &part.batches(), &path)?;
        let byte_size = bytes.len() as u64;

        self.storage
            .put_bytes(&path, bytes)
            .await
            .map_err(|source| CompactorError::WriteParquet {
                path: path.clone(),
                source: Box::new(source),
            })?;

        let published_bytes = self.storage.get_bytes(&path).await.map_err(|source| {
            CompactorError::ValidateParquetRead {
                path: path.clone(),
                source: Box::new(source),
            }
        })?;
        let row_count = validate_parquet_bytes(published_bytes, &path)?;

        let created_at = Utc::now();
        Ok(PublishedParquetFile {
            file_id,
            node_id: part.key.node_id.clone(),
            output_id: part.key.output_id.clone(),
            schema_fingerprint: part.key.schema_fingerprint.clone(),
            path: path.to_string(),
            row_count,
            byte_size,
            min_node_timestamp: part.min_node_timestamp,
            max_node_timestamp: part.max_node_timestamp,
            min_amber_timestamp: part.min_amber_timestamp,
            max_amber_timestamp: part.max_amber_timestamp,
            created_at,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CompactionGroupKey {
    node_id: String,
    output_id: String,
    schema_fingerprint: String,
}

#[derive(Clone)]
struct LoadedWalSegment {
    segment: FoldedWalSegment,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl LoadedWalSegment {
    fn group_key(&self) -> CompactionGroupKey {
        CompactionGroupKey {
            node_id: self.segment.node_id.clone(),
            output_id: self.segment.output_id.clone(),
            schema_fingerprint: self.segment.schema_fingerprint.clone(),
        }
    }

    fn segment_id(&self) -> crate::WalSegmentId {
        self.segment.segment_id.clone()
    }
}

struct CompactionPart {
    key: CompactionGroupKey,
    schema: SchemaRef,
    segments: Vec<LoadedWalSegment>,
    min_node_timestamp: i64,
    max_node_timestamp: i64,
    min_amber_timestamp: i64,
    max_amber_timestamp: i64,
}

impl CompactionPart {
    fn batches(&self) -> Vec<RecordBatch> {
        self.segments
            .iter()
            .flat_map(|segment| segment.batches.iter().cloned())
            .collect()
    }
}

fn partition_segments(
    mut segments: Vec<LoadedWalSegment>,
    target_file_mb: u64,
) -> Vec<CompactionPart> {
    segments.sort_by(|left, right| left.segment.segment_id.cmp(&right.segment.segment_id));
    let target_bytes = target_file_mb.saturating_mul(BYTES_PER_MB);

    let mut parts = Vec::new();
    let mut current_segments = Vec::new();
    let mut current_input_bytes = 0u64;

    for segment in segments {
        if !current_segments.is_empty()
            && current_input_bytes.saturating_add(segment.segment.byte_size) > target_bytes
        {
            parts.push(build_part(std::mem::take(&mut current_segments)));
            current_input_bytes = 0;
        }

        current_input_bytes = current_input_bytes.saturating_add(segment.segment.byte_size);
        current_segments.push(segment);
    }

    if !current_segments.is_empty() {
        parts.push(build_part(current_segments));
    }

    parts
}

fn build_part(segments: Vec<LoadedWalSegment>) -> CompactionPart {
    let first = segments
        .first()
        .expect("compaction parts should always contain at least one segment");
    let key = first.group_key();
    let schema = Arc::clone(&first.schema);
    let mut min_node_timestamp = first.segment.min_node_timestamp;
    let mut max_node_timestamp = first.segment.max_node_timestamp;
    let mut min_amber_timestamp = first.segment.min_amber_timestamp;
    let mut max_amber_timestamp = first.segment.max_amber_timestamp;

    for segment in &segments[1..] {
        min_node_timestamp = min_node_timestamp.min(segment.segment.min_node_timestamp);
        max_node_timestamp = max_node_timestamp.max(segment.segment.max_node_timestamp);
        min_amber_timestamp = min_amber_timestamp.min(segment.segment.min_amber_timestamp);
        max_amber_timestamp = max_amber_timestamp.max(segment.segment.max_amber_timestamp);
    }

    CompactionPart {
        key,
        schema,
        segments,
        min_node_timestamp,
        max_node_timestamp,
        min_amber_timestamp,
        max_amber_timestamp,
    }
}

fn write_parquet_bytes(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    path: &ObjectPath,
) -> Result<Vec<u8>, CompactorError> {
    let mut buffer = Vec::new();
    {
        let mut writer =
            ArrowWriter::try_new(&mut buffer, Arc::clone(schema), None).map_err(|source| {
                CompactorError::CreateParquetWriter {
                    path: path.clone(),
                    source: Box::new(source),
                }
            })?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|source| CompactorError::WriteParquetBatch {
                    path: path.clone(),
                    source: Box::new(source),
                })?;
        }
        writer
            .close()
            .map_err(|source| CompactorError::FinalizeParquet {
                path: path.clone(),
                source: Box::new(source),
            })?;
    }
    Ok(buffer)
}

fn validate_parquet_bytes(bytes: Vec<u8>, path: &ObjectPath) -> Result<u64, CompactorError> {
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes)).map_err(|source| {
            CompactorError::DecodeParquet {
                path: path.clone(),
                source: Box::new(source),
            }
        })?;
    let schema = builder.schema().clone();
    for column_name in [
        crate::SESSION_ID_COLUMN,
        crate::NODE_ID_COLUMN,
        crate::OUTPUT_ID_COLUMN,
        NODE_TIMESTAMP_COLUMN,
        AMBER_TIMESTAMP_COLUMN,
    ] {
        if schema.column_with_name(column_name).is_none() {
            return Err(CompactorError::MissingMetadataColumn {
                path: path.clone(),
                column_name,
            });
        }
    }

    let reader = builder
        .build()
        .map_err(|source| CompactorError::DecodeParquet {
            path: path.clone(),
            source: Box::new(source.into()),
        })?;
    let mut row_count = 0u64;
    for batch in reader {
        let batch = batch.map_err(|source| CompactorError::DecodeParquet {
            path: path.clone(),
            source: Box::new(source.into()),
        })?;
        row_count += batch.num_rows() as u64;
    }
    Ok(row_count)
}

#[derive(Debug, Error)]
pub enum CompactorError {
    #[error("failed to load catalog state: {source}")]
    LoadCatalog {
        #[source]
        source: Box<CatalogError>,
    },
    #[error("failed to read WAL segment '{path}': {source}")]
    ReadWalSegment {
        path: ObjectPath,
        #[source]
        source: Box<StorageError>,
    },
    #[error("failed to decode WAL segment '{path}': {source}")]
    DecodeWalSegment {
        path: ObjectPath,
        #[source]
        source: Box<ArrowError>,
    },
    #[error(
        "WAL segment '{path}' does not match schema fingerprint boundary '{schema_fingerprint}'"
    )]
    SchemaMismatch {
        path: ObjectPath,
        schema_fingerprint: String,
    },
    #[error("failed to create Parquet writer for '{path}': {source}")]
    CreateParquetWriter {
        path: ObjectPath,
        #[source]
        source: Box<ParquetError>,
    },
    #[error("failed to write Parquet batch for '{path}': {source}")]
    WriteParquetBatch {
        path: ObjectPath,
        #[source]
        source: Box<ParquetError>,
    },
    #[error("failed to finalize Parquet file '{path}': {source}")]
    FinalizeParquet {
        path: ObjectPath,
        #[source]
        source: Box<ParquetError>,
    },
    #[error("failed to write Parquet file '{path}': {source}")]
    WriteParquet {
        path: ObjectPath,
        #[source]
        source: Box<StorageError>,
    },
    #[error("failed to re-read Parquet file '{path}': {source}")]
    ValidateParquetRead {
        path: ObjectPath,
        #[source]
        source: Box<StorageError>,
    },
    #[error("failed to decode Parquet file '{path}': {source}")]
    DecodeParquet {
        path: ObjectPath,
        #[source]
        source: Box<ParquetError>,
    },
    #[error("validated Parquet file '{path}' is missing metadata column '{column_name}'")]
    MissingMetadataColumn {
        path: ObjectPath,
        column_name: &'static str,
    },
    #[error("failed to save compaction catalog event: {source}")]
    SaveCatalogEvent {
        #[source]
        source: Box<CatalogError>,
    },
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use chrono::Utc;
    use tempfile::TempDir;

    use crate::{
        AmberConfig, CatalogEvent, RecordBatchMetadata, SessionId, SessionManifest, Storage,
        WalWriteRequest, WalWriter, prepend_metadata_columns,
    };

    use super::*;

    #[tokio::test]
    async fn compact_pending_writes_grouped_parquet_and_commits_single_event() {
        let storage_dir = TempDir::new().expect("storage dir should be created");
        let staging_dir = TempDir::new().expect("staging dir should be created");
        let storage = Storage::new_local(storage_dir.path(), None::<&str>).expect("storage");

        let session_a = SessionId::new();
        let session_b = SessionId::new();
        SessionManifest::create(
            &storage,
            session_a.clone(),
            Utc::now(),
            AmberConfig::default(),
        )
        .await
        .expect("session A manifest should be created");
        SessionManifest::create(
            &storage,
            session_b.clone(),
            Utc::now(),
            AmberConfig::default(),
        )
        .await
        .expect("session B manifest should be created");

        let mut writer = WalWriter::spawn_local(storage.clone(), staging_dir.path());
        writer
            .write(WalWriteRequest::new(
                session_a.clone(),
                "camera",
                "image",
                "schema-v1",
                metadata_enriched_batch(vec![1], vec![Some("a")], vec![100], vec![110]),
            ))
            .await
            .expect("first write should succeed");
        writer
            .rotate(crate::WalRotateRequest::new(
                session_a.clone(),
                "camera",
                "image",
            ))
            .await
            .expect("rotation should succeed");
        writer
            .write(WalWriteRequest::new(
                session_b.clone(),
                "camera",
                "image",
                "schema-v1",
                metadata_enriched_batch(vec![2], vec![Some("b")], vec![200], vec![210]),
            ))
            .await
            .expect("second write should succeed");
        writer
            .rotate(crate::WalRotateRequest::new(
                session_b.clone(),
                "camera",
                "image",
            ))
            .await
            .expect("second rotation should succeed");
        writer
            .write(WalWriteRequest::new(
                session_a.clone(),
                "joint_states",
                "state",
                "schema-v2",
                metadata_enriched_batch(vec![3], vec![Some("c")], vec![300], vec![310]),
            ))
            .await
            .expect("third write should succeed");
        writer.shutdown().await.expect("shutdown should succeed");

        let compactor = Compactor::new(storage.clone(), 0);
        let event = compactor
            .compact_pending()
            .await
            .expect("compaction should succeed")
            .expect("pending segments should produce a compaction");

        assert_eq!(event.source_wal_segments.len(), 3);
        assert_eq!(event.created_parquet_files.len(), 3);

        let events = CatalogEvent::list(&storage)
            .await
            .expect("catalog events should list");
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, CatalogEvent::CompactionCommitted(_)))
                .count(),
            1
        );

        let state = CatalogState::load(&storage)
            .await
            .expect("catalog state should load");
        assert_eq!(state.wal_segments.len(), 3);
        assert!(
            state
                .wal_segments
                .values()
                .all(|segment| segment.state == FoldedWalSegmentState::Compacted)
        );
        assert_eq!(state.published_parquet_files.len(), 3);

        let paths = event
            .created_parquet_files
            .iter()
            .map(|file| file.path.clone())
            .collect::<HashSet<_>>();
        assert!(paths.iter().any(|path| {
            path.starts_with(
                "parquet/node_id=camera/output_id=image/schema_fingerprint=schema-v1/part-",
            )
        }));
        assert!(paths.iter().any(|path| {
            path.starts_with(
                "parquet/node_id=joint_states/output_id=state/schema_fingerprint=schema-v2/part-",
            )
        }));

        for file in &event.created_parquet_files {
            assert!(file.row_count > 0);
            assert!(file.byte_size > 0);
            let path = ObjectPath::from(file.path.clone());
            let bytes = storage
                .get_bytes(&path)
                .await
                .expect("published parquet should be readable");
            let row_count = validate_parquet_bytes(bytes, &path).expect("parquet should validate");
            assert_eq!(row_count, file.row_count);
        }
    }

    #[tokio::test]
    async fn compact_pending_returns_none_when_catalog_has_no_pending_segments() {
        let storage_dir = TempDir::new().expect("storage dir should be created");
        let storage = Storage::new_local(storage_dir.path(), None::<&str>).expect("storage");
        let compactor = Compactor::new(storage, 1);

        let result = compactor
            .compact_pending()
            .await
            .expect("compaction should succeed when nothing is pending");

        assert!(result.is_none());
    }

    fn metadata_enriched_batch(
        values: Vec<i32>,
        labels: Vec<Option<&str>>,
        node_timestamps: Vec<i64>,
        amber_timestamps: Vec<i64>,
    ) -> RecordBatch {
        let payload = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Int32, false),
                Field::new("label", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int32Array::from(values)),
                Arc::new(StringArray::from(labels)),
            ],
        )
        .expect("payload batch should build");

        prepend_metadata_columns(
            &payload,
            &RecordBatchMetadata::new(
                "session-1",
                "node-a",
                "output-x",
                node_timestamps,
                amber_timestamps,
            ),
        )
        .expect("metadata enrichment should work")
    }
}
