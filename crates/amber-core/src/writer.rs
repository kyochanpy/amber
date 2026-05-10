use std::{
    collections::HashMap,
    fs::{self, File},
    io::BufWriter,
    path::{Path, PathBuf},
};

use arrow::{
    datatypes::SchemaRef, error::ArrowError, ipc::writer::StreamWriter, record_batch::RecordBatch,
};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinError,
};

use crate::{ObjectPath, SessionId, Storage, WalSegmentId, storage::paths};

const DEFAULT_WRITER_QUEUE_CAPACITY: usize = 64;

#[derive(Debug, Clone)]
pub struct WalWriter {
    command_tx: mpsc::Sender<CommandEnvelope>,
}

impl WalWriter {
    pub fn spawn_local(storage: Storage, staging_root: impl Into<PathBuf>) -> Self {
        Self::spawn_local_with_capacity(storage, staging_root, DEFAULT_WRITER_QUEUE_CAPACITY)
    }

    pub fn spawn_local_with_capacity(
        storage: Storage,
        staging_root: impl Into<PathBuf>,
        capacity: usize,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::channel(capacity);
        tokio::spawn(run_writer_task(storage, staging_root.into(), command_rx));
        Self { command_tx }
    }

    pub async fn write(&self, request: WalWriteRequest) -> Result<WalWriteReceipt, WalWriterError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(CommandEnvelope {
                command: WriteCommand::Write(request),
                response_tx,
            })
            .await
            .map_err(|_| WalWriterError::TaskUnavailable)?;

        response_rx
            .await
            .map_err(|_| WalWriterError::TaskUnavailable)?
    }
}

#[derive(Debug)]
pub enum WriteCommand {
    Write(WalWriteRequest),
}

#[derive(Debug, Clone)]
pub struct WalWriteRequest {
    pub session_id: SessionId,
    pub node_id: String,
    pub output_id: String,
    pub schema_fingerprint: String,
    pub batch: RecordBatch,
}

impl WalWriteRequest {
    pub fn new(
        session_id: SessionId,
        node_id: impl Into<String>,
        output_id: impl Into<String>,
        schema_fingerprint: impl Into<String>,
        batch: RecordBatch,
    ) -> Self {
        Self {
            session_id,
            node_id: node_id.into(),
            output_id: output_id.into(),
            schema_fingerprint: schema_fingerprint.into(),
            batch,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalWriteReceipt {
    pub segment_id: WalSegmentId,
    pub path: ObjectPath,
    pub row_count: usize,
}

#[derive(Debug, Error)]
pub enum WalWriterError {
    #[error("WAL writer task is not available")]
    TaskUnavailable,
    #[error(
        "schema fingerprint changed for session '{session_id}', node '{node_id}', output '{output_id}': existing='{existing_schema_fingerprint}', new='{new_schema_fingerprint}'"
    )]
    SchemaFingerprintChanged {
        session_id: SessionId,
        node_id: String,
        output_id: String,
        existing_schema_fingerprint: String,
        new_schema_fingerprint: String,
    },
    #[error(
        "Arrow schema mismatch for session '{session_id}', node '{node_id}', output '{output_id}' despite matching schema fingerprint '{schema_fingerprint}'"
    )]
    SchemaMismatch {
        session_id: SessionId,
        node_id: String,
        output_id: String,
        schema_fingerprint: String,
    },
    #[error("failed to create WAL segment directory '{path}': {source}")]
    CreateSegmentDirectory {
        path: PathBuf,
        #[source]
        source: Box<std::io::Error>,
    },
    #[error("failed to open WAL segment file '{path}': {source}")]
    OpenSegmentFile {
        path: PathBuf,
        #[source]
        source: Box<std::io::Error>,
    },
    #[error("failed to append to WAL segment '{path}': {source}")]
    AppendSegment {
        path: ObjectPath,
        #[source]
        source: Box<ArrowError>,
    },
    #[error("failed to flush WAL stream '{path}': {source}")]
    FlushStream {
        path: ObjectPath,
        #[source]
        source: Box<ArrowError>,
    },
    #[error("writer task failed: {source}")]
    JoinTask {
        #[from]
        source: JoinError,
    },
}

#[derive(Debug)]
struct CommandEnvelope {
    command: WriteCommand,
    response_tx: oneshot::Sender<Result<WalWriteReceipt, WalWriterError>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WalStreamKey {
    session_id: SessionId,
    node_id: String,
    output_id: String,
}

struct OpenWalSegment {
    segment_id: WalSegmentId,
    path: ObjectPath,
    schema_fingerprint: String,
    schema: SchemaRef,
    row_count: usize,
    writer: StreamWriter<BufWriter<File>>,
}

async fn run_writer_task(
    _storage: Storage,
    staging_root: PathBuf,
    mut command_rx: mpsc::Receiver<CommandEnvelope>,
) {
    let mut open_segments = HashMap::<WalStreamKey, OpenWalSegment>::new();

    while let Some(envelope) = command_rx.recv().await {
        let result = match envelope.command {
            WriteCommand::Write(request) => {
                handle_write(&staging_root, &mut open_segments, request).await
            }
        };

        let _ = envelope.response_tx.send(result);
    }
}

async fn handle_write(
    staging_root: &Path,
    open_segments: &mut HashMap<WalStreamKey, OpenWalSegment>,
    request: WalWriteRequest,
) -> Result<WalWriteReceipt, WalWriterError> {
    let WalWriteRequest {
        session_id,
        node_id,
        output_id,
        schema_fingerprint,
        batch,
    } = request;

    let key = WalStreamKey {
        session_id: session_id.clone(),
        node_id: node_id.clone(),
        output_id: output_id.clone(),
    };

    let segment = match open_segments.entry(key) {
        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
        std::collections::hash_map::Entry::Vacant(entry) => entry.insert(OpenWalSegment::new(
            staging_root,
            &session_id,
            &node_id,
            &output_id,
            &schema_fingerprint,
            &batch,
        )?),
    };

    if segment.schema_fingerprint != schema_fingerprint {
        return Err(WalWriterError::SchemaFingerprintChanged {
            session_id,
            node_id,
            output_id,
            existing_schema_fingerprint: segment.schema_fingerprint.clone(),
            new_schema_fingerprint: schema_fingerprint,
        });
    }

    if segment.schema.as_ref() != batch.schema().as_ref() {
        return Err(WalWriterError::SchemaMismatch {
            session_id,
            node_id,
            output_id,
            schema_fingerprint,
        });
    }

    segment.append(&batch)?;

    Ok(WalWriteReceipt {
        segment_id: segment.segment_id.clone(),
        path: segment.path.clone(),
        row_count: segment.row_count,
    })
}

impl OpenWalSegment {
    fn new(
        staging_root: &Path,
        session_id: &SessionId,
        node_id: &str,
        output_id: &str,
        schema_fingerprint: &str,
        batch: &RecordBatch,
    ) -> Result<Self, WalWriterError> {
        let segment_id = WalSegmentId::new();
        let path = paths::wal_segment(
            session_id.as_str(),
            node_id,
            output_id,
            &format!("segment-{segment_id}.arrow"),
        );
        let local_path = staging_root.join(path.to_string());
        let parent_dir = local_path
            .parent()
            .expect("WAL segment path should always have a parent")
            .to_path_buf();

        fs::create_dir_all(&parent_dir).map_err(|source| {
            WalWriterError::CreateSegmentDirectory {
                path: parent_dir,
                source: Box::new(source),
            }
        })?;

        let file = File::create(&local_path).map_err(|source| WalWriterError::OpenSegmentFile {
            path: local_path,
            source: Box::new(source),
        })?;
        let writer =
            StreamWriter::try_new_buffered(file, batch.schema().as_ref()).map_err(|source| {
                WalWriterError::AppendSegment {
                    path: path.clone(),
                    source: Box::new(source),
                }
            })?;

        Ok(Self {
            segment_id,
            path,
            schema_fingerprint: schema_fingerprint.to_owned(),
            schema: batch.schema(),
            row_count: 0,
            writer,
        })
    }

    fn append(&mut self, batch: &RecordBatch) -> Result<(), WalWriterError> {
        self.writer
            .write(batch)
            .map_err(|source| WalWriterError::AppendSegment {
                path: self.path.clone(),
                source: Box::new(source),
            })?;
        self.writer
            .flush()
            .map_err(|source| WalWriterError::FlushStream {
                path: self.path.clone(),
                source: Box::new(source),
            })?;
        self.row_count += batch.num_rows();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc};

    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        ipc::reader::StreamReader,
    };
    use tempfile::TempDir;

    use crate::{
        RecordBatchMetadata, SESSION_ID_COLUMN, Storage, is_metadata_column,
        prepend_metadata_columns,
    };

    use super::*;

    #[tokio::test]
    async fn write_creates_segment_under_expected_session_node_output_path() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let storage = Storage::new_local(temp_dir.path(), None::<&str>).expect("storage");
        let writer = WalWriter::spawn_local(storage.clone(), temp_dir.path());
        let session_id = SessionId::new();
        let batch = metadata_enriched_batch(vec![1], vec![Some("first")], vec![10], vec![11]);

        let receipt = writer
            .write(WalWriteRequest::new(
                session_id.clone(),
                "joint/states",
                "state/raw",
                "schema-v1",
                batch,
            ))
            .await
            .expect("write should succeed");

        assert_eq!(
            receipt.path.as_ref(),
            format!(
                "wal/session_id={}/node_id=joint%2Fstates/output_id=state%2Fraw/segment-{}.arrow",
                session_id, receipt.segment_id
            )
        );
        assert!(
            storage
                .exists(&receipt.path)
                .await
                .expect("segment should exist")
        );
    }

    #[tokio::test]
    async fn writes_append_to_same_open_segment_for_same_stream_and_fingerprint() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let storage = Storage::new_local(temp_dir.path(), None::<&str>).expect("storage");
        let writer = WalWriter::spawn_local(storage.clone(), temp_dir.path());
        let session_id = SessionId::new();

        let first = writer
            .write(WalWriteRequest::new(
                session_id.clone(),
                "joint_states",
                "state",
                "schema-v1",
                metadata_enriched_batch(
                    vec![1, 2],
                    vec![Some("a"), Some("b")],
                    vec![10, 20],
                    vec![11, 21],
                ),
            ))
            .await
            .expect("first write should succeed");

        let second = writer
            .write(WalWriteRequest::new(
                session_id.clone(),
                "joint_states",
                "state",
                "schema-v1",
                metadata_enriched_batch(vec![3], vec![Some("c")], vec![30], vec![31]),
            ))
            .await
            .expect("second write should succeed");

        assert_eq!(first.segment_id, second.segment_id);
        assert_eq!(first.path, second.path);
        assert_eq!(second.row_count, 3);

        let stored_batches = read_stream_batches(&storage, &second.path).await;
        assert_eq!(stored_batches.len(), 2);
        assert_eq!(
            stored_batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            3
        );
    }

    #[tokio::test]
    async fn fingerprint_change_within_session_stream_is_rejected() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let storage = Storage::new_local(temp_dir.path(), None::<&str>).expect("storage");
        let writer = WalWriter::spawn_local(storage, temp_dir.path());
        let session_id = SessionId::new();

        writer
            .write(WalWriteRequest::new(
                session_id.clone(),
                "joint_states",
                "state",
                "schema-v1",
                metadata_enriched_batch(vec![1], vec![Some("a")], vec![10], vec![11]),
            ))
            .await
            .expect("initial write should succeed");

        let error = writer
            .write(WalWriteRequest::new(
                session_id.clone(),
                "joint_states",
                "state",
                "schema-v2",
                metadata_enriched_batch(vec![2], vec![Some("b")], vec![20], vec![21]),
            ))
            .await
            .expect_err("fingerprint change should fail");

        assert!(matches!(
            error,
            WalWriterError::SchemaFingerprintChanged {
                session_id: ref actual_session_id,
                ref node_id,
                ref output_id,
                ref existing_schema_fingerprint,
                ref new_schema_fingerprint,
            }
            if *actual_session_id == session_id
                && node_id == "joint_states"
                && output_id == "state"
                && existing_schema_fingerprint == "schema-v1"
                && new_schema_fingerprint == "schema-v2"
        ));
    }

    #[tokio::test]
    async fn schema_mismatch_with_same_fingerprint_is_rejected() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let storage = Storage::new_local(temp_dir.path(), None::<&str>).expect("storage");
        let writer = WalWriter::spawn_local(storage, temp_dir.path());
        let session_id = SessionId::new();

        writer
            .write(WalWriteRequest::new(
                session_id.clone(),
                "joint_states",
                "state",
                "schema-v1",
                metadata_enriched_batch(vec![1], vec![Some("a")], vec![10], vec![11]),
            ))
            .await
            .expect("initial write should succeed");

        let mismatched_payload = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Int32, false),
                Field::new("label", DataType::Utf8, true),
                Field::new("extra", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![2])),
                Arc::new(StringArray::from(vec![Some("b")])),
                Arc::new(StringArray::from(vec![Some("extra")])),
            ],
        )
        .expect("payload batch should build");
        let mismatched_batch = prepend_metadata_columns(
            &mismatched_payload,
            &RecordBatchMetadata::new("session", "node", "output", vec![20], vec![21]),
        )
        .expect("metadata enrichment should work");

        let error = writer
            .write(WalWriteRequest::new(
                session_id.clone(),
                "joint_states",
                "state",
                "schema-v1",
                mismatched_batch,
            ))
            .await
            .expect_err("schema mismatch should fail");

        assert!(matches!(
            error,
            WalWriterError::SchemaMismatch {
                session_id: ref actual_session_id,
                ref node_id,
                ref output_id,
                ref schema_fingerprint,
            }
            if *actual_session_id == session_id
                && node_id == "joint_states"
                && output_id == "state"
                && schema_fingerprint == "schema-v1"
        ));
    }

    async fn read_stream_batches(storage: &Storage, path: &ObjectPath) -> Vec<RecordBatch> {
        let bytes = storage
            .get_bytes(path)
            .await
            .expect("stored segment should load");
        let reader = StreamReader::try_new(Cursor::new(bytes), None).expect("stream should parse");

        reader
            .map(|batch| batch.expect("batch should decode"))
            .collect::<Vec<_>>()
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

        let batch = prepend_metadata_columns(
            &payload,
            &RecordBatchMetadata::new(
                "session-1",
                "node-a",
                "output-x",
                node_timestamps,
                amber_timestamps,
            ),
        )
        .expect("metadata enrichment should work");

        assert!(
            batch
                .schema()
                .fields()
                .iter()
                .take_while(|field| is_metadata_column(field.name()))
                .count()
                >= 5
        );
        assert_eq!(batch.schema().field(0).name(), SESSION_ID_COLUMN);
        batch
    }

    #[test]
    fn wal_segment_ids_use_uuid_v7() {
        let segment_id = WalSegmentId::new();
        assert_eq!(
            uuid::Uuid::parse_str(segment_id.as_str())
                .expect("segment id should parse")
                .get_version(),
            Some(uuid::Version::SortRand)
        );
    }
}
