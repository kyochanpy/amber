use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use arrow::{
    array::Int64Array, datatypes::SchemaRef, error::ArrowError, ipc::writer::StreamWriter,
    record_batch::RecordBatch,
};
use chrono::{DateTime, TimeZone, Utc};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    task::{JoinError, JoinHandle},
};

use crate::{
    AMBER_TIMESTAMP_COLUMN, CatalogError, CatalogEvent, ClosedWalStreamUpdate,
    NODE_TIMESTAMP_COLUMN, ObjectPath, SessionId, SessionManifest, SessionManifestError, Storage,
    WalSegmentClosedEvent, WalSegmentId, storage::paths,
};

const DEFAULT_WRITER_QUEUE_CAPACITY: usize = 64;

pub struct WalWriter {
    command_tx: mpsc::Sender<CommandEnvelope>,
    join_handle: Option<JoinHandle<()>>,
    is_shutdown: bool,
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
        let join_handle = tokio::spawn(run_writer_task(storage, staging_root.into(), command_rx));

        Self {
            command_tx,
            join_handle: Some(join_handle),
            is_shutdown: false,
        }
    }

    pub async fn write(&self, request: WalWriteRequest) -> Result<WalWriteReceipt, WalWriterError> {
        match self.send_command(WriteCommand::Write(request)).await? {
            CommandResult::Write(receipt) => Ok(receipt),
            _ => Err(WalWriterError::UnexpectedResponse {
                expected: "write",
                actual: "non-write",
            }),
        }
    }

    pub async fn flush(&self) -> Result<(), WalWriterError> {
        match self.send_command(WriteCommand::Flush).await? {
            CommandResult::Flushed => Ok(()),
            _ => Err(WalWriterError::UnexpectedResponse {
                expected: "flush",
                actual: "non-flush",
            }),
        }
    }

    pub async fn shutdown(&mut self) -> Result<(), WalWriterError> {
        if self.is_shutdown {
            return self.await_join().await;
        }

        self.is_shutdown = true;

        match self.send_command(WriteCommand::Shutdown).await {
            Ok(CommandResult::Shutdown) => {}
            Ok(_) => {
                return Err(WalWriterError::UnexpectedResponse {
                    expected: "shutdown",
                    actual: "non-shutdown",
                });
            }
            Err(WalWriterError::TaskUnavailable) => {}
            Err(error) => {
                self.await_join().await?;
                return Err(error);
            }
        }

        self.await_join().await
    }

    async fn send_command(&self, command: WriteCommand) -> Result<CommandResult, WalWriterError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(CommandEnvelope {
                command,
                response_tx,
            })
            .await
            .map_err(|_| WalWriterError::TaskUnavailable)?;

        response_rx
            .await
            .map_err(|_| WalWriterError::TaskUnavailable)?
    }

    async fn await_join(&mut self) -> Result<(), WalWriterError> {
        if let Some(join_handle) = self.join_handle.take() {
            join_handle
                .await
                .map_err(|source| WalWriterError::JoinTask { source })?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum WriteCommand {
    Write(WalWriteRequest),
    Flush,
    Shutdown,
}

#[derive(Debug)]
enum CommandResult {
    Write(WalWriteReceipt),
    Flushed,
    Shutdown,
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
    #[error("writer command expected '{expected}' response but received '{actual}'")]
    UnexpectedResponse {
        expected: &'static str,
        actual: &'static str,
    },
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
    #[error("metadata column '{column_name}' is missing from WAL batch")]
    MissingMetadataColumn { column_name: &'static str },
    #[error("metadata column '{column_name}' has unexpected type in WAL batch")]
    InvalidMetadataColumnType { column_name: &'static str },
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
    #[error("failed to sync WAL segment '{path}': {source}")]
    SyncSegment {
        path: ObjectPath,
        #[source]
        source: Box<std::io::Error>,
    },
    #[error("blocking WAL I/O task failed: {source}")]
    BlockingIoTask {
        #[source]
        source: JoinError,
    },
    #[error("failed to finalize WAL segment '{path}': {source}")]
    FinalizeSegment {
        path: ObjectPath,
        #[source]
        source: Box<ArrowError>,
    },
    #[error("failed to flush finalized WAL segment '{path}': {source}")]
    FinalizeFlushSegment {
        path: ObjectPath,
        #[source]
        source: Box<std::io::Error>,
    },
    #[error("failed to read staged WAL segment '{path}': {source}")]
    ReadStagedSegment {
        path: PathBuf,
        #[source]
        source: Box<std::io::Error>,
    },
    #[error("failed to publish WAL segment '{path}': {source}")]
    PublishSegment {
        path: ObjectPath,
        #[source]
        source: Box<crate::StorageError>,
    },
    #[error("published WAL segment '{path}' could not be re-read: {source}")]
    VerifyPublishedSegment {
        path: ObjectPath,
        #[source]
        source: Box<crate::StorageError>,
    },
    #[error("failed to save catalog event for WAL segment '{path}': {source}")]
    SaveCatalogEvent {
        path: ObjectPath,
        #[source]
        source: Box<CatalogError>,
    },
    #[error("failed to load manifest for session '{session_id}': {source}")]
    LoadSessionManifest {
        session_id: SessionId,
        #[source]
        source: Box<SessionManifestError>,
    },
    #[error("failed to save manifest for session '{session_id}': {source}")]
    SaveSessionManifest {
        session_id: SessionId,
        #[source]
        source: Box<SessionManifestError>,
    },
    #[error("failed to remove staged WAL segment '{path}': {source}")]
    RemoveStagedSegment {
        path: PathBuf,
        #[source]
        source: Box<std::io::Error>,
    },
    #[error("failed to join writer task: {source}")]
    JoinTask {
        #[source]
        source: tokio::task::JoinError,
    },
}

#[derive(Debug)]
struct CommandEnvelope {
    command: WriteCommand,
    response_tx: oneshot::Sender<Result<CommandResult, WalWriterError>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WalStreamKey {
    session_id: SessionId,
    node_id: String,
    output_id: String,
}

struct OpenWalSegment {
    segment_id: WalSegmentId,
    session_id: SessionId,
    node_id: String,
    output_id: String,
    path: ObjectPath,
    local_path: PathBuf,
    schema_fingerprint: String,
    schema: SchemaRef,
    row_count: usize,
    stats: TimestampStats,
    opened_at: DateTime<Utc>,
    writer: StreamWriter<BufWriter<File>>,
}

struct ClosedWalSegment {
    event: WalSegmentClosedEvent,
    update: ClosedWalStreamUpdate,
    staged_path: PathBuf,
}

#[derive(Debug, Clone, Copy)]
struct TimestampStats {
    min_node_timestamp: Option<i64>,
    max_node_timestamp: Option<i64>,
    min_amber_timestamp: Option<i64>,
    max_amber_timestamp: Option<i64>,
}

impl TimestampStats {
    fn new() -> Self {
        Self {
            min_node_timestamp: None,
            max_node_timestamp: None,
            min_amber_timestamp: None,
            max_amber_timestamp: None,
        }
    }

    fn update(&mut self, batch: &RecordBatch) -> Result<(), WalWriterError> {
        let node_bounds = metadata_bounds(batch, NODE_TIMESTAMP_COLUMN)?;
        let amber_bounds = metadata_bounds(batch, AMBER_TIMESTAMP_COLUMN)?;

        if let Some((min_value, max_value)) = node_bounds {
            self.min_node_timestamp = Some(
                self.min_node_timestamp
                    .map_or(min_value, |current| current.min(min_value)),
            );
            self.max_node_timestamp = Some(
                self.max_node_timestamp
                    .map_or(max_value, |current| current.max(max_value)),
            );
        }

        if let Some((min_value, max_value)) = amber_bounds {
            self.min_amber_timestamp = Some(
                self.min_amber_timestamp
                    .map_or(min_value, |current| current.min(min_value)),
            );
            self.max_amber_timestamp = Some(
                self.max_amber_timestamp
                    .map_or(max_value, |current| current.max(max_value)),
            );
        }

        Ok(())
    }
}

async fn run_writer_task(
    storage: Storage,
    staging_root: PathBuf,
    mut command_rx: mpsc::Receiver<CommandEnvelope>,
) {
    let mut open_segments = HashMap::<WalStreamKey, OpenWalSegment>::new();

    while let Some(envelope) = command_rx.recv().await {
        let should_exit = matches!(envelope.command, WriteCommand::Shutdown);
        let result = handle_command(
            &storage,
            &staging_root,
            &mut open_segments,
            envelope.command,
        )
        .await;
        let _ = envelope.response_tx.send(result);

        if should_exit {
            break;
        }
    }
}

async fn handle_command(
    storage: &Storage,
    staging_root: &Path,
    open_segments: &mut HashMap<WalStreamKey, OpenWalSegment>,
    command: WriteCommand,
) -> Result<CommandResult, WalWriterError> {
    match command {
        WriteCommand::Write(request) => handle_write(staging_root, open_segments, request)
            .await
            .map(CommandResult::Write),
        WriteCommand::Flush => {
            flush_segments(open_segments).await?;
            Ok(CommandResult::Flushed)
        }
        WriteCommand::Shutdown => {
            shutdown_segments(storage, open_segments).await?;
            Ok(CommandResult::Shutdown)
        }
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

async fn flush_segments(
    open_segments: &mut HashMap<WalStreamKey, OpenWalSegment>,
) -> Result<(), WalWriterError> {
    for segment in open_segments.values_mut() {
        segment.flush_durable().await?;
    }
    Ok(())
}

async fn shutdown_segments(
    storage: &Storage,
    open_segments: &mut HashMap<WalStreamKey, OpenWalSegment>,
) -> Result<(), WalWriterError> {
    let segments = open_segments
        .drain()
        .map(|(_, segment)| segment)
        .collect::<Vec<_>>();
    let mut closed_segments = Vec::with_capacity(segments.len());

    for segment in segments {
        closed_segments.push(segment.close(storage).await?);
    }

    let mut updates_by_session = HashMap::<SessionId, Vec<ClosedWalStreamUpdate>>::new();
    for closed in &closed_segments {
        CatalogEvent::WalSegmentClosed(closed.event.clone())
            .save(storage)
            .await
            .map_err(|source| WalWriterError::SaveCatalogEvent {
                path: ObjectPath::from(closed.event.path.clone()),
                source: Box::new(source),
            })?;
        updates_by_session
            .entry(closed.event.session_id.clone())
            .or_default()
            .push(closed.update.clone());
    }

    for (session_id, updates) in updates_by_session {
        let mut manifest = SessionManifest::load(storage, &session_id)
            .await
            .map_err(|source| WalWriterError::LoadSessionManifest {
                session_id: session_id.clone(),
                source: Box::new(source),
            })?;

        let observed_at = Utc::now();
        for update in updates {
            manifest.observe_closed_wal_stream(update, observed_at);
        }

        manifest
            .save(storage)
            .await
            .map_err(|source| WalWriterError::SaveSessionManifest {
                session_id,
                source: Box::new(source),
            })?;
    }

    for closed in closed_segments {
        fs::remove_file(&closed.staged_path).map_err(|source| {
            WalWriterError::RemoveStagedSegment {
                path: closed.staged_path,
                source: Box::new(source),
            }
        })?;
    }

    Ok(())
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
            path: local_path.clone(),
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
            session_id: session_id.clone(),
            node_id: node_id.to_owned(),
            output_id: output_id.to_owned(),
            path,
            local_path,
            schema_fingerprint: schema_fingerprint.to_owned(),
            schema: batch.schema(),
            row_count: 0,
            stats: TimestampStats::new(),
            opened_at: Utc::now(),
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
        self.row_count += batch.num_rows();
        self.stats.update(batch)?;
        Ok(())
    }

    async fn flush_durable(&mut self) -> Result<(), WalWriterError> {
        self.writer
            .flush()
            .map_err(|source| WalWriterError::FlushStream {
                path: self.path.clone(),
                source: Box::new(source),
            })?;
        let sync_file = self
            .writer
            .get_mut()
            .get_ref()
            .try_clone()
            .map_err(|source| WalWriterError::SyncSegment {
                path: self.path.clone(),
                source: Box::new(source),
            })?;
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            sync_file
                .sync_data()
                .map_err(|source| WalWriterError::SyncSegment {
                    path,
                    source: Box::new(source),
                })
        })
        .await
        .map_err(|source| WalWriterError::BlockingIoTask { source })??;
        Ok(())
    }

    async fn close(self, storage: &Storage) -> Result<ClosedWalSegment, WalWriterError> {
        let closed_at = Utc::now();
        let mut buffered_file =
            self.writer
                .into_inner()
                .map_err(|source| WalWriterError::FinalizeSegment {
                    path: self.path.clone(),
                    source: Box::new(source),
                })?;
        buffered_file
            .flush()
            .map_err(|source| WalWriterError::FinalizeFlushSegment {
                path: self.path.clone(),
                source: Box::new(source),
            })?;
        let sync_file =
            buffered_file
                .get_ref()
                .try_clone()
                .map_err(|source| WalWriterError::SyncSegment {
                    path: self.path.clone(),
                    source: Box::new(source),
                })?;
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            sync_file
                .sync_data()
                .map_err(|source| WalWriterError::SyncSegment {
                    path,
                    source: Box::new(source),
                })
        })
        .await
        .map_err(|source| WalWriterError::BlockingIoTask { source })??;
        drop(buffered_file);

        let bytes =
            fs::read(&self.local_path).map_err(|source| WalWriterError::ReadStagedSegment {
                path: self.local_path.clone(),
                source: Box::new(source),
            })?;
        let byte_size = bytes.len() as u64;

        storage
            .put_bytes(&self.path, bytes)
            .await
            .map_err(|source| WalWriterError::PublishSegment {
                path: self.path.clone(),
                source: Box::new(source),
            })?;
        storage.get_bytes(&self.path).await.map_err(|source| {
            WalWriterError::VerifyPublishedSegment {
                path: self.path.clone(),
                source: Box::new(source),
            }
        })?;

        let min_node_timestamp = self.stats.min_node_timestamp.unwrap_or(0);
        let max_node_timestamp = self.stats.max_node_timestamp.unwrap_or(0);
        let min_amber_timestamp = self.stats.min_amber_timestamp.unwrap_or(0);
        let max_amber_timestamp = self.stats.max_amber_timestamp.unwrap_or(0);

        let first_seen_at = if self.row_count == 0 {
            self.opened_at
        } else {
            Utc.timestamp_nanos(min_amber_timestamp)
        };
        let last_seen_at = if self.row_count == 0 {
            closed_at
        } else {
            Utc.timestamp_nanos(max_amber_timestamp)
        };

        let event = WalSegmentClosedEvent {
            event_id: crate::CatalogEventId::new(),
            segment_id: self.segment_id.clone(),
            session_id: self.session_id.clone(),
            node_id: self.node_id.clone(),
            output_id: self.output_id.clone(),
            schema_fingerprint: self.schema_fingerprint.clone(),
            path: self.path.to_string(),
            row_count: self.row_count as u64,
            byte_size,
            min_node_timestamp,
            max_node_timestamp,
            min_amber_timestamp,
            max_amber_timestamp,
            opened_at: self.opened_at,
            closed_at,
        };

        let update = ClosedWalStreamUpdate::new(
            self.node_id,
            self.output_id,
            self.schema_fingerprint,
            first_seen_at,
            last_seen_at,
        )
        .with_row_count(self.row_count as u64)
        .with_byte_size(byte_size);

        Ok(ClosedWalSegment {
            event,
            update,
            staged_path: self.local_path,
        })
    }
}

fn metadata_bounds(
    batch: &RecordBatch,
    column_name: &'static str,
) -> Result<Option<(i64, i64)>, WalWriterError> {
    let column = batch
        .column_by_name(column_name)
        .ok_or(WalWriterError::MissingMetadataColumn { column_name })?;
    let values = column
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or(WalWriterError::InvalidMetadataColumnType { column_name })?;

    if values.is_empty() {
        return Ok(None);
    }

    let mut min_value = values.value(0);
    let mut max_value = values.value(0);
    for index in 1..values.len() {
        let value = values.value(index);
        min_value = min_value.min(value);
        max_value = max_value.max(value);
    }

    Ok(Some((min_value, max_value)))
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
        AmberConfig, CatalogState, RecordBatchMetadata, SESSION_ID_COLUMN, SessionManifest,
        SessionStatus, Storage, is_metadata_column, prepend_metadata_columns,
    };

    use super::*;

    #[tokio::test]
    async fn write_creates_segment_under_expected_session_node_output_path() {
        let storage_dir = TempDir::new().expect("storage dir should be created");
        let staging_dir = TempDir::new().expect("staging dir should be created");
        let storage = Storage::new_local(storage_dir.path(), None::<&str>).expect("storage");
        let writer = WalWriter::spawn_local(storage.clone(), staging_dir.path());
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
        let staged_path = staging_dir.path().join(receipt.path.to_string());
        assert!(staged_path.exists());
        assert!(
            !storage
                .exists(&receipt.path)
                .await
                .expect("storage exists should work")
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

    #[tokio::test]
    async fn flush_makes_open_segments_durable_without_catalog_visibility() {
        let storage_dir = TempDir::new().expect("storage dir should be created");
        let staging_dir = TempDir::new().expect("staging dir should be created");
        let storage = Storage::new_local(storage_dir.path(), None::<&str>).expect("storage");
        let writer = WalWriter::spawn_local(storage.clone(), staging_dir.path());
        let session_id = SessionId::new();

        let receipt = writer
            .write(WalWriteRequest::new(
                session_id,
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
            .expect("write should succeed");

        assert!(
            !storage
                .exists(&receipt.path)
                .await
                .expect("storage exists should work")
        );

        writer.flush().await.expect("flush should succeed");

        let staging_path = staging_dir.path().join(receipt.path.to_string());
        assert!(staging_path.exists());
        assert!(
            !storage
                .exists(&receipt.path)
                .await
                .expect("segment should remain unpublished")
        );
        assert!(
            CatalogEvent::list(&storage)
                .await
                .expect("catalog list should work")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn shutdown_publishes_segments_emits_events_and_updates_manifest() {
        let storage_dir = TempDir::new().expect("storage dir should be created");
        let staging_dir = TempDir::new().expect("staging dir should be created");
        let storage = Storage::new_local(storage_dir.path(), None::<&str>).expect("storage");
        let session_id = SessionId::new();
        SessionManifest::create(&storage, session_id.clone(), Utc::now(), test_config())
            .await
            .expect("manifest should be created");

        let mut writer = WalWriter::spawn_local(storage.clone(), staging_dir.path());
        writer
            .write(WalWriteRequest::new(
                session_id.clone(),
                "joint_states",
                "state",
                "schema-v1",
                metadata_enriched_batch(
                    vec![1, 2, 3],
                    vec![Some("a"), Some("b"), Some("c")],
                    vec![100, 200, 150],
                    vec![110, 210, 160],
                ),
            ))
            .await
            .expect("write should succeed");

        writer.shutdown().await.expect("shutdown should succeed");
        writer
            .shutdown()
            .await
            .expect("shutdown should be idempotent");

        let catalog = CatalogState::load(&storage)
            .await
            .expect("catalog should load after shutdown");
        assert_eq!(catalog.wal_segments.len(), 1);
        let closed_segment = catalog
            .wal_segments
            .values()
            .next()
            .expect("closed segment should exist");
        assert_eq!(closed_segment.session_id, session_id);
        assert_eq!(closed_segment.node_id, "joint_states");
        assert_eq!(closed_segment.output_id, "state");
        assert_eq!(closed_segment.schema_fingerprint, "schema-v1");
        assert_eq!(closed_segment.row_count, 3);
        assert_eq!(closed_segment.min_node_timestamp, 100);
        assert_eq!(closed_segment.max_node_timestamp, 200);
        assert_eq!(closed_segment.min_amber_timestamp, 110);
        assert_eq!(closed_segment.max_amber_timestamp, 210);

        let published_bytes = storage
            .get_bytes(&ObjectPath::from(closed_segment.path.clone()))
            .await
            .expect("published segment should be readable");
        let reader =
            StreamReader::try_new(Cursor::new(published_bytes), None).expect("stream should parse");
        let batches = reader
            .map(|batch| batch.expect("batch should decode"))
            .collect::<Vec<_>>();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);

        let manifest = SessionManifest::load(&storage, &session_id)
            .await
            .expect("manifest should reload");
        assert_eq!(manifest.status, SessionStatus::Open);
        assert_eq!(manifest.observed_streams.len(), 1);
        let observed = &manifest.observed_streams[0];
        assert_eq!(observed.node_id, "joint_states");
        assert_eq!(observed.output_id, "state");
        assert_eq!(observed.schema_fingerprints, vec!["schema-v1".to_owned()]);
        assert_eq!(observed.row_count, Some(3));
        assert_eq!(observed.byte_size, Some(closed_segment.byte_size));
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

    fn test_config() -> AmberConfig {
        AmberConfig::default()
    }
}
