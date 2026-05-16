use std::{collections::HashSet, sync::Arc, time::Duration};

use amber_core::{
    AmberConfig, SessionId, WalRotateRequest, WalRotationConfig, WalWriteRequest, WalWriterHandle,
};
use anyhow::{Result, anyhow, bail};
use tokio::{
    sync::oneshot,
    task::JoinHandle,
    time::{MissedTickBehavior, interval},
};
use tracing::warn;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ActiveWalStream {
    session_id: SessionId,
    node_id: String,
    output_id: String,
}

impl ActiveWalStream {
    fn new(
        session_id: SessionId,
        node_id: impl Into<String>,
        output_id: impl Into<String>,
    ) -> Self {
        Self {
            session_id,
            node_id: node_id.into(),
            output_id: output_id.into(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct WalRotationRuntime {
    active_streams: Arc<tokio::sync::Mutex<HashSet<ActiveWalStream>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<JoinHandle<()>>,
}

impl WalRotationRuntime {
    pub(crate) fn start(config: &AmberConfig, writer: WalWriterHandle) -> Result<Option<Self>> {
        Self::from_rotation_config(&config.wal.rotation, writer)
    }

    pub(crate) fn from_rotation_config(
        rotation: &WalRotationConfig,
        writer: WalWriterHandle,
    ) -> Result<Option<Self>> {
        if rotation.max_size_mb != WalRotationConfig::DEFAULT_MAX_SIZE_MB {
            bail!(
                "WAL size-based rotation is not yet supported in amber-node; configured max_size_mb={} (only the default placeholder value {} is currently accepted)",
                rotation.max_size_mb,
                WalRotationConfig::DEFAULT_MAX_SIZE_MB
            );
        }

        if rotation.max_duration_sec == 0 {
            return Ok(None);
        }

        Ok(Some(Self::spawn(
            Duration::from_secs(rotation.max_duration_sec),
            writer,
        )))
    }

    fn spawn(interval_duration: Duration, writer: WalWriterHandle) -> Self {
        let active_streams = Arc::new(tokio::sync::Mutex::new(HashSet::<ActiveWalStream>::new()));
        let streams_for_task = Arc::clone(&active_streams);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let join_handle = tokio::spawn(async move {
            let mut ticker = interval(interval_duration);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    _ = ticker.tick() => {
                        let streams = {
                            let guard = streams_for_task.lock().await;
                            guard.iter().cloned().collect::<Vec<_>>()
                        };

                        for stream in streams {
                            if let Err(error) = writer.rotate(WalRotateRequest::new(
                                stream.session_id.clone(),
                                stream.node_id.clone(),
                                stream.output_id.clone(),
                            )).await {
                                warn!(
                                    session_id = %stream.session_id,
                                    node_id = %stream.node_id,
                                    output_id = %stream.output_id,
                                    error = %error,
                                    "failed to rotate WAL stream",
                                );
                            }
                        }
                    }
                }
            }
        });

        Self {
            active_streams,
            shutdown_tx: Some(shutdown_tx),
            join_handle: Some(join_handle),
        }
    }

    pub(crate) async fn record_write(&self, request: &WalWriteRequest) {
        let mut guard = self.active_streams.lock().await;
        guard.insert(ActiveWalStream::new(
            request.session_id.clone(),
            request.node_id.clone(),
            request.output_id.clone(),
        ));
    }

    pub(crate) async fn shutdown(&mut self) -> Result<()> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        if let Some(join_handle) = self.join_handle.take() {
            join_handle
                .await
                .map_err(|source| anyhow!("failed to join WAL rotation task: {source}"))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use amber_core::{
        CatalogState, RecordBatchMetadata, SESSION_ID_COLUMN, SessionManifest, Storage,
        WalWriteRequest, WalWriter, prepend_metadata_columns,
    };
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use chrono::Utc;
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn rotation_runtime_rejects_non_default_size_policy() {
        let writer = WalWriter::spawn_local(
            Storage::new_local(TempDir::new().expect("storage dir").path(), None::<&str>)
                .expect("storage"),
            TempDir::new().expect("staging dir").path(),
        );

        let error = WalRotationRuntime::from_rotation_config(
            &WalRotationConfig {
                max_size_mb: 64,
                max_duration_sec: 1,
            },
            writer.handle(),
        )
        .expect_err("non-default size rotation should be rejected");

        assert!(
            error
                .to_string()
                .contains("size-based rotation is not yet supported")
        );
    }

    #[tokio::test]
    async fn rotation_runtime_rotates_active_streams_on_timer() {
        let storage_dir = TempDir::new().expect("storage dir should be created");
        let staging_dir = TempDir::new().expect("staging dir should be created");
        let storage = Storage::new_local(storage_dir.path(), None::<&str>).expect("storage");
        let session_id = SessionId::new();
        SessionManifest::create(
            &storage,
            session_id.clone(),
            Utc::now(),
            AmberConfig::default(),
        )
        .await
        .expect("manifest should be created");

        let mut writer = WalWriter::spawn_local(storage.clone(), staging_dir.path());
        let mut runtime = WalRotationRuntime::spawn(Duration::from_millis(50), writer.handle());

        let first_request = WalWriteRequest::new(
            session_id.clone(),
            "camera",
            "image",
            "schema-v1",
            metadata_enriched_batch(vec![1], vec![Some("frame-1")], vec![100], vec![110]),
        );
        runtime.record_write(&first_request).await;
        let first_receipt = writer
            .write(first_request)
            .await
            .expect("first write should succeed");

        wait_for(Duration::from_secs(2), || async {
            CatalogState::load(&storage)
                .await
                .map(|state| state.wal_segments.len() == 1)
                .unwrap_or(false)
        })
        .await;

        let second_request = WalWriteRequest::new(
            session_id.clone(),
            "camera",
            "image",
            "schema-v1",
            metadata_enriched_batch(vec![2], vec![Some("frame-2")], vec![200], vec![210]),
        );
        runtime.record_write(&second_request).await;
        let second_receipt = writer
            .write(second_request)
            .await
            .expect("second write should succeed");

        runtime
            .shutdown()
            .await
            .expect("rotation runtime shutdown should succeed");
        drop(runtime);

        writer
            .flush()
            .await
            .expect("writer flush after rotation should succeed");

        assert_ne!(first_receipt.segment_id, second_receipt.segment_id);
        assert_ne!(first_receipt.path, second_receipt.path);

        writer
            .shutdown()
            .await
            .expect("writer shutdown should succeed");

        let state = CatalogState::load(&storage)
            .await
            .expect("catalog state should load");
        assert_eq!(state.wal_segments.len(), 2);
    }

    async fn wait_for<F, Fut>(timeout: Duration, mut condition: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = Instant::now() + timeout;
        loop {
            if condition().await {
                return;
            }

            assert!(
                Instant::now() < deadline,
                "condition was not met before timeout"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
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

        assert_eq!(batch.schema().field(0).name(), SESSION_ID_COLUMN);
        batch
    }
}
