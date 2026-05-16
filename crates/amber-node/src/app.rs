use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use amber_core::{AmberConfig, SessionManifest, Storage, WalWriteReceipt, WalWriter};
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use dora_node_api::{
    ArrowData, DoraNode, Event,
    dora_core::config::{DataId, NodeRunConfig},
    futures::StreamExt,
};
use tracing::{error, info, warn};

use crate::{
    config::{
        amber_config_path_from_env, initialize_storage, load_config, prepare_staging_root,
        start_session,
    },
    dora_arrow::metadata_timestamp_nanos,
    ingest::{IngestRuntime, InputHandlingError},
    rotation::WalRotationRuntime,
    streams::{ConfiguredStream, StreamSchemaKey, build_selected_inputs},
};

pub async fn run() -> Result<()> {
    let (node, mut events) = DoraNode::init_from_env()
        .map_err(|error| anyhow!("failed to initialize Dora node from environment: {error}"))?;
    let mut runtime = NodeRuntime::initialize_from_env().await?;
    runtime.configure_inputs(node.node_config());

    info!(
        session_id = %runtime.session_manifest.session_id,
        config_path = %runtime.config_path.display(),
        storage_backend = %runtime.config.storage.backend,
        selected_inputs = runtime.selected_inputs.len(),
        "amber-node startup completed"
    );

    let mut stop_requested = false;
    while let Some(event) = events.next().await {
        if runtime.handle_event(event).await? == EventHandling::StopRequested {
            stop_requested = true;
            break;
        }
    }

    if stop_requested {
        runtime.shutdown().await?;
    } else {
        warn!(
            session_id = %runtime.session_manifest.session_id,
            "Dora event stream ended without a stop event; skipping normal session close"
        );
    }

    Ok(())
}

pub struct NodeRuntime {
    config_path: PathBuf,
    config: AmberConfig,
    storage: Storage,
    session_manifest: SessionManifest,
    writer: Option<WalWriter>,
    rotation_runtime: Option<WalRotationRuntime>,
    staging_root: PathBuf,
    selected_inputs: HashMap<String, ConfiguredStream>,
    frame_counters: HashMap<StreamSchemaKey, u64>,
    stream_schemas: HashMap<StreamSchemaKey, String>,
}

impl NodeRuntime {
    pub async fn initialize_from_env() -> Result<Self> {
        let config_path = amber_config_path_from_env()?;
        Self::initialize_from_path(config_path).await
    }

    pub async fn initialize_from_path(config_path: impl Into<PathBuf>) -> Result<Self> {
        let config_path = config_path.into();
        let config = load_config(&config_path)?;
        let storage = initialize_storage(&config, &config_path)?;
        let session_manifest = start_session(&storage, &config).await?;
        let staging_root = prepare_staging_root(&config.storage, &session_manifest.session_id)
            .with_context(|| {
                format!(
                    "failed to prepare WAL staging root for config '{}'",
                    config_path.display()
                )
            })?;
        let writer = WalWriter::spawn_local(storage.clone(), staging_root.clone());
        let rotation_runtime = WalRotationRuntime::start(&config, writer.handle())
            .context("failed to initialize WAL rotation runtime")?;

        Ok(Self {
            config_path,
            config,
            storage,
            session_manifest,
            writer: Some(writer),
            rotation_runtime,
            staging_root,
            selected_inputs: HashMap::new(),
            frame_counters: HashMap::new(),
            stream_schemas: HashMap::new(),
        })
    }

    pub fn configure_inputs(&mut self, node_config: &NodeRunConfig) {
        self.selected_inputs = build_selected_inputs(&self.config, node_config);
    }

    async fn handle_event(&mut self, event: Event) -> Result<EventHandling> {
        match event {
            Event::Input { id, metadata, data } => {
                let node_timestamp = metadata_timestamp_nanos(&metadata)
                    .context("failed to extract Dora input timestamp")?;
                match self.handle_input(id, node_timestamp, data).await {
                    Ok(Some(receipt)) => {
                        info!(
                            path = %receipt.path,
                            row_count = receipt.row_count,
                            "wrote input batch to WAL"
                        );
                    }
                    Ok(None) => {}
                    Err(InputHandlingError::Recoverable(error)) => {
                        warn!(error = %error, "skipping recoverable input handling failure");
                    }
                    Err(InputHandlingError::Fatal(error)) => return Err(error),
                }
                Ok(EventHandling::Continue)
            }
            Event::InputClosed { id } => {
                info!(input_id = %id, "Dora input closed");
                Ok(EventHandling::Continue)
            }
            Event::Stop(cause) => {
                info!(?cause, "received Dora stop event");
                Ok(EventHandling::StopRequested)
            }
            Event::Reload { operator_id } => {
                info!(?operator_id, "received Dora reload event");
                Ok(EventHandling::Continue)
            }
            Event::Error(message) => {
                warn!(error = %message, "received Dora event stream error");
                Ok(EventHandling::Continue)
            }
            _ => Ok(EventHandling::Continue),
        }
    }

    pub async fn handle_input(
        &mut self,
        input_id: DataId,
        node_timestamp: i64,
        data: ArrowData,
    ) -> Result<Option<WalWriteReceipt>, InputHandlingError> {
        IngestRuntime {
            storage: &self.storage,
            session_manifest: &self.session_manifest,
            writer: self.writer.as_ref(),
            rotation_runtime: self.rotation_runtime.as_ref(),
            selected_inputs: &self.selected_inputs,
            frame_counters: &mut self.frame_counters,
            stream_schemas: &mut self.stream_schemas,
        }
        .handle_input(input_id, node_timestamp, data)
        .await
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        info!(
            session_id = %self.session_manifest.session_id,
            "starting graceful shutdown"
        );

        if let Some(rotation_runtime) = self.rotation_runtime.as_mut()
            && let Err(error) = rotation_runtime.shutdown().await
        {
            error!(error = %error, "failed to stop WAL rotation runtime");
            return Err(error);
        }
        self.rotation_runtime = None;

        let Some(mut writer) = self.writer.take() else {
            return Ok(());
        };

        if let Err(error) = writer.flush().await {
            error!(error = %error, "failed to flush WAL writer during shutdown");
            return Err(error.into());
        }

        if let Err(error) = writer.shutdown().await {
            error!(error = %error, "failed to shutdown WAL writer");
            return Err(error.into());
        }

        let latest_manifest =
            match SessionManifest::load(&self.storage, &self.session_manifest.session_id).await {
                Ok(manifest) => manifest,
                Err(error) => {
                    error!(error = %error, "failed to reload session manifest before close");
                    return Err(error.into());
                }
            };
        self.session_manifest = latest_manifest;

        if let Err(error) = self
            .session_manifest
            .close_and_save(&self.storage, Utc::now())
            .await
        {
            error!(error = %error, "failed to close session manifest");
            return Err(error.into());
        }

        match fs::remove_dir_all(&self.staging_root) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                warn!(
                    error = %error,
                    path = %self.staging_root.display(),
                    "failed to remove WAL staging directory"
                );
            }
        }

        info!(
            session_id = %self.session_manifest.session_id,
            "graceful shutdown completed"
        );
        Ok(())
    }

    pub fn config(&self) -> &AmberConfig {
        &self.config
    }

    pub fn config_path(&self) -> &Path {
        &self.config_path
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    pub fn session_manifest(&self) -> &SessionManifest {
        &self.session_manifest
    }

    pub fn staging_root(&self) -> &Path {
        &self.staging_root
    }

    pub fn has_writer(&self) -> bool {
        self.writer.is_some()
    }

    pub async fn flush_writer(&self) -> Result<()> {
        let writer = self
            .writer
            .as_ref()
            .ok_or_else(|| anyhow!("WAL writer is unavailable"))?;
        writer.flush().await.map_err(Into::into)
    }

    pub fn has_rotation_runtime(&self) -> bool {
        self.rotation_runtime.is_some()
    }

    pub fn stream_schema_fingerprint(&self, node_id: &str, output_id: &str) -> Option<&str> {
        self.stream_schemas
            .get(&StreamSchemaKey {
                node_id: node_id.to_owned(),
                output_id: output_id.to_owned(),
            })
            .map(String::as_str)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventHandling {
    Continue,
    StopRequested,
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
    };

    use dora_node_api::StopCause;
    use tempfile::TempDir;

    use super::{Event, EventHandling, NodeRuntime};

    #[tokio::test]
    async fn handle_stop_requests_event_loop_shutdown() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let config_path = write_config(
            temp_dir.path(),
            r#"
amber:
  storage:
    backend: local
    path: ./amber_data
  wal:
    rotation:
      max_duration_sec: 0
"#,
        );
        let mut runtime = NodeRuntime::initialize_from_path(&config_path)
            .await
            .expect("startup should succeed");

        let outcome = runtime
            .handle_event(Event::Stop(StopCause::Manual))
            .await
            .expect("stop handling should succeed");

        assert_eq!(outcome, EventHandling::StopRequested);
    }

    fn write_config(root: &Path, contents: &str) -> PathBuf {
        let path = root.join("amber.yaml");
        fs::write(&path, contents).expect("config file should be written");
        path
    }
}
