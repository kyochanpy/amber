use std::{
    collections::{HashMap, HashSet},
    env, fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use amber_core::{
    AmberConfig, RecordBatchMetadata, SchemaCatalogEntry, SessionId, SessionManifest, Storage,
    StorageBackend, WalRotateRequest, WalRotationConfig, WalWriteRequest, WalWriter,
    WalWriterHandle,
    normalized_payload_schema, prepend_metadata_columns, schema_fingerprint_for_payload,
};
use anyhow::{Context, Result, anyhow, bail};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use dora_node_api::{
    ArrowData, DoraNode, Event,
    arrow as dora_arrow,
    dora_core::{
        config::{DataId, InputMapping, NodeRunConfig},
    },
    futures::StreamExt,
};
use tokio::{
    sync::oneshot,
    task::JoinHandle,
    time::{MissedTickBehavior, interval},
};
use tracing::{Level, error, info, warn};

const AMBER_CONFIG_ENV: &str = "AMBER_CONFIG";
const STAGING_ROOT_DIR: &str = "_staging";

#[tokio::main]
async fn main() {
    init_tracing();

    if let Err(error) = run().await {
        error!(error = %error, "amber-node startup failed");
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
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

    while let Some(event) = events.next().await {
        runtime.handle_event(event).await?;
    }

    Ok(())
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .try_init();
}

struct NodeRuntime {
    config_path: PathBuf,
    config: AmberConfig,
    storage: Storage,
    session_manifest: SessionManifest,
    writer: Option<WalWriter>,
    rotation_runtime: Option<WalRotationRuntime>,
    #[allow(dead_code)]
    staging_root: PathBuf,
    selected_inputs: HashMap<String, ConfiguredStream>,
    stream_schemas: HashMap<StreamSchemaKey, String>,
}

impl NodeRuntime {
    async fn initialize_from_env() -> Result<Self> {
        let config_path = amber_config_path_from_env()?;
        Self::initialize_from_path(config_path).await
    }

    async fn initialize_from_path(config_path: impl Into<PathBuf>) -> Result<Self> {
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
            stream_schemas: HashMap::new(),
        })
    }

    fn configure_inputs(&mut self, node_config: &NodeRunConfig) {
        self.selected_inputs = build_selected_inputs(&self.config, node_config);
    }

    async fn handle_event(&mut self, event: Event) -> Result<()> {
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
            }
            Event::InputClosed { id } => {
                info!(input_id = %id, "Dora input closed");
            }
            Event::Stop(cause) => {
                info!(?cause, "received Dora stop event");
            }
            Event::Reload { operator_id } => {
                info!(?operator_id, "received Dora reload event");
            }
            Event::Error(message) => {
                warn!(error = %message, "received Dora event stream error");
            }
            _ => {}
        }

        Ok(())
    }

    async fn handle_input(
        &mut self,
        input_id: DataId,
        node_timestamp: i64,
        data: ArrowData,
    ) -> Result<Option<amber_core::WalWriteReceipt>, InputHandlingError> {
        let input_id = input_id.to_string();
        let Some(stream) = self.selected_inputs.get(&input_id).cloned() else {
            return Ok(None);
        };

        let payload_batch = dora_data_to_record_batch(data).map_err(InputHandlingError::recoverable)?;
        let schema_fingerprint =
            schema_fingerprint_for_payload(payload_batch.schema().as_ref());

        let stream_key = stream.schema_key();
        if let Some(existing_schema_fingerprint) = self.stream_schemas.get(&stream_key)
            && existing_schema_fingerprint != &schema_fingerprint
        {
            return Err(InputHandlingError::fatal(anyhow!(
                "schema fingerprint changed within session for node '{}' output '{}': existing='{}', new='{}'",
                stream.node_id,
                stream.output_id,
                existing_schema_fingerprint,
                schema_fingerprint,
            )));
        }

        SchemaCatalogEntry::new(
            schema_fingerprint.clone(),
            normalized_payload_schema(payload_batch.schema().as_ref()),
        )
        .save_if_absent(&self.storage)
        .await
        .map_err(|source| {
            InputHandlingError::fatal(anyhow!(
                "failed to persist schema catalog entry for node '{}' output '{}': {source}",
                stream.node_id,
                stream.output_id,
            ))
        })?;

        let row_count = payload_batch.num_rows();
        let amber_timestamp = current_time_nanos()
            .context("failed to compute amber timestamp")
            .map_err(InputHandlingError::fatal)?;
        let enriched_batch = prepend_metadata_columns(
            &payload_batch,
            &RecordBatchMetadata::new(
                self.session_manifest.session_id.as_str(),
                &stream.node_id,
                &stream.output_id,
                vec![node_timestamp; row_count],
                vec![amber_timestamp; row_count],
            ),
        )
        .map_err(InputHandlingError::recoverable)?;

        let request = WalWriteRequest::new(
            self.session_manifest.session_id.clone(),
            stream.node_id.clone(),
            stream.output_id.clone(),
            schema_fingerprint.clone(),
            enriched_batch,
        );

        if let Some(rotation_runtime) = &self.rotation_runtime {
            rotation_runtime.record_write(&request).await;
        }

        let writer = self
            .writer
            .as_ref()
            .ok_or_else(|| InputHandlingError::fatal(anyhow!("WAL writer is unavailable")))?;
        let receipt = writer.write(request).await.map_err(|source| {
            InputHandlingError::fatal(anyhow!(
                "failed to write WAL batch for node '{}' output '{}': {source}",
                stream.node_id,
                stream.output_id,
            ))
        })?;

        self.stream_schemas
            .entry(stream_key)
            .or_insert(schema_fingerprint);

        Ok(Some(receipt))
    }
}

fn amber_config_path_from_env() -> Result<PathBuf> {
    env::var_os(AMBER_CONFIG_ENV)
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("{AMBER_CONFIG_ENV} is not set"))
}

fn load_config(path: &Path) -> Result<AmberConfig> {
    AmberConfig::from_file(path)
        .with_context(|| format!("failed to load amber config from '{}'", path.display()))
}

fn initialize_storage(config: &AmberConfig, config_path: &Path) -> Result<Storage> {
    if config.storage.backend == StorageBackend::Local {
        let root = config.storage.resolved_local_path();
        fs::create_dir_all(&root).with_context(|| {
            format!(
                "failed to create local storage root '{}' for '{}'",
                root.display(),
                config_path.display()
            )
        })?;
    }

    Storage::from_config(&config.storage).with_context(|| {
        format!(
            "failed to initialize storage backend '{}' from '{}'",
            config.storage.backend,
            config_path.display()
        )
    })
}

async fn start_session(storage: &Storage, config: &AmberConfig) -> Result<SessionManifest> {
    let session_id = SessionId::new();
    let started_at = Utc::now();

    SessionManifest::create(storage, session_id, started_at, config.clone())
        .await
        .context("failed to create open session manifest")
}

fn prepare_staging_root(
    storage: &amber_core::StorageConfig,
    session_id: &SessionId,
) -> Result<PathBuf> {
    let staging_root = match storage.backend {
        StorageBackend::Local => storage
            .resolved_local_path()
            .join(STAGING_ROOT_DIR)
            .join(format!("session_id={session_id}")),
        _ => {
            bail!(
                "storage backend '{}' is not yet supported by amber-node startup",
                storage.backend
            )
        }
    };

    // Issue 20 is expected to own staging cleanup as part of graceful shutdown.
    fs::create_dir_all(&staging_root).with_context(|| {
        format!(
            "failed to create WAL staging directory '{}'",
            staging_root.display()
        )
    })?;

    Ok(staging_root)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConfiguredStream {
    node_id: String,
    output_id: String,
}

impl ConfiguredStream {
    fn new(node_id: impl Into<String>, output_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            output_id: output_id.into(),
        }
    }

    fn schema_key(&self) -> StreamSchemaKey {
        StreamSchemaKey {
            node_id: self.node_id.clone(),
            output_id: self.output_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StreamSchemaKey {
    node_id: String,
    output_id: String,
}

#[derive(Debug)]
enum InputHandlingError {
    Recoverable(anyhow::Error),
    Fatal(anyhow::Error),
}

impl InputHandlingError {
    fn recoverable(error: impl Into<anyhow::Error>) -> Self {
        Self::Recoverable(error.into())
    }

    fn fatal(error: impl Into<anyhow::Error>) -> Self {
        Self::Fatal(error.into())
    }
}

fn build_selected_inputs(
    config: &AmberConfig,
    node_config: &NodeRunConfig,
) -> HashMap<String, ConfiguredStream> {
    let selected_outputs = config
        .nodes
        .iter()
        .map(|node| {
            (
                node.id.clone(),
                node.outputs
                    .iter()
                    .map(|output| output.id.clone())
                    .collect::<HashSet<_>>(),
            )
        })
        .collect::<HashMap<_, _>>();

    node_config
        .inputs
        .iter()
        .filter_map(|(input_id, input)| {
            let InputMapping::User(mapping) = &input.mapping else {
                return None;
            };

            let node_id = mapping.source.to_string();
            let output_id = mapping.output.to_string();
            let outputs = selected_outputs.get(&node_id)?;
            outputs
                .contains(&output_id)
                .then(|| (input_id.to_string(), ConfiguredStream::new(node_id, output_id)))
        })
        .collect()
}

fn dora_data_to_record_batch(data: ArrowData) -> Result<RecordBatch> {
    let array = data.0;

    let dora_batch = if let Some(struct_array) = array.as_any().downcast_ref::<dora_arrow::array::StructArray>() {
        if dora_arrow::array::Array::null_count(struct_array) > 0 {
            bail!("nullable top-level struct arrays are not supported for ingest");
        }
        dora_arrow::record_batch::RecordBatch::from(struct_array)
    } else {
        let field = dora_arrow::datatypes::Field::new(
            "value",
            array.data_type().clone(),
            array.null_count() > 0,
        );
        dora_arrow::record_batch::RecordBatch::try_new(
            Arc::new(dora_arrow::datatypes::Schema::new(vec![field])),
            vec![array],
        )
        .context("failed to wrap Dora array into a single-column record batch")?
    };

    // dora-node-api links a separate copy of arrow; cross the crate boundary via IPC.
    let mut encoded = Vec::new();
    {
        let mut writer = dora_arrow::ipc::writer::StreamWriter::try_new(
            &mut encoded,
            &dora_batch.schema(),
        )
        .context("failed to create Dora Arrow IPC writer")?;
        writer
            .write(&dora_batch)
            .context("failed to encode Dora payload batch to IPC")?;
        writer
            .finish()
            .context("failed to finalize Dora payload IPC stream")?;
    }

    let mut reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(encoded), None)
        .context("failed to decode Dora IPC stream into amber Arrow batch")?;
    reader
        .next()
        .transpose()
        .context("failed to read converted payload batch")?
        .ok_or_else(|| anyhow!("Dora IPC stream did not contain a payload batch"))
}

fn metadata_timestamp_nanos(metadata: &dora_node_api::Metadata) -> Result<i64> {
    system_time_to_nanos(metadata.timestamp().get_time().to_system_time())
        .context("failed to convert Dora metadata timestamp to unix nanoseconds")
}

fn current_time_nanos() -> Result<i64> {
    system_time_to_nanos(SystemTime::now())
}

fn system_time_to_nanos(timestamp: SystemTime) -> Result<i64> {
    let duration = timestamp
        .duration_since(UNIX_EPOCH)
        .context("timestamp predates unix epoch")?;
    i64::try_from(duration.as_nanos()).context("timestamp exceeds i64 nanosecond range")
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(dead_code)]
struct ActiveWalStream {
    session_id: SessionId,
    node_id: String,
    output_id: String,
}

#[allow(dead_code)]
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
#[allow(dead_code)]
struct WalRotationRuntime {
    active_streams: Arc<tokio::sync::Mutex<HashSet<ActiveWalStream>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<JoinHandle<()>>,
}

#[allow(dead_code)]
impl WalRotationRuntime {
    fn start(config: &AmberConfig, writer: WalWriterHandle) -> Result<Option<Self>> {
        Self::from_rotation_config(&config.wal.rotation, writer)
    }

    fn from_rotation_config(
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

    async fn record_write(&self, request: &WalWriteRequest) {
        let mut guard = self.active_streams.lock().await;
        guard.insert(ActiveWalStream::new(
            request.session_id.clone(),
            request.node_id.clone(),
            request.output_id.clone(),
        ));
    }

    async fn shutdown(&mut self) -> Result<()> {
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
    use std::collections::{BTreeMap, BTreeSet};
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use amber_core::{
        CatalogState, NODE_ID_COLUMN, OUTPUT_ID_COLUMN, RecordBatchMetadata, SESSION_ID_COLUMN,
        SchemaCatalogEntry, SessionManifest, SessionStatus, Storage,
        prepend_metadata_columns,
    };
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        ipc::reader::StreamReader,
        record_batch::RecordBatch,
    };
    use chrono::Utc;
    use dora_node_api::dora_core::config::{Input, NodeRunConfig, UserInputMapping};
    use tempfile::TempDir;

    use super::*;

    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test]
    async fn startup_initializes_storage_manifest_writer_and_rotation_runtime() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let config_path = write_config(
            temp_dir.path(),
            r#"
amber:
  storage:
    backend: local
    path: ./amber_data
"#,
        );

        let runtime = NodeRuntime::initialize_from_path(&config_path)
            .await
            .expect("startup should succeed");

        assert_eq!(runtime.config.storage.backend, StorageBackend::Local);
        assert_eq!(runtime.session_manifest.status, SessionStatus::Open);
        assert!(
            runtime
                .storage
                .exists(&runtime.session_manifest.path())
                .await
                .expect("manifest lookup should succeed")
        );
        assert!(
            runtime
                .staging_root
                .starts_with(temp_dir.path().join("amber_data")),
            "staging root should live under the configured local storage root"
        );
        runtime
            .writer
            .as_ref()
            .expect("writer should be initialized")
            .flush()
            .await
            .expect("writer should accept startup flush");
        assert!(runtime.rotation_runtime.is_some());
    }

    #[tokio::test]
    async fn startup_reads_config_path_from_amber_config_env() {
        let _guard = ENV_LOCK.lock().await;
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

        // SAFETY: these tests serialize all AMBER_CONFIG mutations behind ENV_LOCK.
        unsafe {
            env::set_var(AMBER_CONFIG_ENV, &config_path);
        }

        let runtime = NodeRuntime::initialize_from_env()
            .await
            .expect("env-based startup should succeed");

        assert_eq!(runtime.config_path, config_path);
        assert!(runtime.rotation_runtime.is_none());

        // SAFETY: these tests serialize all AMBER_CONFIG mutations behind ENV_LOCK.
        unsafe {
            env::remove_var(AMBER_CONFIG_ENV);
        }
    }

    #[tokio::test]
    async fn startup_reports_missing_amber_config_env() {
        let _guard = ENV_LOCK.lock().await;
        // SAFETY: these tests serialize all AMBER_CONFIG mutations behind ENV_LOCK.
        unsafe {
            env::remove_var(AMBER_CONFIG_ENV);
        }

        let error = match NodeRuntime::initialize_from_env().await {
            Ok(_) => panic!("startup should fail without AMBER_CONFIG"),
            Err(error) => error,
        };

        assert!(error.to_string().contains(AMBER_CONFIG_ENV));
    }

    #[test]
    fn build_selected_inputs_filters_unconfigured_streams() {
        let config = AmberConfig {
            nodes: vec![amber_core::NodeConfig {
                id: "camera".to_owned(),
                outputs: vec![amber_core::OutputConfig {
                    id: "image".to_owned(),
                    every_n_frames: None,
                }],
            }],
            ..AmberConfig::default()
        };
        let node_config = NodeRunConfig {
            inputs: BTreeMap::from([
                (
                    DataId::from("camera_image".to_owned()),
                    Input {
                        mapping: InputMapping::User(UserInputMapping {
                            source: "camera".to_owned().into(),
                            output: "image".to_owned().into(),
                        }),
                        queue_size: None,
                    },
                ),
                (
                    DataId::from("camera_depth".to_owned()),
                    Input {
                        mapping: InputMapping::User(UserInputMapping {
                            source: "camera".to_owned().into(),
                            output: "depth".to_owned().into(),
                        }),
                        queue_size: None,
                    },
                ),
            ]),
            outputs: BTreeSet::new(),
        };

        let selected = build_selected_inputs(&config, &node_config);

        assert_eq!(
            selected,
            HashMap::from([(
                "camera_image".to_owned(),
                ConfiguredStream::new("camera", "image"),
            )])
        );
    }

    #[tokio::test]
    async fn handle_input_persists_schema_and_writes_metadata_enriched_wal() {
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
  nodes:
    - id: camera
      outputs:
        - id: image
"#,
        );
        let mut runtime = NodeRuntime::initialize_from_path(&config_path)
            .await
            .expect("startup should succeed");
        runtime.configure_inputs(&NodeRunConfig {
            inputs: BTreeMap::from([(
                DataId::from("camera_image".to_owned()),
                Input {
                    mapping: InputMapping::User(UserInputMapping {
                        source: "camera".to_owned().into(),
                        output: "image".to_owned().into(),
                    }),
                    queue_size: None,
                },
            )]),
            outputs: BTreeSet::new(),
        });

        let receipt = runtime
            .handle_input(
                DataId::from("camera_image".to_owned()),
                1_700_000_000_123_456_789,
                structured_arrow_data(),
            )
            .await
            .expect("input handling should succeed")
            .expect("selected input should produce a WAL receipt");

        let stream_schemas = runtime.stream_schemas.clone();
        let schema_fingerprint = stream_schemas
            .get(&StreamSchemaKey {
                node_id: "camera".to_owned(),
                output_id: "image".to_owned(),
            })
            .expect("stream schema fingerprint should be tracked")
            .clone();
        let schema_entry = SchemaCatalogEntry::load(&runtime.storage, &schema_fingerprint)
            .await
            .expect("schema catalog entry should load");
        assert_eq!(schema_entry.schema_fingerprint, schema_fingerprint);

        let storage = runtime.storage.clone();
        let mut writer = runtime.writer.take().expect("writer should still be initialized");
        writer
            .shutdown()
            .await
            .expect("writer shutdown should publish WAL segment");

        let wal_bytes = storage
            .get_bytes(&receipt.path)
            .await
            .expect("published WAL segment should be readable");
        let mut reader = StreamReader::try_new(std::io::Cursor::new(wal_bytes), None)
            .expect("WAL IPC reader should open");
        let batch = reader
            .next()
            .expect("one WAL batch should exist")
            .expect("WAL batch should decode");

        assert_eq!(batch.schema().field(0).name(), SESSION_ID_COLUMN);
        assert_eq!(batch.schema().field(1).name(), NODE_ID_COLUMN);
        assert_eq!(batch.schema().field(2).name(), OUTPUT_ID_COLUMN);
    }

    #[tokio::test]
    async fn handle_input_rejects_same_session_schema_changes() {
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
  nodes:
    - id: camera
      outputs:
        - id: image
"#,
        );
        let mut runtime = NodeRuntime::initialize_from_path(&config_path)
            .await
            .expect("startup should succeed");
        runtime.selected_inputs.insert(
            "camera_image".to_owned(),
            ConfiguredStream::new("camera", "image"),
        );

        runtime
            .handle_input(
                DataId::from("camera_image".to_owned()),
                1_700_000_000_123_456_789,
                structured_arrow_data(),
            )
            .await
            .expect("first input should succeed");

        let error = runtime
            .handle_input(
                DataId::from("camera_image".to_owned()),
                1_700_000_000_123_456_790,
                primitive_arrow_data(),
            )
            .await
            .expect_err("schema change should be rejected");

        match error {
            InputHandlingError::Fatal(error) => {
                assert!(error.to_string().contains("schema fingerprint changed within session"));
            }
            InputHandlingError::Recoverable(error) => {
                panic!("expected fatal schema change error, got recoverable: {error}");
            }
        }
    }

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

    fn write_config(root: &Path, contents: &str) -> PathBuf {
        let path = root.join("amber.yaml");
        fs::write(&path, contents).expect("config file should be written");
        path
    }

    fn structured_arrow_data() -> ArrowData {
        let batch = dora_arrow::record_batch::RecordBatch::try_new(
            Arc::new(dora_arrow::datatypes::Schema::new(vec![
                dora_arrow::datatypes::Field::new(
                    "value",
                    dora_arrow::datatypes::DataType::Int32,
                    false,
                ),
                dora_arrow::datatypes::Field::new(
                    "label",
                    dora_arrow::datatypes::DataType::Utf8,
                    true,
                ),
            ])),
            vec![
                Arc::new(dora_arrow::array::Int32Array::from(vec![1, 2])),
                Arc::new(dora_arrow::array::StringArray::from(vec![Some("front"), Some("rear")])),
            ],
        )
        .expect("payload batch should build");

        ArrowData(Arc::new(dora_arrow::array::StructArray::from(batch)))
    }

    fn primitive_arrow_data() -> ArrowData {
        ArrowData(Arc::new(dora_arrow::array::Int32Array::from(vec![1, 2, 3])))
    }
}
