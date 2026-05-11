use std::{
    collections::HashSet,
    env, fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use amber_core::{
    AmberConfig, SessionId, SessionManifest, Storage, StorageBackend, WalRotateRequest,
    WalRotationConfig, WalWriteRequest, WalWriter,
};
use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
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
    let runtime = NodeRuntime::initialize_from_env().await?;

    info!(
        session_id = %runtime.session_manifest.session_id,
        config_path = %runtime.config_path.display(),
        storage_backend = %runtime.config.storage.backend,
        "amber-node startup completed"
    );

    // Issue 18 stops after startup wiring. Issue 20 is responsible for
    // introducing an explicit shutdown path that flushes and closes the writer.
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
    #[allow(dead_code)]
    storage: Storage,
    session_manifest: SessionManifest,
    #[allow(dead_code)]
    writer: Arc<WalWriter>,
    #[allow(dead_code)]
    rotation_runtime: Option<WalRotationRuntime>,
    #[allow(dead_code)]
    staging_root: PathBuf,
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
        let writer = Arc::new(WalWriter::spawn_local(
            storage.clone(),
            staging_root.clone(),
        ));
        let rotation_runtime = WalRotationRuntime::start(&config, Arc::clone(&writer))
            .context("failed to initialize WAL rotation runtime")?;

        Ok(Self {
            config_path,
            config,
            storage,
            session_manifest,
            writer,
            rotation_runtime,
            staging_root,
        })
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
    fn start(config: &AmberConfig, writer: Arc<WalWriter>) -> Result<Option<Self>> {
        Self::from_rotation_config(&config.wal.rotation, writer)
    }

    fn from_rotation_config(
        rotation: &WalRotationConfig,
        writer: Arc<WalWriter>,
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

    fn spawn(interval_duration: Duration, writer: Arc<WalWriter>) -> Self {
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
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use amber_core::{
        CatalogState, RecordBatchMetadata, SESSION_ID_COLUMN, SessionManifest, SessionStatus,
        Storage, prepend_metadata_columns,
    };
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use chrono::Utc;
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

    #[tokio::test]
    async fn rotation_runtime_rejects_non_default_size_policy() {
        let writer = Arc::new(WalWriter::spawn_local(
            Storage::new_local(TempDir::new().expect("storage dir").path(), None::<&str>)
                .expect("storage"),
            TempDir::new().expect("staging dir").path(),
        ));

        let error = WalRotationRuntime::from_rotation_config(
            &WalRotationConfig {
                max_size_mb: 64,
                max_duration_sec: 1,
            },
            writer,
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

        let writer = Arc::new(WalWriter::spawn_local(storage.clone(), staging_dir.path()));
        let mut runtime = WalRotationRuntime::spawn(Duration::from_millis(50), Arc::clone(&writer));

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
            .expect("runtime shutdown should succeed");

        assert_ne!(first_receipt.segment_id, second_receipt.segment_id);
        assert_ne!(first_receipt.path, second_receipt.path);

        let catalog = CatalogState::load(&storage)
            .await
            .expect("catalog should load after timed rotation");
        assert_eq!(catalog.wal_segments.len(), 1);
        assert!(
            catalog
                .wal_segments
                .values()
                .any(|segment| segment.node_id == "camera" && segment.output_id == "image")
        );

        let mut writer = Arc::try_unwrap(writer)
            .map_err(|_| ())
            .expect("writer should be unique");
        writer
            .shutdown()
            .await
            .expect("writer shutdown should succeed");
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
}
