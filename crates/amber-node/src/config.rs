use std::{
    env, fs,
    path::{Path, PathBuf},
};

use amber_core::{AmberConfig, SessionId, SessionManifest, Storage, StorageBackend};
use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;

const AMBER_CONFIG_ENV: &str = "AMBER_CONFIG";
const STAGING_ROOT_DIR: &str = "_staging";

pub(crate) fn amber_config_path_from_env() -> Result<PathBuf> {
    env::var_os(AMBER_CONFIG_ENV)
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("{AMBER_CONFIG_ENV} is not set"))
}

pub(crate) fn load_config(path: &Path) -> Result<AmberConfig> {
    AmberConfig::from_file(path)
        .with_context(|| format!("failed to load amber config from '{}'", path.display()))
}

pub(crate) fn initialize_storage(config: &AmberConfig, config_path: &Path) -> Result<Storage> {
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

pub(crate) async fn start_session(
    storage: &Storage,
    config: &AmberConfig,
) -> Result<SessionManifest> {
    let session_id = SessionId::new();
    let started_at = Utc::now();

    SessionManifest::create(storage, session_id, started_at, config.clone())
        .await
        .context("failed to create open session manifest")
}

pub(crate) fn prepare_staging_root(
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

    fs::create_dir_all(&staging_root).with_context(|| {
        format!(
            "failed to create WAL staging directory '{}'",
            staging_root.display()
        )
    })?;

    Ok(staging_root)
}
