use std::path::Path;

use amber_core::{AmberConfig, Storage, StorageBackend};
use anyhow::{Context, Result};

pub fn load_config(path: &Path) -> Result<AmberConfig> {
    AmberConfig::from_file(path)
        .with_context(|| format!("failed to load amber config from '{}'", path.display()))
}

pub fn load_storage(config_path: &Path, data_dir: Option<&Path>) -> Result<Storage> {
    let mut config = load_config(config_path)?;
    if let Some(data_dir) = data_dir {
        if config.storage.backend != StorageBackend::Local {
            anyhow::bail!(
                "--data-dir only supports the local storage backend, found '{}'",
                config.storage.backend
            );
        }
        config.storage.path = Some(data_dir.to_path_buf());
    }

    Storage::from_config(&config.storage).with_context(|| {
        format!(
            "failed to initialize storage backend '{}' from '{}'",
            config.storage.backend,
            config_path.display()
        )
    })
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use tempfile::TempDir;

    use super::*;
    use crate::cli::ListArgs;
    use crate::commands::list::run_list;
    use crate::test_support::{create_manifest, write_config};
    use amber_core::SessionStatus;

    #[tokio::test]
    async fn list_command_supports_local_data_dir_override() {
        let configured_storage_dir = TempDir::new().expect("configured storage dir should exist");
        let actual_storage_dir = TempDir::new().expect("actual storage dir should exist");
        let config_path =
            write_config(configured_storage_dir.path()).expect("config should be written");
        let storage = Storage::new_local(actual_storage_dir.path(), None::<&str>).expect("storage");

        let created = create_manifest(
            &storage,
            Utc.with_ymd_and_hms(2026, 5, 10, 12, 0, 0).unwrap(),
            SessionStatus::Open,
            &[],
            1,
        )
        .await;

        let entries = run_list(&ListArgs {
            config: config_path,
            data_dir: Some(actual_storage_dir.path().to_path_buf()),
            latest: false,
            limit: None,
            tag: None,
        })
        .await
        .expect("list with data-dir override should succeed");

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].manifest.session_id, created.manifest.session_id);
    }
}
