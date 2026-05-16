use std::{
    env, fs,
    path::{Path, PathBuf},
};

use amber_core::SessionStatus;
use amber_node::app::NodeRuntime;
use tempfile::TempDir;

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

    assert_eq!(
        runtime.config().storage.backend,
        amber_core::StorageBackend::Local
    );
    assert_eq!(runtime.session_manifest().status, SessionStatus::Open);
    assert!(
        runtime
            .storage()
            .exists(&runtime.session_manifest().path())
            .await
            .expect("manifest lookup should succeed")
    );
    assert!(
        runtime
            .staging_root()
            .starts_with(temp_dir.path().join("amber_data")),
        "staging root should live under the configured local storage root"
    );
    assert!(runtime.has_writer());
    runtime
        .flush_writer()
        .await
        .expect("writer should accept startup flush");
    assert!(runtime.has_rotation_runtime());
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

    unsafe {
        env::set_var("AMBER_CONFIG", &config_path);
    }

    let runtime = NodeRuntime::initialize_from_env()
        .await
        .expect("env-based startup should succeed");

    assert_eq!(runtime.config_path(), config_path.as_path());
    assert!(!runtime.has_rotation_runtime());

    unsafe {
        env::remove_var("AMBER_CONFIG");
    }
}

#[tokio::test]
async fn startup_reports_missing_amber_config_env() {
    let _guard = ENV_LOCK.lock().await;
    unsafe {
        env::remove_var("AMBER_CONFIG");
    }

    let error = match NodeRuntime::initialize_from_env().await {
        Ok(_) => panic!("startup should fail without AMBER_CONFIG"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("AMBER_CONFIG"));
}

fn write_config(root: &Path, contents: &str) -> PathBuf {
    let path = root.join("amber.yaml");
    fs::write(&path, contents).expect("config file should be written");
    path
}
