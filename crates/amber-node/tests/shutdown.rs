use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use amber_core::{CatalogState, SessionManifest, SessionStatus};
use amber_node::app::NodeRuntime;
use dora_node_api::{
    ArrowData, arrow as dora_arrow,
    dora_core::config::{DataId, Input, InputMapping, NodeRunConfig, UserInputMapping},
};
use tempfile::TempDir;

#[tokio::test]
async fn shutdown_closes_session_and_cleans_up_staging_state() {
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

    let session_id = runtime.session_manifest().session_id.clone();
    let staging_root = runtime.staging_root().to_path_buf();
    let storage = runtime.storage().clone();

    runtime
        .shutdown()
        .await
        .expect("runtime shutdown should succeed");

    let manifest = SessionManifest::load(&storage, &session_id)
        .await
        .expect("closed manifest should load");
    assert_eq!(manifest.status, SessionStatus::Closed);
    assert!(manifest.ended_at.is_some());
    assert_eq!(manifest.observed_streams.len(), 1);
    assert!(!staging_root.exists(), "staging root should be removed");

    let state = CatalogState::load(&storage)
        .await
        .expect("catalog state should load");
    assert_eq!(state.wal_segments.len(), 1);
    assert!(
        !storage
            .get_bytes(&receipt.path)
            .await
            .expect("published WAL should be readable")
            .is_empty()
    );
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
            dora_arrow::datatypes::Field::new("label", dora_arrow::datatypes::DataType::Utf8, true),
        ])),
        vec![
            Arc::new(dora_arrow::array::Int32Array::from(vec![1, 2])),
            Arc::new(dora_arrow::array::StringArray::from(vec![
                Some("front"),
                Some("rear"),
            ])),
        ],
    )
    .expect("payload batch should build");

    ArrowData(Arc::new(dora_arrow::array::StructArray::from(batch)))
}
